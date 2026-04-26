"""
1_fetch_wikipedia.py
Fetch clean article text from Wikipedia API using article URLs from articles.csv.
Outputs one folder per article in ./txts/ with content.txt + url.txt.

Usage:
    python 1_fetch_wikipedia.py --csv articles.csv --output ./txts
    python 1_fetch_wikipedia.py --csv articles.csv --output ./txts --force
"""

import re
import csv
import time
import argparse
import logging
from pathlib import Path

try:
    import wikipedia
except ImportError:
    raise ImportError("Install wikipedia: pip install wikipedia")

try:
    from bs4 import BeautifulSoup
except ImportError:
    raise ImportError("Install beautifulsoup4: pip install beautifulsoup4")

try:
    import requests
except ImportError:
    raise ImportError("Install requests: pip install requests")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("fetch_wikipedia.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Sections to cut (with Italian equivalents: Voci correlate, Note, etc.)
CUT_SECTIONS = re.compile(
    r"\n=+\s*(See also|References|Notes|Notes and references|Further reading|"
    r"External links|Bibliography|Sources|Footnotes|Learn more|"
    r"Voci correlate|Note|Bibliografia|Collegamenti esterni|Fonti)\s*=+\s*.*",
    re.IGNORECASE | re.DOTALL,
)

SECTION_HEADER = re.compile(r"^(=+)\s*(.+?)\s*\1$", re.MULTILINE)

CITE_PATTERN = re.compile(
    r"\[\s*\d+\s*\]|\[\s*senza fonte\s*\]|"
    r"\[\s*citazione necessaria\s*\]|\[\s*citation needed\s*\]"
)

MULTI_NEWLINE = re.compile(r"\n{3,}")

# Be nice to Wikipedia. Each article makes 2 API calls (page + html), so
# the effective rate is 1 article per (DELAY + ~response time).
DELAY = 1.5
MAX_RETRIES = 5

# Identifying user agent — Wikipedia throttles anonymous bots harder.
USER_AGENT = "CRIMENET/1.0 (https://github.com/alvarofrancomartins/CRIMENET; research)"

HTTP_HEADERS = {"User-Agent": USER_AGENT}


def is_rate_limit_error(exc: Exception) -> bool:
    """Detect 429 or timeout-style errors regardless of which library raised them."""
    msg = str(exc).lower()
    return ("429" in msg
            or "too many requests" in msg
            or "rate" in msg and "limit" in msg
            or "timed out" in msg
            or "timeout" in msg)


def fetch_html_direct(lang: str, title: str) -> str:
    """Fetch page HTML via the MediaWiki action API with a proper User-Agent.

    Bypasses the `wikipedia` library (which doesn't expose a UA setting) so we
    can identify ourselves and reduce 429 risk.
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": title,
        "prop": "text",
        "format": "json",
        "redirects": 1,
    }
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(api_url, params=params, timeout=20, headers=HTTP_HEADERS)
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)   # 5, 10, 20, 40, 80
                log.warning(f"  HTML fetch 429, sleeping {wait}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            return data.get("parse", {}).get("text", {}).get("*", "")
        except Exception as e:
            if is_rate_limit_error(e) and attempt < MAX_RETRIES - 1:
                wait = 5 * (2 ** attempt)
                log.warning(f"  HTML fetch error, sleeping {wait}s: {e}")
                time.sleep(wait)
                continue
            log.warning(f"  HTML fetch failed for '{title}': {e}")
            return ""
    return ""


def extract_infobox(html: str) -> str:
    """Parse the infobox/sinottico from Wikipedia HTML."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")

    box = soup.find("table", class_=lambda c: c and ("infobox" in c or "sinottico" in c))
    if not box:
        return ""

    cite = re.compile(r'\[\s*\d+\s*\]')
    lines = []

    caption = box.find("caption") or box.find(
        "th", class_=re.compile("testata|title|infobox-title", re.I)
    )
    if caption:
        lines.append(caption.get_text(strip=True))

    for el in box.find_all(class_=re.compile("subheader", re.I)):
        text = el.get_text(strip=True)
        if text:
            lines.append(text)

    for row in box.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if th and td:
            key = th.get_text(" ", strip=True)

            for br in td.find_all(["br", "hr"]):
                br.replace_with(", ")
            for li in td.find_all("li"):
                li.append(", ")

            val = td.get_text(" ", strip=True)
            val = cite.sub("", val)
            val = re.sub(r'\s+', ' ', val)
            val = re.sub(r'(,\s*)+', ', ', val).strip(', ')

            if key and val:
                lines.append(f"{key}: {val}")

    return "\n".join(lines)


def clean_content(text: str) -> str:
    """Clean Wikipedia API content: cut refs, clean headers, normalize whitespace."""
    text = CUT_SECTIONS.sub("", text)

    def replace_header(m):
        title = m.group(2)
        return f"\n{title}\n"

    text = SECTION_HEADER.sub(replace_header, text)
    text = CITE_PATTERN.sub("", text)
    text = "\n".join(line.rstrip() for line in text.splitlines())
    text = MULTI_NEWLINE.sub("\n\n", text)

    return text.strip()


def fetch_article(title: str, lang: str) -> dict:
    """Fetch a Wikipedia article. Returns {title, content, infobox, url} or {error}.

    Uses the `wikipedia` library for the cleaned text and a direct API call
    (with a proper User-Agent) for the infobox HTML. Retries with exponential
    backoff on 429s and timeouts.
    """
    last_err = None

    for attempt in range(MAX_RETRIES):
        try:
            page = wikipedia.page(title, auto_suggest=False)
            html = fetch_html_direct(lang, page.title)
            infobox = extract_infobox(html) if html else ""
            return {
                "title": page.title,
                "content": page.content,
                "infobox": infobox,
                "url": page.url,
            }

        except wikipedia.exceptions.DisambiguationError as e:
            if e.options:
                try:
                    page = wikipedia.page(e.options[0], auto_suggest=False)
                    html = fetch_html_direct(lang, page.title)
                    infobox = extract_infobox(html) if html else ""
                    return {
                        "title": page.title,
                        "content": page.content,
                        "infobox": infobox,
                        "url": page.url,
                    }
                except Exception:
                    pass
            return {"error": f"Disambiguation: {e.options[:5]}"}

        except wikipedia.exceptions.PageError:
            try:
                page = wikipedia.page(title, auto_suggest=True)
                html = fetch_html_direct(lang, page.title)
                infobox = extract_infobox(html) if html else ""
                return {
                    "title": page.title,
                    "content": page.content,
                    "infobox": infobox,
                    "url": page.url,
                }
            except Exception:
                return {"error": "Page not found"}

        except Exception as e:
            last_err = e
            if is_rate_limit_error(e):
                wait = 5 * (2 ** attempt)   # 5, 10, 20, 40, 80
                log.warning(f"  rate-limited or timeout, sleeping {wait}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            return {"error": str(e)}

    return {"error": f"Gave up after {MAX_RETRIES} retries: {last_err}"}


def main():
    parser = argparse.ArgumentParser(description="Fetch Wikipedia articles from CSV")
    parser.add_argument("--csv", "-c", default="articles.csv",
                        help="Input CSV from 0_urls_to_articles.py")
    parser.add_argument("--output", "-o", default="./txts", help="Output directory")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Re-fetch even if content.txt exists")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(args.csv, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    log.info(f"Loaded {total} articles from {args.csv}")

    success, skipped, failed = 0, 0, 0

    for idx, row in enumerate(rows, 1):
        title = row["title"]
        folder_name = row["folder_name"]
        versioned_url = row["url"]

        # Switch Wikipedia language based on URL domain
        if "it.wikipedia.org" in versioned_url:
            lang = "it"
        else:
            lang = "en"
        wikipedia.set_lang(lang)

        folder_path = output_dir / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)
        content_path = folder_path / "content.txt"
        url_path = folder_path / "url.txt"

        if content_path.exists() and not args.force:
            skipped += 1
            log.info(f"[{idx}/{total}] {folder_name} — exists, skip")
            continue

        log.info(f"[{idx}/{total}] {title}...")
        result = fetch_article(title, lang)

        if "error" in result:
            failed += 1
            log.warning(f"  ✗ {result['error']}")
            continue

        content = clean_content(result["content"])
        if result.get("infobox"):
            content += "\n\n--- INFOBOX ---\n\n" + result["infobox"]

        words = len(content.split())
        content_path.write_text(content, encoding="utf-8")
        url_path.write_text(versioned_url, encoding="utf-8")

        log.info(f"  ✓ {words:,} words")
        success += 1

        time.sleep(DELAY)

    log.info("=" * 50)
    log.info(f"Done: {success} fetched, {skipped} skipped, {failed} failed")


if __name__ == "__main__":
    main()