"""
1_fetch_wikipedia.py
Fetch clean article text from Wikipedia using the MediaWiki Action API,
pinned to the exact revision (oldid) from articles.csv.

Two API calls per article, both revision-pinned:
  1. action=query&prop=extracts&explaintext=1  →  well-structured plain text
  2. action=parse&prop=text                    →  HTML for infobox parsing

Outputs one folder per article in ./txts/ with content.txt + url.txt.

Usage:
    python 1_fetch_wikipedia.py --csv articles.csv --output ./txts
"""

import re
import csv
import time
import argparse
import logging
from pathlib import Path
from urllib.parse import urlparse, parse_qs

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

DELAY = 1.5
MAX_RETRIES = 5

USER_AGENT = "CRIMENET/1.0 (https://github.com/alvarofrancomartins/CRIMENET; research)"
HTTP_HEADERS = {"User-Agent": USER_AGENT}

# Sections to cut from the plain-text output (with Italian equivalents).
# These match the section header text on its own line.
CUT_SECTIONS = re.compile(
    r"\n\s*(See also|References|Notes|Notes and references|Further reading|"
    r"External links|Bibliography|Sources|Footnotes|Learn more|"
    r"Voci correlate|Note|Bibliografia|Collegamenti esterni|Fonti)\s*\n.*",
    re.IGNORECASE | re.DOTALL,
)

# Citation brackets that occasionally survive in the extract output.
CITE_PATTERN = re.compile(
    r"\[\s*\d+\s*\]|\[\s*senza fonte\s*\]|"
    r"\[\s*citazione necessaria\s*\]|\[\s*citation needed\s*\]"
)

MULTI_NEWLINE = re.compile(r"\n{3,}")

# Filesystem-unsafe characters that must not appear in folder names.
# Path separators (/, \) would create nested directories instead of one folder.
UNSAFE_PATH_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def is_rate_limit_error(exc: Exception) -> bool:
    """Detect 429 or timeout-style errors regardless of which library raised them."""
    msg = str(exc).lower()
    return ("429" in msg
            or "too many requests" in msg
            or "rate" in msg and "limit" in msg
            or "timed out" in msg
            or "timeout" in msg)


def parse_versioned_url(url: str):
    """Extract (lang, oldid) from a versioned Wikipedia URL.

    Expects URLs like:
      https://en.wikipedia.org/w/index.php?title=Foo&oldid=12345
    Returns (lang, oldid) or (lang, None) if oldid is missing.
    """
    parsed = urlparse(url)
    lang = parsed.netloc.split(".")[0]
    qs = parse_qs(parsed.query)
    oldid = qs.get("oldid", [None])[0]
    return lang, int(oldid) if oldid else None


def safe_folder_name(name: str) -> str:
    """Sanitize a folder name so it never creates nested directories.

    Replaces path separators and reserved characters with underscores.
    Same logic as 0_urls_to_articles.py's slugify(), defensively re-applied
    here to protect against malformed articles.csv inputs.
    """
    s = UNSAFE_PATH_CHARS.sub("_", name)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def api_get(url: str, params: dict, label: str) -> dict:
    """GET a MediaWiki API endpoint with retry on 429s and transient errors.

    Returns the parsed JSON dict on success, or {"_error": ...} on failure.
    """
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30, headers=HTTP_HEADERS)
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)   # 5, 10, 20, 40, 80
                log.warning(f"  {label} 429, sleeping {wait}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if is_rate_limit_error(e) and attempt < MAX_RETRIES - 1:
                wait = 5 * (2 ** attempt)
                log.warning(f"  {label} network error, sleeping {wait}s: {e}")
                time.sleep(wait)
                continue
            return {"_error": str(e)}
    return {"_error": f"Gave up after {MAX_RETRIES} retries: {last_err}"}


def fetch_extract(lang: str, oldid: int) -> dict:
    """Fetch the clean plain-text extract for a specific revision.

    Uses MediaWiki's `extracts` endpoint with `explaintext=1`, which returns
    well-structured prose (same source the wikipedia Python library uses for
    page.content). Pinned to the revision via revids.

    Returns {title, content} or {error}.
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": 1,
        "exsectionformat": "plain",
        "revids": oldid,
        "format": "json",
        "redirects": 1,
    }

    data = api_get(api_url, params, label="extract")
    if "_error" in data:
        return {"error": data["_error"]}

    if "error" in data:
        return {"error": f"API: {data['error'].get('info', 'unknown')}"}

    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return {"error": "No pages returned"}

    # `pages` is keyed by pageid; there's always exactly one when querying by revid.
    page = next(iter(pages.values()))
    if page.get("missing") is not None:
        return {"error": "Revision not found"}

    title = page.get("title", "")
    content = page.get("extract", "")

    if not content:
        return {"error": "Empty extract"}

    return {"title": title, "content": content}


def fetch_html(lang: str, oldid: int) -> str:
    """Fetch the rendered HTML for a specific revision (for infobox parsing).

    Returns the HTML string, or "" on failure.
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "oldid": oldid,
        "prop": "text",
        "format": "json",
        "redirects": 1,
    }

    data = api_get(api_url, params, label="html")
    if "_error" in data or "error" in data:
        return ""

    return data.get("parse", {}).get("text", {}).get("*", "")


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


def clean_extract(text: str) -> str:
    """Clean the plain-text extract: cut trailing sections, strip citations."""
    text = CUT_SECTIONS.sub("", text)
    text = CITE_PATTERN.sub("", text)
    text = "\n".join(line.rstrip() for line in text.splitlines())
    text = MULTI_NEWLINE.sub("\n\n", text)
    return text.strip()


def fetch_revision(lang: str, oldid: int) -> dict:
    """Fetch a Wikipedia article at a specific revision.

    Makes two API calls (extract + html), both pinned to the same oldid.
    Returns {title, content, infobox, url} or {error}.
    """
    # Plain text extract (well-structured, like wikipedia.page().content)
    extract_result = fetch_extract(lang, oldid)
    if "error" in extract_result:
        return extract_result

    # HTML for infobox parsing
    html = fetch_html(lang, oldid)
    infobox = extract_infobox(html) if html else ""

    return {
        "title": extract_result["title"],
        "content": extract_result["content"],
        "infobox": infobox,
        "url": f"https://{lang}.wikipedia.org/w/index.php?oldid={oldid}",
    }


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
        folder_name_raw = row["folder_name"]
        versioned_url = row["url"]

        # Defensive sanity check: folder_name should already be safe (slugify in
        # 0_urls_to_articles.py handles this), but if articles.csv came from an
        # older script version it might contain path separators. Sanitize and
        # warn rather than silently nest directories.
        folder_name = safe_folder_name(folder_name_raw)
        if folder_name != folder_name_raw:
            log.warning(f"[{idx}/{total}] folder_name {folder_name_raw!r} contains "
                        f"unsafe characters, using {folder_name!r} instead. "
                        f"Re-run 0_urls_to_articles.py to clean up articles.csv.")

        lang, oldid = parse_versioned_url(versioned_url)
        if oldid is None:
            log.warning(f"[{idx}/{total}] {folder_name} — URL has no oldid, skipping")
            failed += 1
            continue

        folder_path = output_dir / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)
        content_path = folder_path / "content.txt"
        url_path = folder_path / "url.txt"

        if content_path.exists() and not args.force:
            skipped += 1
            log.info(f"[{idx}/{total}] {folder_name} — exists, skip")
            continue

        log.info(f"[{idx}/{total}] {title} (oldid={oldid})...")
        result = fetch_revision(lang, oldid)

        if "error" in result:
            failed += 1
            log.warning(f"  ✗ {result['error']}")
            continue

        content = clean_extract(result["content"])
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