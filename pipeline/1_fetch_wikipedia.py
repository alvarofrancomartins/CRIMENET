"""
1_fetch_wikipedia.py
Fetch clean article text from Wikipedia using the MediaWiki Action API,
pinned to the exact revision (oldid) from articles.csv.

ONE API call per article: action=parse, prop=text, returns the rendered
HTML for the revision. We then walk the HTML with BeautifulSoup to:

  1. Extract the infobox (kept separately, appended at the end of content.txt
     under a '--- INFOBOX ---' marker).
  2. Convert the rest of the body into clean plain text — paragraphs,
     lists, and section headings preserved; tables, navboxes, edit links,
     reference markers, and unwanted trailing sections dropped.

Outputs one folder per article in ./txts/ with content.txt + url.txt.

Usage:
     python 1_fetch_wikipedia.py --csv ../data/articles.csv --output ../data/txts
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
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DELAY = 1.5
MAX_RETRIES = 5

USER_AGENT = "CRIMENET/1.0 (https://github.com/alvarofrancomartins/CRIMENET; research)"
HTTP_HEADERS = {"User-Agent": USER_AGENT}

# Filesystem-unsafe characters that must not appear in folder names.
UNSAFE_PATH_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')

# ── Text extraction config ─────────────────────────────────────────────────
#
# Section headers at which we stop emitting body content. Compared
# case-insensitively against the cleaned text of <h2>/<h3>/.../<h6>
# elements. Covers English, Italian, Portuguese, Spanish — extend by
# adding the localized term to this set, no other code changes needed.
STOP_SECTIONS = {
    # English
    "see also", "references", "notes", "notes and references",
    "further reading", "external links", "bibliography", "sources",
    "footnotes", "learn more",
    # Italian
    "voci correlate", "note", "bibliografia", "collegamenti esterni",
    "fonti", "altri progetti",
    # Portuguese
    "referências", "referencias", "ver também", "ver tambem",
    "ligações externas", "ligacoes externas",
    "notas", "leitura adicional", "fontes",
    # Spanish
    "véase también", "vease tambien", "enlaces externos",
    "bibliografía", "bibliografia", "lectura adicional", "fuentes",
}

# Citation/quality flag brackets to scrub from body text and infobox values.
# Matches "[49]", "[citation needed]", and the localized equivalents in
# Italian, Portuguese, and Spanish. Also strips the "Erro de citação: …"
# rendering-error line that appears in some pt articles.
CITE_BRACKETS = re.compile(
    r"\[\s*\d+\s*\]|"
    r"\[\s*citation needed\s*\]|"
    r"\[\s*senza fonte\s*\]|\[\s*citazione necessaria\s*\]|"
    r"\[\s*carece de fontes\??\s*\]|\[\s*sem fontes?\s*\]|"
    r"\[\s*fonte\??\s*\]|\[\s*procurar fonte\s*\]|"
    r"\[\s*esclarecer\s*\]|"
    r"Erro de cita[çc][ãa]o:[^\n]*|"
    r"\[\s*cita requerida\s*\]|\[\s*sin fuentes?\s*\]|"
    r"\[\s*fuente requerida\s*\]|"
    r"\[\s*aclaraci[óo]n requerida\s*\]|\[\s*aclarar\s*\]|"
    r"\[\s*¿?cu[áa]ndo\??\s*\]|\[\s*¿?qui[ée]n\??\s*\]|\[\s*¿?d[óo]nde\??\s*\]",
    re.IGNORECASE,
)

# Zero-width and invisible formatting characters (Unicode category Cf)
# that leak from Wikipedia template expansions — they have no semantic
# meaning in plain text and confuse LLMs.  Strip them everywhere.
_INVISIBLE_CHARS = re.compile(
    '[​-‏ -‮⁠-⁯﻿]'
)

# Subtrees to drop from the soup before walking — these never carry useful
# body content. The infobox table is also dropped here, AFTER its text has
# been extracted separately. Anything not in this list and not in the walker
# whitelist (p, ul, ol, dl, blockquote, headings, section/div wrappers) is
# silently ignored.
DROP_SELECTORS = [
    "table.infobox", "table.sinottico", "table.ficha",  # infoboxes (extracted separately)
    "table.navbox", "div.navbox",           # bottom-of-page navigation
    "table.metadata",                        # cleanup banners
    "div.toc",                               # table of contents
    "div.reflist", "ol.references",          # footnote lists at end of article
    "div.thumb", "figure",                   # image figures + captions
    "div.hatnote",                           # "See also: …" pointers above sections
    "div.sistersitebox",                     # links to Wikidata / Wikinews etc.
    "ul.gallery", "div.gallery",             # image galleries
    "audio", "video",
    "span.mw-editsection",                   # the "[edit]" link next to every header
    "sup.reference", "sup.noprint",          # inline footnote markers
    "style", "script",
]


# ── HTTP / API helpers ─────────────────────────────────────────────────────

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
    """Sanitize a folder name so it never creates nested directories."""
    s = UNSAFE_PATH_CHARS.sub("_", name)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def api_get(url: str, params: dict, label: str) -> dict:
    """GET a MediaWiki API endpoint with retry on 429s and transient errors."""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30, headers=HTTP_HEADERS)
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
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


def fetch_parse(lang: str, oldid: int) -> dict:
    """Fetch the rendered HTML + title for a specific revision.

    Returns {title, html} on success, {error} on failure.
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "oldid": oldid,
        "prop": "text",
        "format": "json",
        "redirects": 1,
    }
    data = api_get(api_url, params, label="parse")
    if "_error" in data:
        return {"error": data["_error"]}
    if "error" in data:
        return {"error": f"API: {data['error'].get('info', 'unknown')}"}
    parse = data.get("parse", {})
    html = parse.get("text", {}).get("*", "")
    if not html:
        return {"error": "Empty HTML"}
    return {"title": parse.get("title", ""), "html": html}


# ── HTML → text helpers ────────────────────────────────────────────────────

def _clean_inline(text: str) -> str:
    """Strip citation/quality brackets, zero-width chars, and collapse whitespace."""
    text = CITE_BRACKETS.sub("", text)
    text = _INVISIBLE_CHARS.sub("", text)
    text = text.replace(" ", " ")  # NBSP → regular space (common in Wikipedia)
    return re.sub(r"\s+", " ", text).strip()


def _extract_infobox_text(soup) -> str:
    """Pull the infobox out of the soup as plain-text key-value lines.

    Mutates the infobox table (replaces <br>/<li> with commas before
    flattening), which is fine because DROP_SELECTORS removes the
    whole table from the soup right after this call.

    Languages: English, Portuguese & Spanish use class="infobox" (or
    variants like infobox_v2); Italian uses class="sinottico"; older
    Spanish pages may use class="ficha".  Substring-check catches all.
    """
    box = soup.find("table", class_=lambda c: c and (
        "infobox" in c.lower() or "sinottico" in c.lower() or "ficha" in c.lower()
    ))
    if not box:
        return ""

    lines = []
    caption = box.find("caption") or box.find(
        "th", class_=re.compile("testata|title|infobox-title|cabecera", re.I)
    )
    if caption:
        t = _clean_inline(caption.get_text(strip=True))
        if t:
            lines.append(t)

    for el in box.find_all(class_=re.compile("subheader", re.I)):
        t = _clean_inline(el.get_text(strip=True))
        if t:
            lines.append(t)

    for row in box.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if not (th and td):
            continue
        key = _clean_inline(th.get_text(" ", strip=True))
        for br in td.find_all(["br", "hr"]):
            br.replace_with(", ")
        for li in td.find_all("li"):
            li.append(", ")
        val = td.get_text(" ", strip=True)
        val = _clean_inline(val)
        val = re.sub(r"(,\s*)+", ", ", val).strip(", ")
        if key and val:
            lines.append(f"{key}: {val}")

    return "\n".join(lines)


def _walk_to_text(soup) -> str:
    """Walk the soup tree in document order, emitting plain text.

    Preserves: paragraphs, lists (ul/ol/dl), blockquotes, section headers.
    Drops: tables, images, anything in DROP_SELECTORS (already decomposed
    before this is called).

    Stops as soon as a heading whose text matches STOP_SECTIONS is hit.
    The `stopped` mutable closure is the simplest way to propagate that
    signal out through nested recursion.
    """
    lines = []
    stopped = [False]

    def walk(node):
        if stopped[0]:
            return
        for child in node.children:
            if stopped[0]:
                return
            name = getattr(child, "name", None)
            if name is None:
                continue

            if name in ("h1", "h2", "h3", "h4", "h5", "h6"):
                text = _clean_inline(child.get_text(" ", strip=True))
                # End-matter (References, See also, External links, Notes, …)
                # is always a top-level (==h2==) section on Wikipedia, so only
                # stop there. A deeper subsection that happens to share a name
                # — an h3 "Notes", or a content "Sources" (of income) — must
                # not truncate the rest of the article.
                if name == "h2" and text.lower() in STOP_SECTIONS:
                    stopped[0] = True
                    return
                if text:
                    lines.extend(["", text, ""])
            elif name == "p":
                text = _clean_inline(child.get_text(" ", strip=True))
                if text:
                    lines.extend([text, ""])
            elif name in ("ul", "ol"):
                # find_all(..., recursive=False) keeps nested lists from
                # being double-counted. The nested list's text is still
                # picked up because li.get_text() descends into children.
                for li in child.find_all("li", recursive=False):
                    text = _clean_inline(li.get_text(" ", strip=True))
                    if text:
                        lines.append("- " + text)
                lines.append("")
            elif name == "dl":
                for el in child.find_all(["dt", "dd"], recursive=False):
                    text = _clean_inline(el.get_text(" ", strip=True))
                    if not text:
                        continue
                    prefix = "" if el.name == "dt" else "  "
                    lines.append(prefix + text)
                lines.append("")
            elif name == "blockquote":
                text = _clean_inline(child.get_text(" ", strip=True))
                if text:
                    lines.extend(["> " + text, ""])
            elif name in ("section", "div"):
                # Wrappers: Parsoid <section> containers, the
                # <div class="mw-heading"> wrappers around modern h2s,
                # template-rendered <div typeof="mw:Transclusion">
                # wrappers around lists (this is the {{Divcol}} case
                # that motivated the rewrite). Recurse into them.
                walk(child)
            # Everything else (tables, spans, headers we don't recognize)
            # is silently skipped.

    walk(soup)
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_article_html(html: str) -> tuple:
    """Single-pass conversion of Wikipedia article HTML to (body, infobox).

    1. Extract infobox text first (the function mutates the table but that
       only matters within this soup).
    2. Decompose every DROP_SELECTORS subtree, including the infobox table
       we just extracted, so the walker doesn't re-emit any of it.
    3. Walk the remaining tree, emitting prose/lists/headers.
    """
    soup = BeautifulSoup(html, "html.parser")
    infobox = _extract_infobox_text(soup)
    for selector in DROP_SELECTORS:
        for el in soup.select(selector):
            el.decompose()
    body = _walk_to_text(soup)
    return body, infobox


# ── Per-article fetch ──────────────────────────────────────────────────────

def fetch_revision(lang: str, oldid: int) -> dict:
    """Fetch a Wikipedia article at a specific revision.

    One API call: action=parse&prop=text returns both the page title and
    the rendered HTML. Body text and infobox are then parsed out of that
    HTML locally.

    Returns {title, content, infobox} or {error}. (The caller writes the
    versioned URL from articles.csv to url.txt, so this need not return one.)
    """
    result = fetch_parse(lang, oldid)
    if "error" in result:
        return result

    body, infobox = parse_article_html(result["html"])
    if not body:
        return {"error": "Empty body text after HTML parse"}

    return {
        "title": result["title"],
        "content": body,
        "infobox": infobox,
    }


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch Wikipedia articles from CSV")
    parser.add_argument("--csv", "-c", default="articles.csv",
                        help="Input CSV from 0_urls_to_articles.py")
    parser.add_argument("--output", "-o", default="./txts", help="Output directory")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Re-fetch even if content.txt exists")
    parser.add_argument("--filter", "-F", type=str, default="",
                        help="Only process rows whose folder_name contains this substring")
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

        # Defensive sanity check on folder_name.
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

        if args.filter and args.filter not in folder_name:
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

        content = result["content"]
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