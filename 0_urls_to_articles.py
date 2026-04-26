"""
0_urls_to_articles.py

Reads a list of plain Wikipedia URLs (no oldid) from page_hyperlinks.csv,
queries the Wikipedia API for the current revision ID of each, and writes
articles.csv with title, folder_name, and versioned URL.

Usage:
    python 0_urls_to_articles.py --input page_hyperlinks.csv --output articles.csv
"""

import csv
import re
import time
import argparse
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DELAY = 1.5          # seconds between API calls
MAX_RETRIES = 5      # for 429s and transient errors

USER_AGENT = "CRIMENET/1.0 (https://github.com/alvarofrancomartins/CRIMENET; research)"
HTTP_HEADERS = {"User-Agent": USER_AGENT}


def parse_url(url: str):
    """
    Extract (lang, title) from a plain Wikipedia URL.
    Supports both /wiki/Title and /w/index.php?title=Title forms.
    """
    parsed = urlparse(url)
    lang = parsed.netloc.split(".")[0]   # 'en', 'it', etc.

    if "/wiki/" in parsed.path:
        title = parsed.path.split("/wiki/")[-1]
    elif "title=" in parsed.query:
        m = re.search(r"title=([^&]+)", parsed.query)
        title = m.group(1) if m else ""
    else:
        title = ""

    title = unquote(title).replace("_", " ")
    return lang, title


def is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return ("429" in msg
            or "too many requests" in msg
            or "timed out" in msg
            or "timeout" in msg)


def fetch_current_oldid(lang: str, title: str) -> Optional[int]:
    """Hit the Wikipedia API and return the latest revision ID for `title`.
    Retries with exponential backoff on 429s and transient errors.
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": title,
        "prop": "revisions",
        "rvprop": "ids",
        "rvlimit": 1,
        "format": "json",
        "redirects": 1,
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(api_url, params=params, timeout=15,
                             headers=HTTP_HEADERS)

            if r.status_code == 429:
                wait = 5 * (2 ** attempt)   # 5, 10, 20, 40, 80
                log.warning(f"  429 rate-limited, sleeping {wait}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            r.raise_for_status()
            pages = r.json().get("query", {}).get("pages", {})
            for page_id, page in pages.items():
                if page_id == "-1":
                    return None  # page not found
                revs = page.get("revisions", [])
                if revs:
                    return revs[0]["revid"]
            return None

        except Exception as e:
            if is_rate_limit_error(e) and attempt < MAX_RETRIES - 1:
                wait = 5 * (2 ** attempt)
                log.warning(f"  network error, sleeping {wait}s: {e}")
                time.sleep(wait)
                continue
            log.warning(f"  API error for '{title}': {e}")
            return None

    log.warning(f"  gave up after {MAX_RETRIES} retries: {title}")
    return None


def slugify(title: str) -> str:
    """Title -> folder_name (replace spaces with underscores, keep it simple)."""
    return title.replace(" ", "_")


def write_csv(out_path: Path, rows: list):
    """Write CSV atomically: write to temp, then rename."""
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "folder_name", "url"])
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(out_path)


def main():
    parser = argparse.ArgumentParser(description="Plain URLs -> versioned articles.csv")
    parser.add_argument("--input", "-i", default="page_hyperlinks.csv",
                        help="Input CSV with one URL per row (header 'url' optional)")
    parser.add_argument("--output", "-o", default="articles.csv",
                        help="Output CSV with title, folder_name, url")
    args = parser.parse_args()

    # Read URLs (accept either headered or headerless single-column CSV)
    urls = []
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            cell = row[0].strip()
            if cell.lower() == "url":
                continue   # skip header
            if cell.startswith("http"):
                urls.append(cell)

    log.info(f"Loaded {len(urls)} URLs from {args.input}")

    # Resume: load already-processed rows from output if it exists
    out_path = Path(args.output)
    done_titles = set()
    out_rows = []
    if out_path.exists():
        with open(out_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                out_rows.append(row)
                done_titles.add(row["title"])
        log.info(f"Resuming: {len(done_titles)} already in {args.output}")

    for idx, url in enumerate(urls, 1):
        lang, title = parse_url(url)
        if not title:
            log.warning(f"[{idx}/{len(urls)}] could not parse: {url}")
            continue

        if title in done_titles:
            log.info(f"[{idx}/{len(urls)}] {title} — exists, skip")
            continue

        log.info(f"[{idx}/{len(urls)}] {lang}:{title}")
        oldid = fetch_current_oldid(lang, title)
        if oldid is None:
            log.warning(f"  ✗ no revision found")
            time.sleep(DELAY)
            continue

        versioned = (
            f"https://{lang}.wikipedia.org/w/index.php"
            f"?title={title.replace(' ', '_')}&oldid={oldid}"
        )
        out_rows.append({
            "title": title,
            "folder_name": slugify(title),
            "url": versioned,
        })

        # Write incrementally so a crash doesn't lose progress
        write_csv(out_path, out_rows)

        time.sleep(DELAY)

    log.info(f"Done: wrote {len(out_rows)} rows to {out_path}")


if __name__ == "__main__":
    main()