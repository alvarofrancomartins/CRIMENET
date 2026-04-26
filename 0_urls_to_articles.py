"""
0_urls_to_articles.py

Reads a list of plain Wikipedia URLs (no oldid) from page_hyperlinks.csv,
queries the Wikipedia API for the current revision ID of each, and writes
articles.csv with title, folder_name, and versioned URL.

Every row written to articles.csv is guaranteed to have an oldid. Titles
that fail after retries are listed at the end of the run, never saved.

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
from urllib.parse import unquote, urlparse, quote

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DELAY = 1.5             # seconds between API calls
NETWORK_RETRIES = 5     # for 429s and transient network errors
NO_OLDID_RETRIES = 3    # if API responds OK but returns no oldid, retry this many times

USER_AGENT = "CRIMENET/1.0 (https://github.com/alvarofrancomartins/CRIMENET; research)"
HTTP_HEADERS = {"User-Agent": USER_AGENT}


def parse_url(url: str):
    """Extract (lang, title) from a plain Wikipedia URL."""
    parsed = urlparse(url)
    lang = parsed.netloc.split(".")[0]

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


def _fetch_oldid_once(lang: str, title: str) -> tuple:
    """Single fetch attempt. Returns (oldid_or_None, reason).

    reason is a short string explaining what happened, useful for diagnostics.
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

    for attempt in range(NETWORK_RETRIES):
        try:
            r = requests.get(api_url, params=params, timeout=15,
                             headers=HTTP_HEADERS)

            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
                log.warning(f"  429 rate-limited, sleeping {wait}s "
                            f"(attempt {attempt + 1}/{NETWORK_RETRIES})")
                time.sleep(wait)
                continue

            r.raise_for_status()
            data = r.json()

            if "error" in data:
                return None, f"API error: {data['error'].get('info', 'unknown')}"

            pages = data.get("query", {}).get("pages", {})
            if not pages:
                return None, "no pages in response"

            for page_id, page in pages.items():
                if page_id == "-1":
                    return None, "page not found"
                revs = page.get("revisions", [])
                if revs and "revid" in revs[0]:
                    return revs[0]["revid"], "ok"
                return None, "page exists but has no revisions"

            return None, "unexpected empty response"

        except Exception as e:
            if is_rate_limit_error(e) and attempt < NETWORK_RETRIES - 1:
                wait = 5 * (2 ** attempt)
                log.warning(f"  network error, sleeping {wait}s: {e}")
                time.sleep(wait)
                continue
            return None, f"network error: {e}"

    return None, f"gave up after {NETWORK_RETRIES} network retries"


def fetch_oldid(lang: str, title: str) -> Optional[int]:
    """Fetch the current oldid for a title. Retries on transient empty responses.

    Returns the oldid on success, None after all retries are exhausted.
    Logs the reason for each failure.
    """
    for attempt in range(1, NO_OLDID_RETRIES + 1):
        oldid, reason = _fetch_oldid_once(lang, title)
        if oldid is not None:
            return oldid

        # Don't retry if the page is genuinely missing or the API rejected the request
        if reason in ("page not found",) or reason.startswith("API error:"):
            log.warning(f"  ✗ {reason} (no retry)")
            return None

        if attempt < NO_OLDID_RETRIES:
            wait = 3 * attempt
            log.warning(f"  ⚠ {reason}, retrying in {wait}s "
                        f"(attempt {attempt}/{NO_OLDID_RETRIES})")
            time.sleep(wait)
        else:
            log.warning(f"  ✗ {reason} after {NO_OLDID_RETRIES} attempts")

    return None


def slugify(title: str) -> str:
    """Title -> folder_name. Filesystem-safe.

    - Replaces spaces with underscores.
    - Replaces filesystem-unsafe characters (path separators, reserved chars,
      control chars) with underscores. Without this, titles like 'CBL/BFL'
      would create nested directories instead of a single folder.
    - Collapses runs of underscores and trims leading/trailing underscores.
    """
    s = title.replace(" ", "_")
    # Replace characters that are invalid in paths on Linux/macOS/Windows.
    # Covers: / \ : * ? " < > | and ASCII control characters.
    s = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", s)
    # Collapse runs of underscores so "a__b" doesn't appear from double-replacement
    s = re.sub(r"_+", "_", s).strip("_")
    return s


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
                        help="Input CSV with one URL per row")
    parser.add_argument("--output", "-o", default="articles.csv",
                        help="Output CSV with title, folder_name, url")
    args = parser.parse_args()

    # Read URLs
    urls = []
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            cell = row[0].strip()
            if cell.lower() == "url":
                continue
            if cell.startswith("http"):
                urls.append(cell)

    log.info(f"Loaded {len(urls)} URLs from {args.input}")

    # Resume: keep rows from previous runs ONLY if they have a valid oldid URL.
    # Bare /wiki/ rows from older script versions get re-fetched.
    out_path = Path(args.output)
    done_titles = set()
    out_rows = []
    dropped = 0
    if out_path.exists():
        with open(out_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "oldid=" in row.get("url", ""):
                    out_rows.append(row)
                    done_titles.add(row["title"])
                else:
                    dropped += 1
        log.info(f"Resuming: {len(done_titles)} valid rows kept, "
                 f"{dropped} bare-URL rows will be re-fetched")

    failed = []   # titles that didn't resolve to an oldid this run

    for idx, url in enumerate(urls, 1):
        lang, title = parse_url(url)
        if not title:
            log.warning(f"[{idx}/{len(urls)}] could not parse: {url}")
            failed.append(("?", url, "could not parse URL"))
            continue

        if title in done_titles:
            log.info(f"[{idx}/{len(urls)}] {title} — exists, skip")
            continue

        log.info(f"[{idx}/{len(urls)}] {lang}:{title}")
        oldid = fetch_oldid(lang, title)

        if oldid is None:
            failed.append((lang, title, url))
            time.sleep(DELAY)
            continue

        # Properly encode the title for URL query strings.
        # quote() escapes anything that's not URL-safe (apostrophes, slashes,
        # accented characters, etc.). We pre-replace spaces with underscores
        # to match Wikipedia's title convention.
        encoded_title = quote(title.replace(" ", "_"), safe="")
        versioned = (
            f"https://{lang}.wikipedia.org/w/index.php"
            f"?title={encoded_title}&oldid={oldid}"
        )

        out_rows.append({
            "title": title,
            "folder_name": slugify(title),
            "url": versioned,
        })
        write_csv(out_path, out_rows)
        time.sleep(DELAY)

    # End-of-run summary
    log.info("=" * 60)
    log.info(f"Done: {len(out_rows)} rows in {out_path}")

    if failed:
        log.warning("=" * 60)
        log.warning(f"FAILED to resolve oldid for {len(failed)} title(s):")
        for lang, title, url in failed:
            log.warning(f"  - [{lang}] {title}")
            log.warning(f"      url: {url}")
        log.warning("These titles were NOT written to the output CSV.")
        log.warning("Re-run the script to retry them, or investigate manually.")
    else:
        log.info("All titles resolved successfully.")


if __name__ == "__main__":
    main()