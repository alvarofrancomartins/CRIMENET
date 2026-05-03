"""
6_assign_country.py

Tags each entity in crimenet.json with its country of origin and the list of
countries it operates in, by sending the actual Wikipedia text to the LLM.

For each entity, picks the richest source available:

  1. own_source   — the org's own Wikipedia article. Read txts/<folder>/
                    content.txt and send the full text (capped at MAX_WORDS).
  2. mentioned_in — for each article that mentions the org, extract every
                    paragraph mentioning the org's name and send those.
  3. name_only    — fallback when no article text is found anywhere; classify
                    from name + type + the LLM-summarized description.

Adds four fields to each entity:
  - country            : single string ("Brazil", "United States", "Unknown", …)
  - countries_active   : list of countries the org operates in (incl. origin)
  - country_confidence : "high" | "medium" | "low"   (LLM-reported)
  - country_method     : "own_source" | "mentioned_in" | "name_only"

Resumable: skips entities that already have a `country` field, so a crashed
or interrupted run picks up where it left off. Checkpoints to disk every
CHECKPOINT_EVERY completions.

Usage:
    python 6_assign_country.py --input crimenet.json --txts-dir ./txts
"""

import json
import re
import time
import argparse
import logging
import threading
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────

API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
RETRIES = 3
DELAY = 0.2
DEFAULT_WORKERS = 20

MAX_WORDS = 8000           # cap text sent to LLM per call (~11k tokens)
MIN_TEXT_WORDS = 30        # below this, fall through to the next tier
MAX_MENTION_ARTICLES = 8   # cap how many mentioning articles to scan per org
CHECKPOINT_EVERY = 100     # save progress every N completions


# ── Path helpers ──────────────────────────────────────────────────────────

def build_url_index(txts_dir):
    """Build {url: folder_path} by scanning every txts/<folder>/url.txt.

    Mapping comes from url.txt (ground truth written by step 1) rather than
    re-slugifying titles, which has too many edge cases.
    """
    idx = {}
    if not txts_dir.exists():
        log.warning(f"txts dir {txts_dir} does not exist; "
                    f"all entities will fall back to name_only")
        return idx
    for folder in sorted(txts_dir.iterdir()):
        if not folder.is_dir():
            continue
        url_file = folder / "url.txt"
        if not url_file.exists():
            continue
        try:
            url = url_file.read_text("utf-8").strip()
            if url:
                idx[url] = folder
        except Exception as e:
            log.debug(f"skip {url_file}: {e}")
    return idx


def load_article_text(folder):
    """Read content.txt for a folder, or '' if missing/unreadable."""
    if not folder:
        return ""
    p = folder / "content.txt"
    if not p.exists():
        return ""
    try:
        return p.read_text("utf-8")
    except Exception:
        return ""


def cap_words(text, max_words=MAX_WORDS):
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "\n[... article truncated ...]"


def extract_mentioning_paragraphs(article_text, name):
    """Return paragraphs from article_text that mention `name` (case-insensitive).

    Plain substring match on the canonical name. The LLM gets the matched
    paragraphs verbatim and decides which references are actually about THIS
    org (vs. a coincidental name collision).
    """
    if not article_text or not name:
        return ""
    name_lower = name.strip().lower()
    if not name_lower:
        return ""
    paragraphs = re.split(r"\n{2,}", article_text)
    matched = [p for p in paragraphs if name_lower in p.lower()]
    return "\n\n".join(matched)


# ── Strategy picker ───────────────────────────────────────────────────────

def pick_text(entity, url_index):
    """Return (text, method) using the richest available source for this org."""
    name = entity["name"]

    # Tier 1: own Wikipedia article
    own = entity.get("own_source")
    if own and isinstance(own, dict) and own.get("url"):
        folder = url_index.get(own["url"])
        if folder:
            text = load_article_text(folder)
            if text and len(text.split()) >= MIN_TEXT_WORDS:
                return cap_words(text), "own_source"

    # Tier 2: paragraphs mentioning this org from articles about OTHER orgs
    mentioned = entity.get("mentioned_in") or []
    if mentioned:
        chunks = []
        total_words = 0
        for ref in mentioned[:MAX_MENTION_ARTICLES]:
            url = ref.get("url") if isinstance(ref, dict) else None
            folder = url_index.get(url) if url else None
            if not folder:
                continue
            article = load_article_text(folder)
            if not article:
                continue
            passages = extract_mentioning_paragraphs(article, name)
            if not passages:
                continue
            title = ref.get("title") or "Wikipedia"
            chunks.append(f"[from article: {title}]\n{passages}")
            total_words += len(passages.split())
            if total_words >= MAX_WORDS:
                break
        combined = "\n\n".join(chunks)
        if combined and len(combined.split()) >= MIN_TEXT_WORDS:
            return cap_words(combined), "mentioned_in"

    # Tier 3: fall back to the LLM-summarized description
    desc = ""
    if entity.get("descriptions"):
        desc = (entity["descriptions"][0] or "").strip()
    return desc, "name_only"


# ── Prompt + LLM ──────────────────────────────────────────────────────────

SYSTEM = """You are an expert on global organized crime. Read the provided text about a criminal organization and identify:

1. country — the country where the organization originated or is primarily based. Single value, English short name (e.g. "Brazil", "Italy", "United States", "Mexico", "Russia", "Japan", "Colombia", "Hong Kong"). If the org spans multiple countries with no clear origin, pick the one most closely associated with its founding or primary base. If genuinely unknown, return "Unknown".

2. countries_active — a list of countries where the organization is known to operate, including the origin. Same naming convention. [] if unknown.

3. confidence — your confidence in the classification:
   - "high"   : the text clearly states origin and where the org operates.
   - "medium" : origin/activity is implied but not stated outright; you're inferring from indirect evidence (mentioned cities, individuals, conflicts).
   - "low"    : very thin evidence, mostly inference, or the org is barely described.

CRITICAL FORMAT RULES:
- ALL country names in English.
- "United States" — not "USA", "US", or "America".
- "United Kingdom" — not "UK" or "Britain".
- "Hong Kong" is its own entry, distinct from China (relevant for triads).
- "Northern Ireland" rolls into "United Kingdom".
- "Czech Republic" — not "Czechia".
- "Myanmar" — not "Burma".
- Continents and regions ("Latin America", "Europe", "Balkans") are NOT valid. Pick a country.
- If you cannot determine the country with reasonable confidence, return "Unknown" / [] and confidence "low". Do not guess wildly.

Return ONLY a JSON object, nothing else:
{"country": "Brazil", "countries_active": ["Brazil", "Paraguay"], "confidence": "high"}"""


def build_user_prompt(name, org_type, method, text):
    method_blurb = {
        "own_source":   "The text below is the Wikipedia article about this organization.",
        "mentioned_in": "The text below contains paragraphs mentioning this organization, "
                        "extracted from Wikipedia articles about OTHER organizations.",
        "name_only":    "No article text is available. Classify from the brief description below.",
    }.get(method, "")
    return (
        f"ORGANIZATION: {name}\n"
        f"TYPE: {org_type or 'unknown'}\n"
        f"\n{method_blurb}\n\n"
        f"--- TEXT ---\n{text}\n--- END ---"
    )


def call_api(api_key, prompt):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.0,
        "max_tokens": 1024,
    }
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(API_URL, headers=headers, json=payload, timeout=180)
            if r.status_code == 429:
                wait = 5 * attempt * 2
                log.warning(f"  Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            raw = r.json()["choices"][0]["message"]["content"].strip()
            return json.loads(raw)
        except json.JSONDecodeError as e:
            log.warning(f"  JSON decode error (attempt {attempt}): {e}")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            log.warning(f"  Request error (attempt {attempt}): {e}")
            time.sleep(3 * attempt)
        except Exception as e:
            log.warning(f"  Unexpected error (attempt {attempt}): {e}")
            time.sleep(2)
    return None


def coerce_record(rec):
    """Normalize one LLM record into (country, countries_active, confidence)."""
    if not isinstance(rec, dict):
        return "Unknown", [], "low"

    country = rec.get("country")
    if not isinstance(country, str) or not country.strip():
        country = "Unknown"
    country = country.strip()

    active = rec.get("countries_active")
    if not isinstance(active, list):
        active = []
    active = [str(c).strip() for c in active
              if isinstance(c, (str, int)) and str(c).strip()]

    if country and country != "Unknown" and country not in active:
        active = [country] + active

    conf = (rec.get("confidence") or "").strip().lower()
    if conf not in {"high", "medium", "low"}:
        conf = "low"

    return country, active, conf


# ── Worker ────────────────────────────────────────────────────────────────

def process_one(api_key, entity, url_index):
    """Classify one entity. Returns (name, record_or_None, method)."""
    name = entity["name"]
    org_type = entity.get("type", "")
    text, method = pick_text(entity, url_index)
    if not text:
        text = "(no description available)"
    prompt = build_user_prompt(name, org_type, method, text)
    rec = call_api(api_key, prompt)
    time.sleep(DELAY)
    return name, rec, method


# ── Main ──────────────────────────────────────────────────────────────────

def load_key(path="deepseek_api_key.txt"):
    return Path(path).read_text("utf-8").strip()


def atomic_write(path, data_dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data_dict, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(path)


def main():
    parser = argparse.ArgumentParser(
        description="Country-tag entities in crimenet.json using full Wikipedia text."
    )
    parser.add_argument("--input", "-i", default="crimenet.json")
    parser.add_argument("--txts-dir", default="./txts",
                        help="Directory with one folder per Wikipedia article")
    parser.add_argument("--workers", "-w", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        log.error(f"Input file not found: {in_path}")
        return

    api_key = load_key()
    data = json.loads(in_path.read_text("utf-8"))
    entities = data.get("entities", [])
    log.info(f"Loaded {len(entities)} entities from {in_path}")

    url_index = build_url_index(Path(args.txts_dir))
    log.info(f"Indexed {len(url_index)} article folders in {args.txts_dir}")

    # Resumability: skip anything already tagged
    to_process = [e for e in entities if not e.get("country")]
    skip = len(entities) - len(to_process)
    log.info(f"  {skip} skipped (already tagged) | {len(to_process)} to process | "
             f"{args.workers} workers")
    if not to_process:
        log.info("Nothing to do.")
        return

    name_to_entity = {e["name"]: e for e in entities}

    save_lock = threading.Lock()
    completed = 0
    failed = 0
    method_counts = Counter()
    conf_counts = Counter()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_one, api_key, e, url_index): e
                   for e in to_process}

        for fut in as_completed(futures):
            try:
                name, rec, method = fut.result()
            except Exception as e:
                failed += 1
                log.error(f"  worker crashed: {e}")
                continue

            ent = name_to_entity.get(name)
            if not ent:
                continue

            country, active, conf = coerce_record(rec)
            ent["country"] = country
            ent["countries_active"] = active
            ent["country_confidence"] = conf
            ent["country_method"] = method
            method_counts[method] += 1
            conf_counts[conf] += 1

            completed += 1
            log.info(f"  [{completed}/{len(to_process)}] {name[:42]:42s} → "
                     f"{country:20s} [{conf}, {method}]")

            if completed % CHECKPOINT_EVERY == 0:
                with save_lock:
                    atomic_write(in_path, data)
                    log.info(f"  ✓ checkpoint saved at {completed} completions")

    log.info("=" * 60)
    log.info(f"Completed: {completed} | Failed: {failed}")
    log.info(f"By method:     {dict(method_counts)}")
    log.info(f"By confidence: {dict(conf_counts)}")

    by_country = Counter(e.get("country", "Unknown") for e in entities)
    log.info("Top 25 origin countries:")
    for c, n in by_country.most_common(25):
        log.info(f"  {c:30s} {n:5d}")

    by_active = Counter()
    for e in entities:
        for c in e.get("countries_active", []) or []:
            by_active[c] += 1
    log.info("Top 25 countries by # of orgs operating there:")
    for c, n in by_active.most_common(25):
        log.info(f"  {c:30s} {n:5d}")

    backup = in_path.with_suffix(in_path.suffix + ".pre-country.bak")
    if not backup.exists():
        backup.write_text(in_path.read_text("utf-8"), encoding="utf-8")
        log.info(f"Backed up original to {backup}")

    atomic_write(in_path, data)
    log.info(f"Wrote tagged output to {in_path}")


if __name__ == "__main__":
    main()