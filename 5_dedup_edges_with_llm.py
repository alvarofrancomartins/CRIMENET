"""
5_dedup_edges_with_llm.py

Pass 4 of the cleanup. Deduplicates edges in crimenet.json so that
each unordered pair of organizations has exactly one edge in the final dataset.

Rules for picking the survivor when multiple edges connect the same pair:

  1. If every edge has a parseable date → pick the latest end date programmatically.
  2. If some edges have dates and others don't → ask the LLM to pick the most current.
  3. If no edges have dates → ask the LLM to pick the most current.

Sources and descriptions from dropped edges are merged into the survivor;
direction (source vs target) is preserved from the survivor.

Pair direction: (A, B) and (B, A) are treated as the same pair.
Edge type (allied_with vs rivals_with) is NOT a pair-distinguishing key —
if an alliance and a rivalry exist between the same pair, they compete and
the latest one wins.

Usage:
    python 5_dedup_edges_with_llm.py
    python 5_dedup_edges_with_llm.py --input crimenet.json
    python 5_dedup_edges_with_llm.py --input crimenet.json --dry-run
"""

import json
import re
import argparse
import logging
import time
from pathlib import Path
from collections import defaultdict

import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── DeepSeek config (same pattern as 2_extract_network.py) ────────────────

API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
RETRIES = 3
DELAY = 0.3


def load_key(path="deepseek_api_key.txt"):
    return Path(path).read_text("utf-8").strip()


# ── Date parsing ──────────────────────────────────────────────────────────

# Matches a 4-digit year between 1700 and 2099. Captures every year in the
# string and we use the maximum as the "end date" of the time period.
YEAR_RE = re.compile(r"\b(1[7-9]\d{2}|20\d{2})\b")

# Words that signal "still active" / "ongoing" — we treat these as the
# current year so a "since 2010" edge beats a "1980-1995" edge.
PRESENT_TOKENS = {
    "present", "today", "current", "currently", "now", "ongoing",
    "active", "still active",
}


def parse_end_year(time_period):
    """Extract the latest end year from a time_period string.

    Returns an int (year) or None if nothing parseable.
    A "present" token is treated as 9999 so it always beats any explicit year.
    """
    if not time_period:
        return None
    s = str(time_period).strip().lower()
    if not s or s in {"null", "none", "unknown", "n/a"}:
        return None

    # If the string mentions present/ongoing, treat as currently-active.
    for tok in PRESENT_TOKENS:
        if tok in s:
            return 9999

    years = YEAR_RE.findall(s)
    if not years:
        return None
    return max(int(y) for y in years)


# ── LLM resolver ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert in global organized crime. Given multiple candidate relationships between the same pair of criminal organizations, choose the ONE that best describes the most CURRENT (most recent / most active today) state of their relationship.

Use what you know about these specific organizations to decide which description is most current. If two relationships are roughly equally current, prefer the one with more specific time information.

Return ONLY this JSON, nothing else:
{"chosen_index": <integer>, "reason": "<one short sentence>"}"""


def ask_llm_to_pick(api_key, source, target, candidates):
    """Ask DeepSeek to pick the most current candidate edge.

    `candidates` is a list of dicts with keys: type, time_period, descriptions.
    Returns the integer index of the chosen candidate, or 0 on failure.
    """
    lines = [f"PAIR: {source} ↔ {target}", ""]
    for i, c in enumerate(candidates):
        tp = c.get("time_period") or "unknown"
        rel = c.get("type", "")
        desc = " ".join(c.get("descriptions", []))[:300]   # truncate long descriptions
        lines.append(f"[{i}] type={rel} | time_period={tp}")
        lines.append(f"    description: {desc}")
        lines.append("")
    user_prompt = "\n".join(lines).strip()

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.0,
        "max_tokens": 200,
    }

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(API_URL, headers=headers, json=payload, timeout=60)
            if r.status_code == 429:
                wait = 5 * attempt * 2
                log.warning(f"  Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            raw = r.json()["choices"][0]["message"]["content"].strip()
            data = json.loads(raw)
            idx = int(data.get("chosen_index", 0))
            if 0 <= idx < len(candidates):
                return idx, data.get("reason", "")
            else:
                log.warning(f"  LLM returned out-of-range index {idx}, defaulting to 0")
                return 0, "out-of-range"
        except Exception as e:
            log.warning(f"  LLM call failed (attempt {attempt}): {e}")
            time.sleep(2)

    log.warning(f"  LLM failed after {RETRIES} attempts, defaulting to first candidate")
    return 0, "llm-failed"


# ── Merging ────────────────────────────────────────────────────────────────

def merge_into_survivor(survivor, dropped):
    """Merge sources and descriptions from `dropped` into `survivor`.

    Sources are deduplicated by URL. Descriptions are deduplicated by
    exact string match.
    """
    seen_urls = {s.get("url") for s in survivor.get("sources", []) if s.get("url")}
    for src in dropped.get("sources", []):
        url = src.get("url")
        if url and url not in seen_urls:
            survivor.setdefault("sources", []).append(src)
            seen_urls.add(url)

    seen_descs = set(survivor.get("descriptions", []))
    for desc in dropped.get("descriptions", []):
        if desc and desc not in seen_descs:
            survivor.setdefault("descriptions", []).append(desc)
            seen_descs.add(desc)


# ── Main pipeline ─────────────────────────────────────────────────────────

def pair_key(source, target):
    """Unordered pair key. (A, B) and (B, A) collapse to the same key."""
    return tuple(sorted([source, target]))


def dedup_edges(relations, api_key, dry_run=False):
    """Deduplicate edges in `relations`. Returns the deduplicated list."""

    # Group edges by unordered pair
    groups = defaultdict(list)
    for edge in relations:
        groups[pair_key(edge["source"], edge["target"])].append(edge)

    survivors = []
    n_groups = len(groups)
    n_unique = sum(1 for g in groups.values() if len(g) == 1)
    n_dup_groups = n_groups - n_unique

    log.info(f"Total unique pairs: {n_groups}")
    log.info(f"  Pairs with single edge: {n_unique}")
    log.info(f"  Pairs with multiple edges: {n_dup_groups}")

    if n_dup_groups == 0:
        log.info("Nothing to deduplicate.")
        return relations

    n_resolved_by_date = 0
    n_resolved_by_llm = 0
    n_dropped = 0

    for pair, edges in groups.items():
        if len(edges) == 1:
            survivors.append(edges[0])
            continue

        source, target = pair

        # Try programmatic resolution first
        with_dates = [(i, parse_end_year(e.get("time_period"))) for i, e in enumerate(edges)]
        all_have_dates = all(yr is not None for _, yr in with_dates)

        if all_have_dates:
            # All edges have parseable dates → pick the latest end year
            chosen_idx = max(with_dates, key=lambda x: x[1])[0]
            n_resolved_by_date += 1
            log.info(f"  📅 {source} ↔ {target}: {len(edges)} edges, "
                     f"chose by date (year {with_dates[chosen_idx][1]})")
        else:
            # At least one edge is undated → ask the LLM
            log.info(f"  🤖 {source} ↔ {target}: {len(edges)} edges, asking LLM...")
            for i, e in enumerate(edges):
                log.info(f"     [{i}] {e['type']} | {e.get('time_period') or 'unknown'}")
            chosen_idx, reason = ask_llm_to_pick(api_key, source, target, edges)
            n_resolved_by_llm += 1
            log.info(f"     ✓ chose [{chosen_idx}] ({edges[chosen_idx]['type']}, "
                     f"{edges[chosen_idx].get('time_period') or 'unknown'}) — {reason}")

        # Merge dropped into survivor
        survivor = edges[chosen_idx]
        for i, dropped in enumerate(edges):
            if i == chosen_idx:
                continue
            merge_into_survivor(survivor, dropped)
            n_dropped += 1

        survivors.append(survivor)

    log.info("=" * 60)
    log.info(f"Resolved {n_resolved_by_date} pair(s) by latest date")
    log.info(f"Resolved {n_resolved_by_llm} pair(s) via LLM")
    log.info(f"Dropped {n_dropped} duplicate edge(s)")
    log.info(f"Final edge count: {len(survivors)}")

    return survivors


def main():
    parser = argparse.ArgumentParser(description="Dedup edges in crimenet.json")
    parser.add_argument("--input", "-i", default="crimenet.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute deduplication but don't write the output file")
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        log.error(f"Input file not found: {in_path}")
        return

    api_key = load_key()
    data = json.loads(in_path.read_text("utf-8"))

    n_before = len(data["relations"])
    log.info(f"Loaded {len(data['entities'])} entities and {n_before} relations")

    deduped = dedup_edges(data["relations"], api_key, dry_run=args.dry_run)
    data["relations"] = deduped

    log.info(f"Edges: {n_before} → {len(deduped)} ({n_before - len(deduped)} removed)")

    if args.dry_run:
        log.info("Dry run — not writing output file.")
        return

    # Backup before overwriting
    backup_path = in_path.with_suffix(in_path.suffix + ".bak")
    backup_path.write_text(in_path.read_text("utf-8"), encoding="utf-8")
    log.info(f"Backed up original to {backup_path}")

    in_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"Wrote deduplicated output to {in_path}")


if __name__ == "__main__":
    main()