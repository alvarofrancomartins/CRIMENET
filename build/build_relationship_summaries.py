#!/usr/bin/env python3
"""Pre-compute LLM relationship summaries for every directly-connected pair in CRIMENET.

Two phases:
  1. (parallel) Each worker processes one pair → writes one file to output_dir.
  2. (fast, serial) Read all pair files, assemble into 128 FNV-1a shard buckets.

Resumable: pair files already on disk are skipped.

Usage:
    python build/build_relationship_summaries.py --input data/crimenet.json --output app/data/relationship_summaries
"""

import argparse
import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

DEFAULT_BUCKETS = 128
API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
RETRIES = 3
MAX_RL_RETRIES = 6
MAX_TOKENS = 8192
MAX_TOKENS_RETRY = 16384
DEFAULT_WORKERS = 50


def load_key():
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError(
            "DeepSeek API key not found. Set it with:\n"
            '    export DEEPSEEK_API_KEY="sk-..."'
        )
    return key.strip()


def fnv1a(s: str) -> int:
    h = 0x811C9DC5
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def pair_key(a: str, b: str) -> str:
    return "||".join(sorted([a, b]))


def safe_filename(key: str) -> str:
    """Encode a pair_key into a filesystem-safe filename."""
    return key.replace("/", "_").replace("||", "__") + ".json"


def filename_to_key(fname: str) -> str:
    """Recover pair_key from a safe filename."""
    return fname.replace(".json", "").replace("__", "||").replace("_", "/")


# ── Prompt ────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = (
    "You are an expert analyst of global organized crime. Given two criminal "
    "organizations and their documented interactions, write a concise summary "
    "of their relationship.\n\n"
    "Write a single paragraph of ~150-250 words describing the interaction "
    "between these two organizations. Cover:\n"
    "- What kind of relationship exists: are they allies, enemies, or "
    "structurally linked (one is a faction/splinter/successor of the other)\n"
    "- The themes and patterns visible across their interactions\n"
    "- Key events that are explicitly dated in the evidence — but anchor each "
    "date to the specific interaction it belongs to, not to a larger timeline\n\n"
    "CRITICAL RULES — you MUST follow these:\n"
    "- Only describe a relationship shift or evolution if the evidence itself "
    "explicitly states that a change occurred (e.g., a description like "
    "\"split from\" or \"formerly allied with\"). Do NOT construct a "
    "chronological narrative from scattered dates.\n"
    "- Never imply that the evidence covers a complete or continuous "
    "timeline — the data are fragmentary Wikipedia extracts with many gaps.\n"
    "- Use phrases like \"the evidence shows\" or \"documented interactions "
    "include\" rather than presenting events as a single seamless story.\n"
    "- Do not invent facts, list individual citations, speculate about the "
    "future, or use markdown formatting.\n\n"
    "Return ONLY valid JSON with this field:\n"
    '{"summary": "The paragraph text."}'
)


def build_user_prompt(org_a: dict, org_b: dict, edges: list) -> str:
    parts = []

    def describe_org(label, org):
        lines = [f"Organization {label}: {org.get('standard_name', 'Unknown')}"]
        desc = org.get("description")
        if desc and desc.strip():
            lines.append(f"Description: {desc.strip()}")
        country = org.get("country")
        if country and country != "Unknown":
            lines.append(f"Country: {country}")
        tp = org.get("time_period")
        if tp:
            lines.append(f"Active period: {tp}")
        if org.get("is_defunct") is True:
            lines.append("Status: Defunct")
        return lines

    parts.extend(describe_org("A", org_a))
    parts.append("")
    parts.extend(describe_org("B", org_b))

    rel_label = {"cooperation": "Cooperation", "conflict": "Conflict", "other": "Other"}
    parts.append(f"\n--- {len(edges)} documented interaction(s) ---")

    for i, e in enumerate(edges):
        rel = e.get("relationship", "other")
        parts.append(f"\nInteraction {i+1}: {rel_label.get(rel, rel)}")
        descs = e.get("descriptions", [])
        if descs:
            parts.append(f"  Description: {'; '.join(descs)}")
        times = e.get("time_periods", [])
        if times:
            parts.append(f"  Time period: {', '.join(times)}")
        quote = e.get("evidence_quote", "")
        if quote:
            parts.append(f"  Evidence: {quote}")

    parts.append("\nWrite a relationship summary for these two organizations.")
    return "\n".join(parts)


# ── API client ─────────────────────────────────────────────────────────────────

def call_api(api_key: str, org_a: dict, org_b: dict, edges: list):
    prompt = build_user_prompt(org_a, org_b, edges)
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for rl in range(MAX_RL_RETRIES):
        for attempt in range(1, RETRIES + 1):
            max_tok = MAX_TOKENS if attempt == 1 else MAX_TOKENS_RETRY
            user_content = prompt
            if attempt > 1:
                user_content += (
                    "\n\nIMPORTANT: a previous attempt was truncated. "
                    "Keep the summary concise — under 200 words."
                )

            payload = {
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.0,
                "max_tokens": max_tok,
                "response_format": {"type": "json_object"},
            }

            try:
                r = requests.post(API_URL, headers=headers, json=payload, timeout=180)
            except requests.RequestException:
                time.sleep(2 * attempt)
                continue

            if r.status_code == 429:
                time.sleep(min(60, 5 * (2 ** rl)))
                break

            if r.status_code != 200:
                time.sleep(2 * attempt)
                continue

            try:
                body = r.json()
                content = body["choices"][0]["message"]["content"].strip()
                if content.startswith("```"):
                    content = content.split("\n", 1)[1]
                    if content.endswith("```"):
                        content = content[: content.rfind("```")].strip()
                return json.loads(content)
            except (json.JSONDecodeError, KeyError, IndexError):
                time.sleep(2 * attempt)
                continue

        return None

    return None


# ── Phase 1: worker (one pair = one file) ──────────────────────────────────────

def process_pair(pair_str, edges, api_key, by_name, output_dir, idx, total):
    """Process one pair: call LLM, write pair file. Returns 'ok' or 'fail'."""
    a_name, b_name = pair_str.split("||", 1)
    org_a = by_name.get(a_name, {"standard_name": a_name})
    org_b = by_name.get(b_name, {"standard_name": b_name})
    result = call_api(api_key, org_a, org_b, edges)
    if result and isinstance(result, dict) and result.get("summary"):
        entry = {
            "summary": result["summary"],
        }
        fp = output_dir / safe_filename(pair_str)
        fp.write_text(json.dumps(entry, ensure_ascii=False), "utf-8")
        return "ok"
    print(f"  [{idx}/{total}] {pair_str} ✗")
    return "fail"


# ── Phase 2: assemble pair files → 128 shard buckets ───────────────────────────

def assemble_shards(output_dir: Path, n_buckets: int, valid_keys: set):
    """Rebuild shard buckets, preserving prior summaries and dropping stale keys.

    Existing ``NNN.json`` shard contents are loaded first so a partial re-run
    (resumable) does not discard summaries it did not regenerate. Only keys
    still present in ``valid_keys`` survive; renamed or removed pairs are
    dropped rather than left as stale entries.
    """
    buckets = defaultdict(dict)

    # Preserve summaries already assembled into shards by a prior run.
    for fp in sorted(output_dir.glob("*.json")):
        name = fp.name
        stem = name[: name.rfind(".")]
        if not stem.isdigit():
            continue
        try:
            shard = json.loads(fp.read_text("utf-8"))
        except Exception:
            continue
        for key, entry in shard.items():
            if key in valid_keys:
                buckets[fnv1a(key) % n_buckets][key] = entry

    # Overlay individual pair files written by this run.
    for fp in sorted(output_dir.glob("*.json")):
        name = fp.name
        stem = name[: name.rfind(".")]
        if stem.isdigit():
            continue
        try:
            key = filename_to_key(name)
            entry = json.loads(fp.read_text("utf-8"))
        except Exception:
            continue
        if key in valid_keys:
            buckets[fnv1a(key) % n_buckets][key] = entry

    width = max(3, len(str(n_buckets - 1)))
    for i in range(n_buckets):
        payload = buckets.get(i, {})
        (output_dir / f"{i:0{width}d}.json").write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "utf-8",
        )

    # Remove individual pair files (now in shards).
    for fp in sorted(output_dir.glob("*.json")):
        name = fp.name
        if name[: name.rfind(".")].isdigit():
            continue
        fp.unlink()

    nonempty = sum(1 for b in buckets.values() if b)
    total = sum(len(b) for b in buckets.values())
    print(f"Assembled {total} summaries → {n_buckets} shards ({nonempty} non-empty)")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", "-i", default="data/crimenet.json")
    ap.add_argument("--output", "-o", default="app/data/relationship_summaries")
    ap.add_argument("--buckets", "-b", type=int, default=DEFAULT_BUCKETS)
    ap.add_argument("--workers", "-w", type=int, default=DEFAULT_WORKERS)
    args = ap.parse_args()

    api_key = load_key()
    input_path = Path(args.input)
    output_dir = Path(args.output)
    n_buckets = args.buckets
    workers = args.workers

    data = json.loads(input_path.read_text("utf-8"))
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    by_name = {n["standard_name"]: n for n in nodes if n.get("standard_name")}

    # Collect unique undirected pairs with their edges.
    pair_edges = defaultdict(list)
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if not s or not t or s == t:
            continue
        pair_edges[pair_key(s, t)].append(e)

    total_pairs = len(pair_edges)
    print(f"Found {total_pairs} unique pairs from {len(edges)} edges")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Resumability: skip pairs that already have a pair file or are in a shard.
    existing = set()
    for fp in sorted(output_dir.glob("*.json")):
        name = fp.name
        # Shard files are exactly "NNN.json" (all digits, then .json).
        if name[: name.rfind(".")].isdigit():
            try:
                for k in json.loads(fp.read_text("utf-8")):
                    existing.add(k)
            except Exception:
                pass
        else:
            # Individual pair file from an interrupted run.
            existing.add(filename_to_key(name))

    to_process = [(k, v) for k, v in pair_edges.items() if k not in existing]
    skipped = total_pairs - len(to_process)

    if skipped:
        print(f"  {skipped} pairs already summarized — will skip")
    if not to_process:
        print("All pairs already summarized. Nothing to do.")
        return

    print(f"  {len(to_process)} pairs to process with {workers} workers")

    # Phase 1: process pairs in parallel (each worker writes its own file).
    done, fail = 0, 0
    total = len(to_process)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                process_pair,
                k, v, api_key, by_name, output_dir,
                i + 1, total,
            ): k
            for i, (k, v) in enumerate(to_process)
        }
        for future in as_completed(futures):
            try:
                status = future.result()
                if status == "ok":
                    done += 1
                else:
                    fail += 1
            except Exception:
                fail += 1

            total_done = done + fail
            if total_done % 100 == 0 or total_done == total:
                print(f"  {total_done}/{total} | ok {done} | failed {fail}")

    # Phase 2: assemble pair files into shard buckets.
    assemble_shards(output_dir, n_buckets, set(pair_edges.keys()))


if __name__ == "__main__":
    main()
