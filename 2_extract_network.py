"""
extract_network.py
Extract criminal organizations and relationships from Wikipedia texts via DeepSeek.
Outputs extracted.json per folder in ./txts/<article>/

Runs up to 50 folders in parallel (DeepSeek has no rate limit).

Usage:
    python 2_extract_network.py --dir ./txts
    python 2_extract_network.py --dir ./txts --force
    python 2_extract_network.py --dir ./txts --force-failed
    python 2_extract_network.py --dir ./txts --workers 20
"""

import json
import re
import time
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────

API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
MAX_CHUNK_WORDS = 3000
RETRIES = 3
DELAY = 0.3

# ── Prompt ─────────────────────────────────────────────────────────────────

SYSTEM = """You are an expert in global organized crime. Extract criminal organizations and their relationships from the provided text.

══ NODES ══

Extract every named criminal entity: cartels, mafias, gangs, triads, crime families, motorcycle clubs, syndicates, militias, terrorist groups, factions, clans, crews — any organized criminal group.

Node format:
{
  "standard_name": "Most recognized international name",
  "original_text_name": "Exactly as written in the text",
  "aliases": ["other names", "abbreviations"],
  "type": "One of: cartel, crime_family, gang, mafia, triad, motorcycle_club, militia, terrorist_organization, faction, clan, crew, crime_syndicate, organized_crime_group, criminal_organization",
  "context": "1-2 sentences: what they do, where they operate.",
  "time_period": "When active, e.g. '1980s-present', 'founded 1969', '1990s-2010'. null if unknown."
}

══ EDGES ══

Extract relationships between pairs of organizations.

Edge format:
{
  "source": "standard_name of org A",
  "target": "standard_name of org B",
  "relationship": "alliance | rivalry | other",
  "detail": "For 'other': specify what — e.g. splinter, armed_wing, successor, merger, faction_of, founded_by_members_of, evolved_into, reformation. For alliance/rivalry: null.",
  "context": "Explain the relationship in 1-2 sentences.",
  "time_period": "When this relationship held, e.g. '2006-2012', 'since 1990s'. null if unknown."
}

══ RULES ══

1. ALL text output in English. Org names may stay in original language if internationally known ('Ndrangheta, Yamaguchi-gumi, Primeiro Comando da Capital).
2. ONLY organizations as nodes. No individuals, places, or events.
3. STANDARDIZE names: most recognized name as standard_name, variants in aliases.
4. Every edge MUST have a context. Never empty.
5. Do NOT invent. Only extract what the text states or strongly implies.
6. Return ONLY valid JSON: {"nodes": [...], "edges": [...]}
7. If nothing relevant found: {"nodes": [], "edges": []}"""


# ── Chunking ───────────────────────────────────────────────────────────────

def chunk_text(text: str, max_words: int = MAX_CHUNK_WORDS) -> List[str]:
    paragraphs = re.split(r"\n{2,}", text)
    chunks, buf, wc = [], [], 0
    for p in paragraphs:
        p = p.strip()
        if not p:
            continue
        pw = len(p.split())
        if pw > max_words:
            if buf:
                chunks.append("\n\n".join(buf)); buf, wc = [], 0
            chunks.append(p)
            continue
        if wc + pw > max_words and buf:
            chunks.append("\n\n".join(buf)); buf, wc = [], 0
        buf.append(p); wc += pw
    if buf:
        chunks.append("\n\n".join(buf))
    return chunks


# ── API ────────────────────────────────────────────────────────────────────

def load_key(path="deepseek_api_key.txt"):
    return Path(path).read_text("utf-8").strip()


def call_api(api_key: str, chunk: str, article: str, url: str, i: int, n: int, page_summary: str = "") -> Optional[Dict]:
    summary_block = ""
    if page_summary and n > 1:
        summary_block = f"PAGE CONTEXT (for reference — extract from the SECTION below, not this summary):\n{page_summary}\n\n"

    prompt = (
        f"ARTICLE: {article}\n"
        f"SOURCE: {url}\n"
        f"SECTION: {i}/{n}\n\n"
        f"{summary_block}"
        f"Extract all criminal organizations and their relationships from this text.\n\n"
        f"--- TEXT ---\n{chunk}\n--- END ---"
    )
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.0,
        "max_tokens": 8192,
    }

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(API_URL, headers=headers, json=payload, timeout=120)
            if r.status_code == 429:
                wait = 5 * attempt * 2
                log.warning(f"  [{article}] Rate limited, waiting {wait}s")
                time.sleep(wait); continue
            r.raise_for_status()
            raw = r.json()["choices"][0]["message"]["content"].strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            result = json.loads(raw.strip())
            result.setdefault("nodes", [])
            result.setdefault("edges", [])
            return result
        except json.JSONDecodeError as e:
            log.warning(f"  [{article}] JSON error (attempt {attempt}): {e}")
            time.sleep(3)
        except requests.exceptions.RequestException as e:
            log.warning(f"  [{article}] Request error (attempt {attempt}): {e}")
            time.sleep(5 * attempt)
        except (KeyError, TypeError, IndexError) as e:
            log.warning(f"  [{article}] Parse error (attempt {attempt}): {e}")
            time.sleep(3)
    return None


# ── Merge chunks ───────────────────────────────────────────────────────────

def merge_chunks(results: List[Dict]) -> Dict:
    nodes, edges = [], []
    seen_nodes = set()

    for r in results:
        for node in r.get("nodes", []):
            key = node.get("standard_name", "").strip().lower()
            if not key:
                continue
            if key not in seen_nodes:
                seen_nodes.add(key)
                nodes.append(node)
            else:
                for existing in nodes:
                    if existing.get("standard_name", "").strip().lower() == key:
                        existing["aliases"] = list(
                            set(existing.get("aliases") or [])
                            | set(node.get("aliases") or [])
                        )
                        break
        edges.extend(r.get("edges", []))

    unique, seen_e = [], set()
    for e in edges:
        k = (
            (e.get("source") or "").strip().lower(),
            (e.get("target") or "").strip().lower(),
            (e.get("relationship") or "").strip().lower(),
            (e.get("detail") or "").strip().lower(),
        )
        if k not in seen_e:
            seen_e.add(k)
            unique.append(e)

    return {"nodes": nodes, "edges": unique}


# ── Process one folder (runs in a thread) ──────────────────────────────────

def process_folder(folder: Path, api_key: str, idx: int, total: int) -> str:
    """Process a single folder. Returns a status string: 'done' or 'fail'."""
    name = folder.name
    out = folder / "extracted.json"
    content = folder / "content.txt"
    url_file = folder / "url.txt"

    if not content.exists():
        log.warning(f"[{idx}/{total}] {name} — no content.txt")
        return "fail"

    text = content.read_text("utf-8").strip()
    if not text:
        # Empty file — save valid empty result
        out.write_text(json.dumps({"nodes": [], "edges": [], "source_file": name}, ensure_ascii=False, indent=2), "utf-8")
        log.info(f"[{idx}/{total}] {name} — empty, saved empty result")
        return "done"

    url = url_file.read_text("utf-8").strip() if url_file.exists() else ""
    chunks = chunk_text(text)
    n_chunks = len(chunks)
    log.info(f"[{idx}/{total}] {name}: {len(text.split())} words → {n_chunks} chunk(s)")

    # First ~300 words of the article = Wikipedia lead section = natural summary
    words = text.split()
    page_summary = " ".join(words[:300]) if len(words) > 300 else ""

    # Process chunks sequentially within this folder
    results = []
    for i, chunk in enumerate(chunks, 1):
        result = call_api(api_key, chunk, name.replace("_", " "), url, i, n_chunks, page_summary)
        if result:
            results.append(result)
        time.sleep(DELAY)

    merged = merge_chunks(results) if results else {"nodes": [], "edges": []}
    merged["source_file"] = name

    out.write_text(json.dumps(merged, ensure_ascii=False, indent=2), "utf-8")
    n_nodes = len(merged["nodes"])
    n_edges = len(merged["edges"])
    log.info(f"[{idx}/{total}] {name} ✓ {n_nodes} nodes, {n_edges} edges")
    return "done"


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extract criminal network via DeepSeek (parallel)")
    parser.add_argument("--dir", "-d", required=True)
    parser.add_argument("--force", "-f", action="store_true", help="Re-extract everything")
    parser.add_argument("--force-failed", action="store_true", help="Retry missing/broken only")
    parser.add_argument("--workers", "-w", type=int, default=50, help="Parallel workers (default 50)")
    args = parser.parse_args()

    api_key = load_key()
    root = Path(args.dir)
    all_folders = sorted(f for f in root.iterdir() if f.is_dir())
    total = len(all_folders)

    # Filter: decide which folders need processing
    to_process = []
    skip = 0
    for folder in all_folders:
        out = folder / "extracted.json"
        if out.exists() and not args.force:
            if args.force_failed:
                try:
                    d = json.loads(out.read_text("utf-8"))
                    if "source_file" in d:
                        skip += 1; continue
                except Exception:
                    pass  # Broken JSON → re-extract
            else:
                skip += 1; continue
        to_process.append(folder)

    log.info(f"Found {total} folders | {skip} skipped | {len(to_process)} to process | {args.workers} workers")

    if not to_process:
        log.info("Nothing to do.")
        return

    # Run in parallel
    done, fail = 0, 0
    folder_idx = {f: i + 1 for i, f in enumerate(all_folders)}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_folder, folder, api_key, folder_idx[folder], total): folder
            for folder in to_process
        }
        for future in as_completed(futures):
            try:
                status = future.result()
                if status == "done":
                    done += 1
                else:
                    fail += 1
            except Exception as e:
                fail += 1
                folder = futures[future]
                log.error(f"  {folder.name} ✗ {e}")

    log.info(f"{'='*50}")
    log.info(f"Done: {done} extracted, {skip} skipped, {fail} failed")


if __name__ == "__main__":
    main()