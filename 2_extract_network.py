"""
2_extract_network.py

Extract criminal organizations and relationships from Wikipedia texts via DeepSeek.
Outputs extracted.json per folder in ./txts/<article>/

Runs folders in parallel.

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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────

API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
MAX_CHUNK_WORDS = 2500   # was 3000; lowered to give max_tokens more room per chunk
RETRIES = 3
DELAY = 0.3
MAX_TOKENS = 8192        # initial budget; auto-doubles on retry up to 16384

# ── Prompt ─────────────────────────────────────────────────────────────────
#
# Schema constraints encoded directly here so the LLM produces output that's
# already canonical. Three things matter most:
#
#  1. NODE TYPES are one of 9 canonical values, or "other" if genuinely
#     ambiguous. Don't invent new types, don't return synonyms.
#  2. RELATIONSHIPS are exactly alliance / rivalry / other. If two orgs
#     "collaborate" or "cooperate", that's an alliance. If they "feud" or
#     "fight", that's a rivalry. Reserve "other" for structural relations
#     (parent-child, splinter, etc.).
#  3. DETAIL (for "other" edges) is from a fixed vocabulary.

SYSTEM = """You are an expert in global organized crime. Extract criminal organizations and the relationships between them from the provided text.

══ NODES ══

Extract every named criminal entity: cartels, mafias, gangs, triads, crime families, motorcycle clubs, militias, terrorist groups, factions, clans, crews — any organized criminal group named in the text.

Node format:
{
  "standard_name": "Most recognized international name",
  "original_text_name": "Exactly as written in the text",
  "aliases": ["other names", "abbreviations"],
  "type": "exactly one of the canonical types below",
  "context": "1-2 sentences: what they do, where they operate.",
  "time_period": "When active, e.g. '1980s-present', 'founded 1969', '1990s-2010'. null if unknown."
}

CANONICAL NODE TYPES (use exactly one, lowercase, with underscores where shown):

  cartel                  — drug-trafficking cartels (Sinaloa, Medellín, etc.)
  mafia                   — mafias and crime families (Cosa Nostra, 'Ndrangheta, all American Mafia families)
  gang                    — street gangs, prison gangs, biker-style crews that are NOT formal motorcycle clubs
  motorcycle_club         — outlaw motorcycle clubs (Hells Angels, Bandidos, Outlaws, Mongols, Pagans)
  triad                   — Chinese triads (14K, Sun Yee On, Wo Shing Wo)
  clan                    — Camorra clans, Albanian clans, Hungarian clans
  faction                 — splinter groups, armed wings, internal factions of larger orgs
  militia                 — paramilitary groups, death squads, vigilante groups, insurgents
  terrorist_organization  — designated terrorist groups (Al-Qaeda, IRA, ETA, Hezbollah)
  other                   — only when genuinely none of the above fits

CRITICAL NODE TYPE RULES:
  - Map yakuza groups to "mafia".
  - Map secret societies, cybercriminal groups, and hacker groups to "other".
  - Map street crews and small criminal cells to "gang".
  - Don't invent new types like "crime_syndicate", "organized_crime_group", or "criminal_organization" — pick the closest canonical type, or use "other".
  - NEVER use a relationship word as a node type. "alliance", "coalition", "federation", "rivalry", "war", "conflict" are NOT node types. If a group has "Alliance" in its name (e.g., "Wolfpack Alliance"), classify it by what it actually is (gang, motorcycle_club, etc.) — not by the word "alliance".

══ EDGES ══

Extract relationships between pairs of organizations.

Edge format:
{
  "source": "standard_name of org A",
  "target": "standard_name of org B",
  "relationship": "alliance | rivalry | other",
  "detail": "For 'other': specify the relationship in 1-2 words. For alliance/rivalry: strictly null.",
  "context": "Explain the relationship in 1-2 sentences.",
  "time_period": "When this relationship held, e.g. '2006-2012', 'since 1990s'. null if unknown."
}

RELATIONSHIP CLASSIFICATION — READ CAREFULLY:

  alliance — Any cooperation, partnership, mutual support, working together,
             hierarchy (sub-groups, sub-units, factions, chapters, support clubs,
             puppet clubs, branches, divisions), business collaboration, joint
             operation, formal pact, ceasefire that becomes cooperation, family/
             personal/blood ties between groups, drug-trafficking partnerships,
             smuggling cooperation, friendship, splinter groups (when the parent-
             splinter relationship is non-hostile), successor/predecessor groups,
             founded-by-members-of relationships, mergers.

             ALL of the following are alliance:
               • A is a sub-group of B
               • A is a chapter of B
               • A is a faction of B
               • A is a puppet/support club of B
               • A is the armed wing of B
               • A is a splinter of B (and they are not hostile)
               • A is the successor of B
               • A is the predecessor of B
               • A merged into B
               • A was founded by members of B
               • A and B are sister organizations

  rivalry  — Any conflict, hostility, fighting, war, feud, competition with
             violence, targeting, retaliation, killings, contract hits, alliance
             that turned into conflict, financial dispute that became hostile,
             being conquered or vanquished by the other group, hostile splinter
             (when A broke off from B and they fight).

  other    — VERY RARE. Use only when the relationship is genuinely neither
             cooperation nor conflict — for example, distant historical
             references with no ongoing connection. If you find yourself wanting
             to write "sub-group", "successor", "predecessor", "faction",
             "splinter", or "merger" as the detail, the relationship is
             alliance, NOT other. Do not use "other" for any structural
             relationship between organizations.

══ RULES ══

1. ALL output text in English. Organization names may stay in their original language if internationally known ('Ndrangheta, Yamaguchi-gumi, Primeiro Comando da Capital).
2. ONLY organizations as nodes. No individuals, places, events, government agencies, or law enforcement.
3. STANDARDIZE names: most recognized international name as standard_name, all variants in aliases.
4. Every edge MUST have a non-empty context.
5. Do NOT invent. Only extract what the text states or strongly implies.
6. Use canonical types and relationship values exactly as listed. No synonyms.
7. Return ONLY valid JSON: {"nodes": [...], "edges": [...]}
8. If nothing relevant found: {"nodes": [], "edges": []}
9. Keep context fields concise (1-2 sentences, under 30 words). This prevents truncation."""


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


def call_api(api_key: str, chunk: str, article: str, url: str,
             i: int, n: int, page_summary: str = "") -> Optional[Dict]:
    """Call DeepSeek for a single chunk. Retries handle 429s, JSON errors,
    and `finish_reason=length` truncation by escalating max_tokens.
    """
    summary_block = ""
    if page_summary and n > 1:
        summary_block = (
            f"PAGE CONTEXT (for reference — extract from the SECTION below, not this summary):\n"
            f"{page_summary}\n\n"
        )

    base_prompt = (
        f"ARTICLE: {article}\n"
        f"SOURCE: {url}\n"
        f"SECTION: {i}/{n}\n\n"
        f"{summary_block}"
        f"Extract all criminal organizations and their relationships from this text.\n\n"
        f"--- TEXT ---\n{chunk}\n--- END ---"
    )

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for attempt in range(1, RETRIES + 1):
        # Escalate max_tokens on retry (8192 → 16384) and add a terseness nudge
        # if the previous attempt got truncated.
        max_tokens = MAX_TOKENS if attempt == 1 else min(16384, MAX_TOKENS * 2)
        prompt = base_prompt
        if attempt > 1:
            prompt = base_prompt + (
                "\n\nIMPORTANT: A previous attempt was truncated. "
                "Keep all 'context' fields under 25 words. Be concise."
            )

        payload = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.0,
            "max_tokens": max_tokens,
        }

        try:
            r = requests.post(API_URL, headers=headers, json=payload, timeout=180)

            if r.status_code == 429:
                wait = 5 * attempt * 2
                log.warning(f"  [{article}] Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            response = r.json()
            choice = response["choices"][0]

            # Detect API-level truncation before attempting JSON parse.
            finish_reason = choice.get("finish_reason", "")
            if finish_reason == "length":
                log.warning(f"  [{article}] Response hit max_tokens "
                            f"(attempt {attempt}/{RETRIES}, max_tokens={max_tokens})")
                if attempt < RETRIES:
                    time.sleep(2)
                    continue
                # Last attempt: try to parse anyway, but it'll likely fail.

            raw = choice["message"]["content"].strip()
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
            key = (node.get("standard_name") or "").strip().lower()
            if not key:
                continue
            if key not in seen_nodes:
                seen_nodes.add(key)
                nodes.append(node)
            else:
                for existing in nodes:
                    if (existing.get("standard_name") or "").strip().lower() == key:
                        existing["aliases"] = sorted(
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
        if k not in seen_e and k[0] and k[1] and k[2]:
            seen_e.add(k)
            unique.append(e)

    return {"nodes": nodes, "edges": unique}


# ── Process one folder (runs in a thread) ──────────────────────────────────

def process_folder(folder: Path, api_key: str, idx: int, total: int) -> str:
    """Process a single folder. Returns 'done', 'partial', or 'fail'.

    'partial' means at least one chunk failed but at least one succeeded;
    the article's extracted.json is incomplete and the warning is logged.
    """
    name = folder.name
    out = folder / "extracted.json"
    content = folder / "content.txt"
    url_file = folder / "url.txt"

    if not content.exists():
        log.warning(f"[{idx}/{total}] {name} — no content.txt")
        return "fail"

    text = content.read_text("utf-8").strip()
    if not text:
        out.write_text(
            json.dumps({"nodes": [], "edges": [], "source_file": name},
                       ensure_ascii=False, indent=2),
            "utf-8")
        log.info(f"[{idx}/{total}] {name} — empty, saved empty result")
        return "done"

    url = url_file.read_text("utf-8").strip() if url_file.exists() else ""
    chunks = chunk_text(text)
    n_chunks = len(chunks)
    log.info(f"[{idx}/{total}] {name}: {len(text.split())} words → {n_chunks} chunk(s)")

    # First ~300 words = Wikipedia lead section, used as context for chunks 2+
    words = text.split()
    page_summary = " ".join(words[:300]) if len(words) > 300 else ""

    results = []
    failed_chunks = 0
    for i, chunk in enumerate(chunks, 1):
        result = call_api(api_key, chunk, name.replace("_", " "), url,
                          i, n_chunks, page_summary)
        if result:
            results.append(result)
        else:
            failed_chunks += 1
            log.warning(f"  [{name}] chunk {i}/{n_chunks} failed after retries")
        time.sleep(DELAY)

    merged = merge_chunks(results) if results else {"nodes": [], "edges": []}
    merged["source_file"] = name
    if failed_chunks:
        merged["incomplete"] = True
        merged["failed_chunks"] = failed_chunks
        merged["total_chunks"] = n_chunks

    out.write_text(json.dumps(merged, ensure_ascii=False, indent=2), "utf-8")
    n_nodes = len(merged["nodes"])
    n_edges = len(merged["edges"])

    if failed_chunks == 0:
        log.info(f"[{idx}/{total}] {name} ✓ {n_nodes} nodes, {n_edges} edges")
        return "done"
    elif failed_chunks < n_chunks:
        log.warning(f"[{idx}/{total}] {name} ⚠ INCOMPLETE — {failed_chunks}/{n_chunks} "
                    f"chunks failed; saved {n_nodes} nodes, {n_edges} edges from "
                    f"{n_chunks - failed_chunks} successful chunk(s)")
        return "partial"
    else:
        log.error(f"[{idx}/{total}] {name} ✗ ALL {n_chunks} chunks failed")
        return "fail"


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extract criminal network via DeepSeek (parallel)")
    parser.add_argument("--dir", "-d", required=True)
    parser.add_argument("--force", "-f", action="store_true", help="Re-extract everything")
    parser.add_argument("--force-failed", action="store_true",
                        help="Retry only folders with missing, broken, or partial extracted.json")
    parser.add_argument("--workers", "-w", type=int, default=50,
                        help="Parallel workers (default 50)")
    args = parser.parse_args()

    api_key = load_key()
    root = Path(args.dir)
    all_folders = sorted(f for f in root.iterdir() if f.is_dir())
    total = len(all_folders)

    # Decide which folders need processing  
    to_process = []
    skip = 0
    for folder in all_folders:
        out = folder / "extracted.json"
        if out.exists() and not args.force:
            if args.force_failed:
                # Re-process if file is broken JSON, missing source_file, or marked incomplete
                try:
                    d = json.loads(out.read_text("utf-8"))
                    if "source_file" in d and not d.get("incomplete"):
                        skip += 1
                        continue
                except Exception:
                    pass  # broken JSON → re-extract
            else:
                skip += 1
                continue
        to_process.append(folder)

    log.info(f"Found {total} folders | {skip} skipped | "
             f"{len(to_process)} to process | {args.workers} workers")

    if not to_process:
        log.info("Nothing to do.")
        return

    done, partial, fail = 0, 0, 0
    folder_idx = {f: i + 1 for i, f in enumerate(all_folders)}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_folder, folder, api_key,
                        folder_idx[folder], total): folder
            for folder in to_process
        }
        for future in as_completed(futures):
            try:
                status = future.result()
                if status == "done":
                    done += 1
                elif status == "partial":
                    partial += 1
                else:
                    fail += 1
            except Exception as e:
                fail += 1
                folder = futures[future]
                log.error(f"  {folder.name} ✗ {e}")

    log.info("=" * 60)
    log.info(f"Done: {done} extracted, {partial} partial, "
             f"{skip} skipped, {fail} failed")
    if partial:
        log.warning(f"⚠ {partial} folder(s) have incomplete extractions. "
                    f"Re-run with --force-failed to retry them.")


if __name__ == "__main__":
    main()