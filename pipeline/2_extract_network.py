"""
2_extract_network.py

Extract criminal organizations and their relationships from Wikipedia
texts via DeepSeek. Outputs article_graph.json per folder in ./txts/<article>/.

Every extracted node and edge carries the source_url of the article it
came from, so the merged graph is fully traceable back to specific
Wikipedia revisions.

Runs folders in parallel.

Usage:
    python 2_extract_network.py --dir ../data/txts
"""

import json
import re
import time
import argparse
import requests

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.common import load_key, split_body_and_infobox, chunk_text, MAX_CHUNK_WORDS

API_URL         = "https://api.deepseek.com/chat/completions"
MODEL           = "deepseek-chat"
RETRIES         = 3
DELAY           = 0.3
MAX_TOKENS      = 8192    # first-attempt output cap (most chunks fit; cheaper)
MAX_TOKENS_RETRY = 16384  # escalate on a length-truncation retry — deepseek-chat
                          # really does generate up to 16384 (verified), so a
                          # dense chunk gets room rather than re-truncating
MAX_RL_RETRIES  = 6      # 429 backoff retries, separate from RETRIES


# ── Prompt ────────────────────────────────────────────────────────────────
# Knowledge-graph edge typology: three relationship types — cooperation,
# conflict, and a constrained "other" — kept deliberately coarse so the
# model can classify reliably from the source text without overreaching.

SYSTEM = """You are an expert in global organized crime. Extract criminal organizations and the relationships between them from the provided text.

══ NODES ══

Extract every named criminal organization in the text.

Node format:
{
  "standard_name": "Most recognized international name",
  "original_text_name": "Exactly as written in the text",
  "aliases": ["other names", "abbreviations"],
  "context": "Informative description of the organization: what it is, what it does, where it operates.",
  "time_period": "When the org was active — ONLY as stated in the text (see TIME PERIOD RULES). null if no dates are given."
}

══ EDGES ══

Extract relationships between pairs of organizations.

Edge format — fill the fields IN THIS ORDER (the order is the reasoning):
{
  "source": "standard_name of org A",
  "target": "standard_name of org B",
  "evidence_quote": "Verbatim snippet that states a tie between these two orgs. See EVIDENCE QUOTE RULES below.",
  "context": "In your own words, state the actual tie the text asserts between A and B (1-2 sentences).",
  "relationship": "exactly one of the canonical relationships below — chosen to fit the tie you just wrote in context",
  "time_period": "When this relationship held — ONLY as stated in the text (see TIME PERIOD RULES). null if no dates are given."
}

CANONICAL RELATIONSHIPS (use exactly one, lowercase):

  cooperation — The two orgs act WITH each other: joint operations, mutual
                support, alliances, pacts, protection, coordinated action,
                or commercial dealings between them (one buys from, sells
                to, supplies, or launders for the other).

  conflict    — The two orgs act AGAINST each other: fighting, war, clashes,
                killings, hits, raids, retaliation, or a rivalry the text
                frames as being between the two of them.

  other       — A real tie that is neither cooperative
                nor hostile: structural ties (A is a sub-unit,
                faction, wing, splinter, successor of, or merged into B), a
                truce or ceasefire between them, or a tie the text asserts but
                leaves unspecified ("linked to", "associated with").

CRITICAL EDGE RULES:
  - Work each edge in field order: (1) quote the supporting sentence, (2)
    describe the actual tie between the two orgs in your own words, (3) only
    then classify it. If step (2) cannot be written without negating or
    hedging the tie, emit NO edge for that pair.
  - Classify by the dominant character of the tie: with → cooperation,
    against → conflict, anything else real → other.
  - All three types are symmetric — emit one edge per pair.
  - If a relationship existed but is no longer current, still emit it and
    record the dates in time_period. A relationship that later turned
    hostile gets a separate conflict edge.
  - AN EDGE REQUIRES A RELATIONSHIP BETWEEN THE TWO ORGS THEMSELVES.
    Two orgs merely CO-OCCURRING — named in the same sentence, list,
    event, ranking, or enumeration, or each independently related to some
    outside party (a government, a country, a market, an event) — is NOT
    a relationship between them. If the only thing connecting A and B is
    that a third party mentioned, listed, classified, targeted, or
    affected both, emit NO edge.
  - Every edge MUST have a non-empty context.

══ TIME PERIOD RULES ══

time_period must be GROUNDED IN THE TEXT — never inferred, assumed, or filled
from outside knowledge. When in doubt, use null.

  • Record ONLY dates the text states for this org / this relationship. If the
    text gives no date, write null. Do not guess from context or prior knowledge.
  • NEVER write "present" / "ongoing" / "current" unless the text EXPLICITLY says
    it continues to now — e.g. "since 2004", "to this day", "remains active",
    "as of 2023". A start date alone is NOT "present": "founded in 2004" or
    "fighting since 2004 began" → "2004", never "2004-present", unless the text
    states the activity is still ongoing.
  • For an EDGE, the dates must describe THIS relationship between the two orgs,
    not the orgs' own lifespans. If nothing in the text dates the tie itself
    (even if founding years are given), write null.
  • Use the most specific stated form and keep it short: a year ("2012"), a
    range ("2006-2012"), a decade ("1990s"), or a phrase the text supports
    ("since the 1990s").

══ EVIDENCE QUOTE RULES ══

The evidence_quote field is the VERBATIM text from the source that
supports this specific edge. The dataset is meant to be auditable — a
reader should be able to take the quote, search for it in the source
Wikipedia article, and find it letter-for-letter. This matters more
than concise prose.

  • COPY EXACTLY: character-for-character from the source text. Same
    punctuation, capitalization, spelling, accents. Do not translate.
    Do not correct typos. Do not normalize quotation marks. Do not
    expand abbreviations. If the source has "PCC" with quotes, keep
    the quotes. If it uses British spelling, keep British spelling.

  • PICK THE LOAD-BEARING SENTENCE. Pick the SINGLE sentence (or two
    consecutive sentences, if a single one is insufficient) that most
    directly establishes the relationship. Prefer the most specific
    statement over a generic one. If the article says "PCC has been
    allied with 'Ndrangheta since 2014 to traffic cocaine to Europe",
    that one sentence is the quote — not a multi-paragraph reconstruction.

  • DO NOT STITCH. If the relationship is supported by sentences in
    different paragraphs, pick the most specific one and emit just that.
    Never concatenate fragments from non-adjacent locations.

  • DO NOT PARAPHRASE OR TRIM. evidence_quote is NOT a place to
    summarize. The context field is for that. The quote is verbatim
    text; the context is your interpretation of it.

  • LENGTH: keep under 50 words. If the only supporting span is longer,
    truncate at sentence boundaries with "…" rather than mid-word.

  • INFOBOX-DERIVED EDGES: when emitting an edge from an infobox field
    (Allies, Rivals, Sub-groups, etc.), the quote is the infobox row
    itself. Example: evidence_quote: "Allies: 'Ndrangheta, Hezbollah".

  • IF YOU CAN'T FIND A CLEAR QUOTE: set evidence_quote to null. Better
    null than fabricated. This should be rare.

══ INFOBOX ══

Some passages include an "--- INFOBOX ---" block — the article's structured
summary of its subject org. Treat it as just another source of relationships,
the same as body text (no special authority over the body).

  • For each org listed under these labels, emit one edge from the article's
    subject org to that listed org:
        Allies / Aliados / Alleati / Allied with        → cooperation
        Rivals / Rivais / Rivali / Enemies / Opponents  → conflict
        Any structural label (Sub-groups / Chapters / Branches / Divisions /
          Factions / Political wing / Support / Puppet clubs / Splinters /
          Armed wing / Successor / Predecessor / Founded by)  → other
  • Infobox names may be ABBREVIATIONS or LOCAL FORMS (e.g. "CV", "PCC",
    "TCP"). Resolve them to canonical standard_names using context.
  • The evidence_quote for an infobox-derived edge is the infobox row itself
    (e.g. "Allies: 'Ndrangheta, Hezbollah").

══ RULES ══

1. ALL output text in English. Organization names may stay in their original language if internationally known ('Ndrangheta, Yamaguchi-gumi, Primeiro Comando da Capital).
2. ONLY organizations as nodes. No individuals, places, events, government agencies, or law enforcement.
3. STANDARDIZE names: most recognized international name as standard_name, all variants in aliases.
4. Every edge MUST have a non-empty context that AFFIRMS the relationship. If you cannot describe an actual tie between the two orgs without hedging or negating it, do not emit the edge.
5. Do NOT invent. Only extract relationships the text actually asserts between the two orgs. Shared context, co-mention, or a common external cause is not a relationship.
6. Return ONLY valid JSON: {"nodes": [...], "edges": [...]}
7. If nothing relevant found: {"nodes": [], "edges": []}
8. Keep context fields concise (1-2 sentences, under 30 words)."""


# ── Chunking ──────────────────────────────────────────────────────────────
# split_body_and_infobox() and chunk_text() now live in lib/common.py so step
# 2 and step 3 chunk identically.

LEAD_PARAGRAPHS = 2


def lead_paragraphs(text, n=LEAD_PARAGRAPHS):
    """First n non-empty paragraphs — passed to every chunk as context so
    the model knows the article's subject org (mirrors step 3 pass 2)."""
    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    return "\n\n".join(paras[:n])


# ── API ───────────────────────────────────────────────────────────────────

def _post_with_rate_limit(headers, payload):
    """POST to DeepSeek, retrying 429s on their own backoff budget so a burst
    of rate-limit responses doesn't burn the content-retry attempts. Returns
    the final Response (the caller calls raise_for_status / parses it)."""
    r = None
    for rl in range(MAX_RL_RETRIES):
        r = requests.post(API_URL, headers=headers, json=payload, timeout=180)
        if r.status_code == 429:
            time.sleep(min(60, 5 * (2 ** rl)))
            continue
        return r
    return r  # give up on 429s; let the caller's raise_for_status surface it


def call_api(api_key, chunk, article, url, i, n, infobox="", lead=""):
    """Call DeepSeek for one chunk. Retries handle 429s, JSON errors, and
    truncation (escalates max_tokens, adds a terseness nudge)."""
    lead_block = ""
    if lead:
        lead_block = (
            f"--- ABOUT THIS ARTICLE'S SUBJECT (context only, identifies the "
            f"main org; extract relationships from the PASSAGE below) ---\n"
            f"{lead}\n--- END CONTEXT ---\n\n"
        )
    infobox_block = ""
    if infobox:
        infobox_block = (
            f"\n\n--- INFOBOX ---\n{infobox}\n--- END INFOBOX ---\n"
        )
    base_prompt = (
        f"ARTICLE: {article}\n"
        f"SOURCE: {url}\n"
        f"SECTION: {i}/{n}\n\n"
        f"{lead_block}"
        f"Extract all criminal organizations and their relationships from this passage.\n\n"
        f"--- PASSAGE ---\n{chunk}\n--- END ---"
        f"{infobox_block}"
    )

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for attempt in range(1, RETRIES + 1):
        # First attempt uses the smaller cap; on a length-truncation retry give
        # the model the higher ceiling AND nudge for terser output, so a dense
        # chunk fits instead of truncating again.
        max_tokens = MAX_TOKENS if attempt == 1 else MAX_TOKENS_RETRY
        prompt = base_prompt
        if attempt > 1:
            prompt += ("\n\nIMPORTANT: a previous attempt was truncated. "
                       "Keep all 'context' fields under 25 words. Be concise.")

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
            r = _post_with_rate_limit(headers, payload)
            r.raise_for_status()
            choice = r.json()["choices"][0]
            if choice.get("finish_reason") == "length" and attempt < RETRIES:
                time.sleep(2)
                continue
            raw = choice["message"]["content"].strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            result = json.loads(raw)
            result.setdefault("nodes", [])
            result.setdefault("edges", [])
            return result
        except (json.JSONDecodeError, requests.exceptions.RequestException,
                KeyError, TypeError, IndexError):
            time.sleep(3 * attempt)

    return None


# ── Process one folder ────────────────────────────────────────────────────

def tag_source(items, url):
    """Stamp every node/edge with the article's source URL."""
    for item in items:
        item["source_url"] = url
    return items


def extract_chunk(api_key, chunk, name, url, idx, total, infobox, lead, max_words):
    """Extract one chunk; if the call fails, re-chunk it smaller and recurse.

    A few articles (e.g. the Italian 'Ndrangheta page) have passages so dense
    with orgs and feuds that the JSON output overruns even the top token cap and
    truncates no matter what. Halving such a chunk lets each part fit. Normal
    chunks succeed on the first call and never split, so this adds no cost for
    the 99.9% that already fit. Returns (nodes, edges, ok)."""
    result = call_api(api_key, chunk, name, url, idx, total, infobox, lead)
    time.sleep(DELAY)
    if result:
        return result.get("nodes", []), result.get("edges", []), True
    # Failed (usually persistent truncation). Sub-split if still worth halving.
    if max_words <= 400 or len(chunk.split()) <= 400:
        return [], [], False
    sub_max = max(400, max_words // 2)
    subs = chunk_text(chunk, max_words=sub_max)
    if len(subs) <= 1:
        return [], [], False  # couldn't divide further
    print(f"  [{name}] sub-splitting a truncating chunk → {len(subs)} part(s) "
          f"(≤{sub_max} words)")
    nodes, edges, ok_all = [], [], True
    for j, sub in enumerate(subs):
        sub_ib = infobox if (idx == 1 and j == 0) else ""
        n2, e2, ok = extract_chunk(api_key, sub, name, url, idx, total,
                                   sub_ib, lead, sub_max)
        nodes += n2; edges += e2; ok_all = ok_all and ok
    return nodes, edges, ok_all


def process_folder(folder, api_key, idx, total):
    """Process a single folder. Returns 'done', 'partial', or 'fail'."""
    name = folder.name
    out = folder / "article_graph.json"
    content_path = folder / "content.txt"
    url_path = folder / "url.txt"

    if not content_path.exists():
        print(f"[{idx}/{total}] {name} — no content.txt")
        return "fail"

    text = content_path.read_text("utf-8").strip()
    url = url_path.read_text("utf-8").strip() if url_path.exists() else ""

    if not text:
        out.write_text(json.dumps({"nodes": [], "edges": [], "source_url": url,
                                   "source_file": name},
                                  ensure_ascii=False, indent=2), "utf-8")
        print(f"[{idx}/{total}] {name} — empty")
        return "done"

    body, infobox = split_body_and_infobox(text)
    chunks = chunk_text(body)
    lead = lead_paragraphs(body)
    n_chunks = len(chunks)
    print(f"[{idx}/{total}] {name}: {len(body.split())} words → {n_chunks} chunk(s)"
          f"{' + infobox' if infobox else ''}")

    nodes, edges = [], []
    failed = 0
    for i, chunk in enumerate(chunks, 1):
        # The infobox goes to the FIRST chunk only. Its edges (Allies / Rivals
        # / structural rows) are deterministic at temperature 0, so re-sending
        # it with every chunk just re-extracts identical edges that collapse in
        # step 4 — pure token cost, worst on the long, many-chunk articles.
        chunk_infobox = infobox if i == 1 else ""
        nlist, elist, ok = extract_chunk(api_key, chunk, name.replace("_", " "),
                                         url, i, n_chunks, chunk_infobox, lead,
                                         MAX_CHUNK_WORDS)
        nodes.extend(tag_source(nlist, url))
        edges.extend(tag_source(elist, url))
        if not ok:
            failed += 1
            print(f"  [{name}] chunk {i}/{n_chunks} failed after retries")

    output = {
        "nodes": nodes,
        "edges": edges,
        "source_url": url,
        "source_file": name,
    }
    if failed:
        output["incomplete"] = True
        output["failed_chunks"] = failed
        output["total_chunks"] = n_chunks

    out.write_text(json.dumps(output, ensure_ascii=False, indent=2), "utf-8")

    if failed == 0:
        print(f"[{idx}/{total}] {name} ✓ {len(nodes)} nodes, {len(edges)} edges")
        return "done"
    if failed < n_chunks:
        print(f"[{idx}/{total}] {name} ⚠ {failed}/{n_chunks} chunks failed; "
              f"saved {len(nodes)} nodes, {len(edges)} edges")
        return "partial"
    print(f"[{idx}/{total}] {name} ✗ all {n_chunks} chunks failed")
    return "fail"


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", "-d", required=True)
    parser.add_argument("--force", "-f", action="store_true",
                        help="Re-extract everything")
    parser.add_argument("--force-failed", action="store_true",
                        help="Retry only folders with missing, broken, or partial article_graph.json")
    parser.add_argument("--workers", "-w", type=int, default=50)
    args = parser.parse_args()

    api_key = load_key()
    root = Path(args.dir)
    all_folders = sorted(f for f in root.iterdir() if f.is_dir())
    total = len(all_folders)

    to_process = []
    skip = 0
    for folder in all_folders:
        out = folder / "article_graph.json"
        if out.exists() and not args.force:
            if args.force_failed:
                # Re-run if file is broken, missing source_file, or marked incomplete
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

    print(f"Found {total} folders | {skip} skipped | "
          f"{len(to_process)} to process | {args.workers} workers")
    if not to_process:
        return

    done, partial, fail = 0, 0, 0
    folder_idx = {f: i + 1 for i, f in enumerate(all_folders)}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_folder, f, api_key, folder_idx[f], total): f
                   for f in to_process}
        for future in as_completed(futures):
            try:
                status = future.result()
                if status == "done":      done += 1
                elif status == "partial": partial += 1
                else:                     fail += 1
            except Exception as e:
                fail += 1
                print(f"  {futures[future].name} ✗ {e}")

    print("=" * 60)
    print(f"Done: {done} extracted, {partial} partial, {skip} skipped, {fail} failed")
    if partial:
        print(f"⚠ {partial} folder(s) incomplete. Re-run with --force-failed to retry.")


if __name__ == "__main__":
    main()