#!/usr/bin/env python3
"""Build community detection and LLM characterization for the browse page's
Communities tab and the Ask CRIMENET AI get_community tool.

Self-contained pipeline (reads only crimenet.json):
  1. Build cooperation graph from crimenet.json edges
  2. Run Infomap community detection (deterministic: sorted nodes, seed 42)
  3. Characterize each community via DeepSeek: title + full summary + short summary
  4. Cache by frozenset(org_names) gated by PROMPT_VERSION, resumable on interrupt
  5. Write compact app/data/communities.json

Usage:
    python build/build_communities_data.py \
        --input data/crimenet.json \
        --output app/data/communities.json \
        --workers 10

A re-run where the partition is unchanged (same crimenet.json, same PROMPT_VERSION)
hits the cache and makes zero API calls.  Bump PROMPT_VERSION in this file when the
prompt construction changes to force a full recharacterize.
"""

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import networkx as nx
import requests
from infomap import Infomap

# ── Constants ────────────────────────────────────────────────────────────────────

API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
MAX_TOKENS = 8192
MAX_TOKENS_RETRY = 16384
RETRIES = 5                 # content-level (parse errors, truncation)
MAX_RL_RETRIES = 8          # rate-limit / 5xx / network backoff
DEFAULT_WORKERS = 10
TEMPERATURE = 0.0
TOP_HUBS = 10

# Bump when the prompt construction changes so cached results are invalidated.
PROMPT_VERSION = 4

SYSTEM_PROMPT = (
    "You are profiling communities of criminal organizations from CRIMENET, a "
    "knowledge graph of global organized crime built from Wikipedia.\n\n"
    "Each community was detected by Infomap, an algorithm that identifies clusters "
    "of densely interconnected nodes. Organizations that cooperate frequently end up "
    "in the same community.\n\n"
    "You are given every organization in the community (name, description, country, "
    "and degree centrality) and every known cooperation tie between them, with a "
    "description of each tie.\n\n"
    "Write a concise profile of this community. What ties these organizations "
    "together? What is the nature of their cooperation? Produce:\n"
    '  \"title\": a short descriptive label (4-8 words).\n'
    '  \"summary\": a clear, informative paragraph (4-6 sentences) that profiles '
    "this community.\n"
    '  \"short_summary\": exactly one sentence (~20 words) capturing the essence '
    "of this community for quick scanning.\n\n"
    "Return a JSON object with {title, summary, short_summary}."
)

SHORT_SUMMARY_ONLY_PROMPT = (
    "You are looking at a community of criminal organizations from CRIMENET, a "
    "knowledge graph of global organized crime built from Wikipedia.\n\n"
    "The community already has a title and full summary. You only need to produce "
    "a one-sentence short summary.\n\n"
    "Given the existing title, full summary, and the organization list, produce:\n"
    '  \"short_summary\": exactly one sentence (~20 words) that captures the '
    "essence of this community for quick scanning.\n\n"
    "Return a JSON object with {short_summary}."
)


# ── Helpers ───────────────────────────────────────────────────────────────────────

def load_key():
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError(
            "DeepSeek API key not found. Set it with:\n"
            '    export DEEPSEEK_API_KEY="sk-..."'
        )
    return key.strip()


def real_title(t):
    """True for a usable characterization title (not None, empty, or '?')."""
    return isinstance(t, str) and t.strip() not in ("", "?")


def real_summary(s):
    """True for a usable summary (not None, empty)."""
    return isinstance(s, str) and s.strip() != ""


def real_short_summary(s):
    """True for a usable short summary (not None, empty)."""
    return isinstance(s, str) and s.strip() != ""


def load_cache(output_path, prompt_version):
    """Map frozenset(org_names) -> (title, summary, short_summary) from an existing
    communities.json, used only when prompt_version matches.

    Returns (full_cache, partial_cache) where:
      full_cache: entries with title + summary + short_summary (no API calls needed)
      partial_cache: entries with title + summary but NO short_summary (lighter prompt needed)

    Also handles the transition from old format (no prompt_version field):
    existing entries with t+m but no b are treated as partial cache.
    """
    if not output_path.exists():
        return {}, {}
    try:
        prev = json.loads(output_path.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}
    # Only use cache if prompt_version matches OR if we're upgrading from
    # the old format (no prompt_version). In the latter case, we keep existing
    # t+m as partial cache and only need short_summary.
    has_prompt_version = "prompt_version" in prev
    if has_prompt_version and prev["prompt_version"] != prompt_version:
        return {}, {}
    full = {}
    partial = {}
    for info in (prev.get("communities") or []):
        orgs_key = frozenset(info.get("o") or [])
        if not orgs_key:
            continue
        has_title = real_title(info.get("t"))
        has_summary = real_summary(info.get("m"))
        has_short = real_short_summary(info.get("b"))
        if has_title and has_summary and has_short:
            full[orgs_key] = (info["t"], info["m"], info["b"])
        elif has_title and has_summary and not has_short:
            partial[orgs_key] = (info["t"], info["m"])
    return full, partial


# ── API client ────────────────────────────────────────────────────────────────────

def call_llm(api_key, user_prompt, system_prompt=None):
    """One DeepSeek chat call returning parsed JSON dict, or None on failure."""
    sp = system_prompt or SYSTEM_PROMPT
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for rl in range(MAX_RL_RETRIES):
        for attempt in range(1, RETRIES + 1):
            max_tok = MAX_TOKENS if attempt == 1 else MAX_TOKENS_RETRY
            prompt = user_prompt
            if attempt > 1:
                prompt += (
                    "\n\nIMPORTANT: a previous attempt failed or was truncated. "
                    "Return STRICT JSON {\"title\": ..., \"summary\": ..., "
                    "\"short_summary\": ...} and keep the summary to 4-6 sentences."
                )

            payload = {
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": sp},
                    {"role": "user", "content": prompt},
                ],
                "response_format": {"type": "json_object"},
                "temperature": TEMPERATURE,
                "max_tokens": max_tok,
            }

            try:
                r = requests.post(API_URL, headers=headers, json=payload, timeout=180)
            except requests.RequestException:
                time.sleep(min(60, 5 * (2 ** rl)))
                continue

            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(60, 5 * (2 ** rl)))
                continue

            if r.status_code != 200:
                time.sleep(3 * attempt)
                continue

            try:
                body = r.json()
                choice = body["choices"][0]
                if choice.get("finish_reason") == "length" and attempt < RETRIES:
                    time.sleep(2)
                    continue
                raw = choice["message"]["content"].strip()
                raw = re.sub(r"^```(?:json)?\s*", "", raw)
                raw = re.sub(r"\s*```$", "", raw)
                return json.loads(raw)
            except (json.JSONDecodeError, KeyError, TypeError, IndexError):
                time.sleep(3 * attempt)
                continue

        return None

    return None


# ── Prompt construction ───────────────────────────────────────────────────────────

def build_prompt(cid, orgs, internal_edges):
    """Assemble the user prompt for one community."""
    lines = [
        f"Community {cid} — {len(orgs)} organizations, "
        f"{len(internal_edges)} cooperation ties",
        "\nOrganizations:",
    ]
    for org in orgs:
        desc = org.get("description") or "(no description)"
        lines.append(
            f"  - {org['name']} (degree={org['degree']}, "
            f"country={org.get('country', '?')}): {desc}"
        )
    if internal_edges:
        lines.append("\nCooperation ties within this community:")
        for u, v, desc in internal_edges:
            if desc:
                lines.append(f"  {u} — {v}: {desc}")
            else:
                lines.append(f"  {u} — {v}")
    return "\n".join(lines)


def build_short_summary_only_prompt(cid, title, summary, org_names, orgs):
    """Assemble a lighter user prompt that only asks for short_summary."""
    lines = [
        f"Community {cid}",
        f"Title: {title}",
        f"Summary: {summary}",
        f"\nOrganizations ({len(org_names)} total):",
    ]
    for org in orgs[:30]:  # top 30 is enough for a short-summary-only call
        desc = org.get("description") or ""
        line = f"  - {org['name']} ({org.get('country', '?')})"
        if desc:
            line += f": {desc}"
        lines.append(line)
    if len(orgs) > 30:
        lines.append(f"  ... and {len(orgs) - 30} more organizations")
    lines.append(
        "\nProduce a one-sentence short summary (~20 words) for this community."
    )
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="data/crimenet.json", type=Path,
                    help="Path to crimenet.json (default: data/crimenet.json)")
    ap.add_argument("--output", default="app/data/communities.json", type=Path,
                    help="Output path (default: app/data/communities.json)")
    ap.add_argument("--workers", "-w", type=int, default=DEFAULT_WORKERS,
                    help=f"Parallel DeepSeek workers (default: {DEFAULT_WORKERS})")
    ap.add_argument("--force", "-f", action="store_true",
                    help="Ignore cache, recharacterize all communities from scratch")
    args = ap.parse_args()

    # ── 1. Load crimenet.json ─────────────────────────────────────────────────────
    print(f"Loading {args.input} ...")
    data = json.loads(args.input.read_text(encoding="utf-8"))
    nodes = data["nodes"]
    edges = data["edges"]

    # Build org lookup
    org_index = {}
    for n in nodes:
        org_index[n["standard_name"]] = n

    # ── 2. Build cooperation graph ─────────────────────────────────────────────────
    print("Building cooperation graph ...")
    G = nx.Graph()
    for n in nodes:
        G.add_node(n["standard_name"])

    coop_pairs = set()
    for e in edges:
        if e.get("relationship") != "cooperation":
            continue
        s, t = e["source"], e["target"]
        if not s or not t:
            continue
        pair = tuple(sorted([s, t]))
        coop_pairs.add(pair)

    for a, b in coop_pairs:
        G.add_edge(a, b)

    # Annotate nodes
    for name in G.nodes:
        n = org_index.get(name, {})
        G.nodes[name].update({
            "country": n.get("country") or "",
            "description": n.get("description") or "",
            "is_defunct": n.get("is_defunct", "unknown"),
        })

    print(f"  {G.number_of_nodes():,} nodes, {G.number_of_edges():,} cooperation edges")

    # ── 3. Infomap community detection ─────────────────────────────────────────────
    print("Running Infomap ...")
    name_list = sorted(G.nodes())
    name2id = {name: i for i, name in enumerate(name_list)}

    im = Infomap("--two-level --silent --seed 42")
    for a, b in coop_pairs:
        if a in name2id and b in name2id:
            im.add_link(name2id[a], name2id[b])
    im.run()

    # Extract communities
    communities: dict = {}
    for node in im.tree:
        if node.is_leaf:
            communities.setdefault(node.module_id, []).append(
                name_list[node.node_id]
            )

    # Sort by size descending, assign stable ids
    sorted_cids = sorted(communities, key=lambda c: len(communities[c]), reverse=True)
    cid2nodes = {}
    for new_id, old_cid in enumerate(sorted_cids, 1):
        cid2nodes[new_id] = communities[old_cid]

    sizes = sorted((len(v) for v in cid2nodes.values()), reverse=True)
    print(f"  {len(cid2nodes)} communities, size range {sizes[-1]}–{sizes[0]}, "
          f"mean {sum(sizes)/len(sizes):.1f}")
    singletons = sum(1 for s in sizes if s == 1)
    print(f"  Singletons: {singletons}")

    # ── 4. Collect per-community evidence ──────────────────────────────────────────
    print("Collecting per-community evidence ...")

    # All unique cooperation descriptions per undirected pair
    edge_descs: dict = {}
    for e in edges:
        if e.get("relationship") != "cooperation":
            continue
        s, t = e.get("source"), e.get("target")
        if not s or not t:
            continue
        key = tuple(sorted([s, t]))
        descs = e.get("descriptions") or []
        edge_descs.setdefault(key, [])
        for d in descs:
            if d not in edge_descs[key]:
                edge_descs[key].append(d)

    all_orgs: dict = {}
    internal_edges: dict = {}
    for cid in cid2nodes:
        names = cid2nodes[cid]
        sub = G.subgraph(names)
        ranked = sorted(sub.degree(), key=lambda x: (-x[1], x[0]))
        orgs = []
        for name, deg in ranked:
            node = G.nodes[name]
            orgs.append({
                "name": name,
                "degree": deg,
                "country": node.get("country", ""),
                "description": node.get("description", ""),
            })
        all_orgs[cid] = orgs

        nameset = set(names)
        edges_list = []
        for u, v in G.edges():
            if u in nameset and v in nameset:
                key = tuple(sorted([u, v]))
                descs = edge_descs.get(key, [])
                if descs:
                    for d in descs:
                        edges_list.append((u, v, d))
                else:
                    edges_list.append((u, v, ""))
        internal_edges[cid] = edges_list

    # ── 5. Load cache ─────────────────────────────────────────────────────────────
    full_cache, partial_cache = ({}, {}) if args.force else load_cache(args.output, PROMPT_VERSION)

    # Build communities list and classify each community
    communities_list = []
    need_full = []       # needs title + summary + short_summary (full prompt)
    need_short_only = []  # has title + summary, just needs short_summary
    full_cached = 0
    partial_cached = 0

    for cid in sorted(cid2nodes):
        names = cid2nodes[cid]
        key = frozenset(names)

        # Check full cache first
        if key in full_cache:
            title, summary, short_summary = full_cache[key]
            full_cached += 1
        elif key in partial_cache:
            title, summary = partial_cache[key]
            short_summary = None
            need_short_only.append(cid)
            partial_cached += 1
        else:
            title = summary = short_summary = None
            need_full.append(cid)

        communities_list.append({
            "i": cid,
            "s": len(names),
            "t": title,
            "m": summary,
            "b": short_summary,
            "k": [o["name"] for o in all_orgs[cid][:TOP_HUBS]],
            "o": names,
        })

    def flush():
        out = {
            "network_type": "cooperation",
            "prompt_version": PROMPT_VERSION,
            "communities": communities_list,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(out, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )

    flush()
    total_cached = full_cached + partial_cached
    print(f"\nCharacterization: {full_cached} fully cached, {partial_cached} need "
          f"short_summary only, {len(need_full)} need full characterization "
          f"(PROMPT_VERSION={PROMPT_VERSION})")

    # ── 6. Characterize communities that need it ───────────────────────────────────
    total_api_calls = len(need_full) + len(need_short_only)
    if total_api_calls == 0:
        print("  All communities fully cached. Zero API calls needed.")
        flush()
        total_orgs = sum(len(c["o"]) for c in communities_list)
        print(f"\nWrote {len(communities_list)} communities, {total_orgs} org assignments "
              f"({args.output.stat().st_size:,} bytes) -> {args.output}")
        return
    else:
        print(f"  {total_api_calls} total API calls needed.")
        api_key = load_key()

    def characterize_batch(todo, workers, label):
        if not todo:
            return []
        print(f"  {label}: {len(todo)} community(ies), {workers} worker(s)")
        still = []
        n = len(todo)
        done = 0

        def do(cid):
            return cid, call_llm(api_key, build_prompt(
                cid, all_orgs[cid], internal_edges.get(cid, [])
            ))

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(do, cid): cid for cid in todo}
            for fut in as_completed(futs):
                cid = futs[fut]
                done += 1
                try:
                    _, res = fut.result()
                except Exception:
                    res = None
                if res and real_title(res.get("title")) and real_short_summary(res.get("short_summary")):
                    entry = communities_list[
                        next(i for i, c in enumerate(communities_list) if c["i"] == cid)
                    ]
                    entry["t"] = res["title"].strip()
                    entry["m"] = (res.get("summary") or "").strip()
                    entry["b"] = (res.get("short_summary") or "").strip()
                    flush()
                    print(f"    [{done}/{n}] community {cid} "
                          f"(size {entry['s']}): {entry['t']}")
                else:
                    still.append(cid)
                    print(f"    [{done}/{n}] community {cid}: FAILED (will retry)")
        return still

    def characterize_short_only(todo, workers, label):
        """Lightweight calls that only need short_summary (title + summary already cached)."""
        if not todo:
            return []
        print(f"  {label}: {len(todo)} community(ies), {workers} worker(s)")
        still = []
        n = len(todo)
        done = 0

        # Find the cached title and summary for each
        cached_info = {}
        for cid in todo:
            entry = communities_list[next(i for i, c in enumerate(communities_list) if c["i"] == cid)]
            cached_info[cid] = (entry["t"], entry["m"])

        def do_short(cid):
            title, summary = cached_info[cid]
            prompt = build_short_summary_only_prompt(
                cid, title, summary,
                cid2nodes[cid], all_orgs[cid]
            )
            return cid, call_llm(api_key, prompt, system_prompt=SHORT_SUMMARY_ONLY_PROMPT)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(do_short, cid): cid for cid in todo}
            for fut in as_completed(futs):
                cid = futs[fut]
                done += 1
                try:
                    _, res = fut.result()
                except Exception:
                    res = None
                if res and real_short_summary(res.get("short_summary")):
                    entry = communities_list[
                        next(i for i, c in enumerate(communities_list) if c["i"] == cid)
                    ]
                    entry["b"] = res["short_summary"].strip()
                    flush()
                    print(f"    [{done}/{n}] community {cid} "
                          f"(size {entry['s']}): short_summary added")
                else:
                    still.append(cid)
                    print(f"    [{done}/{n}] community {cid}: FAILED short_summary (will retry)")
        return still

    # Phase A: full characterization for communities that need everything
    remaining_full = characterize_batch(need_full, max(1, args.workers),
                                         "phase A (full characterization)")
    if remaining_full:
        remaining_full = characterize_batch(remaining_full, 1,
                                            "phase A retry (sequential)")

    # Phase B: short_summary only for communities that already have title + summary
    remaining_short = characterize_short_only(need_short_only, max(1, args.workers),
                                              "phase B (short_summary only)")
    if remaining_short:
        remaining_short = characterize_short_only(remaining_short, 1,
                                                  "phase B retry (sequential)")

    # Report
    remaining = (remaining_full or []) + (remaining_short or [])
    if remaining:
        print(f"  WARNING: {len(remaining)} community(ies) still uncharacterized "
              f"after retries: {remaining}")
        print(f"  Re-run the same command to retry only these "
              f"(everything else is cached).")
    else:
        print("  All communities characterized.")

    flush()
    total_orgs = sum(len(c["o"]) for c in communities_list)
    print(f"\nWrote {len(communities_list)} communities, {total_orgs} org assignments "
          f"({args.output.stat().st_size:,} bytes) -> {args.output}")


if __name__ == "__main__":
    main()
