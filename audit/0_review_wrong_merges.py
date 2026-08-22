"""
0_review_wrong_merges.py

Catch WRONG merges, BEFORE you trust crimenet.json: two different orgs that
step 4 folded into one node because their names collided — like the neo-Nazi
"The Base" being absorbed into Al-Qaeda because "the base" is also an Al-Qaeda
alias. The mirror image of 1_review_missed_merges.py:

  • 0_review_wrong_merges.py  → BLOCKLIST       (merges step 4 made WRONGLY).
  • 1_review_missed_merges.py → KNOWN_DUPLICATES (merges step 4 SHOULD have made).

It reuses step 4's OWN matching machinery (build_maps / make_resolvers /
node_names), so what it sees is exactly what step 4 will do — not an
approximation. It builds the full merge map in memory (every name folded into
each canonical, tagged exact / alias / core with a ⚠ via-alias risk flag),
prints a one-line summary, then asks DeepSeek (shared judge.py machinery)
to judge the suspect merges (the alias/core ones) and writes the ONE file you
actually use:

  0_review_wrong_merges.py — a ready-to-paste BLOCKLIST of the merges the model thinks
  are wrong, ordered by canonical degree. The LLM only ADVISES; you review
  before pasting into curated_corrections.py.

Methods:
  exact  — folded name equals a profile canonical/alias (high confidence)
  alias  — matched via one of the node's OWN aliases (where cross-org name
           collisions show up — REVIEW these)
  core   — matched on significant-token core after stripping generic words
           (the fuzziest — REVIEW these too)

Usage:
    python 0_review_wrong_merges.py -w 50

Needs the DEEPSEEK_API_KEY environment variable set.
"""

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from collections import defaultdict

import judge


def load_step4(path):
    spec = importlib.util.spec_from_file_location("step4", path)
    mod = importlib.util.module_from_spec(spec)
    # step4 does `from lib.countries import normalize_country`; make sure
    # pipeline/ is importable regardless of cwd so lib.* resolves.
    step4_dir = str(Path(path).resolve().parent)
    if step4_dir not in sys.path:
        sys.path.insert(0, step4_dir)
    spec.loader.exec_module(mod)
    return mod


# ── Evidence gathering (txts/ side — specific to this tool) ──────────────────
#
# The canonical org's evidence comes from its own_profile.json; the merged-in
# name's evidence comes from the article it was extracted in (article_graph.json
# context). Both feed the shared SAME_ORG_SYSTEM judge.

def url_to_folder_index(txts_dir):
    """Map every article source_url → its folder, via each folder's url.txt."""
    idx = {}
    if not txts_dir.exists():
        return idx
    for folder in sorted(txts_dir.iterdir()):
        if not folder.is_dir():
            continue
        uf = folder / "url.txt"
        if uf.exists():
            try:
                u = uf.read_text("utf-8").strip()
                if u:
                    idx[u] = folder
            except Exception:
                pass
    return idx


def _load_profile(folder):
    p = folder / "org_profile.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return None


def _find_name_context(folder, name):
    """Pull the context the merged name was extracted with, from this folder's
    article_graph.json (match on standard_name or alias, then edges)."""
    p = folder / "article_graph.json"
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text("utf-8"))
    except Exception:
        return None
    target = judge.fold(name)
    for node in data.get("nodes", []):
        names = [node.get("standard_name", "")] + (node.get("aliases") or [])
        if any(judge.fold(n) == target for n in names):
            return {"context": node.get("context", ""),
                    "time_period": node.get("time_period", "")}
    for edge in data.get("edges", []):
        if judge.fold(edge.get("target")) == target or judge.fold(edge.get("source")) == target:
            if edge.get("context"):
                return {"context": edge["context"],
                        "time_period": edge.get("time_period", "")}
    return None


def _describe_canonical(entry, url_index):
    """Best available description + aliases for the canonical org."""
    for src in entry.get("own_sources", []):
        folder = url_index.get(src)
        if not folder:
            continue
        prof = _load_profile(folder)
        if not prof:
            continue
        node = (prof.get("nodes") or [{}])[0] if "nodes" in prof else prof
        return {
            "name": entry["canonical"],
            "description": node.get("description") or node.get("context") or "",
            "aliases": node.get("aliases") or [],
            "sources": entry.get("own_sources", []),
        }
    return {"name": entry["canonical"], "description": "", "aliases": [],
            "sources": entry.get("own_sources", [])}


def _describe_merged(name, from_urls, url_index):
    """Best available context for the merged-in name, from its source article(s)."""
    for u in from_urls:
        folder = url_index.get(u)
        if not folder:
            continue
        ctx = _find_name_context(folder, name)
        if ctx and ctx.get("context"):
            return {"name": name, "context": ctx["context"],
                    "time_period": ctx.get("time_period", ""), "source": u}
    return {"name": name, "context": "", "time_period": "",
            "source": from_urls[0] if from_urls else ""}


def _build_prompt(canon, merged):
    al = ", ".join(canon["aliases"][:12]) or "(none listed)"
    return (
        "The two records below were MERGED into one node because their names "
        "collided. Decide if they are the same organization.\n\n"
        f"RECORD A (the canonical org merged INTO):\n"
        f"  name: {canon['name']}\n"
        f"  description: {canon['description'] or '(no description available)'}\n"
        f"  known aliases: {al}\n\n"
        f"RECORD B (the name merged into A):\n"
        f"  name: {merged['name']}\n"
        f"  description: {merged['context'] or '(no description available)'}\n"
        f"  time period: {merged['time_period'] or '(unknown)'}\n\n"
        "Are RECORD A and RECORD B the same organization?"
    )


def run_llm_pass(canon_entries, txts, methods, workers, out_path, degree):
    """Judge the suspect merges in the in-memory canon_entries and write a
    BLOCKLIST suggestion file (shared renderer). Returns (n_wrong, n_review)."""
    url_index = url_to_folder_index(txts)

    # Build the suspect list from the in-memory model (no JSON reload).
    suspects = []   # (canon_desc, merged_desc, canonical_name, merged_name)
    for entry in canon_entries:
        canon_desc = _describe_canonical(entry, url_index)
        for mr in entry.get("merged_from", []):
            if mr["method"] not in methods and not mr.get("via_alias"):
                continue
            merged_desc = _describe_merged(mr["name"], mr.get("from", []), url_index)
            suspects.append((canon_desc, merged_desc, entry["canonical"], mr["name"]))

    print(f"\nLLM: checking {len(suspects)} suspect merge(s) "
          f"(methods: {','.join(sorted(methods))}) with {workers} workers…")
    if not suspects:
        print("LLM: nothing to check.")
        judge.render_dict_file(out_path, "BLOCKLIST",
                            ["# 0_node_blocklist.py",
                             "# No suspect merges to check.", ""], [])
        return 0, 0

    def build_prompt(item):
        return _build_prompt(item[0], item[1])

    def line_fmt(done, n, item, v):
        _, _, cn, mn = item
        same, conf, reason = judge.parse_verdict(v)
        flag = "err " if same is None else ("ok  " if same else "WRONG")
        return (f"  [{done}/{n}] {flag} {mn[:32]:32s} → {cn[:32]:32s} "
                f"({conf}) {reason[:48]}")

    verdicts = judge.run_pass(suspects, build_prompt, workers, line_fmt)

    # Classify: same_org=False → BLOCKLIST; thin/unsure → review.
    blocklist = defaultdict(list)   # canonical → [(merged_name, conf, reason, src, canon_srcs)]
    review = []
    for (canon_desc, merged_desc, cn, mn), v in zip(suspects, verdicts):
        same, conf, reason = judge.parse_verdict(v)
        if same is False:
            blocklist[cn].append((mn, conf, reason, merged_desc.get("source", ""),
                                  canon_desc.get("sources", [])))
        elif same is None or conf == "low":
            review.append(f"#   {mn!r} → {cn!r}  ({conf}) {reason}")

    def cdeg(name):
        return degree.get(name, 0)

    # Sections ordered by canonical degree (descending), name as tie-break.
    sections = []
    for cn in sorted(blocklist, key=lambda c: (-cdeg(c), c.lower())):
        rows = sorted(blocklist[cn], key=lambda r: r[0].lower())
        canon_srcs = next((r[4] for r in rows if r[4]), [])
        header = [f"    # {cn}: " + " | ".join(canon_srcs)] if canon_srcs else []
        items = []
        for mn, conf, reason, src, _ in rows:
            note = f"# ({conf}) {reason}".rstrip() if (conf or reason) else ""
            comments = [f"        #     ↳ {mn}: {src}"] if src else []
            items.append({"name": mn, "note": note, "comments": comments})
        sections.append({"key": cn, "header": header, "items": items})

    n_wrong = sum(len(v) for v in blocklist.values())
    file_header = [
        "# 0_node_blocklist.py",
        "# Auto-generated by 0_review_wrong_merges.py — REVIEW before pasting into",
        "# curated_corrections.py. Each entry is a merge the LLM judged to be WRONG",
        "# (two different orgs sharing a name). Format: {canonical: {wrong_name}}.",
        f"# {len(blocklist)} canonical(s), {n_wrong} wrong merge(s), "
        f"{len(review)} flagged for review.",
        "",
    ]
    review_lines = []
    if review:
        review_lines.append("# ── LOW-CONFIDENCE / errors — check these by hand ──")
        review_lines.extend(sorted(review, key=str.lower))
        review_lines.append("")

    judge.render_dict_file(out_path, "BLOCKLIST", file_header, sections, review_lines)
    print(f"LLM: {n_wrong} wrong merge(s) across {len(blocklist)} canonical(s), "
          f"{len(review)} flagged for manual review.")
    print(f"LLM: saved → {out_path}  (review, then paste into curated_corrections.py)")
    return n_wrong, len(review)


def main():
    ap = argparse.ArgumentParser(
        description="Audit step-4 merges and have DeepSeek judge the suspect "
                    "ones. Needs the DEEPSEEK_API_KEY environment variable set.")
    ap.add_argument("--dir", "-d", default=judge.root("data", "txts"))
    ap.add_argument("--step4", default=judge.root("pipeline", "4_merge.py"))
    ap.add_argument("--crimenet", default=judge.root("data", "crimenet_raw.json"),
                    help="crimenet snapshot used only to order the BLOCKLIST by "
                         "canonical degree (optional)")
    ap.add_argument("--workers", "-w", type=int, default=judge.DEFAULT_WORKERS,
                    help="parallel DeepSeek API workers (default 50)")
    ap.add_argument("--blocklist-output", default=judge.data_path("0_node_blocklist.py"),
                    help="where to write the BLOCKLIST suggestion")
    args = ap.parse_args()

    # The audit reports every merge; DeepSeek then judges the fuzzy ones
    # (alias/core), which is where cross-org name collisions hide.
    want = {"exact", "alias", "core"}
    methods = {"alias", "core"}

    m = load_step4(args.step4)
    txts = Path(args.dir)

    profiles = m.load_profiles(txts)
    # unify cross-language profiles first, exactly as build() does, so the
    # audit reflects the real post-unification map.
    if hasattr(m, "unify_mutual_profiles"):
        profiles = m.unify_mutual_profiles(list(profiles))
    raw_nodes, raw_edges, n_files = m.load_graphs(txts)

    canon_map, sig_map, canonical_folds, conflicts = m.build_maps(profiles)
    resolve_exact, resolve_core, resolve_node = m.make_resolvers(
        canon_map, sig_map, canonical_folds)

    # Folds that are an org's CANONICAL name (its primary identity) vs folds
    # that exist only because some org listed them as an ALIAS. A mention
    # whose own name matches only an alias-fold of the target is the risk
    # signal — that's exactly the "The Base" → Al-Qaeda collision (the
    # neo-Nazi group's name equals an Al-Qaeda alias). We surface those as
    # [via-alias] regardless of the resolver's exact/alias/core method.
    alias_only_folds = set()
    for p in profiles:
        c = (p.get("canonical_name") or "").strip()
        for a in p.get("aliases") or []:
            fa = m.fold(a)
            if fa and fa != m.fold(c) and fa not in canonical_folds:
                alias_only_folds.add(fa)

    # Each profiled org's OWN article URL(s), keyed by canonical name. After
    # cross-language unification a canonical can have several (one per
    # language). Shown on the █ header so you can click straight to the
    # org's own page to check the merges under it.
    own_urls = defaultdict(list)
    for p in profiles:
        c = (p.get("canonical_name") or "").strip()
        u = (p.get("source_url") or "").strip()
        if c and u and u not in own_urls[c]:
            own_urls[c].append(u)

    # For each raw node, resolve it and record (canonical ← name [method] from src)
    merges = defaultdict(lambda: defaultdict(lambda: {"method": "", "srcs": set(), "via_alias": False}))
    for node in raw_nodes:
        names = m.node_names(node)
        if not names:
            continue
        canonical, method = resolve_node(names)
        primary = names[0]
        if m.fold(primary) == m.fold(canonical):
            continue  # didn't merge into a different name
        rec = merges[canonical][primary]
        order = {"exact": 0, "alias": 1, "core": 2, "self": 3}
        if not rec["method"] or order.get(method, 9) < order.get(rec["method"], 9):
            rec["method"] = method
        # risk flag: the mention's own name matches an alias-only fold of
        # the target (not the target's canonical).
        if m.fold(primary) in alias_only_folds:
            rec["via_alias"] = True
        src = (node.get("source_url") or "").strip()
        if src:
            rec["srcs"].add(src)

    # ── Build a structured model (single source of truth for both outputs) ──
    method_counts = defaultdict(int)
    canon_entries = []
    for canon in sorted(merges.keys(), key=lambda x: x.lower()):
        names = merges[canon]
        shown = [(nm, rec) for nm, rec in names.items() if rec["method"] in want]
        if not shown:
            continue
        merged_list = []
        for nm, rec in sorted(shown, key=lambda x: (x[1]["method"], x[0].lower())):
            method_counts[rec["method"]] += 1
            merged_list.append({
                "name": nm,
                "method": rec["method"],
                "via_alias": rec["via_alias"],
                "from": sorted(rec["srcs"]),
            })
        canon_entries.append({
            "canonical": canon,
            "own_sources": own_urls.get(canon, []),
            "profiled": bool(own_urls.get(canon)),
            "n_merged": len(merged_list),
            "merged_from": merged_list,
        })

    # ── Console summary (nothing is written to disk except the BLOCKLIST
    #    suggestion below — that's the only file you actually use). ────────
    total_merges = sum(c["n_merged"] for c in canon_entries)
    via_alias_count = sum(1 for c in canon_entries for mr in c["merged_from"]
                          if mr["via_alias"])
    print(f"Audited {len(profiles)} profiles / {len(raw_nodes)} raw nodes.")
    print(f"  {total_merges} merge(s) across {len(canon_entries)} canonical(s) | "
          f"⚠ via-alias {via_alias_count}")
    print(f"  by method: exact={method_counts.get('exact', 0)}, "
          f"alias={method_counts.get('alias', 0)}, "
          f"core={method_counts.get('core', 0)}")

    # Degree (optional) just orders the BLOCKLIST, most-connected canonical first.
    degree = {}
    cn_path = Path(args.crimenet)
    if cn_path.exists():
        try:
            degree = judge.compute_degree(json.loads(cn_path.read_text("utf-8")).get("edges", []))
        except Exception as e:
            print(f"WARNING: failed to load degree from {cn_path}: {e}")
            degree = {}

    # Judge the suspect merges straight from the in-memory model and write the
    # ready-to-paste BLOCKLIST suggestion — the one file you actually use.
    run_llm_pass(canon_entries, txts, methods, args.workers,
                 args.blocklist_output, degree)


if __name__ == "__main__":
    main()
