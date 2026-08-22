"""
1_review_missed_merges.py

Find merges step 4 MISSED — two nodes that are really one organization recorded
under two spellings — then have DeepSeek judge each candidate and emit the
confirmed duplicates. The mirror image of 0_review_wrong_merges.py:

  • 0_review_wrong_merges.py  → BLOCKLIST       (merges step 4 made WRONGLY).
  • 1_review_missed_merges.py → KNOWN_DUPLICATES (merges step 4 SHOULD have made).

It unifies three ways a real duplicate slips past step 4, running them all and
judging the union of their candidate node pairs:

  fuzzy     — standard_names that are similar but not identical (typos, an
              added/dropped prefix, punctuation, accents), scored with difflib
              ≥ --threshold.
  reorder   — names that are the SAME significant words in a DIFFERENT order
              ("Clan Misso" vs "Misso clan"). difflib scores these low, so the
              fuzzy pass alone misses them — hence both.
  aliasxref — one node lists another node's canonical name as its alias or
              original_text_name (the alias-canonical conflicts step 4 prints
              but can't auto-resolve: e.g. EN "18th Street Gang" lists
              "Barrio 18" as an alias, ES "Barrio 18" is its own profile).
              Catches cross-language duplicates fuzzy/reorder can't see
              because the names share zero tokens.

For every candidate (canonical, variant) node pair — canonical = the more-
connected node — DeepSeek is shown both records' crimenet_raw.json evidence
(description, aliases, country, era) and asked whether they are the same org.
The confirmed pairs are written to 1_known_duplicates.py as a ready-to-paste
KNOWN_DUPLICATES dict, ordered by canonical degree (descending), each annotated
with how it was found, the model's confidence/reason, and the source URLs (own
page + where mentioned) so you can click through and verify. The LLM only
ADVISES; you review before pasting into curated_corrections.py.

Needs the DEEPSEEK_API_KEY environment variable set.

Usage:
    python 1_review_missed_merges.py

Needs the DEEPSEEK_API_KEY environment variable set.
"""

import re
import json
import argparse
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

import judge

# Generic words stripped before blocking, so candidate pairs hinge on the
# distinctive part of a name rather than "cartel"/"gang"/"the".
GENERIC_FUZZY = {
    "the", "crime", "family", "families", "mafia", "clan", "clans",
    "organization", "organisation", "syndicate", "mob", "cosca", "cosche",
    "borgata", "ndrina", "ndrine", "mc", "mcc", "motorcycle", "club",
    "cartel", "cartels", "gang", "gangs", "group", "los", "las", "la", "el",
    "de", "del", "and",
}
# Reorder uses step 4's own (smaller) generic set, so the token-set comparison
# matches step 4's core logic ("Misso clan" and "Clan Misso" → {misso}).
GENERIC_REORDER = {
    "the", "crime", "family", "families", "mafia", "clan", "clans",
    "organization", "organisation", "syndicate", "mob", "cosca", "cosche",
    "borgata", "ndrina", "ndrine", "mc", "mcc", "motorcycle", "club",
}
GENERIC_FUZZY_FOLD = {judge.norm(g) for g in GENERIC_FUZZY}
GENERIC_REORDER_FOLD = {judge.fold(g) for g in GENERIC_REORDER}

# Aliases too generic to be a useful cross-reference signal — the same set
# step 4 uses to keep "mafia"/"cartel"/"gang" from becoming matching keys.
GENERIC_ALIAS_BLOCK = {
    judge.fold(x) for x in {
        "mafia", "the mob", "mob", "camorra", "cartel", "gang", "triad",
        "the syndicate", "syndicate", "organized crime", "organised crime",
        "the family", "the firm", "the outfit",
        "the mafia", "drug cartel", "crime family", "crime syndicate",
        "the honoured society", "the honored society", "onorata societa",
    }
}


def find_alias_crossref_pairs(node_index, fold_to_node):
    """Candidate node pairs where one node's standard_name appears in another
    node's aliases or original_text_names — the same alias-canonical conflict
    step 4 reports but can't auto-resolve (e.g. EN "18th Street Gang" lists
    "Barrio 18" as an alias; ES "Barrio 18" is its own profiled node).

    Returns set of frozenset({i, j})."""
    pairs = set()

    # Build a map from fold(standard_name) → node index for quick lookup.
    # fold_to_node already maps every name to a node, but standard_name is
    # authoritative — use it directly for precision.
    sn_fold_to_idx = {}
    for idx, node in enumerate(node_index):
        sn = (node.get("standard_name") or "").strip()
        if sn:
            sn_fold_to_idx.setdefault(judge.fold(sn), idx)

    for idx, node in enumerate(node_index):
        # Collect every name this node claims that isn't its own standard_name
        sn_fold = judge.fold((node.get("standard_name") or "").strip())
        claimed = set()
        for a in node.get("aliases") or []:
            if isinstance(a, str) and a.strip():
                fa = judge.fold(a)
                if fa and fa != sn_fold and fa not in GENERIC_ALIAS_BLOCK:
                    claimed.add(fa)
        for o in node.get("original_text_names") or []:
            if isinstance(o, str) and o.strip():
                fo = judge.fold(o)
                if fo and fo != sn_fold and fo not in GENERIC_ALIAS_BLOCK:
                    claimed.add(fo)

        for claimed_fold in claimed:
            other_idx = sn_fold_to_idx.get(claimed_fold)
            if other_idx is not None and other_idx != idx:
                pairs.add(frozenset((idx, other_idx)))

    return pairs

# Tokens shared by more than this many nodes are too common to discriminate;
# pairing every node in such a block is noise (and quadratic), so we skip them.
MAX_BLOCK = 60


def fuzzy_tokens(name):
    """Significant whole words of a name (for fuzzy blocking)."""
    return {t for t in judge.norm(name).split()
            if len(t) >= 3 and t not in GENERIC_FUZZY_FOLD}


def reorder_tokens(name, strict):
    """Folded token SET of a name (for reorder grouping)."""
    out = set()
    for tok in re.split(r"[\s\-_/]+", (name or "").strip()):
        ft = judge.fold(tok)
        if ft and (strict or ft not in GENERIC_REORDER_FOLD):
            out.add(ft)
    return frozenset(out)


def ordered_fold(name):
    return tuple(t for t in (judge.fold(x) for x in re.split(r"[\s\-_/]+", name)) if t)


def find_fuzzy_pairs(node_index, threshold):
    """Candidate node-index pairs whose standard_names are fuzzily similar.
    Returns {frozenset({i, j}): ratio}."""
    norms = [judge.norm(n["standard_name"]) for n in node_index]
    by_token = defaultdict(list)
    for i, n in enumerate(node_index):
        for t in fuzzy_tokens(n["standard_name"]):
            by_token[t].append(i)

    seen = set()
    pairs = {}
    for idxs in by_token.values():
        if len(idxs) > MAX_BLOCK:
            continue
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                i, j = idxs[a], idxs[b]
                key = (i, j) if i < j else (j, i)
                if key in seen:
                    continue
                seen.add(key)
                if norms[i] == norms[j]:
                    continue  # same surface form, nothing to merge by name
                ratio = SequenceMatcher(None, norms[i], norms[j]).ratio()
                if ratio >= threshold:
                    pairs[frozenset((i, j))] = ratio
    return pairs


def find_reorder_pairs(node_index, fold_to_node, strict):
    """Candidate node-index pairs that are word-order variants living in
    DIFFERENT nodes. Returns set of frozenset({i, j})."""
    by_tokens = defaultdict(set)
    for n in node_index:
        for name in judge.node_names(n):
            toks = reorder_tokens(name, strict)
            if toks:
                by_tokens[toks].add(name)

    pairs = set()
    for names in by_tokens.values():
        distinct = {}
        for nm in names:
            distinct.setdefault(judge.fold(nm), nm)
        if len(distinct) < 2:
            continue
        forms = list(distinct.values())
        ordered = {nm: ordered_fold(nm) for nm in forms}
        sets = {nm: frozenset(ordered[nm]) for nm in forms}
        is_reorder = any(
            sets[forms[i]] == sets[forms[j]] and ordered[forms[i]] != ordered[forms[j]]
            for i in range(len(forms)) for j in range(i + 1, len(forms)))
        if not is_reorder:
            continue
        # Map the spellings to their owning nodes; keep only cross-node pairs.
        idxs = []
        for nm in forms:
            ri = fold_to_node.get(judge.fold(nm))
            if ri is not None and ri not in idxs:
                idxs.append(ri)
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                pairs.add(frozenset((idxs[a], idxs[b])))
    return pairs


def main():
    ap = argparse.ArgumentParser(
        description="Find missed merges (similar / reordered node names), have "
                    "DeepSeek judge which are the same org, and emit a ready-to-"
                    "paste KNOWN_DUPLICATES .py file ordered by degree. Needs the "
                    "DEEPSEEK_API_KEY environment variable set.")
    ap.add_argument("--input", "-i", default=judge.root("data", "crimenet_raw.json"))
    ap.add_argument("--output", "-o", default=judge.data_path("1_known_duplicates.py"))
    ap.add_argument("--max-mentions", type=int, default=8,
                    help="max 'mentioned in' URLs to list per name (default 8)")
    ap.add_argument("--txts", default=judge.root("data", "txts"),
                    help="dir of <page>/{url.txt,content.txt} for mention context")
    ap.add_argument("--no-context", action="store_true",
                    help="judge from the profile alone (skip article mention context)")
    ap.add_argument("--workers", "-w", type=int, default=judge.DEFAULT_WORKERS,
                    help="parallel DeepSeek API workers (default 50)")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"Input not found: {in_path}")
        return

    data = json.loads(in_path.read_text("utf-8"))
    nodes = data.get("nodes", [])
    degree = judge.compute_degree(data.get("edges", []))

    # One entry per named node, plus a map from every spelling it is known by
    # (folded) → that node's index. A node's own standard_name always wins.
    node_index = []
    fold_to_node = {}
    for n in nodes:
        if not (n.get("standard_name") or "").strip():
            continue
        idx = len(node_index)
        node_index.append(n)
        for name in judge.node_names(n):
            fold_to_node.setdefault(judge.fold(name), idx)
    for idx, n in enumerate(node_index):
        fold_to_node[judge.fold(n["standard_name"])] = idx

    # Run all three blockers and merge their findings per node pair.
    fuzzy = find_fuzzy_pairs(node_index, 0.85)
    reorder = find_reorder_pairs(node_index, fold_to_node, strict=False)
    alias_xref = find_alias_crossref_pairs(node_index, fold_to_node)

    meta = {}   # frozenset({i,j}) → {"fuzzy": ratio|None, "reorder": bool, "alias_xref": bool}
    for key, ratio in fuzzy.items():
        meta.setdefault(key, {"fuzzy": None, "reorder": False, "alias_xref": False})["fuzzy"] = ratio
    for key in reorder:
        meta.setdefault(key, {"fuzzy": None, "reorder": False, "alias_xref": False})["reorder"] = True
    for key in alias_xref:
        meta.setdefault(key, {"fuzzy": None, "reorder": False, "alias_xref": False})["alias_xref"] = True

    def deg(i):
        return degree.get(node_index[i]["standard_name"], 0)

    # canonical = the more-connected node of the pair.
    suspects = []   # (canon_i, variant_i, meta)
    for key, m in meta.items():
        i, j = tuple(key)
        canon_i, var_i = (i, j) if deg(i) >= deg(j) else (j, i)
        suspects.append((canon_i, var_i, m))

    print(f"Candidates: fuzzy={len(fuzzy)}, reorder={len(reorder)}, "
          f"alias_xref={len(alias_xref)}, "
          f"union={len(suspects)} pair(s) to judge.")
    if not suspects:
        judge.render_dict_file(args.output, "KNOWN_DUPLICATES",
                            ["# 1_known_duplicates.py",
                             "# No missed-merge candidates found.", ""], [])
        print(f"Saved → {args.output}")
        return

    # Article context for any description-less node in a suspect pair, so the
    # judge sees real text instead of just two similar names.
    ctx_of = {}
    if not args.no_context:
        url_index = judge.build_url_index(args.txts)
        cache = {}
        need = {idx for ci, vi, _m in suspects for idx in (ci, vi)
                if not (node_index[idx].get("description") or "").strip()}
        for idx in need:
            ctxs = judge.node_mention_contexts(node_index[idx], url_index, cache)
            if ctxs:
                ctx_of[node_index[idx]["standard_name"]] = ctxs
        print(f"  attached mention context to {len(ctx_of)} description-less node(s).")

    def how(m):
        bits = []
        if m["fuzzy"] is not None:
            bits.append(f"fuzzy sim {m['fuzzy']:.2f}")
        if m["reorder"]:
            bits.append("reorder")
        if m.get("alias_xref"):
            bits.append("alias xref")
        return " · ".join(bits)

    def build_prompt(item):
        ci, vi, m = item
        if m.get("alias_xref"):
            intro = ("cross-reference each other (one lists the other's name "
                     "among its aliases or alternative names)")
        elif m.get("reorder"):
            intro = "have rearranged names (same significant words in a different order)"
        elif m["fuzzy"] is not None:
            intro = "have similar names"
        else:
            intro = "have similar or rearranged names"
        return judge.build_pair_prompt(
            judge.describe_node(node_index[ci]),
            judge.describe_node(node_index[vi]),
            intro,
            ctx_of.get(node_index[ci]["standard_name"]),
            ctx_of.get(node_index[vi]["standard_name"]))

    def line_fmt(done, n, item, v):
        ci, vi, m = item
        same, conf, reason = judge.parse_verdict(v)
        flag = "err " if same is None else ("SAME" if same else "diff")
        cn, vn = node_index[ci]["standard_name"], node_index[vi]["standard_name"]
        return (f"  [{done}/{n}] {flag} {how(m):22s} "
                f"{vn[:30]:30s} → {cn[:30]:30s} ({conf}) {reason[:42]}")

    print(f"LLM: judging {len(suspects)} pair(s) with {args.workers} workers…")
    verdicts = judge.run_pass(suspects, build_prompt, args.workers, line_fmt)

    # Classify into a CONFIDENT bucket (same org, high/medium) and a
    # LOW-CONFIDENCE bucket (any uncertain verdict: same-but-low, different-but-
    # low, or API error). Both render in the SAME structured dict form, so the
    # uncertain candidates are just as easy to check and to promote if correct.
    hi = defaultdict(list)   # canon_i → [(variant_i, note)]
    lo = defaultdict(list)
    for (ci, vi, m), v in zip(suspects, verdicts):
        same, conf, reason = judge.parse_verdict(v)
        note = f"# {how(m)}  ({conf}) {reason}".rstrip()
        if same and conf in ("high", "medium"):
            hi[ci].append((vi, note))
        elif same is None or conf == "low":
            lo[ci].append((vi, note))
        # same=False with high/medium confidence → confidently not a duplicate; drop.

    def sections_for(bucket):
        """Canonical sections (degree-ordered) for render, from a bucket."""
        secs = []
        for ci in sorted(bucket, key=lambda i: (-deg(i),
                                                node_index[i]["standard_name"].lower())):
            item_objs = []
            for vi, note in sorted(bucket[ci], key=lambda x: -deg(x[0])):
                item_objs.append({
                    "name": node_index[vi]["standard_name"],
                    "note": note,
                    "comments": judge.source_comment_lines(node_index[vi], 8, args.max_mentions),
                })
            secs.append({
                "key": node_index[ci]["standard_name"],
                "header": judge.node_section_header(node_index[ci], degree, args.max_mentions),
                "items": item_objs,
            })
        return secs

    n_hi = sum(len(v) for v in hi.values())
    n_lo = sum(len(v) for v in lo.values())
    lines = [
        "# 1_known_duplicates.py",
        "# Auto-generated by 1_review_missed_merges.py — DeepSeek judged each similar /",
        "# reordered node-name pair; below are the ones it judged to be the SAME",
        "# organization (true duplicates step 4 missed). REVIEW, then paste the good",
        "# entries into curated_corrections.py KNOWN_DUPLICATES.",
        "#",
        "# To verify: open the 'own:' page(s) of each name (or the 'mentioned:'",
        "# pages when it has none of its own) and confirm they are one org.",
        "#",
        "# Format matches curated_corrections.KNOWN_DUPLICATES: {canonical: {variant, ...}}.",
        f"# {n_hi} confident duplicate(s) / {len(hi)} canonical(s); "
        f"{n_lo} low-confidence / {len(lo)} canonical(s).",
        "",
    ]
    lines += judge.dict_block_lines("KNOWN_DUPLICATES", sections_for(hi))
    if lo:
        lines += [
            "",
            "# ══════════════════════════════════════════════════════════════════",
            "# LOW-CONFIDENCE — DeepSeek was unsure (same format as above). Review,",
            "# and move any that are correct up into KNOWN_DUPLICATES (or straight",
            "# into curated_corrections.py). NOT applied while named like this.",
            "# ══════════════════════════════════════════════════════════════════",
            "",
        ]
        lines += judge.dict_block_lines("KNOWN_DUPLICATES_LOW_CONFIDENCE",
                                        sections_for(lo))
    Path(args.output).write_text("\n".join(lines), encoding="utf-8")
    print(f"\nLLM: {n_hi} confident duplicate(s) across {len(hi)} canonical(s), "
          f"{n_lo} low-confidence flagged for review.")
    print(f"Saved → {args.output}  (review, then paste into curated_corrections.py)")


if __name__ == "__main__":
    main()
