#!/usr/bin/env python3
"""
Build triadic_signals.json: candidate undocumented ally pairs discovered through
triadic closure. Finds pairs of organizations that share multiple common
cooperation partners (Signal 1, threshold >=3) or common adversaries
(Signal 2, threshold >=2), plus pairs that score on both signals (Combined).

Each row tracks which signal(s) found it, raw partner/adversary counts, and
weighted scores (sum of min pairwise statement counts per common neighbor).

Self-contained: reads crimenet.json directly. No notebook dependency.

Usage:
    python build/build_triadic_data.py \
        --input data/crimenet.json \
        --output app/data/triadic_signals.json
"""

import argparse
import json
from collections import defaultdict, Counter
from pathlib import Path


def find_pairs(adj, threshold, all_edges, org_country, opposite_adj=None,
               pair_counts=None):
    """
    Find pairs that share >= threshold common neighbors in `adj`,
    have no direct edge in `all_edges`, and are both in `org_country`.
    If `pair_counts` is provided, score is Σ min(statements A↔C, statements B↔C).
    Otherwise score is simply the number of items (raw count).
    """
    # Inverted index: neighbor -> orgs connected to it
    nb2orgs = defaultdict(set)
    for org, neighbors in adj.items():
        for nb in neighbors:
            nb2orgs[nb].add(org)

    # Accumulate common-neighbor data
    pair_data = defaultdict(lambda: {"items": set(), "score": 0})
    for neighbor, orgs in nb2orgs.items():
        org_list = sorted(orgs)
        for i in range(len(org_list)):
            for j in range(i + 1, len(org_list)):
                a, b = org_list[i], org_list[j]
                key = (a, b)
                pair_data[key]["items"].add(neighbor)
                if pair_counts is not None:
                    w_a = pair_counts.get(tuple(sorted([a, neighbor])), 1)
                    w_b = pair_counts.get(tuple(sorted([b, neighbor])), 1)
                    pair_data[key]["score"] += min(w_a, w_b)
                else:
                    pair_data[key]["score"] += 1

    # Filter
    results = []
    for (a, b), data in pair_data.items():
        if len(data["items"]) < threshold:
            continue
        if b in all_edges.get(a, set()):
            continue
        if a not in org_country or b not in org_country:
            continue

        # Opposition guard: common item that one side has opposite relationship with
        opposing = set()
        if opposite_adj:
            for c in data["items"]:
                if b in opposite_adj.get(c, set()) or a in opposite_adj.get(c, set()):
                    opposing.add(c)
        if len(opposing) > 1:
            continue

        results.append({
            "a": a,
            "b": b,
            "country_a": org_country[a],
            "country_b": org_country[b],
            "items": sorted(data["items"]),
            "n_items": len(data["items"]),
            "score": data["score"],
            "opposing": sorted(opposing),
            "same_country": (org_country[a] and org_country[b]
                             and org_country[a] == org_country[b]),
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="data/crimenet.json", type=Path)
    ap.add_argument("--output", default="app/data/triadic_signals.json", type=Path)
    args = ap.parse_args()

    # ── Load crimenet ─────────────────────────────────────────────────────────
    crimenet = json.loads(args.input.read_text(encoding="utf-8"))
    nodes = crimenet["nodes"]
    edges = crimenet["edges"]

    # ── Build adjacency dicts ─────────────────────────────────────────────────
    coop = defaultdict(set)
    conflict = defaultdict(set)
    all_edges = defaultdict(set)
    pair_counts = Counter()          # (a,b) sorted -> cooperation statement count
    conflict_pair_counts = Counter()  # (a,b) sorted -> conflict statement count

    org_country = {}
    for n in nodes:
        org_country[n["standard_name"]] = n.get("country", "")

    for e in edges:
        s, t = e["source"], e["target"]
        rel = e["relationship"]
        all_edges[s].add(t)
        all_edges[t].add(s)
        if rel == "cooperation":
            coop[s].add(t)
            coop[t].add(s)
            pair_counts[tuple(sorted([s, t]))] += 1
        elif rel == "conflict":
            conflict[s].add(t)
            conflict[t].add(s)
            conflict_pair_counts[tuple(sorted([s, t]))] += 1

    coop = dict(coop)
    conflict = dict(conflict)
    all_edges = dict(all_edges)

    print(f"Loaded {len(nodes):,} nodes, {len(edges):,} edges")
    print(f"  Cooperation pairs: {len(pair_counts):,}")
    print(f"  Conflict pairs: {len(conflict_pair_counts):,}")

    # ── Signal 1: Common cooperation partners (threshold >=3) ─────────────────
    cp_weighted = find_pairs(coop, threshold=3, all_edges=all_edges,
                             org_country=org_country, opposite_adj=conflict,
                             pair_counts=pair_counts)
    print(f"\nSignal 1 (common cooperation partners, weighted):  {len(cp_weighted):5d} pairs")

    # ── Signal 2: Common adversaries (threshold >=2) ──────────────────────────
    adv_weighted = find_pairs(conflict, threshold=2, all_edges=all_edges,
                              org_country=org_country, opposite_adj=coop,
                              pair_counts=conflict_pair_counts)
    print(f"Signal 2 (common adversaries, weighted):  {len(adv_weighted):5d} pairs")

    # ── Combined: pairs scoring on BOTH signals (threshold=1 each) ────────────
    cp_raw = find_pairs(coop, threshold=1, all_edges=all_edges,
                        org_country=org_country, opposite_adj=conflict,
                        pair_counts=pair_counts)
    adv_raw = find_pairs(conflict, threshold=1, all_edges=all_edges,
                         org_country=org_country, opposite_adj=coop,
                         pair_counts=conflict_pair_counts)

    cp_set = {(r["a"], r["b"]) for r in cp_raw}
    adv_set = {(r["a"], r["b"]) for r in adv_raw}
    combined_pairs = cp_set & adv_set

    combined_weighted = []
    for (a, b) in combined_pairs:
        fr = next(r for r in cp_raw if (r["a"], r["b"]) == (a, b))
        er = next(r for r in adv_raw if (r["a"], r["b"]) == (a, b))
        combined_weighted.append({
            "a": a, "b": b,
            "country_a": fr["country_a"], "country_b": fr["country_b"],
            "coop_partners": fr["items"], "n_coop_partners": fr["n_items"],
            "score_coop_partners": fr["score"],
            "adversaries": er["items"], "n_adversaries": er["n_items"],
            "score_adversaries": er["score"],
            "same_country": fr["same_country"],
        })
    combined_weighted.sort(
        key=lambda r: (r["score_coop_partners"] + r["score_adversaries"]),
        reverse=True)
    print(f"Combined (both signals, >=1 each):  {len(combined_weighted):5d} pairs")

    # ── Build lookup dicts for merge ──────────────────────────────────────────
    s1 = {(r["a"], r["b"]): r for r in cp_weighted}
    s2 = {(r["a"], r["b"]): r for r in adv_weighted}
    s3 = {(r["a"], r["b"]): r for r in combined_weighted}

    # ── Merge every unique pair into one row ──────────────────────────────────
    all_keys = set(s1.keys()) | set(s2.keys()) | set(s3.keys())
    merged = []

    for key in all_keys:
        a, b = key
        row = {"a": a, "b": b}

        # Signal 1 (coop partners, threshold >=3)
        r1 = s1.get(key)
        if r1:
            row["cp_count"] = r1["n_items"]
            row["cp_items"] = r1["items"]
            row["cp_score"] = r1["score"]
            row["country_a"] = r1["country_a"]
            row["country_b"] = r1["country_b"]
            row["same_country"] = r1["same_country"]
        else:
            row["cp_count"] = 0
            row["cp_items"] = []
            row["cp_score"] = 0

        # Signal 2 (common adversaries, threshold >=2)
        r2 = s2.get(key)
        if r2:
            row["adv_count"] = r2["n_items"]
            row["adv_items"] = r2["items"]
            row["adv_score"] = r2["score"]
            if "country_a" not in row:
                row["country_a"] = r2["country_a"]
                row["country_b"] = r2["country_b"]
                row["same_country"] = r2["same_country"]
        else:
            row["adv_count"] = 0
            row["adv_items"] = []
            row["adv_score"] = 0

        # Signal 3 (combined: >=1 cooperation partner AND >=1 common adversary)
        r3 = s3.get(key)
        if r3:
            if not r1:
                row["cp_count"] = r3["n_coop_partners"]
                row["cp_items"] = r3["coop_partners"]
                row["cp_score"] = r3["score_coop_partners"]
            if not r2:
                row["adv_count"] = r3["n_adversaries"]
                row["adv_items"] = r3["adversaries"]
                row["adv_score"] = r3["score_adversaries"]
            if "country_a" not in row:
                row["country_a"] = r3["country_a"]
                row["country_b"] = r3["country_b"]
                row["same_country"] = r3["same_country"]

        # Country fallback
        if "country_a" not in row:
            row["country_a"] = ""
            row["country_b"] = ""
            row["same_country"] = False

        # Which signals detected this pair?
        has_cp = row["cp_count"] > 0
        has_sa = row["adv_count"] > 0
        row["is_cp"] = has_cp
        row["is_sa"] = has_sa

        if has_cp and has_sa:
            row["signal"] = "Both"
        elif has_cp:
            row["signal"] = "Only Common Partners"
        elif has_sa:
            row["signal"] = "Only Common Adversaries"
        else:
            row["signal"] = ""

        # Totals
        row["total_weighted"] = row["cp_score"] + row["adv_score"]

        merged.append(row)

    # ── Sort by weighted score descending ─────────────────────────────────────
    merged.sort(key=lambda r: r["total_weighted"], reverse=True)
    for i, r in enumerate(merged):
        r["rank"] = i + 1

    # ── Output ────────────────────────────────────────────────────────────────
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(merged, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    size = args.output.stat().st_size
    cp_only = sum(1 for r in merged if r["is_cp"] and not r["is_sa"])
    sa_only = sum(1 for r in merged if r["is_sa"] and not r["is_cp"])
    both = sum(1 for r in merged if r["is_cp"] and r["is_sa"])
    print(f"\nWrote {args.output} ({size:,} bytes)")
    print(f"  {len(merged)} total unique pairs")
    print(f"  Common Partners only: {cp_only}   Common Adversaries only: {sa_only}   Both: {both}")


if __name__ == "__main__":
    main()
