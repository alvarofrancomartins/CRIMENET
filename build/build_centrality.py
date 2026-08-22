#!/usr/bin/env python3
"""Pre-compute network centrality metrics (degree, betweenness, PageRank)
from the full CRIMENET graph and produce a compact JSON file for the
Ask CRIMENET AI get_centrality tool.

Metrics computed on three graph views:
  - Full graph (all relationship types)     — overall importance
  - Cooperation-only subgraph               — alliance network
  - Conflict-only subgraph                  — rivalry network

Ranks are 1-indexed (1 = top). Output file is ~300 KB minified.

Usage:
    python build/build_centrality.py \
        --input data/crimenet.json \
        --output app/data/centrality.json
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import networkx as nx


def build_centrality(nodes: list, edges: list) -> dict:
    """Compute centrality on full, cooperation, and conflict graphs."""

    # Build name -> node info lookup
    node_meta = {}
    for n in nodes:
        name = n["standard_name"]
        node_meta[name] = {
            "country": n.get("country"),
            "is_defunct": n.get("is_defunct", False),
            "description": n.get("description"),
            "degree_raw": 0,  # will be filled
        }

    # Build NetworkX graphs for each view
    G_full = nx.Graph()
    G_coop = nx.Graph()
    G_conflict = nx.Graph()

    for e in edges:
        s, t, r = e["source"], e["target"], e["relationship"]
        if s not in node_meta or t not in node_meta:
            continue
        G_full.add_edge(s, t)
        if r == "cooperation":
            G_coop.add_edge(s, t)
        elif r == "conflict":
            G_conflict.add_edge(s, t)

    print(f"Full graph: {G_full.number_of_nodes():,} nodes, {G_full.number_of_edges():,} edges")
    print(f"Cooperation graph: {G_coop.number_of_nodes():,} nodes, {G_coop.number_of_edges():,} edges")
    print(f"Conflict graph: {G_conflict.number_of_nodes():,} nodes, {G_conflict.number_of_edges():,} edges")

    def compute_metrics(G, label):
        """Compute degree, betweenness, PageRank on a graph. Returns dict of org->metrics."""
        n = G.number_of_nodes()
        print(f"  Computing {label} betweenness ({n:,} nodes)...")
        bc = nx.betweenness_centrality(G, normalized=True, weight=None)
        print(f"  Computing {label} PageRank ({n:,} nodes)...")
        pr = nx.pagerank(G, alpha=0.85)
        print(f"  Computing {label} degree ({n:,} nodes)...")
        deg = dict(G.degree())

        # Also compute weighted degree (counts cooperation edges from full graph)
        # This is the "popularity" degree — how many orgs this one cooperates with
        metrics = {}
        for node in G.nodes():
            metrics[node] = {
                "d": deg.get(node, 0),          # degree in this subgraph
                "b": round(bc.get(node, 0), 8),  # betweenness
                "p": round(pr.get(node, 0), 8),  # PageRank
            }
        return metrics

    full_metrics = compute_metrics(G_full, "full")
    coop_metrics = compute_metrics(G_coop, "cooperation")
    conflict_metrics = compute_metrics(G_conflict, "conflict")

    # Merge into per-org records and compute ranks
    print("Computing ranks...")
    all_orgs = set(G_full.nodes())

    def rank_orgs(orgs, metric_key, sub_key):
        """Return dict of org->rank (1 = best) for a given metric."""
        scored = [(o, full_metrics.get(o, {}).get(metric_key, 0)) for o in orgs]
        scored.sort(key=lambda x: -x[1])
        ranks = {}
        for i, (org, _) in enumerate(scored):
            ranks[org] = i + 1
        return ranks

    # Ranks for each metric on the full graph
    degree_rank = rank_orgs(all_orgs, "d", "d")
    betweenness_rank = rank_orgs(all_orgs, "b", "b")
    pagerank_rank = rank_orgs(all_orgs, "p", "p")

    # Build output: list of orgs with all metrics + ranks
    output = []
    for org in sorted(all_orgs):
        fm = full_metrics.get(org, {})
        cm = coop_metrics.get(org, {})
        xm = conflict_metrics.get(org, {})
        meta = node_meta.get(org, {})

        record = {
            "n": org,                          # standard_name
            "d": fm.get("d", 0),               # degree (full graph)
            "b": fm.get("b", 0),               # betweenness (full)
            "p": fm.get("p", 0),               # PageRank (full)
            "dr": degree_rank.get(org, 0),      # degree rank
            "br": betweenness_rank.get(org, 0), # betweenness rank
            "pr": pagerank_rank.get(org, 0),    # PageRank rank
            "cd": cm.get("d", 0),              # cooperation degree
            "xd": xm.get("d", 0),              # conflict degree
        }
        if meta.get("country"):
            record["c"] = meta["country"]
        if meta.get("is_defunct") is True:
            record["f"] = True
        # Compact: only include description in top-200 by betweenness
        if betweenness_rank.get(org, 9999) <= 200 and meta.get("description"):
            record["desc"] = meta["description"]

        output.append(record)

    # Sort by betweenness rank (default ordering)
    output.sort(key=lambda x: x["br"])

    return {
        "n_orgs": len(output),
        "n_full_edges": G_full.number_of_edges(),
        "n_coop_edges": G_coop.number_of_edges(),
        "n_conflict_edges": G_conflict.number_of_edges(),
        "orgs": output,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="data/crimenet.json", type=Path)
    ap.add_argument("--output", default="app/data/centrality.json", type=Path)
    args = ap.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    print(f"Loaded {len(nodes):,} nodes, {len(edges):,} edges from {args.input}")

    result = build_centrality(nodes, edges)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    size_kb = args.output.stat().st_size / 1024
    print(f"Wrote {result['n_orgs']:,} orgs ({size_kb:.0f} KB) → {args.output}")
    print(f"  Full graph: {result['n_full_edges']:,} edges")
    print(f"  Cooperation: {result['n_coop_edges']:,} edges")
    print(f"  Conflict: {result['n_conflict_edges']:,} edges")


if __name__ == "__main__":
    main()
