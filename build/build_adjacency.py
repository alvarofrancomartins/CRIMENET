#!/usr/bin/env python3
"""
build_adjacency.py — Pre-compute the static topology index for CRIMENET pathfinding.

CRIMENET ships entirely as static files: light indexes for instant UI/topology and
sharded files for heavy evidence (descriptions, quotes, sources). This script adds
ONE more small static artifact, `crimenet_adj.json`, that lets the browser run
multi-hop pathfinding (all paths between any two orgs) with zero database calls.

It contains ONLY the graph topology — who connects to whom, plus the set of
relationship types on each pair. No descriptions, no quotes, no URLs. That keeps
it tiny (a few hundred KB even for ~3K orgs) and cheap to ship.

Evidence for the hops the user actually expands is still fetched lazily from the
per-org evidence/NNN.json shards (build_evidence_shards.py) — nothing about that changes.

Usage
-----
    python build_adjacency.py --input ../data/crimenet.json --output ../app/data/crimenet_adj.json

Output format (undirected; one entry per node that has at least one edge)
-------------------------------------------------------------------------
{
  "Primeiro Comando da Capital": [
    {"t": "Comando Vermelho", "r": ["conflict", "cooperation"]},
    {"t": "'Ndrangheta",      "r": ["cooperation"]}
  ],
  ...
}

`t` = neighbour's standard_name (same key the cards and evidence shards use).
`r` = sorted, de-duplicated list of relationship types on that pair (both
      directions folded together, since pathfinding treats edges as undirected).
"""

import argparse
import json
import sys
from collections import defaultdict


def build_adjacency(graph: dict) -> dict:
    """Fold every edge into an undirected adjacency map with relationship sets."""
    # Keep only nodes that actually exist in the node list, so a malformed edge
    # pointing at a dropped/missing org can't create a phantom node.
    valid = set()
    for n in graph.get("nodes", []):
        name = n.get("standard_name")
        if isinstance(name, str) and name.strip():
            valid.add(name.strip())

    # pair (frozenset of two names) -> set of relationship types
    pair_rels = defaultdict(set)
    skipped = 0
    for e in graph.get("edges", []):
        src = (e.get("source") or "").strip()
        tgt = (e.get("target") or "").strip()
        if not src or not tgt or src == tgt:
            skipped += 1
            continue
        if src not in valid or tgt not in valid:
            skipped += 1
            continue
        rel = (e.get("relationship") or "other").strip() or "other"
        pair_rels[frozenset((src, tgt))].add(rel)

    adj = defaultdict(dict)  # name -> {neighbour: set(rels)}
    for pair, rels in pair_rels.items():
        a, b = pair
        adj[a].setdefault(b, set()).update(rels)
        adj[b].setdefault(a, set()).update(rels)

    out = {}
    for name, neighbours in adj.items():
        out[name] = [
            {"t": other, "r": sorted(rels)}
            for other, rels in sorted(neighbours.items(), key=lambda kv: kv[0].lower())
        ]

    return out, skipped


def main():
    ap = argparse.ArgumentParser(description="Build crimenet_adj.json topology index.")
    ap.add_argument("--input", default="data/crimenet.json", help="Path to crimenet.json")
    ap.add_argument("--output", default="app/data/crimenet_adj.json", help="Output path")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print (larger file)")
    args = ap.parse_args()

    try:
        with open(args.input, "r", encoding="utf-8") as f:
            graph = json.load(f)
    except FileNotFoundError:
        sys.exit(f"ERROR: input file not found: {args.input}")
    except json.JSONDecodeError as e:
        sys.exit(f"ERROR: could not parse {args.input}: {e}")

    adj, skipped = build_adjacency(graph)

    with open(args.output, "w", encoding="utf-8") as f:
        if args.pretty:
            json.dump(adj, f, ensure_ascii=False, indent=2)
        else:
            json.dump(adj, f, ensure_ascii=False, separators=(",", ":"))

    n_nodes = len(adj)
    n_pairs = sum(len(v) for v in adj.values()) // 2
    print(f"Wrote {args.output}")
    print(f"  nodes with >=1 edge : {n_nodes:,}")
    print(f"  unique pairs        : {n_pairs:,}")
    if skipped:
        print(f"  skipped edges       : {skipped:,} (self-loops or endpoints not in node list)")


if __name__ == "__main__":
    main()