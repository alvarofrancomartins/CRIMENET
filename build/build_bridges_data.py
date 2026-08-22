#!/usr/bin/env python3
"""Build a compact bridges data file for the browse page's Bridges tab.

Self-contained: reads crimenet.json and communities.json (the build output),
computes cross-community bridge nodes, and writes app/data/bridges.json.

Usage:
    python build/build_bridges_data.py \
        --input data/crimenet.json \
        --communities app/data/communities.json \
        --output app/data/bridges.json
"""

import argparse
import json
from collections import defaultdict, Counter
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="data/crimenet.json", type=Path)
    ap.add_argument("--communities", default="app/data/communities.json", type=Path)
    ap.add_argument("--output", default="app/data/bridges.json", type=Path)
    args = ap.parse_args()

    # ── Load crimenet ─────────────────────────────────────────────────────────
    crimenet = json.loads(args.input.read_text(encoding="utf-8"))
    nodes = crimenet["nodes"]
    edges = crimenet["edges"]

    org_country = {n["standard_name"]: n.get("country", "") for n in nodes}

    # ── Load communities, build org→cid mapping ───────────────────────────────
    comm_data = json.loads(args.communities.read_text(encoding="utf-8"))
    communities = comm_data["communities"]
    network_type = comm_data.get("network_type", "cooperation")

    org2cid: dict[str, int] = {}
    cid_size: dict[int, int] = {}
    cid_title: dict[int, str] = {}
    for c in communities:
        cid = c["i"]
        cid_size[cid] = c["s"]
        cid_title[cid] = c["t"]
        for name in c["o"]:
            org2cid[name] = cid

    print(f"Loaded {len(communities)} communities, "
          f"{len(org2cid):,} org→community mappings")

    # ── Find cross-community cooperation edges ────────────────────────────────
    node_communities: dict[str, set[int]] = defaultdict(set)
    node_cross_degree: Counter[str] = Counter()

    for e in edges:
        if e.get("relationship") != "cooperation":
            continue
        s, t = e["source"], e["target"]
        if not s or not t:
            continue
        if s not in org2cid or t not in org2cid:
            continue
        cs, ct = org2cid[s], org2cid[t]
        if cs == ct:
            continue
        # Cross-community edge
        node_communities[s].add(ct)
        node_communities[t].add(cs)
        node_cross_degree[s] += 1
        node_cross_degree[t] += 1

    # ── Build bridge nodes ────────────────────────────────────────────────────
    bridge_nodes = []
    for name, comms in node_communities.items():
        own_cid = org2cid[name]
        reached = sorted(comms, key=lambda c: cid_size[c], reverse=True)
        bridge_nodes.append({
            "r": 0,  # rank assigned after sort
            "n": name,
            "c": org_country.get(name, ""),
            "x": node_cross_degree[name],
            "b": len(comms),
            "s": cid_size[own_cid],
            "d": [
                f"{cid_title.get(c, f'Community {c}')} ({cid_size[c]})"
                for c in reached
            ],
        })

    bridge_nodes.sort(key=lambda n: (-n["x"], -n["b"], n["n"]))
    # Keep top 50
    bridge_nodes = bridge_nodes[:50]
    for i, n in enumerate(bridge_nodes):
        n["r"] = i + 1

    num_multi = sum(1 for n in bridge_nodes if n["b"] >= 2)
    print(f"Bridge nodes (connect >= 2 distinct communities): {num_multi}")
    print(f"Top 3: {[(n['n'], n['x'], n['b']) for n in bridge_nodes[:3]]}")

    # ── Output ────────────────────────────────────────────────────────────────
    out = {
        "network_type": network_type,
        "bridge_nodes": bridge_nodes,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(out, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    print(
        f"Wrote {len(bridge_nodes)} bridge nodes "
        f"({args.output.stat().st_size:,} bytes) → {args.output}"
    )


if __name__ == "__main__":
    main()
