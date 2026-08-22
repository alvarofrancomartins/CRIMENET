"""
Cross-community link analysis: identifies edges (of a given network type) that
bridge Infomap communities, the organizations that serve as ambassadors
connecting the most communities, and which community pairs are most tightly
linked.

Requires data/communities_{network_type}.json (run 4_communities.py first).

Saves:
    data/cross_community_edges_top30_{network_type}.csv
    data/bridge_nodes_{network_type}.csv
    data/community_pairs_{network_type}.csv
"""

import argparse
import csv
import json
from collections import defaultdict, Counter

import networkx as nx

from common import (load_and_preprocess, print_header, ensure_data_dir,
                    build_graph, edge_matches_network_type, VALID_NETWORK_TYPES)


def _get_bet(eb, s, t):
    """Look up betweenness for an edge, trying both key orders."""
    return eb.get((s, t), eb.get((t, s), 0))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--network-type", "-n", default="cooperation",
                    choices=sorted(VALID_NETWORK_TYPES),
                    help="edge types to include (default: cooperation)")
    args = ap.parse_args()

    nt = args.network_type
    nt_label = nt.replace("_", " ").title()

    print_header(f"Cross-Community Links ({nt_label} Network)")
    data, P = load_and_preprocess()

    out = ensure_data_dir()

    # ── Load communities ───────────────────────────────────────────────────
    communities_path = out / f"communities_{nt}.json"
    if not communities_path.exists():
        raise FileNotFoundError(
            f"{communities_path} not found. Run 4_communities.py -n {nt} first."
        )

    with open(communities_path, encoding="utf-8") as f:
        comm_data = json.load(f)

    # Verify network_type matches
    stored_nt = comm_data.get("network_type", "cooperation")
    if stored_nt != nt:
        print("  Warning: communities file has network_type=%s, "
              "but --network-type=%s was requested. Using file's type." %
              (stored_nt, nt))

    org2cid = comm_data["org2community"]
    cid2nodes = {cid: info["orgs"] for cid, info in comm_data["communities"].items()}

    # Build community title lookup
    cid_title = {}
    for cid, info in comm_data["communities"].items():
        cid_title[cid] = info.get("title", f"Community {cid}")

    print(f"Loaded {len(comm_data['communities'])} communities, "
          f"{len(org2cid):,} org→community mappings")

    # ── Build weighted graph for betweenness (on LCC) ──────────────────────
    G, pair_counts = build_graph(P, nt)

    # Edge betweenness is only meaningful within a connected component.
    lcc_nodes = max(nx.connected_components(G), key=len)
    Glcc = G.subgraph(lcc_nodes).copy()

    import time
    t0 = time.time()
    if pair_counts is not None:
        # Weight by 1/count (more independent statements = shorter distance)
        for u, v in Glcc.edges():
            key = tuple(sorted([u, v]))
            count = pair_counts.get(key, 1)
            Glcc[u][v]["weight"] = 1.0 / count
            Glcc[u][v]["count"] = count
        eb = nx.edge_betweenness_centrality(Glcc, weight="weight", normalized=True)
    else:
        # Mixed network — unweighted
        eb = nx.edge_betweenness_centrality(Glcc, normalized=True)
    print(f"Edge betweenness: {time.time() - t0:.1f}s "
          f"(on LCC: {Glcc.number_of_nodes():,} nodes, "
          f"{Glcc.number_of_edges():,} edges)")

    # ── Find cross-community edges (FULL network) ──────────────────────────
    cross_edges = []
    internal_edges = 0

    for e in data["edges"]:
        if not edge_matches_network_type(e, nt):
            continue
        s, t = e["source"], e["target"]
        if not s or not t:
            continue
        if s not in org2cid or t not in org2cid:
            continue

        cs = org2cid[s]
        ct = org2cid[t]

        if cs == ct:
            internal_edges += 1
            continue

        cross_edges.append({
            "source": s,
            "target": t,
            "cid_source": cs,
            "cid_target": ct,
            "size_source": len(cid2nodes[cs]),
            "size_target": len(cid2nodes[ct]),
            "descriptions": e.get("descriptions", []),
            "evidence_quote": (e.get("evidence_quote") or "")[:300],
            "relationship": e.get("relationship", ""),
            "betweenness": _get_bet(eb, s, t),
        })

    total_edges = internal_edges + len(cross_edges)
    print(f"\nTotal {nt_label} edges (full network): {total_edges:,}")
    print(f"  Internal (same community):  {internal_edges:,}")
    print(f"  Cross-community:            {len(cross_edges):,}")
    if total_edges:
        print(f"  % cross-community:          "
              f"{100 * len(cross_edges) / total_edges:.1f}%")

    # ── Bridge nodes ───────────────────────────────────────────────────────
    node_communities = defaultdict(set)
    node_cross_degree = Counter()

    for e in cross_edges:
        node_communities[e["source"]].add(e["cid_target"])
        node_communities[e["target"]].add(e["cid_source"])
        node_cross_degree[e["source"]] += 1
        node_cross_degree[e["target"]] += 1

    org_country_lookup = {n["standard_name"]: n.get("country", "")
                          for n in data["nodes"]
                          if n["standard_name"] in P.orgs}

    bridge_nodes = []
    for name, comms in node_communities.items():
        reached = sorted(comms, key=lambda c: len(cid2nodes[c]), reverse=True)
        bridge_nodes.append({
            "name": name,
            "country": org_country_lookup.get(name, ""),
            "cross_edges": node_cross_degree[name],
            "communities_bridged": len(comms),
            "own_community_size": len(cid2nodes[org2cid[name]]),
            "communities_reached": [
                f"{cid_title.get(c, f'Community {c}')} ({len(cid2nodes[c])})"
                for c in reached
            ],
        })

    bridge_nodes.sort(key=lambda n: n["cross_edges"], reverse=True)

    num_bridges = sum(1 for n in bridge_nodes if n["communities_bridged"] >= 2)
    print(f"\nBridge nodes (connect >= 2 distinct communities): {num_bridges}")
    print(f"Nodes with cross-community edges: {len(bridge_nodes)}")

    print("\nTop 10 bridge nodes:")
    for n in bridge_nodes[:10]:
        print(f"  {n['name']}: {n['cross_edges']} cross-edges, "
              f"{n['communities_bridged']} communities bridged "
              f"(own community size: {n['own_community_size']})")

    # ── Community pairs ────────────────────────────────────────────────────
    community_pairs = Counter()
    for e in cross_edges:
        pair_key = tuple(sorted([e["cid_source"], e["cid_target"]]))
        community_pairs[pair_key] += 1

    comm_pair_list = []
    for (c1, c2), count in community_pairs.most_common():
        comm_pair_list.append({
            "cid_a": c1, "cid_b": c2,
            "title_a": cid_title.get(c1, "?"),
            "title_b": cid_title.get(c2, "?"),
            "size_a": len(cid2nodes[c1]),
            "size_b": len(cid2nodes[c2]),
            "edges": count,
        })

    print(f"\nCommunity pairs with cross-edges: {len(comm_pair_list)}")
    print("Top 10 community pairs:")
    for cp in comm_pair_list[:10]:
        print(f"  {cp['title_a']} ↔ {cp['title_b']}: {cp['edges']} edges "
              f"(sizes: {cp['size_a']} + {cp['size_b']})")

    # ── Save CSVs ──────────────────────────────────────────────────────────
    def _write_csv(filename, rows, columns):
        path = out / filename
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=columns)
            w.writeheader()
            w.writerows(rows)
        print(f"  Saved {len(rows):,} rows → {path}")

    # Cross-community edges (top 30 by betweenness)
    pair_stmt_count = Counter(
        tuple(sorted([e["source"], e["target"]])) for e in cross_edges)
    cross_sorted = sorted(cross_edges, key=lambda e: e["betweenness"], reverse=True)
    ce_rows = []
    seen_pairs = set()
    for e in cross_sorted:
        pair = tuple(sorted([e["source"], e["target"]]))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        desc = (e["descriptions"][0] if e["descriptions"] else "")[:250]
        ce_rows.append({
            "rank": len(ce_rows) + 1,
            "org_a": e["source"],
            "comm_a": f"C{e['cid_source']} ({e['size_source']})",
            "org_b": e["target"],
            "comm_b": f"C{e['cid_target']} ({e['size_target']})",
            "statements": pair_stmt_count[pair],
            "relationship": e.get("relationship", ""),
            "betweenness": f"{e['betweenness']:.6f}",
            "description": desc,
            "evidence": e["evidence_quote"][:250],
        })
        if len(ce_rows) >= 30:
            break
    _write_csv(f"cross_community_edges_top30_{nt}.csv", ce_rows,
               ["rank", "org_a", "comm_a", "org_b", "comm_b",
                "statements", "relationship", "betweenness", "description", "evidence"])

    # Bridge nodes (top 50)
    bn_rows = []
    for n in bridge_nodes[:50]:
        bn_rows.append({
            "rank": len(bn_rows) + 1,
            "organization": n["name"],
            "country": n["country"],
            "cross_edges": n["cross_edges"],
            "communities_bridged": n["communities_bridged"],
            "own_community_size": n["own_community_size"],
            "communities_reached": "; ".join(n["communities_reached"]),
        })
    _write_csv(f"bridge_nodes_{nt}.csv", bn_rows,
               ["rank", "organization", "country", "cross_edges",
                "communities_bridged", "own_community_size",
                "communities_reached"])

    # Community pairs (all)
    cp_rows = []
    for cp in comm_pair_list:
        cp_rows.append({
            "rank": len(cp_rows) + 1,
            "community_a": cp["title_a"],
            "size_a": cp["size_a"],
            "community_b": cp["title_b"],
            "size_b": cp["size_b"],
            "cross_edges": cp["edges"],
        })
    _write_csv(f"community_pairs_{nt}.csv", cp_rows,
               ["rank", "community_a", "size_a", "community_b", "size_b",
                "cross_edges"])

    print("\nDone.")


if __name__ == "__main__":
    main()
