"""
Edge betweenness centrality on the weighted cooperation graph.
Identifies the critical bridges that hold the global organized crime
cooperation network together.

Saves:
    data/edge_betweenness_top30.csv
    images/edge_betweenness_top.png
"""

import csv
import time
import networkx as nx

from common import (load_and_preprocess, print_header, print_dataset_summary,
                    ensure_data_dir, ensure_images_dir, setup_plot,
                    PLOT_DPI, gradient, GREEN_RAMP, COLOR_MUTED)


def main():
    print_header("Edge Betweenness — Bridges of the Cooperation Network")
    data, P = load_and_preprocess()
    print_dataset_summary(P, data)

    out = ensure_data_dir()

    # ── Build weighted cooperation graph ───────────────────────────────────
    # weight = 1/count (more statements = shorter distance = more paths flow)
    Gw = nx.Graph()
    for n in P.orgs:
        Gw.add_node(n)
    for (a, b), count in P.pair_counts.items():
        if a in P.orgs and b in P.orgs:
            Gw.add_edge(a, b, weight=1.0 / count, count=count)

    print(f"\nWeighted cooperation graph: {Gw.number_of_nodes():,} nodes, "
          f"{Gw.number_of_edges():,} pairs")

    # Largest connected component
    lcc_nodes = max(nx.connected_components(Gw), key=len)
    G = Gw.subgraph(lcc_nodes).copy()
    print(f"LCC: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} pairs")
    print(f"  ({Gw.number_of_nodes() - G.number_of_nodes():,} nodes excluded from LCC)")

    # ── Exact edge betweenness (weighted) ──────────────────────────────────
    t0 = time.time()
    eb = nx.edge_betweenness_centrality(G, weight="weight", normalized=True)
    print(f"Edge betweenness: {time.time() - t0:.1f}s (exact, weighted)")

    t0 = time.time()
    nb = nx.betweenness_centrality(G, weight="weight", normalized=True)
    print(f"Node betweenness: {time.time() - t0:.1f}s (exact, weighted)")

    # ── Build edge evidence lookup ─────────────────────────────────────────
    edge_evidence = {}
    for e in data["edges"]:
        if e["relationship"] != "cooperation":
            continue
        key = tuple(sorted([e["source"], e["target"]]))
        if key not in edge_evidence:
            edge_evidence[key] = []
        edge_evidence[key].append({
            "quote": e.get("evidence_quote", "")[:300],
            "desc": ", ".join(e.get("descriptions", []))[:300],
        })

    country_lookup = {n["standard_name"]: n.get("country", "")
                      for n in data["nodes"] if n["standard_name"] in P.orgs}

    # ── Top 30 edges by betweenness ────────────────────────────────────────
    sorted_edges = sorted(eb.items(), key=lambda x: x[1], reverse=True)[:30]

    rows = []
    for (a, b), val in sorted_edges:
        key = tuple(sorted([a, b]))
        count = P.pair_counts.get(key, 0)
        ev = edge_evidence.get(key, [])
        quote = ev[0]["quote"] if ev else ""
        desc = ev[0]["desc"] if ev else ""
        ca = country_lookup.get(a, "")
        cb = country_lookup.get(b, "")
        rows.append({
            "rank": len(rows) + 1,
            "org_a": a, "country_a": ca,
            "org_b": b, "country_b": cb,
            "statements": count,
            "betweenness": f"{val:.6f}",
            "description": desc,
            "evidence_quote": quote,
        })

    # ── Print top 10 ───────────────────────────────────────────────────────
    print("\nTop 10 edges by betweenness:")
    for r in rows[:10]:
        same = " [SAME]" if r["country_a"] and r["country_a"] == r["country_b"] else ""
        print(f"  {r['rank']:2d}. {r['org_a']} ↔ {r['org_b']}{same}")
        print(f"       betweenness={r['betweenness']}  statements={r['statements']}")
        if r["description"]:
            print(f"       {r['description'][:120]}")

    # ── Save CSV ───────────────────────────────────────────────────────────
    cols = ["rank", "org_a", "country_a", "org_b", "country_b",
            "statements", "betweenness", "description", "evidence_quote"]
    path = out / "edge_betweenness_top30.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"\n  Saved {len(rows)} rows → {path}")

    # ── Plot: top 15 edges by betweenness ──────────────────────────────────
    plt = setup_plot()
    img_dir = ensure_images_dir()

    fig, ax = plt.subplots(figsize=(15, 9))
    top15 = rows[:15]
    y = range(15)
    labels = []
    for r in reversed(top15):
        ca = r["country_a"][:3] if r["country_a"] else "??"
        cb = r["country_b"][:3] if r["country_b"] else "??"
        geo = "●" if r["country_a"] and r["country_a"] == r["country_b"] else "○"
        labels.append(f'{geo} {r["org_a"][:30]}  ↔  {r["org_b"][:30]}  [{ca}↔{cb}]')

    values = [float(r["betweenness"]) for r in reversed(top15)]
    bars = ax.barh(y, values, color=gradient(len(values), GREEN_RAMP))
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlabel("Edge betweenness centrality")
    ax.set_title("Top 15 Cooperation Bridges by Weighted Edge Betweenness")

    # Statement count annotations
    for i, (bar, r) in enumerate(zip(bars, reversed(top15))):
        stmts = r["statements"]
        ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height()/2,
                f"{stmts}×", va="center", fontsize=9.5, color=COLOR_MUTED,
                fontweight="bold")

    ax.text(0.98, 0.02, "● same country   ○ transnational   (N× = statements)",
            transform=ax.transAxes, ha="right", va="bottom", fontsize=10,
            color=COLOR_MUTED)

    plt.tight_layout()
    plot_path = img_dir / "edge_betweenness_top.png"
    fig.savefig(plot_path, dpi=PLOT_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved plot → {plot_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
