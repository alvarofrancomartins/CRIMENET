"""
Weighted triadic closure: same signals as 1_triadic_signals.py but each
common neighbor C contributes min(statements_A_C, statements_B_C) to the
pair's score, reflecting the evidentiary strength of each shared connection.

Saves:
    data/weighted_signal1_coop_partners.csv
    data/weighted_signal2_common_adversaries.csv
    data/weighted_signal3_combined.csv
    images/weighted_combined.png
"""

import csv
from collections import defaultdict

from common import (load_and_preprocess, print_header, print_dataset_summary,
                    ensure_data_dir, ensure_images_dir, setup_plot,
                    PLOT_DPI, COLOR_COOP, COLOR_CONFLICT, COLOR_MUTED)


def find_pairs_weighted(adj, pair_counts, threshold, all_edges, org_country,
                        opposite_adj=None):
    """
    Like find_pairs() but scores by sum of min(statements) per common neighbor.

    Score = Σ min(count(A, Cᵢ), count(B, Cᵢ))  for each common neighbor Cᵢ

    Still requires >= threshold common neighbors to qualify.
    All other filters (no direct edge, opposition guard) unchanged.
    """
    nb2orgs = defaultdict(set)
    for org, neighbors in adj.items():
        for nb in neighbors:
            nb2orgs[nb].add(org)

    pair_data = defaultdict(lambda: {"items": set(), "score": 0})
    for neighbor, orgs in nb2orgs.items():
        org_list = sorted(orgs)
        for i in range(len(org_list)):
            for j in range(i + 1, len(org_list)):
                a, b = org_list[i], org_list[j]
                key = (a, b)
                w_a = pair_counts.get(tuple(sorted([a, neighbor])), 1)
                w_b = pair_counts.get(tuple(sorted([b, neighbor])), 1)
                pair_data[key]["items"].add(neighbor)
                pair_data[key]["score"] += min(w_a, w_b)

    results = []
    for (a, b), data in pair_data.items():
        if len(data["items"]) < threshold:
            continue
        if b in all_edges.get(a, set()):
            continue
        if a not in org_country or b not in org_country:
            continue

        opposing = set()
        if opposite_adj:
            for c in data["items"]:
                if b in opposite_adj.get(c, set()) or a in opposite_adj.get(c, set()):
                    opposing.add(c)
        if len(opposing) > 1:
            continue

        results.append({
            "a": a, "b": b,
            "country_a": org_country[a], "country_b": org_country[b],
            "items": sorted(data["items"]),
            "n_items": len(data["items"]),
            "score": data["score"],
            "opposing": sorted(opposing),
            "same_country": (org_country[a] and org_country[b]
                             and org_country[a] == org_country[b]),
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return results


def main():
    print_header("Weighted Suggested Allies")
    data, P = load_and_preprocess()
    print_dataset_summary(P, data)

    out = ensure_data_dir()

    # ── Run weighted signals ───────────────────────────────────────────────
    wf = find_pairs_weighted(P.coop, P.pair_counts, threshold=3,
                             all_edges=P.all_edges, org_country=P.org_country,
                             opposite_adj=P.conflict)
    we = find_pairs_weighted(P.conflict, P.conflict_pair_counts, threshold=2,
                             all_edges=P.all_edges, org_country=P.org_country,
                             opposite_adj=P.coop)

    print(f"\nWeighted Signal 1 (common cooperation partners):  {len(wf):5d} pairs")
    print(f"Weighted Signal 2 (common adversaries):  {len(we):5d} pairs")

    # ── Weighted combined: both signals at threshold=1 ─────────────────────
    # The dual-signal condition is itself a strong filter, so threshold=1
    # per dimension suffices.
    wf_raw = find_pairs_weighted(P.coop, P.pair_counts, threshold=1,
                                 all_edges=P.all_edges, org_country=P.org_country,
                                 opposite_adj=P.conflict)
    we_raw = find_pairs_weighted(P.conflict, P.conflict_pair_counts, threshold=1,
                                 all_edges=P.all_edges, org_country=P.org_country,
                                 opposite_adj=P.coop)

    wf_set = {(r["a"], r["b"]) for r in wf_raw}
    we_set = {(r["a"], r["b"]) for r in we_raw}
    wc_pairs = wf_set & we_set

    wc_results = []
    for (a, b) in wc_pairs:
        fr = next(r for r in wf_raw if (r["a"], r["b"]) == (a, b))
        er = next(r for r in we_raw if (r["a"], r["b"]) == (a, b))
        wc_results.append({
            "a": a, "b": b,
            "country_a": fr["country_a"], "country_b": fr["country_b"],
            "coop_partners": fr["items"], "n_coop_partners": fr["n_items"],
            "score_coop_partners": fr["score"],
            "adversaries": er["items"], "n_adversaries": er["n_items"],
            "score_adversaries": er["score"],
            "same_country": fr["same_country"],
        })
    wc_results.sort(key=lambda r: (r["score_coop_partners"] + r["score_adversaries"]),
                    reverse=True)
    print(f"Weighted Combined (both signals, >=1 each): {len(wc_results):5d} pairs")

    # ── Print top 5 ────────────────────────────────────────────────────────
    print("\nTop 5 Weighted Signal 1:")
    for i, r in enumerate(wf[:5]):
        geo = "SAME" if r["same_country"] else f"{r['country_a']} | {r['country_b']}"
        print(f"  {i+1}. {r['a']} ↔ {r['b']}  [{geo}]  "
              f"coop_partners={r['n_items']} score={r['score']}")

    print("\nTop 5 Weighted Signal 2:")
    for i, r in enumerate(we[:5]):
        geo = "SAME" if r["same_country"] else f"{r['country_a']} | {r['country_b']}"
        print(f"  {i+1}. {r['a']} ↔ {r['b']}  [{geo}]  "
              f"adversaries={r['n_items']} score={r['score']}")

    if wc_results:
        print("\nWeighted Combined:")
        for i, r in enumerate(wc_results):
            print(f"  {i+1}. {r['a']} ↔ {r['b']}  "
                  f"coop_partners={r['n_coop_partners']}({r['score_coop_partners']}) "
                  f"adversaries={r['n_adversaries']}({r['score_adversaries']}) "
                  f"total={r['score_coop_partners'] + r['score_adversaries']}")

    # ── Save CSVs ──────────────────────────────────────────────────────────
    def _write_csv(filename, rows, columns):
        path = out / filename
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(columns)
            for r in rows:
                w.writerow([r.get(c, "") for c in columns])
        print(f"  Saved {len(rows):,} rows → {path}")

    s1_cols = ["a", "b", "country_a", "country_b", "same_country",
               "n_items", "score", "items", "opposing"]
    _write_csv("weighted_signal1_coop_partners.csv", wf, s1_cols)

    s2_cols = ["a", "b", "country_a", "country_b", "same_country",
               "n_items", "score", "items", "opposing"]
    _write_csv("weighted_signal2_common_adversaries.csv", we, s2_cols)

    sc_cols = ["a", "b", "country_a", "country_b", "same_country",
               "n_coop_partners", "score_coop_partners", "n_adversaries", "score_adversaries",
               "coop_partners", "adversaries"]
    _write_csv("weighted_signal3_combined.csv", wc_results, sc_cols)

    # ── Plot: top weighted combined pairs ──────────────────────────────────
    plt = setup_plot()
    img_dir = ensure_images_dir()

    fig, ax = plt.subplots(figsize=(15, 9.5))
    top20 = wc_results[:20]
    y = range(20)
    labels = []
    for r in reversed(top20):
        geo = "●" if r["same_country"] else "○"
        labels.append(f'{geo} {r["a"][:30]}  +  {r["b"][:30]}')

    # Stacked bars: cooperation score (green) + adversaries score (red)
    coop_scores = [r["score_coop_partners"] for r in reversed(top20)]
    adversary_scores = [r["score_adversaries"] for r in reversed(top20)]

    ax.barh(y, coop_scores, color=COLOR_COOP, label="Common cooperation partners score")
    ax.barh(y, adversary_scores, left=coop_scores, color=COLOR_CONFLICT,
            label="Common adversaries score")
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlabel("Weighted combined score")
    ax.set_title(f"Weighted Combined Signal  ·  top 20 of {len(wc_results)} pairs")

    # Annotate with raw counts
    for i, r in enumerate(reversed(top20)):
        total = r["score_coop_partners"] + r["score_adversaries"]
        ax.text(total + 0.3, i,
                f'{r["n_coop_partners"]}c+{r["n_adversaries"]}a',
                va="center", fontsize=9, color=COLOR_MUTED, fontweight="bold")

    ax.legend(loc="lower right")

    plt.tight_layout()
    fig.text(0.5, 0.005,
             "● same country     ○ transnational     (Nc+Na = raw cooperation+adversary counts)",
             ha="center", va="bottom", fontsize=10, color=COLOR_MUTED)
    plot_path = img_dir / "weighted_combined.png"
    fig.savefig(plot_path, dpi=PLOT_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved plot → {plot_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
