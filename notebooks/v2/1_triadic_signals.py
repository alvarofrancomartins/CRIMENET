"""
Triadic closure signals: find pairs of organizations that share common
cooperation partners (Signal 1) or common adversaries (Signal 2), and pairs
that score on both signals simultaneously (Combined).

Saves:
    data/signal1_common_coop_partners.csv
    data/signal2_common_adversaries.csv
    data/signal3_combined.csv
    images/triadic_signals.png
"""

import csv
from collections import defaultdict

from common import (load_and_preprocess, print_header, print_dataset_summary,
                    ensure_data_dir, ensure_images_dir, setup_plot,
                    PLOT_DPI, gradient, GREEN_RAMP, RED_RAMP, COLOR_MUTED)


def find_pairs(adj, threshold, all_edges, org_country, opposite_adj=None):
    """
    Find pairs that share >= threshold common neighbors in `adj`,
    have no direct edge in `all_edges`, and are both in `org_country`.
    `opposite_adj` is the reverse-relationship graph for opposition detection.
    """
    # Inverted index: neighbor -> orgs connected to it
    nb2orgs = defaultdict(set)
    for org, neighbors in adj.items():
        for nb in neighbors:
            nb2orgs[nb].add(org)

    # Accumulate common-neighbor sets
    pair_items = defaultdict(set)
    for neighbor, orgs in nb2orgs.items():
        org_list = sorted(orgs)
        for i in range(len(org_list)):
            for j in range(i + 1, len(org_list)):
                pair_items[(org_list[i], org_list[j])].add(neighbor)

    # Filter
    results = []
    for (a, b), items in pair_items.items():
        if len(items) < threshold:
            continue
        if b in all_edges.get(a, set()):
            continue
        if a not in org_country or b not in org_country:
            continue

        # Opposition guard: common item that one side has opposite relationship with
        opposing = set()
        if opposite_adj:
            for c in items:
                if b in opposite_adj.get(c, set()) or a in opposite_adj.get(c, set()):
                    opposing.add(c)
        if len(opposing) > 1:
            continue

        results.append({
            "a": a,
            "b": b,
            "country_a": org_country[a],
            "country_b": org_country[b],
            "items": sorted(items),
            "n_items": len(items),
            "opposing": sorted(opposing),
            "same_country": (org_country[a] and org_country[b]
                             and org_country[a] == org_country[b]),
        })

    results.sort(key=lambda r: r["n_items"], reverse=True)
    return results


def main():
    print_header("Suggested Allies — Triadic Signals")
    data, P = load_and_preprocess()
    print_dataset_summary(P, data)

    out = ensure_data_dir()

    # ── Signal 1: Common cooperation partners ──────────────────────────────
    coop_partner_results = find_pairs(P.coop, threshold=3, all_edges=P.all_edges,
                                 org_country=P.org_country, opposite_adj=P.conflict)
    print(f"\nSignal 1 (common cooperation partners):  {len(coop_partner_results):5d} pairs")

    # ── Signal 2: Common adversaries ───────────────────────────────────
    shared_adversary_results = find_pairs(P.conflict, threshold=2, all_edges=P.all_edges,
                               org_country=P.org_country, opposite_adj=P.coop)
    print(f"Signal 2 (common adversaries):  {len(shared_adversary_results):5d} pairs")

    # ── Combined: pairs scoring on BOTH signals (threshold=1 each) ──────────
    # The dual-signal condition is itself a strong filter, so we use
    # threshold=1 for each dimension instead of the stricter individual
    # thresholds (3 cooperation partners / 2 adversaries).
    coop_partner_raw = find_pairs(P.coop, threshold=1, all_edges=P.all_edges,
                             org_country=P.org_country, opposite_adj=P.conflict)
    adversary_raw = find_pairs(P.conflict, threshold=1, all_edges=P.all_edges,
                           org_country=P.org_country, opposite_adj=P.coop)

    f_set = {(r["a"], r["b"]) for r in coop_partner_raw}
    e_set = {(r["a"], r["b"]) for r in adversary_raw}
    combined_pairs = f_set & e_set

    combined_results = []
    for (a, b) in combined_pairs:
        fr = next(r for r in coop_partner_raw if (r["a"], r["b"]) == (a, b))
        er = next(r for r in adversary_raw if (r["a"], r["b"]) == (a, b))
        combined_results.append({
            "a": a, "b": b,
            "country_a": fr["country_a"], "country_b": fr["country_b"],
            "coop_partners": fr["items"], "n_coop_partners": fr["n_items"],
            "adversaries": er["items"], "n_adversaries": er["n_items"],
            "same_country": fr["same_country"],
        })
    combined_results.sort(key=lambda r: (r["n_coop_partners"] + r["n_adversaries"]), reverse=True)

    print(f"Combined (both signals):   {len(combined_results):5d} pairs")

    # ── Print top examples ─────────────────────────────────────────────────
    print("\nTop 5 Signal 1 (common cooperation partners):")
    for i, r in enumerate(coop_partner_results[:5]):
        geo = "SAME" if r["same_country"] else f"{r['country_a']} | {r['country_b']}"
        print(f"  {i+1}. {r['a']} ↔ {r['b']}  [{geo}]  {r['n_items']} partners: "
              f"{', '.join(r['items'][:4])}")

    print("\nTop 5 Signal 2 (common adversaries):")
    for i, r in enumerate(shared_adversary_results[:5]):
        geo = "SAME" if r["same_country"] else f"{r['country_a']} | {r['country_b']}"
        print(f"  {i+1}. {r['a']} ↔ {r['b']}  [{geo}]  {r['n_items']} adversaries: "
              f"{', '.join(r['items'][:4])}")

    if combined_results:
        print("\nCombined (both signals):")
        for i, r in enumerate(combined_results):
            print(f"  {i+1}. {r['a']} ↔ {r['b']}  "
                  f"coop_partners={r['n_coop_partners']} adversaries={r['n_adversaries']}")

    # ── Save CSVs ──────────────────────────────────────────────────────────
    def _write_csv(filename, rows, columns):
        path = out / filename
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(columns)
            for r in rows:
                w.writerow([r.get(c, "") for c in columns])
        print(f"  Saved {len(rows):,} rows → {path}")

    # Signal 1
    s1_cols = ["a", "b", "country_a", "country_b", "same_country", "n_items",
               "items", "opposing"]
    _write_csv("signal1_common_coop_partners.csv", coop_partner_results, s1_cols)

    # Signal 2
    s2_cols = ["a", "b", "country_a", "country_b", "same_country", "n_items",
               "items", "opposing"]
    _write_csv("signal2_common_adversaries.csv", shared_adversary_results, s2_cols)

    # Combined
    sc_cols = ["a", "b", "country_a", "country_b", "same_country",
               "n_coop_partners", "coop_partners", "n_adversaries", "adversaries"]
    _write_csv("signal3_combined.csv", combined_results, sc_cols)

    # ── Plot ───────────────────────────────────────────────────────────────
    plt = setup_plot()
    img_dir = ensure_images_dir()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(17, 9.5))

    # Signal 1 — top 20
    top_f = coop_partner_results[:20]
    y1 = range(20)
    labels1 = [f"{r['a'][:26]}  +  {r['b'][:26]}" for r in reversed(top_f)]
    v1 = [r["n_items"] for r in reversed(top_f)]
    ax1.barh(y1, v1, color=gradient(len(v1), GREEN_RAMP))
    ax1.set_yticks(y1)
    ax1.set_yticklabels(labels1, fontsize=9.5)
    ax1.set_xlabel("Common cooperation partners")
    ax1.set_title(f"Common Cooperation Partners  ·  {len(coop_partner_results)} pairs qualify")
    for i, v in enumerate(v1):
        ax1.text(v + max(v1) * 0.01, i, str(v), va="center", fontsize=9,
                 color=COLOR_MUTED, fontweight="bold")

    # Signal 2 — top 20
    top_e = shared_adversary_results[:20]
    y2 = range(20)
    labels2 = [f"{r['a'][:26]}  +  {r['b'][:26]}" for r in reversed(top_e)]
    v2 = [r["n_items"] for r in reversed(top_e)]
    ax2.barh(y2, v2, color=gradient(len(v2), RED_RAMP))
    ax2.set_yticks(y2)
    ax2.set_yticklabels(labels2, fontsize=9.5)
    ax2.set_xlabel("Common adversaries")
    ax2.set_title(f"Common Adversaries  ·  {len(shared_adversary_results)} pairs qualify")
    for i, v in enumerate(v2):
        ax2.text(v + max(v2) * 0.01, i, str(v), va="center", fontsize=9,
                 color=COLOR_MUTED, fontweight="bold")

    plt.tight_layout()
    plot_path = img_dir / "triadic_signals.png"
    fig.savefig(plot_path, dpi=PLOT_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved plot → {plot_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
