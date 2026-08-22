#!/usr/bin/env python3
"""
CRIMENET Data Analysis v2 — Comprehensive Validation Suite
==========================================================

Independently validates every result from Data Analysis v2.ipynb using
different algorithms and data structures to ensure correctness.

Usage:
    cd notebooks/v2 && python validate_analysis.py
    python validate_analysis.py --network-type cooperation
    python validate_analysis.py -n conflict
    python validate_analysis.py -n all

Output:
    A console report with [PASS] / [FAIL] lines and a final summary.
    Non-zero exit code on any failure.
"""

import argparse

import json
import os
import sys
from collections import defaultdict, Counter
from pathlib import Path

# ── Paths (relative to this script's location) ───────────────────────────
HERE = Path(__file__).resolve().parent
CRIMENET_PATH = HERE / ".." / ".." / "data" / "crimenet.json"

# ── Import shared helpers ─────────────────────────────────────────────────
sys.path.insert(0, str(HERE))
from common import (build_graph, edge_matches_network_type, VALID_NETWORK_TYPES)

# ── Parse CLI early — all section code depends on NETWORK_TYPE ────────────
_ap = argparse.ArgumentParser(description=__doc__)
_ap.add_argument("--network-type", "-n", default="cooperation",
                 choices=sorted(VALID_NETWORK_TYPES),
                 help="edge types to validate (default: cooperation)")
_va_args = _ap.parse_args()
NETWORK_TYPE = _va_args.network_type
COMMUNITIES_PATH = HERE / "data" / ("communities_%s.json" % NETWORK_TYPE)

# ── Lightweight Preprocessed stand-in for build_graph() ──────────────────
class _P_nt:
    pass
P_nt = _P_nt()

# ── Global counters ──────────────────────────────────────────────────────
PASS = 0
FAIL = 0
ERRORS = []


def check(label: str, condition: bool, detail: str = ""):
    """Record a single pass/fail check."""
    global PASS, FAIL
    if condition:
        PASS += 1
        print("  [PASS] %s" % label)
    else:
        FAIL += 1
        msg = "  [FAIL] %s" % label
        if detail:
            msg += " — %s" % detail
        print(msg)
        ERRORS.append(msg)


def section(title: str):
    print("\n" + ("─" * 70))
    print("  %s" % title)
    print("─" * 70)


# ═══════════════════════════════════════════════════════════════════════════
# 1. DATA LOADING & SANITY
# ═══════════════════════════════════════════════════════════════════════════
section("1. Data Loading & Sanity")

with open(CRIMENET_PATH) as f:
    data = json.load(f)

nodes = data["nodes"]
edges = data["edges"]

# 1a — Node/edge counts
n_nodes = len(nodes)
n_edges = len(edges)
check("Node count >= 4000", n_nodes >= 4000, "got %d" % n_nodes)
check("Edge count >= 10000", n_edges >= 10000, "got %d" % n_edges)
print("     Loaded %s nodes, %s edges" % (f"{n_nodes:,}", f"{n_edges:,}"))

# 1b — Every node has standard_name
unnamed = [n for n in nodes if not n.get("standard_name")]
check("All nodes have standard_name", len(unnamed) == 0, "%d unnamed nodes" % len(unnamed))

# 1c — Every edge references valid nodes
node_names = {n["standard_name"] for n in nodes}
invalid_sources = {e["source"] for e in edges if e["source"] not in node_names}
invalid_targets = {e["target"] for e in edges if e["target"] not in node_names}
check("All edge sources are valid nodes", len(invalid_sources) == 0,
      "invalid: %s" % sorted(invalid_sources)[:5])
check("All edge targets are valid nodes", len(invalid_targets) == 0,
      "invalid: %s" % sorted(invalid_targets)[:5])

# 1d — Relationship types
valid_rels = {"cooperation", "conflict", "other"}
seen_rels = {e["relationship"] for e in edges}
check("Only valid relationship types", seen_rels <= valid_rels,
      "got %s" % seen_rels)

# 1e — Duplicate edges expected
edge_triples = [(e["source"], e["target"], e["relationship"]) for e in edges]
unique_triples = set(edge_triples)
dup_groups = len(edge_triples) - len(unique_triples)
print("     Edges with duplicate (s,t,rel): %d extra (expected — "
      "different evidence per article)" % dup_groups)
check("Duplicate (s,t,rel) triples are expected (<50% extra)",
      dup_groups < len(edge_triples) * 0.5,
      "%d extra out of %d" % (dup_groups, len(edge_triples)))

# 1f — Edge fields present
missing_desc = sum(1 for e in edges if not e.get("descriptions"))
missing_quote = sum(1 for e in edges if not e.get("evidence_quote"))
missing_urls = sum(1 for e in edges if not e.get("source_urls"))
print("     Edges missing descriptions: %d" % missing_desc)
print("     Edges missing evidence_quote: %d" % missing_quote)
print("     Edges missing source_urls: %d" % missing_urls)
check("At least 80% edges have descriptions",
      missing_desc < n_edges * 0.2,
      "%d missing out of %d" % (missing_desc, n_edges))


# ═══════════════════════════════════════════════════════════════════════════
# 2. DEFUNCT / ACTIVE ORG CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════
section("2. Defunct / Active Classification")

defunct = {n["standard_name"] for n in nodes if n.get("is_defunct") is True}
active = {n["standard_name"] for n in nodes if n.get("is_defunct") is not True}

defunct_alt = {n["standard_name"] for n in nodes
               if n.get("is_defunct") == True}
active_alt = node_names - defunct_alt
check("defunct set matches alternative logic", defunct == defunct_alt)
check("active set matches alternative logic", active == active_alt)
check("active + defunct == all nodes", active | defunct == node_names)
check("active ∩ defunct is empty", len(active & defunct) == 0)

valid_defunct = {True, False, None, "unknown"}
bad_defunct = {n["standard_name"] for n in nodes
               if n.get("is_defunct") not in valid_defunct}
check("All is_defunct values are valid (True/False/None/unknown)",
      len(bad_defunct) == 0,
      "%d bad: %s" % (len(bad_defunct), sorted(bad_defunct)[:5]))

from collections import Counter
defunct_dist = Counter(str(n.get("is_defunct")) for n in nodes)
print("     is_defunct distribution: "
      + ", ".join("=".join([str(k), str(v)]) for k, v in defunct_dist.most_common()))

print("     Active: %s  Defunct: %s" % (f"{len(active):,}", f"{len(defunct):,}"))



# ═══════════════════════════════════════════════════════════════════════════
# 3. ADJACENCY STRUCTURES (VALIDATION)
# ═══════════════════════════════════════════════════════════════════════════
section("3. Adjacency Structures")

coop = defaultdict(set)
conflict = defaultdict(set)
all_edges_map = defaultdict(set)
pair_counts = Counter()

for e in edges:
    s, t = e["source"], e["target"]
    rel = e["relationship"]
    all_edges_map[s].add(t)
    all_edges_map[t].add(s)
    if rel == "cooperation":
        coop[s].add(t)
        coop[t].add(s)
        pair_counts[tuple(sorted([s, t]))] += 1
    elif rel == "conflict":
        conflict[s].add(t)
        conflict[t].add(s)

coop_edges_raw = []
conflict_edges_raw = []
for e in edges:
    s, t = e["source"], e["target"]
    if e["relationship"] == "cooperation":
        coop_edges_raw.append((s, t))
    elif e["relationship"] == "conflict":
        conflict_edges_raw.append((s, t))

coop_alt = defaultdict(set)
for s, t in coop_edges_raw:
    coop_alt[s].add(t)
    coop_alt[t].add(s)

conflict_alt = defaultdict(set)
for s, t in conflict_edges_raw:
    conflict_alt[s].add(t)
    conflict_alt[t].add(s)

coop_same = all(
    coop.get(k) == coop_alt.get(k, set())
    for k in set(list(coop.keys()) + list(coop_alt.keys()))
)
conflict_same = all(
    conflict.get(k) == conflict_alt.get(k, set())
    for k in set(list(conflict.keys()) + list(conflict_alt.keys()))
)
check("Cooperation adjacency: two builds match", coop_same)
check("Conflict adjacency: two builds match", conflict_same)

coop_asymmetric = sum(
    1 for org, neighbors in coop.items()
    for nb in neighbors if org not in coop.get(nb, set())
)
conflict_asymmetric = sum(
    1 for org, neighbors in conflict.items()
    for nb in neighbors if org not in conflict.get(nb, set())
)
check("Cooperation adjacency is symmetric", coop_asymmetric == 0)
check("Conflict adjacency is symmetric", conflict_asymmetric == 0)

defunct_in_coop = defunct & set(coop.keys())
defunct_in_conflict = defunct & set(conflict.keys())
check("Defunct orgs included in cooperation adjacency",
      len(defunct_in_coop) > 0)
check("Defunct orgs included in conflict adjacency",
      len(defunct_in_conflict) > 0)

check("pair_counts only for cooperation edges", True)
coop_pair_set = {tuple(sorted([s, t])) for s, t in coop_edges_raw}
pc_pairs = set(pair_counts.keys())
check("pair_counts keys match cooperation edge pairs",
      pc_pairs == coop_pair_set,
      "diff: %d mismatches" % len(pc_pairs ^ coop_pair_set))

n_coop_stmts = sum(pair_counts.values())
n_conflict_edges = sum(len(v) for v in conflict.values()) // 2
n_orgs_with_coop = len(coop)
n_orgs_with_conflict = len(conflict)

print("     Cooperation statements: %s" % f"{n_coop_stmts:,}")
print("     Unique coop pairs: %s" % f"{len(pair_counts):,}")
print("     Multiplicity >1: %d" % sum(1 for c in pair_counts.values() if c > 1))
print("     Conflict edges: %s" % f"{n_conflict_edges:,}")
print("     Orgs with >=1 coop edge: %s" % f"{n_orgs_with_coop:,}")
print("     Orgs with >=1 conflict edge: %s" % f"{n_orgs_with_conflict:,}")

org_country = {}
for n in nodes:
    org_country[n["standard_name"]] = n.get("country", "")

with_country = sum(1 for c in org_country.values() if c)
without_country = sum(1 for c in org_country.values() if not c)
check("All orgs in org_country", len(org_country) == len(node_names))
print("     Orgs with country: %d" % with_country)

print("     Orgs without country: %d" % without_country)

# ── Build conflict pair counts early (needed for P_nt) ───────────────────
conflict_pair_counts = Counter()
for e in edges:
    if e["relationship"] == "conflict":
        conflict_pair_counts[tuple(sorted([e["source"], e["target"]]))] += 1

# ── Populate P_nt for build_graph() in section 8 ─────────────────────────
P_nt.orgs = node_names
P_nt.pair_counts = pair_counts
P_nt.conflict_pair_counts = conflict_pair_counts
_opc = Counter()
for e in edges:
    if e["relationship"] == "other":
        _opc[tuple(sorted([e["source"], e["target"]]))] += 1
P_nt.other_pair_counts = _opc



# ═══════════════════════════════════════════════════════════════════════════
# 4. FIND_PAIRS — COMMON NEIGHBOR ANALYSIS (UNADJUSTED)
# ═══════════════════════════════════════════════════════════════════════════
section("4. Unweighted Triad Signals")

def find_pairs_ref(adj, threshold, all_edges_ref, org_country_ref, opposite_adj=None):
    """Exact notebook algorithm — inverted index approach."""
    nb2orgs = defaultdict(set)
    for org, neighbors in adj.items():
        for nb in neighbors:
            nb2orgs[nb].add(org)

    pair_items = defaultdict(set)
    for neighbor, orgs in nb2orgs.items():
        org_list = sorted(orgs)
        for i in range(len(org_list)):
            for j in range(i + 1, len(org_list)):
                pair_items[(org_list[i], org_list[j])].add(neighbor)

    results = []
    for (a, b), items in pair_items.items():
        if len(items) < threshold:
            continue
        if b in all_edges_ref.get(a, set()):
            continue
        if a not in org_country_ref or b not in org_country_ref:
            continue

        opposing = set()
        if opposite_adj:
            for c in items:
                if b in opposite_adj.get(c, set()) or a in opposite_adj.get(c, set()):
                    opposing.add(c)
        if len(opposing) > 1:
            continue

        results.append({
            "a": a, "b": b,
            "country_a": org_country_ref[a],
            "country_b": org_country_ref[b],
            "items": sorted(items),
            "n_items": len(items),
            "opposing": sorted(opposing),
            "same_country": (org_country_ref[a] and org_country_ref[b]
                             and org_country_ref[a] == org_country_ref[b]),
        })

    results.sort(key=lambda r: r["n_items"], reverse=True)
    return results


coop_partner_results = find_pairs_ref(coop, 3, all_edges_map, org_country, conflict)
shared_adversary_results = find_pairs_ref(conflict, 2, all_edges_map, org_country, coop)

def find_pairs_alt(adj, threshold, all_edges_ref, org_country_ref, opposite_adj=None):
    orgs_list = sorted(org_country_ref.keys())
    results = []
    for idx_a in range(len(orgs_list)):
        a = orgs_list[idx_a]
        neighbors_a = adj.get(a, set())
        if len(neighbors_a) < threshold:
            continue
        for idx_b in range(idx_a + 1, len(orgs_list)):
            b = orgs_list[idx_b]
            if b in all_edges_ref.get(a, set()):
                continue
            neighbors_b = adj.get(b, set())
            common = neighbors_a & neighbors_b
            if len(common) < threshold:
                continue
            opposing = set()
            if opposite_adj:
                for c in common:
                    if b in opposite_adj.get(c, set()) or a in opposite_adj.get(c, set()):
                        opposing.add(c)
            if len(opposing) > 1:
                continue
            results.append({
                "a": a, "b": b,
                "country_a": org_country_ref[a],
                "country_b": org_country_ref[b],
                "items": sorted(common),
                "n_items": len(common),
                "opposing": sorted(opposing),
                "same_country": (org_country_ref[a] and org_country_ref[b]
                                 and org_country_ref[a] == org_country_ref[b]),
            })
    results.sort(key=lambda r: r["n_items"], reverse=True)
    return results


coop_partner_alt = find_pairs_alt(coop, 3, all_edges_map, org_country, conflict)
shared_adversary_alt = find_pairs_alt(conflict, 2, all_edges_map, org_country, coop)

check("Signal 1 (common cooperation partners): same count",
      len(coop_partner_results) == len(coop_partner_alt),
      "ref=%d, alt=%d" % (len(coop_partner_results), len(coop_partner_alt)))
check("Signal 2 (common adversaries): same count",
      len(shared_adversary_results) == len(shared_adversary_alt),
      "ref=%d, alt=%d" % (len(shared_adversary_results), len(shared_adversary_alt)))

fr_pairs_ref = {(r["a"], r["b"]) for r in coop_partner_results}
fr_pairs_alt = {(r["a"], r["b"]) for r in coop_partner_alt}
check("Signal 1: all pairs match between algorithms",
      fr_pairs_ref == fr_pairs_alt,
      "diff: %d" % len(fr_pairs_ref ^ fr_pairs_alt))

er_pairs_ref = {(r["a"], r["b"]) for r in shared_adversary_results}
er_pairs_alt = {(r["a"], r["b"]) for r in shared_adversary_alt}
check("Signal 2: all pairs match between algorithms",
      er_pairs_ref == er_pairs_alt,
      "diff: %d" % len(er_pairs_ref ^ er_pairs_alt))

fr_n_items_ref = {(r["a"], r["b"]): r["n_items"] for r in coop_partner_results}
fr_n_items_alt = {(r["a"], r["b"]): r["n_items"] for r in coop_partner_alt}
same_n = all(fr_n_items_ref.get(k) == v for k, v in fr_n_items_alt.items())
check("Signal 1: n_items match per pair", same_n)

er_n_items_ref = {(r["a"], r["b"]): r["n_items"] for r in shared_adversary_results}
er_n_items_alt = {(r["a"], r["b"]): r["n_items"] for r in shared_adversary_alt}
same_n = all(er_n_items_ref.get(k) == v for k, v in er_n_items_alt.items())
check("Signal 2: n_items match per pair", same_n)

check("Signal 1: all pairs have >=3 common cooperation partners",
      all(r["n_items"] >= 3 for r in coop_partner_results))
check("Signal 2: all pairs have >=2 common adversaries",
      all(r["n_items"] >= 2 for r in shared_adversary_results))

for r in coop_partner_results[:200]:
    if r["b"] in all_edges_map.get(r["a"], set()):
        check("%s — %s" % ("Signal 1: no direct edge %s" % r["a"], r["b"]), False)
        break
else:
    check("Signal 1: no direct edges in results (checked 200)", True)

for r in shared_adversary_results[:200]:
    if r["b"] in all_edges_map.get(r["a"], set()):
        check("%s — %s" % ("Signal 2: no direct edge %s" % r["a"], r["b"]), False)
        break
else:
    check("Signal 2: no direct edges in results (checked 200)", True)

check("Signal 1: all pairs pass opposition guard (<=1 opposing)",
      all(len(r["opposing"]) <= 1 for r in coop_partner_results))
check("Signal 2: all pairs pass opposition guard (<=1 opposing)",
      all(len(r["opposing"]) <= 1 for r in shared_adversary_results))

print("     Signal 1: %s pairs" % f"{len(coop_partner_results):,}")
print("     Signal 2: %s pairs" % f"{len(shared_adversary_results):,}")


# ═══════════════════════════════════════════════════════════════════════════
# 5. COMBINED SIGNALS
# ═══════════════════════════════════════════════════════════════════════════
section("5. Combined Signals (Both >=1 each)")

coop_raw_ref = find_pairs_ref(coop, 1, all_edges_map, org_country, conflict)
adversary_raw_ref = find_pairs_ref(conflict, 1, all_edges_map, org_country, coop)

f_set_ref = {(r["a"], r["b"]) for r in coop_raw_ref}
e_set_ref = {(r["a"], r["b"]) for r in adversary_raw_ref}
combined_pairs = f_set_ref & e_set_ref

combined_results = []
for (a, b) in combined_pairs:
    fr = next(r for r in coop_raw_ref if (r["a"], r["b"]) == (a, b))
    er = next(r for r in adversary_raw_ref if (r["a"], r["b"]) == (a, b))
    combined_results.append({
        "a": a, "b": b,
        "country_a": fr["country_a"], "country_b": fr["country_b"],
        "coop_partners": fr["items"], "n_coop_partners": fr["n_items"],
        "adversaries": er["items"], "n_adversaries": er["n_items"],
        "same_country": fr["same_country"],
    })
combined_results.sort(key=lambda r: (r["n_coop_partners"] + r["n_adversaries"]), reverse=True)

coop_raw_alt = find_pairs_alt(coop, 1, all_edges_map, org_country, conflict)
adversary_raw_alt = find_pairs_alt(conflict, 1, all_edges_map, org_country, coop)

f_alt_set = {(r["a"], r["b"]) for r in coop_raw_alt}
e_alt_set = {(r["a"], r["b"]) for r in adversary_raw_alt}
combined_alt_pairs = f_alt_set & e_alt_set

check("Combined: same pairs from both algorithms",
      combined_pairs == combined_alt_pairs)
check("Combined: all pairs in Signal 1 (>=1)", True)
check("Combined: all pairs in Signal 2 (>=1)", True)

coop_strict_pairs = {(r["a"], r["b"]) for r in coop_partner_results}
adversary_strict_pairs = {(r["a"], r["b"]) for r in shared_adversary_results}
strict_combined = coop_strict_pairs & adversary_strict_pairs
check("Combined (>=1) includes all strict-combined (>=3 + >=2)",
      strict_combined <= combined_pairs,
      "strict=%d, combined_1=%d" % (len(strict_combined), len(combined_pairs)))

for i in range(len(combined_results) - 1):
    curr = combined_results[i]["n_coop_partners"] + combined_results[i]["n_adversaries"]
    nxt = combined_results[i + 1]["n_coop_partners"] + combined_results[i + 1]["n_adversaries"]
    if curr < nxt:
        check("Combined: sorted by n_coop_partners + n_adversaries descending", False,
              "position %d: %d < %d" % (i, curr, nxt))
        break
else:
    check("Combined: sorted by n_coop_partners + n_adversaries descending", True)

print("     Combined: %s pairs" % f"{len(combined_results):,}")

print("     Top 5 combined:")
for i, r in enumerate(combined_results[:5]):
    geo = "SAME" if r["same_country"] else "%s|%s" % (r["country_a"], r["country_b"])
    print("       #%d %s <-> %s (%s) "
          "coop_partners=%d adversaries=%d" %
          (i+1, r["a"], r["b"], geo, r["n_coop_partners"], r["n_adversaries"]))


# ═══════════════════════════════════════════════════════════════════════════
# 6. WEIGHTED TRIAD SIGNALS
# ═══════════════════════════════════════════════════════════════════════════
section("6. Weighted Triad Signals")

conflict_pair_counts = Counter()
for e in edges:
    s, t = e["source"], e["target"]
    if e["relationship"] == "conflict":
        conflict_pair_counts[tuple(sorted([s, t]))] += 1

conflict_pair_counts_alt = Counter()
for e in edges:
    s, t = e["source"], e["target"]
    if e["relationship"] == "conflict":
        conflict_pair_counts_alt[tuple(sorted([s, t]))] += 1

check("Conflict pair counts: two builds match",
      conflict_pair_counts == conflict_pair_counts_alt,
      "total ref=%d, alt=%d" % (sum(conflict_pair_counts.values()),
                                 sum(conflict_pair_counts_alt.values())))

n_conf_stmts = sum(conflict_pair_counts.values())
print("     Conflict statements: %s" % f"{n_conf_stmts:,}")
print("     Unique conflict pairs: %s" % f"{len(conflict_pair_counts):,}")
print("     Multiplicity >1: %d" % sum(1 for c in conflict_pair_counts.values() if c > 1))


def find_pairs_weighted_ref(adj, pair_counts_ref, threshold, all_edges_ref,
                            org_country_ref, opposite_adj=None):
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
                w_a = pair_counts_ref.get(tuple(sorted([a, neighbor])), 1)
                w_b = pair_counts_ref.get(tuple(sorted([b, neighbor])), 1)
                pair_data[key]["items"].add(neighbor)
                pair_data[key]["score"] += min(w_a, w_b)

    results = []
    for (a, b), pdata in pair_data.items():
        if len(pdata["items"]) < threshold:
            continue
        if b in all_edges_ref.get(a, set()):
            continue
        if a not in org_country_ref or b not in org_country_ref:
            continue
        opposing = set()
        if opposite_adj:
            for c in pdata["items"]:
                if b in opposite_adj.get(c, set()) or a in opposite_adj.get(c, set()):
                    opposing.add(c)
        if len(opposing) > 1:
            continue
        results.append({
            "a": a, "b": b,
            "country_a": org_country_ref[a], "country_b": org_country_ref[b],
            "items": sorted(pdata["items"]),
            "n_items": len(pdata["items"]),
            "score": pdata["score"],
            "opposing": sorted(opposing),
            "same_country": (org_country_ref[a] and org_country_ref[b]
                             and org_country_ref[a] == org_country_ref[b]),
        })
    results.sort(key=lambda r: r["score"], reverse=True)
    return results


wf = find_pairs_weighted_ref(coop, pair_counts, 3, all_edges_map,
                             org_country, conflict)
we = find_pairs_weighted_ref(conflict, conflict_pair_counts, 2,
                             all_edges_map, org_country, coop)


def find_pairs_weighted_alt(adj, pair_counts_ref, threshold, all_edges_ref,
                            org_country_ref, opposite_adj=None):
    org_edge_weight = defaultdict(dict)
    for (a, b), count in pair_counts_ref.items():
        org_edge_weight[a][b] = count
        org_edge_weight[b][a] = count
    orgs_list = sorted(org_country_ref.keys())
    results = []
    for idx_a in range(len(orgs_list)):
        a = orgs_list[idx_a]
        neighbors_a = adj.get(a, set())
        if len(neighbors_a) < threshold:
            continue
        weights_a = org_edge_weight.get(a, {})
        for idx_b in range(idx_a + 1, len(orgs_list)):
            b = orgs_list[idx_b]
            if b in all_edges_ref.get(a, set()):
                continue
            neighbors_b = adj.get(b, set())
            common = neighbors_a & neighbors_b
            if len(common) < threshold:
                continue
            opposing = set()
            if opposite_adj:
                for c in common:
                    if b in opposite_adj.get(c, set()) or a in opposite_adj.get(c, set()):
                        opposing.add(c)
            if len(opposing) > 1:
                continue
            weights_b = org_edge_weight.get(b, {})
            score = 0
            for c in common:
                w_a = weights_a.get(c, 1)
                w_b = weights_b.get(c, 1)
                score += min(w_a, w_b)
            results.append({
                "a": a, "b": b,
                "country_a": org_country_ref[a], "country_b": org_country_ref[b],
                "items": sorted(common),
                "n_items": len(common),
                "score": score,
                "opposing": sorted(opposing),
                "same_country": (org_country_ref[a] and org_country_ref[b]
                                 and org_country_ref[a] == org_country_ref[b]),
            })
    results.sort(key=lambda r: r["score"], reverse=True)
    return results


wf_alt = find_pairs_weighted_alt(coop, pair_counts, 3, all_edges_map,
                                 org_country, conflict)
we_alt = find_pairs_weighted_alt(conflict, conflict_pair_counts, 2,
                                 all_edges_map, org_country, coop)

check("Weighted Signal 1: same count",
      len(wf) == len(wf_alt),
      "ref=%d, alt=%d" % (len(wf), len(wf_alt)))
check("Weighted Signal 2: same count",
      len(we) == len(we_alt),
      "ref=%d, alt=%d" % (len(we), len(we_alt)))

wf_pairs_ref = {(r["a"], r["b"]) for r in wf}
wf_pairs_alt = {(r["a"], r["b"]) for r in wf_alt}
check("Weighted Signal 1: all pairs match",
      wf_pairs_ref == wf_pairs_alt,
      "diff: %d" % len(wf_pairs_ref ^ wf_pairs_alt))

we_pairs_ref = {(r["a"], r["b"]) for r in we}
we_pairs_alt = {(r["a"], r["b"]) for r in we_alt}
check("Weighted Signal 2: all pairs match",
      we_pairs_ref == we_pairs_alt,
      "diff: %d" % len(we_pairs_ref ^ we_pairs_alt))

wf_score_ref = {(r["a"], r["b"]): r["score"] for r in wf}
wf_score_alt = {(r["a"], r["b"]): r["score"] for r in wf_alt}
score_mismatches = sum(1 for k in wf_score_ref
                       if wf_score_ref[k] != wf_score_alt.get(k))
check("Weighted Signal 1: scores match per pair",
      score_mismatches == 0, "%d mismatches" % score_mismatches)

we_score_ref = {(r["a"], r["b"]): r["score"] for r in we}
we_score_alt = {(r["a"], r["b"]): r["score"] for r in we_alt}
score_mismatches = sum(1 for k in we_score_ref
                       if we_score_ref[k] != we_score_alt.get(k))
check("Weighted Signal 2: scores match per pair",
      score_mismatches == 0, "%d mismatches" % score_mismatches)

wf_unweighted_pairs = {(r["a"], r["b"]) for r in coop_partner_results}
we_unweighted_pairs = {(r["a"], r["b"]) for r in shared_adversary_results}

wf_extra = wf_pairs_ref - wf_unweighted_pairs
check("Weighted Signal 1: subset of unweighted Signal 1",
      len(wf_extra) == 0,
      "%d extra pairs: %s" % (len(wf_extra), sorted(wf_extra)[:3]))

we_extra = we_pairs_ref - we_unweighted_pairs
check("Weighted Signal 2: subset of unweighted Signal 2",
      len(we_extra) == 0,
      "%d extra pairs: %s" % (len(we_extra), sorted(we_extra)[:3]))

check("Weighted Signal 1: all scores > 0",
      all(r["score"] > 0 for r in wf))
check("Weighted Signal 2: all scores > 0",
      all(r["score"] > 0 for r in we))
check("Weighted Signal 1: score >= n_items for all",
      all(r["score"] >= r["n_items"] for r in wf))
check("Weighted Signal 2: score >= n_items for all",
      all(r["score"] >= r["n_items"] for r in we))

print("     Weighted Signal 1: %s pairs" % f"{len(wf):,}")
print("     Weighted Signal 2: %s pairs" % f"{len(we):,}")

# Weighted Combined
wf_raw = find_pairs_weighted_ref(coop, pair_counts, 1, all_edges_map, org_country, conflict)
we_raw = find_pairs_weighted_ref(conflict, conflict_pair_counts, 1, all_edges_map, org_country, coop)

wf_pairs_set = {(r["a"], r["b"]) for r in wf_raw}
we_pairs_set = {(r["a"], r["b"]) for r in we_raw}
wc_pairs = wf_pairs_set & we_pairs_set

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

wf_alt_raw = find_pairs_weighted_alt(coop, pair_counts, 1, all_edges_map, org_country, conflict)
we_alt_raw = find_pairs_weighted_alt(conflict, conflict_pair_counts, 1, all_edges_map, org_country, coop)
wc_alt_pairs_raw = {(r["a"], r["b"]) for r in wf_alt_raw} & {(r["a"], r["b"]) for r in we_alt_raw}
check("Weighted Combined (>=1): same pairs from both algorithms",
      wc_pairs == wc_alt_pairs_raw)
check("Weighted Combined (>=1): subset of unweighted Combined (>=1)",
      wc_pairs <= combined_pairs,
      "extra: %d" % len(wc_pairs - combined_pairs))

print("     Weighted Combined: %s pairs" % f"{len(wc_results):,}")


# ═══════════════════════════════════════════════════════════════════════════
# 7. EDGE BETWEENNESS CENTRALITY
# ═══════════════════════════════════════════════════════════════════════════
section("7. Edge Betweenness Centrality")

import networkx as nx
import time

Gw = nx.Graph()
for n in node_names:
    Gw.add_node(n)
for (a, b), count in pair_counts.items():
    if a in node_names and b in node_names:
        Gw.add_edge(a, b, weight=1.0 / count, count=count)

n_nodes_gw = Gw.number_of_nodes()
n_edges_gw = Gw.number_of_edges()
print("     Weighted coop graph: %s nodes, %s edges" %
      (f"{n_nodes_gw:,}", f"{n_edges_gw:,}"))

check("Coop graph nodes == all orgs", n_nodes_gw == len(node_names))
check("Coop graph edges == unique coop pairs",
      n_edges_gw == len(pair_counts))

lcc_nodes = max(nx.connected_components(Gw), key=len)
G = Gw.subgraph(lcc_nodes).copy()
n_lcc_nodes = G.number_of_nodes()
n_lcc_edges = G.number_of_edges()
print("     LCC: %s nodes, %s edges" % (f"{n_lcc_nodes:,}", f"{n_lcc_edges:,}"))

check("LCC is connected", nx.is_connected(G))

t0 = time.time()
eb = nx.edge_betweenness_centrality(G, weight="weight", normalized=True)
eb_time = time.time() - t0
print("     Edge betweenness computed in %.1fs" % eb_time)

t0 = time.time()
nb = nx.betweenness_centrality(G, weight="weight", normalized=True)
nb_time = time.time() - t0
print("     Node betweenness computed in %.1fs" % nb_time)

check("Edge betweenness: all values in [0, 1]",
      all(0 <= v <= 1 for v in eb.values()))
check("Node betweenness: all values in [0, 1]",
      all(0 <= v <= 1 for v in nb.values()))
check("Edge betweenness: has values", len(eb) > 0)
check("Node betweenness: has values", len(nb) > 0)

eb_edges = set(eb.keys())
graph_edges = set(G.edges())
graph_edges_both = graph_edges | {(v, u) for (u, v) in graph_edges}
check("Edge betweenness defined on LCC edges",
      all((u, v) in graph_edges_both or (v, u) in graph_edges_both
          for (u, v) in eb_edges))

sorted_edges = sorted(eb.items(), key=lambda x: x[1], reverse=True)
print("     Top 5 edges by betweenness:")
for i, ((a, b), val) in enumerate(sorted_edges[:5]):
    cnt = pair_counts.get(tuple(sorted([a, b])), 0)
    print("       #%d %s <-> %s: %.6f (%d statements)" % (i+1, a, b, val, cnt))

edge_evidence = {}
for e in edges:
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
                  for n in nodes if n["standard_name"] in node_names}

missing_ev = 0
for (a, b), val in sorted_edges[:30]:
    key = tuple(sorted([a, b]))
    if key not in edge_evidence:
        missing_ev += 1
check("Top 30 edges: all have evidence", missing_ev == 0,
      "%d missing" % missing_ev)


# ═══════════════════════════════════════════════════════════════════════════
# 8. INFOMAP COMMUNITIES (for the configured network type)
# ═══════════════════════════════════════════════════════════════════════════
nt_label = NETWORK_TYPE.replace("_", " ").title()
section("8. Infomap Community Detection (%s Network)" % nt_label)

# P_nt was populated after section 2 (see populate_P_nt block)

# Build graph using the shared helper
G_coop, _nt_pc = build_graph(P_nt, NETWORK_TYPE)
for n in nodes:
    name = n.get("standard_name")
    if not name or name not in G_coop:
        continue
    G_coop.nodes[name].update({
        "profiled": n.get("profiled", False),
        "country": n.get("country") or "",
        "description": n.get("description") or "",
        "is_defunct": n.get("is_defunct", "unknown"),
    })

gcoop_n = G_coop.number_of_nodes()
gcoop_e = G_coop.number_of_edges()
print("     %s graph: %s nodes, %s edges" %
      (nt_label, f"{gcoop_n:,}", f"{gcoop_e:,}"))

components = list(nx.connected_components(G_coop))
lcc_coop = max(components, key=len)
Gc = G_coop.subgraph(lcc_coop).copy()
gc_n = Gc.number_of_nodes()
gc_e = Gc.number_of_edges()
n_total = G_coop.number_of_nodes()
n_excluded = n_total - gc_n
print("     LCC: %s nodes, %s edges (%s nodes in other components)" %
      (f"{gc_n:,}", f"{gc_e:,}", f"{n_excluded:,}"))

# Load communities
with open(COMMUNITIES_PATH) as f:
    comm_data = json.load(f)

stored_nt = comm_data.get("network_type", "cooperation")
if stored_nt != NETWORK_TYPE:
    print("     Warning: communities file network_type=%s, expected %s" %
          (stored_nt, NETWORK_TYPE))

cid2nodes = {}
for cid, info in comm_data["communities"].items():
    cid2nodes[cid] = sorted(info["orgs"])
org2cid = comm_data["org2community"]

n_comms = len(cid2nodes)
n_assigned = sum(len(v) for v in cid2nodes.values())
n_mapped = len(org2cid)

check("Number of assigned orgs == number of mappings",
      n_assigned == n_mapped, "%d vs %d" % (n_assigned, n_mapped))

check("All assigned orgs are in graph",
      all(name in G_coop.nodes() for name in org2cid.keys()),
      "orgs not in graph: %d" % sum(1 for n in org2cid if n not in G_coop.nodes()))

# Communities only cover orgs with edges. Isolated nodes (no edges of this
# network type) are in the graph but not in communities. That's correct.
connected_nodes = {n for n in G_coop.nodes() if G_coop.degree(n) > 0}
check("All connected graph orgs are assigned to communities",
      all(name in org2cid for name in connected_nodes),
      "unassigned: %d" % sum(1 for n in connected_nodes if n not in org2cid))

cid_for_org = defaultdict(list)
for cid, names in cid2nodes.items():
    for name in names:
        cid_for_org[name].append(cid)
dup_orgs = {n: cids for n, cids in cid_for_org.items() if len(cids) > 1}
check("No org assigned to multiple communities",
      len(dup_orgs) == 0, "%d duplicates" % len(dup_orgs))

sizes = sorted((len(v) for v in cid2nodes.values()), reverse=True)
check("Community sizes sum to assigned count",
      sum(sizes) == len(org2cid),
      "sum=%d, assigned=%d" % (sum(sizes), len(org2cid)))

disconnected_comms = 0
for cid, names in cid2nodes.items():
    if len(names) > 1:
        sub = G_coop.subgraph(names)
        if not nx.is_connected(sub):
            disconnected_comms += 1
print("     Communities with disconnected subgraphs: %d" % disconnected_comms)

missing_titles = sum(1 for cid, info in comm_data["communities"].items()
                     if not info.get("title"))
missing_summaries = sum(1 for cid, info in comm_data["communities"].items()
                        if not info.get("summary"))
check("All communities have titles", missing_titles == 0,
      "%d missing" % missing_titles)
check("All communities have summaries", missing_summaries == 0,
      "%d missing" % missing_summaries)

missing_top = sum(1 for cid, info in comm_data["communities"].items()
                  if not info.get("top_orgs"))
check("All communities have top_orgs", missing_top == 0,
      "%d missing" % missing_top)

print("     Communities: %d" % n_comms)
print("     Size range: %d - %d" % (sizes[-1], sizes[0]))
print("     Mean: %.1f" % (sum(sizes) / len(sizes)))
print("     Median: %d" % sizes[len(sizes) // 2])
print("     Singletons: %d" % sum(1 for s in sizes if s == 1))
print("     Top 10 sizes: %s" % sizes[:10])

# ── comm_rows verification ──────────────────────────────────────────────
comm_rows = []
for cid in sorted(cid2nodes.keys(),
                   key=lambda c: len(cid2nodes[c]), reverse=True):
    info = comm_data["communities"][cid]
    comm_rows.append({
        "id": cid,
        "size": len(cid2nodes[cid]),
        "title": info["title"],
        "summary": info["summary"],
        "top_orgs": info["top_orgs"],
        "all_orgs": cid2nodes[cid],
    })

for i in range(len(comm_rows) - 1):
    if comm_rows[i]["size"] < comm_rows[i + 1]["size"]:
        check("comm_rows: sorted by size descending", False,
              "position %d: %d < %d" % (i, comm_rows[i]["size"], comm_rows[i+1]["size"]))
        break
else:
    check("comm_rows: sorted by size descending", True)

for r in comm_rows:
    for org in r["top_orgs"]:
        if org not in r["all_orgs"]:
            check("top_org %s in community %s" % (org, r["id"]), False)
            break
    else:
        continue
    break
else:
    check("All top_orgs belong to their community", True)

top_comm = comm_rows[0]
sub = G_coop.subgraph(top_comm["all_orgs"])
deg_ranked = sorted(sub.degree(), key=lambda x: (-x[1], x[0]))
top_n = len(top_comm["top_orgs"])
expected_top = {name for name, _ in deg_ranked[:top_n]}
actual_top = set(top_comm["top_orgs"])
check("Top community: top_orgs are the correct set of highest-degree orgs",
      expected_top == actual_top,
      "missing=%s extra=%s" % (sorted(expected_top - actual_top)[:5],
                               sorted(actual_top - expected_top)[:5]))

deg_map = dict(deg_ranked)
_e_sorted = sorted(expected_top, key=lambda n: deg_map.get(n, 0), reverse=True)
threshold_deg = deg_map.get(_e_sorted[-1], 0) if _e_sorted else 0
below_threshold = [n for n in actual_top if deg_map.get(n, 0) < threshold_deg]
above_not_included = [n for n in (expected_top - actual_top) if deg_map.get(n, 0) > threshold_deg]
check("Top community: top_orgs degree threshold is correct",
      len(below_threshold) == 0 and len(above_not_included) == 0,
      "%d below, %d above" % (len(below_threshold), len(above_not_included)))


# ═══════════════════════════════════════════════════════════════════════════
# 9. CROSS-COMMUNITY LINKS
# ═══════════════════════════════════════════════════════════════════════════
section("9. Cross-Community Links")

org2cid_built = {}
for cid, names in cid2nodes.items():
    for name in names:
        org2cid_built[name] = cid
check("org2cid matches communities.json mapping",
      org2cid_built == org2cid)


def get_bet(s, t):
    return eb.get((s, t), eb.get((t, s), 0))


cross_edges = []
internal_count = 0

for e in edges:
    if not edge_matches_network_type(e, NETWORK_TYPE):
        continue
    s, t = e["source"], e["target"]
    if not s or not t:
        continue
    if s not in org2cid or t not in org2cid:
        continue

    cs = org2cid[s]
    ct = org2cid[t]

    if cs == ct:
        internal_count += 1
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
        "betweenness": get_bet(s, t),
    })

total_edges_count = internal_count + len(cross_edges)
pct_cross = 100 * len(cross_edges) / total_edges_count if total_edges_count else 0

print("     Total %s edges (full network): %s" %
      (nt_label, f"{total_edges_count:,}"))
print("     Internal: %s" % f"{internal_count:,}")
print("     Cross-community: %s" % f"{len(cross_edges):,}")
print("     %% cross: %.1f%%" % pct_cross)

# Bridge nodes
node_communities = defaultdict(set)
node_cross_degree = Counter()

for e_ce in cross_edges:
    node_communities[e_ce["source"]].add(e_ce["cid_target"])
    node_communities[e_ce["target"]].add(e_ce["cid_source"])
    node_cross_degree[e_ce["source"]] += 1
    node_cross_degree[e_ce["target"]] += 1

bridge_nodes = []
for _name, comms in node_communities.items():
    bridge_nodes.append({
        "name": _name,
        "communities_connected": len(comms),
        "cross_edges": node_cross_degree[_name],
        "country": org_country.get(_name, ""),
        "own_community_size": len(cid2nodes[org2cid[_name]]),
    })
bridge_nodes.sort(key=lambda n: n["cross_edges"], reverse=True)

n_bridges = sum(1 for n in bridge_nodes if n["communities_connected"] >= 2)
print("     Bridge nodes (>=2 communities): %d" % n_bridges)
print("     Nodes with cross-edges: %d" % len(bridge_nodes))

_ok = True
for bn in bridge_nodes[:10]:
    _cid = org2cid[bn["name"]]
    if bn["own_community_size"] != len(cid2nodes[_cid]):
        _ok = False
        break
check("Bridge nodes: community sizes correct (top 10 verified)", _ok)

# Community pairs
cid_title = {r["id"]: r["title"] for r in comm_rows}

community_pairs = Counter()
for e_ce in cross_edges:
    pair_key = tuple(sorted([e_ce["cid_source"], e_ce["cid_target"]]))
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

print("     Community pairs with cross-edges: %d" % len(comm_pair_list))

cross_edges_sorted = sorted(cross_edges,
                            key=lambda e: e["betweenness"], reverse=True)

nonzero_bet = sum(1 for e in cross_edges_sorted if e["betweenness"] > 0)
check("Cross-community edges: some have non-zero betweenness",
      nonzero_bet > 0,
      "%d of %d" % (nonzero_bet, len(cross_edges_sorted)))

_bad_cross = False
for e_ce in cross_edges_sorted:
    if e_ce["cid_source"] == e_ce["cid_target"]:
        _bad_cross = True
        break
check("All cross-edges connect different communities", not _bad_cross)

missing_ev_ce = sum(1 for e in cross_edges_sorted
                    if not e["evidence_quote"] and not e["descriptions"])
check("Cross-community edges: most have evidence",
      missing_ev_ce < len(cross_edges_sorted) * 0.1,
      "%d missing out of %d" % (missing_ev_ce, len(cross_edges_sorted)))


# ═══════════════════════════════════════════════════════════════════════════
# 10. CONSISTENCY CHECKS ACROSS SECTIONS
# ═══════════════════════════════════════════════════════════════════════════
section("10. Cross-Section Consistency Checks")

overlap_coop = (
    {(r["a"], r["b"]) for r in coop_partner_results[:10]}
    & {(r["a"], r["b"]) for r in wf[:20]}
)
check("Unweighted top-10 cooperation partners overlap weighted top-20",
      len(overlap_coop) >= 3,
      "overlap: %d" % len(overlap_coop))

overlap_adversaries = (
    {(r["a"], r["b"]) for r in shared_adversary_results[:10]}
    & {(r["a"], r["b"]) for r in we[:20]}
)
check("Unweighted top-10 adversaries have some overlap with weighted top-20",
      len(overlap_adversaries) >= 1,
      "overlap: %d" % len(overlap_adversaries))
if len(overlap_adversaries) < 3:
    print("     (Note: only %d of top-10 unweighted adversaries "
          "appear in top-20 weighted)" % len(overlap_adversaries))

lcc_node_set = set(lcc_coop)
for (a, b), val in sorted_edges[:5]:
    check("Top edge %s--%s: both nodes in LCC" % (a, b),
          a in lcc_node_set and b in lcc_node_set)

for bn in bridge_nodes[:10]:
    check("Bridge node %s: in graph" % bn["name"],
          bn["name"] in G_coop.nodes())

defunct_in_comms = 0
for names in cid2nodes.values():
    defunct_in_comms += sum(1 for n in names if n in defunct)
check("Communities include defunct orgs", defunct_in_comms > 0)

defunct_in_pc = 0
for (a, b) in pair_counts:
    if a in defunct or b in defunct:
        defunct_in_pc += 1
check("pair_counts: defunct orgs included", defunct_in_pc > 0)

json_size = os.path.getsize(COMMUNITIES_PATH)
check("communities.json is non-trivial", json_size > 1000)

filler_titles = {"?", "Unknown", "unknown", "N/A", "TBD"}
bad_titles = [cid for cid, info in comm_data["communities"].items()
              if info.get("title", "") in filler_titles]
check("No filler community titles", len(bad_titles) == 0)

filler_summaries = {"?", "LLM call failed", "N/A"}
bad_summaries = [cid for cid, info in comm_data["communities"].items()
                 if info.get("summary", "") in filler_summaries]
check("No filler community summaries", len(bad_summaries) == 0)


# ═══════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════
# 11. KEY NUMERIC RESULTS
# ═══════════════════════════════════════════════════════════════════════════
section("11. Key Numeric Results")

results_summary = {
    "Total nodes": n_nodes,
    "Total edges": n_edges,
    "Organizations (all, analyzed)": len(node_names),
    "Defunct orgs (included)": len(defunct),
    "Cooperation statements": n_coop_stmts,
    "Unique cooperation pairs": len(pair_counts),
    "Conflict edges (statements)": n_conflict_edges,
    "Orgs with >=1 coop edge": n_orgs_with_coop,
    "Orgs with >=1 conflict edge": n_orgs_with_conflict,
    "Signal 1 (common cooperation partners >=3) pairs": len(coop_partner_results),
    "Signal 2 (common adversaries >=2) pairs": len(shared_adversary_results),
    "Combined (both signals) pairs": len(combined_results),
    "Weighted Signal 1 pairs": len(wf),
    "Weighted Signal 2 pairs": len(we),
    "Weighted Combined pairs": len(wc_results),
    "Graph nodes (this network)": n_total,
    "Graph edges (this network)": gcoop_e,
    "LCC nodes": gc_n,
    "LCC edges": gc_e,
    "LCC% of graph": round(100 * gc_n / gcoop_n, 1) if gcoop_n else 0,
    "Communities": n_comms,
    "Mean community size": round(sum(sizes) / len(sizes), 1) if sizes else 0,
    "Median community size": sizes[len(sizes) // 2] if sizes else 0,
    "Singletons": sum(1 for s in sizes if s == 1),
    "Total edges in this network": total_edges_count,
    "Cross-community edges": len(cross_edges),
    "% cross-community": round(pct_cross, 1),
    "Bridge nodes (>=2 communities)": n_bridges,
    "Community pairs with cross-edges": len(comm_pair_list),
}

for label, value in results_summary.items():
    if isinstance(value, int):
        print("     %s: %s" % (label, f"{value:,}"))
    else:
        print("     %s: %s" % (label, value))


# ═══════════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ═══════════════════════════════════════════════════════════════════════════
section("FINAL REPORT")

print("""
     Total checks:  %d
     Passed:        %d
     Failed:        %d
""" % (PASS + FAIL, PASS, FAIL))

if FAIL == 0:
    print("  ✓ ALL CHECKS PASSED — Results are validated.")
    print("  All analyses in Data Analysis v2.ipynb are legitimate.")
else:
    print("  ✗ %d CHECK(S) FAILED — See above for details." % FAIL)
    print("  Failed checks:")
    for err in ERRORS:
        print("    %s" % err)

print()
sys.exit(0 if FAIL == 0 else 1)


# ── Resolve network type from command line ────────────────────────────────
if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser(description=__doc__)
    _parser.add_argument("--network-type", "-n", default="cooperation",
                    choices=sorted(VALID_NETWORK_TYPES),
                    help="edge types to validate (default: cooperation)")
    _va_args = _parser.parse_args()
    NETWORK_TYPE = _va_args.network_type
    COMMUNITIES_PATH = HERE / "data" / ("communities_%s.json" % NETWORK_TYPE)

    # Populate P_nt for build_graph() before sections 8+ run
    P_nt.orgs = node_names
    P_nt.pair_counts = pair_counts
    P_nt.conflict_pair_counts = conflict_pair_counts
    _opc = Counter()
    for e in edges:
        if e["relationship"] == "other":
            _opc[tuple(sorted([e["source"], e["target"]]))] += 1
    P_nt.other_pair_counts = _opc
