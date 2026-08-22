"""
7_apply_corrections.py

Read crimenet_raw.json (the pipeline's raw merge output), apply every
correction — both auto-suggestions from audit_data/ (confident LLM judgments)
and hand-curated overrides from curated_corrections.py — and write crimenet.json.

This is the bridge between the pipeline and the corrected dataset. Run it
after the pipeline produces crimenet_raw.json, and before building the app.

Two sources of corrections, merged with manual winning on conflict:
  1. auto-suggestions  — audit/audit_data/*.py (confident entries only;
                          low-confidence ones are skipped)
  2. manual overrides  — curated_corrections.py (your baseline; add fixes here)

Corrections applied, in order:
  1. TO_BE_EXCLUDED        — drop umbrella/collective + non-criminal entity nodes + their edges
  2. KNOWN_DUPLICATES       — merge variant names into their canonical
  3. BLOCKLIST              — split nodes that were wrongly merged
  4. NODE_OVERRIDES         — force specific fields on named nodes
  5. EDGE_BLOCKLIST         — drop edges whose evidence doesn't hold up
  6. COUNTRY_LINK_BLOCKLIST — drop country links only incidentally mentioned

Makes NO LLM calls. Uses the same fold() normalization as step 4 and all
other audit tools so matching is always case/accent-insensitive.

Usage:
    python 7_apply_corrections.py
"""

import argparse
import importlib.util
import json
import re
import subprocess
import sys
import unicodedata
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import judge


def fold(s):
    """NFKD-decompose, drop combining marks, lowercase, alphanumeric-only."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(s))
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return "".join(c for c in ascii_str.lower() if c.isalnum())


def extract_wiki_title(url):
    """Best-effort Wikipedia article title from a versioned URL."""
    if not url:
        return "Wikipedia"
    m = re.search(r"title=([^&]+)", url)
    if m:
        raw = m.group(1)
    elif "/wiki/" in url:
        raw = url.split("/wiki/")[-1].split("?")[0].split("#")[0]
    else:
        return "Wikipedia"
    return unquote(raw).replace("_", " ")


def _coerce_override_value(field, value):
    """Normalize string-y override values to real Python types so a
    NODE_OVERRIDES is_defunct='false' actually becomes the boolean False."""
    if isinstance(value, str):
        low = value.strip().lower()
        if field == "is_defunct":
            if low in ("false", "no", "0", ""):
                return False
            if low in ("true", "yes", "1"):
                return True
            if low in ("unknown", "unsure"):
                return "unknown"
        if low in ("null", "none", "n/a"):
            return None
    return value


# ── loading ────────────────────────────────────────────────────────────────────

def load_curated_data(path):
    """Import a curated_corrections.py and return its six dicts. Missing ones → empty."""
    spec = importlib.util.spec_from_file_location("curated_data_for_apply", path)
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(Path(path).resolve().parent))
    spec.loader.exec_module(mod)
    g = lambda n, d: getattr(mod, n, d)
    return {
        "KNOWN_DUPLICATES": g("KNOWN_DUPLICATES", {}),
        "NODE_OVERRIDES": g("NODE_OVERRIDES", {}),
        "TO_BE_EXCLUDED": g("TO_BE_EXCLUDED", set()),
        "BLOCKLIST": g("BLOCKLIST", {}),
        "EDGE_BLOCKLIST": g("EDGE_BLOCKLIST", {}),
        "COUNTRY_LINK_BLOCKLIST": g("COUNTRY_LINK_BLOCKLIST", {}),
    }


def load_auto_suggestions(audit_data_dir):
    """Import every audit_data/*.py suggestion file. Only the confident dicts
    are returned (e.g., BLOCKLIST, not BLOCKLIST_LOW_CONFIDENCE). Missing files
    are silently skipped. Returns a dict with the same shape as load_curated_data.

    The files and their dict names:
      0_node_blocklist.py        → BLOCKLIST
      1_known_duplicates.py      → KNOWN_DUPLICATES
      2_edge_blocklist.py        → EDGE_BLOCKLIST
      3_country_link_blocklist.py → COUNTRY_LINK_BLOCKLIST
      4_umbrella_orgs.py         → TO_BE_EXCLUDED
      5_non_criminal_orgs.py     → TO_BE_EXCLUDED (unioned with audit 4)
    """
    ad = Path(audit_data_dir)
    if not ad.is_dir():
        return {}

    file_map = {
        "0_node_blocklist.py":        {"BLOCKLIST": "BLOCKLIST"},
        "1_known_duplicates.py":      {"KNOWN_DUPLICATES": "KNOWN_DUPLICATES"},
        "2_edge_blocklist.py":        {"EDGE_BLOCKLIST": "EDGE_BLOCKLIST"},
        "3_country_link_blocklist.py": {"COUNTRY_LINK_BLOCKLIST": "COUNTRY_LINK_BLOCKLIST"},
        "4_umbrella_orgs.py":         {"TO_BE_EXCLUDED": "TO_BE_EXCLUDED"},
        "5_non_criminal_orgs.py":     {"TO_BE_EXCLUDED": "TO_BE_EXCLUDED"},
    }

    result = {
        "KNOWN_DUPLICATES": {},
        "NODE_OVERRIDES": {},
        "TO_BE_EXCLUDED": set(),
        "BLOCKLIST": {},
        "EDGE_BLOCKLIST": {},
        "COUNTRY_LINK_BLOCKLIST": {},
    }

    for filename, mapping in file_map.items():
        fpath = ad / filename
        if not fpath.exists():
            continue
        try:
            spec = importlib.util.spec_from_file_location(
                f"auto_{filename.replace('.py', '')}", str(fpath))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            for src_attr, dst_key in mapping.items():
                val = getattr(mod, src_attr, None)
                if val is None:
                    continue
                # Union/merge into the accumulator so several files can feed the
                # same correction (TO_BE_EXCLUDED comes from BOTH audit 4 and 5).
                cur = result.get(dst_key)
                if isinstance(cur, set) and isinstance(val, (set, frozenset)):
                    cur |= set(val)
                elif isinstance(cur, dict) and isinstance(val, dict):
                    cur.update(val)
                else:
                    result[dst_key] = val
        except Exception as e:
            print(f"  (auto-load: skipping {filename} — {e})")

    return result


def load_verdict_vetoes(path):
    """From audit 6's report (6_llm_verdicts.json), collect the suggestions it
    judged WRONG at HIGH confidence. A 'rejected' entry with confidence 'high'
    means audit 6 confidently CONTRADICTS the original audit (same↔different),
    regardless of the report's --min-confidence.

    Scoped to the two IDENTITY corrections only — wrong-merge BLOCKLIST and
    duplicate KNOWN_DUPLICATES — where a confident-but-wrong auto-apply
    structurally damages the graph (a spurious split or merge) AND "don't apply
    on disagreement" introduces no wart (the node simply stays merged, or stays
    separate). The other auto audits are deliberately NOT vetoed, because there
    audit 6's contradicting verdict is itself error-prone and vetoing it would
    re-admit clearly-bad data: it keeps spurious country links (it counts
    attacks on US bases abroad as a US footprint) and keeps umbrella terms it
    misreads as specific orgs (it defended "Mafia" as a single organization).

    Returns per-kind veto structures, or None if there's no usable report."""
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        rep = json.loads(p.read_text("utf-8"))
    except Exception as e:
        print(f"  (audit-6 veto: could not read {p.name} — {e})")
        return None
    v = rep.get("verdicts", {})

    def hi(kind):
        return [e for e in v.get(kind, {}).get("rejected", [])
                if str(e.get("confidence", "")).lower() == "high"]

    return {
        "blocklist":  {frozenset((fold(e.get("outer")), fold(e.get("inner")))) for e in hi("blocklist")},
        "duplicates": {frozenset((fold(e.get("outer")), fold(e.get("inner")))) for e in hi("duplicates")},
    }


def apply_verdict_vetoes(auto, vetoes):
    """Drop the wrong-merge / duplicate auto-suggestions audit 6 high-confidence
    contradicted. Mutates `auto` in place (auto only — manual/curated overrides
    are never vetoed). Returns a Counter of how many entries were suppressed."""
    counts = Counter()
    if not vetoes:
        return counts

    for dict_key, veto_key in (("BLOCKLIST", "blocklist"),
                               ("KNOWN_DUPLICATES", "duplicates")):
        d = auto.get(dict_key) or {}
        for canon in list(d):
            kept = {x for x in d[canon]
                    if frozenset((fold(canon), fold(x))) not in vetoes[veto_key]}
            counts[dict_key] += len(set(d[canon])) - len(kept)
            if kept:
                d[canon] = kept
            else:
                del d[canon]

    return counts


def merge_corrections(auto, manual):
    """Merge auto-suggestions (base) with manual overrides. Manual always wins
    on conflict: when both define the same key, manual's value is kept.

    For dict-type corrections (BLOCKLIST, KNOWN_DUPLICATES, etc.): auto +
    manual on top. For set-type (TO_BE_EXCLUDED): union. NODE_OVERRIDES is
    a dict; manual keys override auto."""
    merged = {}
    all_keys = set(auto.keys()) | set(manual.keys())
    for key in all_keys:
        auto_val = auto.get(key)
        manual_val = manual.get(key)

        if key == "TO_BE_EXCLUDED":
            # set: union
            if isinstance(auto_val, set) and isinstance(manual_val, set):
                merged[key] = auto_val | manual_val
            else:
                merged[key] = auto_val if auto_val else manual_val
        elif isinstance(auto_val, dict) and isinstance(manual_val, dict):
            # dict: manual overrides auto on key conflict
            merged[key] = dict(auto_val)
            merged[key].update(manual_val)
        elif manual_val or (isinstance(manual_val, dict) and not manual_val):
            # manual is truthy (or explicitly empty dict) → use it
            merged[key] = manual_val
        else:
            merged[key] = auto_val

    return merged


def build_url_to_org(txts_dir):
    """Map every article URL → canonical org name (from its org_profile.json).
    Returns a dict: url (raw, as stored in own_sources/source_urls) → canonical name."""
    url_to_canonical = {}
    if not txts_dir.exists():
        return url_to_canonical
    for folder in sorted(txts_dir.iterdir()):
        if not folder.is_dir():
            continue
        uf = folder / "url.txt"
        if not uf.exists():
            continue
        url = uf.read_text("utf-8").strip()
        if not url:
            continue
        pf = folder / "org_profile.json"
        if not pf.exists():
            continue
        try:
            prof = json.loads(pf.read_text("utf-8"))
        except Exception:
            continue
        # step 3 writes a flat org_profile.json with canonical_name at top level.
        cn = (prof.get("canonical_name") or prof.get("standard_name") or "").strip()
        if cn:
            url_to_canonical[url] = cn
    return url_to_canonical


# ── correction steps ───────────────────────────────────────────────────────────

def apply_to_be_excluded(nodes, edges, to_be_excluded):
    """Drop nodes whose canonical name is in TO_BE_EXCLUDED, and every edge
    touching them. Returns (kept_nodes, kept_edges, n_dropped)."""
    excluded_folds = {fold(n) for n in to_be_excluded if n and str(n).strip()}
    if not excluded_folds:
        return nodes, edges, 0

    kept_nodes = []
    dropped_node_folds = set()
    for n in nodes:
        if fold(n.get("standard_name", "")) in excluded_folds:
            dropped_node_folds.add(fold(n.get("standard_name", "")))
        else:
            kept_nodes.append(n)

    kept_edges = []
    dropped_edge_count = 0
    for e in edges:
        sf = fold(e.get("source", ""))
        tf = fold(e.get("target", ""))
        if sf in dropped_node_folds or tf in dropped_node_folds:
            dropped_edge_count += 1
        else:
            kept_edges.append(e)

    n_dropped = len(nodes) - len(kept_nodes)
    if n_dropped:
        print(f"  TO_BE_EXCLUDED: dropped {n_dropped} node(s), "
              f"{dropped_edge_count} edge(s)")
    return kept_nodes, kept_edges, n_dropped


def apply_known_duplicates(nodes, edges, known_duplicates):
    """Merge variant names into their canonical. Uses union-find to handle
    cascading (A→B, B→C → all three merge). Returns (merged_nodes, merged_edges,
    n_merged)."""
    if not known_duplicates:
        return nodes, edges, 0

    # Build fold → node index lookup
    fold_to_idx = {}
    for i, n in enumerate(nodes):
        f = fold(n.get("standard_name", ""))
        if f:
            fold_to_idx.setdefault(f, i)

    # Union-find over node indices
    parent = list(range(len(nodes)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[ry] = rx

    # Union each canonical→variant pair. Track the user-specified canonical for
    # each cluster so we can prefer it over the longest-name heuristic.
    for canonical, variants in known_duplicates.items():
        ci = fold_to_idx.get(fold(canonical))
        if ci is None:
            continue
        for v in variants:
            vi = fold_to_idx.get(fold(v))
            if vi is not None:
                union(ci, vi)

    # Build clusters
    clusters = defaultdict(list)
    for i in range(len(nodes)):
        clusters[find(i)].append(i)

    # Build a set of user-specified canonical folds per cluster root
    specified_canonical_folds = defaultdict(set)
    for canonical in known_duplicates:
        fc = fold(canonical)
        ci = fold_to_idx.get(fc)
        if ci is not None:
            specified_canonical_folds[find(ci)].add(fc)

    # Merge each cluster (skip singletons)
    merged_nodes = []
    idx_to_new_idx = {}
    n_merged = 0

    for root, members in sorted(clusters.items()):
        if len(members) == 1:
            idx_to_new_idx[members[0]] = len(merged_nodes)
            merged_nodes.append(nodes[members[0]])
            continue

        # Pick canonical: prefer profiled nodes, then a user-specified
        # canonical, then fall back to longest standard_name (then
        # alphabetical). Profiled-first prevents unprofiled nodes from
        # winning on name length and erasing the profiled node's is_defunct
        # and other authoritative fields (the inheritance guard at line 466
        # checks not chosen.get(field), and "unknown" is truthy, so is_defunct
        # with a real value like False would never be copied).
        member_nodes = [nodes[i] for i in members]
        specified_folds = specified_canonical_folds.get(root, set())
        chosen = sorted(member_nodes,
                       key=lambda n: (
                           -(n.get("profiled") or False),
                           -(fold(n["standard_name"]) in specified_folds),
                           -len(n["standard_name"]),
                           n["standard_name"].lower()))[0]
        chosen_name = chosen["standard_name"]

        # Merge all members into the chosen one
        aliases = []
        seen_alias_folds = {fold(chosen_name)}
        for a in chosen.get("aliases") or []:
            fa = fold(a)
            if fa not in seen_alias_folds:
                seen_alias_folds.add(fa)
                aliases.append(a)

        original_text_names = set(chosen.get("original_text_names") or [])
        own_sources = list(chosen.get("own_sources") or [])
        seen_own_urls = {s.get("url") for s in own_sources if isinstance(s, dict)}
        country_links = list(chosen.get("country_links") or [])
        descriptions = [chosen.get("description")] if chosen.get("description") else []
        time_periods = [chosen.get("time_period")] if chosen.get("time_period") else []
        all_source_urls = set()
        for s in chosen.get("own_sources") or []:
            if isinstance(s, dict) and s.get("url"):
                all_source_urls.add(s["url"])
        for m in chosen.get("mentioned_in") or []:
            if isinstance(m, dict) and m.get("url"):
                all_source_urls.add(m["url"])

        for mn in member_nodes:
            if mn is chosen:
                continue
            n_merged += 1
            # Add the variant's standard_name as an alias so edges pointing
            # to it resolve to the canonical.
            vsn = mn.get("standard_name", "").strip()
            if vsn and fold(vsn) not in seen_alias_folds:
                seen_alias_folds.add(fold(vsn))
                aliases.append(vsn)
            # Aliases
            for a in mn.get("aliases") or []:
                fa = fold(a)
                if fa not in seen_alias_folds:
                    seen_alias_folds.add(fa)
                    aliases.append(a)
            # Original text names
            original_text_names.update(mn.get("original_text_names") or [])
            # Own sources
            for s in mn.get("own_sources") or []:
                url = s.get("url") if isinstance(s, dict) else None
                if url and url not in seen_own_urls:
                    seen_own_urls.add(url)
                    own_sources.append(s)
            # Accumulate source URLs for mentioned_in rebuild
            for s in mn.get("own_sources") or []:
                if isinstance(s, dict) and s.get("url"):
                    all_source_urls.add(s["url"])
            for m in mn.get("mentioned_in") or []:
                if isinstance(m, dict) and m.get("url"):
                    all_source_urls.add(m["url"])
            # Description / time period
            if mn.get("description"):
                descriptions.append(mn["description"])
            if mn.get("time_period"):
                time_periods.append(mn["time_period"])
            # Country links — accumulate distinct evidence quotes per country
            # so multi-language profiles both contribute visible entries.
            for cl in mn.get("country_links") or []:
                cn = (cl.get("country") if isinstance(cl, dict) else cl)
                existing = [c for c in country_links
                          if fold((c.get("country") if isinstance(c, dict) else c)) == fold(cn)]
                if not existing:
                    country_links.append(cl)
                elif all(
                    fold((c.get("evidence_quote") if isinstance(c, dict) else c) or "") !=
                    fold((cl.get("evidence_quote") if isinstance(cl, dict) else cl) or "")
                    for c in existing
                ):
                    country_links.append(cl)
            # If this member was profiled and the chosen wasn't, use its profile data
            if mn.get("profiled") and not chosen.get("profiled"):
                for field in ("description", "country", "time_period", "is_defunct",
                             "founded_year", "dissolved_year"):
                    if not chosen.get(field) and mn.get(field):
                        chosen[field] = mn[field]

        # Rebuild mentioned_in: all source URLs minus own URLs
        mentioned_in = [
            {"url": u, "title": extract_wiki_title(u)}
            for u in sorted(all_source_urls) if u not in seen_own_urls
        ]

        chosen["aliases"] = sorted(aliases, key=lambda x: x.lower())
        chosen["original_text_names"] = sorted(original_text_names)
        chosen["own_sources"] = own_sources
        chosen["mentioned_in"] = mentioned_in
        chosen["country_links"] = country_links
        if descriptions:
            chosen["description"] = descriptions[0]  # keep the canonical's description
        if time_periods:
            chosen["time_period"] = time_periods[0]

        for member in members:
            idx_to_new_idx[member] = len(merged_nodes)
        merged_nodes.append(chosen)

    # Update edges: re-point source/target to the merged canonical
    name_to_merged = {}
    for new_idx, n in enumerate(merged_nodes):
        f = fold(n["standard_name"])
        if f:
            name_to_merged[f] = n["standard_name"]
        for a in n.get("aliases") or []:
            fa = fold(a)
            if fa and fa not in name_to_merged:
                name_to_merged[fa] = n["standard_name"]
        for otn in n.get("original_text_names") or []:
            fotn = fold(otn)
            if fotn and fotn not in name_to_merged:
                name_to_merged[fotn] = n["standard_name"]

    merged_edges = []
    for e in edges:
        sf = fold(e.get("source", ""))
        tf = fold(e.get("target", ""))
        new_s = name_to_merged.get(sf, e.get("source", ""))
        new_t = name_to_merged.get(tf, e.get("target", ""))
        if fold(new_s) == fold(new_t):
            continue  # self-loop after merge
        e["source"] = new_s
        e["target"] = new_t
        merged_edges.append(e)

    if n_merged:
        print(f"  KNOWN_DUPLICATES: merged {n_merged} variant(s) "
              f"({len(clusters) - len(merged_nodes) + n_merged} cluster(s))")
    return merged_nodes, merged_edges, n_merged


def apply_blocklist(nodes, edges, blocklist, url_to_canonical):
    """Split nodes that were wrongly merged by auto-dedup. For each blocked pair
    (A, B) that ended up in the same node, uses article URL provenance from
    data/txts/ to determine which data belongs to which org. Returns (split_nodes,
    split_edges, n_split)."""
    if not blocklist:
        return nodes, edges, 0

    # Build lookup: fold → node index
    def node_folds(n):
        """All folds a node is known by."""
        fs = {fold(n.get("standard_name", ""))}
        for a in n.get("aliases") or []:
            fs.add(fold(a))
        for otn in n.get("original_text_names") or []:
            fs.add(fold(otn))
        fs.discard("")
        return fs

    n_split = 0
    result_nodes = list(nodes)
    result_edges = list(edges)

    seen_pairs = set()
    for a_name, b_names in blocklist.items():
        fa = fold(a_name)
        if not fa:
            continue
        for b_name in b_names:
            fb = fold(b_name)
            if not fb or (fa, fb) in seen_pairs:
                continue
            seen_pairs.add((fa, fb))
            seen_pairs.add((fb, fa))

            # Find if both names are in the same node
            a_idx = b_idx = None
            for i, n in enumerate(result_nodes):
                fs = node_folds(n)
                if fa in fs and fb in fs:
                    a_idx = b_idx = i
                    break
                if fa in fs and a_idx is None:
                    a_idx = i
                if fb in fs and b_idx is None:
                    b_idx = i

            if a_idx is None or b_idx is None:
                continue  # one or both names don't exist
            if a_idx != b_idx:
                continue  # already separate — nothing to split

            # Both names are in the same node — split it
            merged_node = result_nodes[a_idx]
            merged_node_folds = node_folds(merged_node)

            # Determine which own_sources belong to A vs B
            a_own = []
            b_own = []
            a_urls = set()
            b_urls = set()
            for s in merged_node.get("own_sources") or []:
                url = s.get("url") if isinstance(s, dict) else str(s)
                profiled_canon = url_to_canonical.get(url, "")
                if fold(profiled_canon) == fa:
                    a_own.append(s)
                    a_urls.add(url)
                elif fold(profiled_canon) == fb:
                    b_own.append(s)
                    b_urls.add(url)
                elif fold(profiled_canon) in merged_node_folds:
                    # profiles some other name that folded into this node
                    a_own.append(s)  # keep with the canonical (A)
                    a_urls.add(url)
                else:
                    # unknown — keep with A
                    a_own.append(s)
                    a_urls.add(url)

            # Partition source URLs (for mentioned_in calculation)
            a_source_urls = set(a_urls)
            b_source_urls = set(b_urls)
            for m in merged_node.get("mentioned_in") or []:
                url = m.get("url") if isinstance(m, dict) else str(m)
                profiled_canon = url_to_canonical.get(url, "")
                if fold(profiled_canon) == fa:
                    a_source_urls.add(url)
                elif fold(profiled_canon) == fb:
                    b_source_urls.add(url)
                else:
                    a_source_urls.add(url)  # keep with A

            # Partition original_text_names
            a_otn = set()
            b_otn = set()
            for otn in merged_node.get("original_text_names") or []:
                fotn = fold(otn)
                if fotn == fb:
                    b_otn.add(otn)
                else:
                    a_otn.add(otn)

            # Partition aliases
            a_aliases = []
            b_aliases = []
            for al in merged_node.get("aliases") or []:
                fal = fold(al)
                if fal == fb:
                    b_aliases.append(al)
                else:
                    a_aliases.append(al)

            # If B already exists as its own node, the split-out data is merged
            # into it (handled below). Otherwise we'd create a fresh B node.
            existing_b_idx = None
            for i, n in enumerate(result_nodes):
                if i != a_idx and fb in node_folds(n):
                    existing_b_idx = i
                    break

            # Skip the split when it would only produce an empty husk: B has no
            # article of its own (b_own empty) and no edge would move to it, so
            # splitting just adds a content-less node (and a content-less page).
            # Leave A — with B as an alias — untouched instead.
            if existing_b_idx is None and not b_own:
                b_has_edge = any(
                    (fold(e.get("source", "")) in merged_node_folds
                     or fold(e.get("target", "")) in merged_node_folds)
                    and any(fold(url_to_canonical.get(u, "")) == fb
                            for u in (e.get("source_urls") or []))
                    for e in result_edges)
                if not b_has_edge:
                    print(f"  BLOCKLIST: '{b_name}' kept as an alias of '{a_name}' "
                          f"(no own article or edges to split out)")
                    continue

            # Build node for B.  Start from unprofiled defaults (matching
            # step 4's output for nodes with no profile of their own), so
            # that if B is unprofiled it does not inherit A's profile
            # fields or country_links.
            b_node = {
                "standard_name": b_name,
                "aliases": sorted(b_aliases, key=lambda x: x.lower()),
                "original_text_names": sorted(b_otn),
                "description": None,
                "country": None,
                "country_links": [],
                "time_period": None,
                "is_defunct": "unknown",
                "founded_year": None,
                "dissolved_year": None,
                "profiled": False,
                "own_sources": b_own,
                "mentioned_in": [
                    {"url": u, "title": extract_wiki_title(u)}
                    for u in sorted(b_source_urls) if u not in b_urls
                ],
            }
            # If B has own_sources with profile data, use that profile.
            b_is_profiled = False
            for s in b_own:
                url = s.get("url") if isinstance(s, dict) else str(s)
                profiled_canon = url_to_canonical.get(url, "")
                if fold(profiled_canon) == fb:
                    b_is_profiled = True
                    break
            if b_is_profiled:
                b_node["profiled"] = True
                # Inherit profile fields from the merged node (we cannot
                # separate A's and B's contributions post-merge, so both
                # sides keep the merged data; the BLOCKLIST split at least
                # ensures the two orgs are distinct nodes).
                b_node["description"] = merged_node.get("description")
                b_node["country"] = merged_node.get("country")
                b_node["country_links"] = list(merged_node.get("country_links") or [])
                b_node["time_period"] = merged_node.get("time_period")
                b_node["is_defunct"] = merged_node.get("is_defunct", "unknown")
                b_node["founded_year"] = merged_node.get("founded_year")
                b_node["dissolved_year"] = merged_node.get("dissolved_year")

            # Update node A (in-place)
            merged_node["aliases"] = sorted(a_aliases, key=lambda x: x.lower())
            merged_node["original_text_names"] = sorted(a_otn)
            merged_node["own_sources"] = a_own
            merged_node["mentioned_in"] = [
                {"url": u, "title": extract_wiki_title(u)}
                for u in sorted(a_source_urls) if u not in a_urls
            ]
            merged_node["standard_name"] = a_name  # ensure canonical name

            # (existing_b_idx was computed above, before the empty-husk check.)
            if existing_b_idx is not None:
                # Merge split-out data into existing B node
                existing_b = result_nodes[existing_b_idx]
                existing_folds = node_folds(existing_b)
                # Merge aliases
                ea = list(existing_b.get("aliases") or [])
                for a in b_aliases:
                    if fold(a) not in {fold(x) for x in ea}:
                        ea.append(a)
                existing_b["aliases"] = sorted(ea, key=lambda x: x.lower())
                # Merge own_sources
                eos = list(existing_b.get("own_sources") or [])
                eos_urls = {s.get("url") for s in eos if isinstance(s, dict)}
                for s in b_own:
                    url = s.get("url") if isinstance(s, dict) else str(s)
                    if url not in eos_urls:
                        eos_urls.add(url)
                        eos.append(s)
                existing_b["own_sources"] = eos
                # Merge original_text_names
                otns = set(existing_b.get("original_text_names") or [])
                otns.update(b_otn)
                existing_b["original_text_names"] = sorted(otns)
                # If B side was profiled, upgrade the existing node
                if b_node.get("profiled"):
                    existing_b["profiled"] = True
                    for field in ("description", "country", "time_period", "is_defunct",
                                 "founded_year", "dissolved_year"):
                        if b_node.get(field) and not existing_b.get(field):
                            existing_b[field] = b_node[field]
                # Merge country_links — accumulate distinct evidence quotes
                ecl = list(existing_b.get("country_links") or [])
                ecl_countries = {fold(c.get("country") if isinstance(c, dict) else c) for c in ecl}
                ecl_quotes = {fold((c.get("evidence_quote") if isinstance(c, dict) else c) or "") for c in ecl}
                for cl in b_node.get("country_links") or []:
                    cn = cl.get("country") if isinstance(cl, dict) else cl
                    cq = fold((cl.get("evidence_quote") if isinstance(cl, dict) else cl) or "")
                    if fold(cn) not in ecl_countries or cq not in ecl_quotes:
                        ecl_countries.add(fold(cn))
                        ecl_quotes.add(cq)
                        ecl.append(cl)
                existing_b["country_links"] = ecl
                # Merge mentioned_in
                emi = list(existing_b.get("mentioned_in") or [])
                emi_urls = {m.get("url") for m in emi if isinstance(m, dict)}
                for m in b_node.get("mentioned_in") or []:
                    url = m.get("url") if isinstance(m, dict) else str(m)
                    if url not in emi_urls and url not in eos_urls:
                        emi_urls.add(url)
                        emi.append(m)
                existing_b["mentioned_in"] = emi

                # Edge reassignment: edges to B go to the existing B node's name
                b_final_name = existing_b["standard_name"]
                print(f"  BLOCKLIST: split '{b_name}' out of '{a_name}' "
                      f"(merged into existing '{b_final_name}')")
            else:
                b_final_name = b_name

            # Edge reassignment
            updated_edges = []
            for e in result_edges:
                sf = fold(e.get("source", ""))
                tf = fold(e.get("target", ""))
                # Edges carry source_urls (a list); an edge belongs to B when it
                # was extracted from B's own article. (The old code read a
                # singular "source_url" key that edges never have, so every edge
                # silently stayed with A and the split-out node was orphaned.)
                assign_to_b = any(
                    fold(url_to_canonical.get(u, "")) == fb
                    for u in (e.get("source_urls") or []))

                # Update source/target names
                if sf in merged_node_folds:
                    if assign_to_b:
                        e["source"] = b_final_name
                    else:
                        e["source"] = a_name
                if tf in merged_node_folds:
                    if assign_to_b:
                        e["target"] = b_final_name
                    else:
                        e["target"] = a_name

                # An edge between the merged node and one of its own split-out
                # names collapses onto one side here; drop the resulting
                # self-loop (mirrors apply_known_duplicates).
                if fold(e.get("source", "")) == fold(e.get("target", "")):
                    continue
                updated_edges.append(e)

            result_edges = updated_edges
            if existing_b_idx is None:
                result_nodes.append(b_node)
            n_split += 1
            if existing_b_idx is None:
                print(f"  BLOCKLIST: split '{b_name}' out of '{a_name}'")

    return result_nodes, result_edges, n_split


def apply_node_overrides_func(nodes, node_overrides):
    """Patch fields on named nodes. Returns n_applied."""
    if not node_overrides:
        return 0

    n_applied = 0
    overrides_by_fold = {fold(k): v for k, v in node_overrides.items()}

    for node in nodes:
        ov = overrides_by_fold.get(fold(node["standard_name"]))
        if not ov:
            continue
        n_applied += 1
        for field, value in ov.items():
            if field == "own_sources":
                node["own_sources"] = [
                    {"url": url, "title": extract_wiki_title(url)}
                    for url in value
                ]
            else:
                node[field] = _coerce_override_value(field, value)

    if n_applied:
        print(f"  NODE_OVERRIDES: applied to {n_applied} node(s)")
    return n_applied


def apply_edge_blocklist(edges, edge_blocklist, name_index=None):
    """Drop edges whose evidence doesn't support the claimed relationship.
    Returns (kept_edges, n_dropped).

    `name_index` (fold(any-name) → current standard_name) resolves blocklist
    keys through earlier renames: an entry keyed on a pre-merge variant name
    still matches the edges, which now carry the canonical name."""
    if not edge_blocklist:
        return edges, 0

    name_index = name_index or {}

    def canon_fold(nm):
        return fold(name_index.get(fold(nm), nm))

    # Build lookup: frozenset({foldA, foldB}) → {blocked rels}
    block = defaultdict(set)
    for a, tgts in edge_blocklist.items():
        fa = canon_fold(a)
        for b, rels in tgts.items():
            fb = canon_fold(b)
            if fa and fb:
                block[frozenset((fa, fb))] |= {str(r).strip().lower() for r in rels}

    kept = []
    n_dropped = 0
    for e in edges:
        sf = canon_fold(e.get("source", ""))
        tf = canon_fold(e.get("target", ""))
        rel = (e.get("relationship") or "").strip().lower()
        blk = block.get(frozenset((sf, tf)))
        if blk and (rel in blk or "*" in blk):
            n_dropped += 1
        else:
            kept.append(e)

    if n_dropped:
        print(f"  EDGE_BLOCKLIST: dropped {n_dropped} edge(s)")
    return kept, n_dropped


def strip_home_country_links(nodes):
    """Drop any country_link that points at the node's own home country.

    Step 4 already filters home-country leaks at merge time, but a later
    KNOWN_DUPLICATES merge can union in a duplicate's footprint link to the
    home country (e.g. an it.wiki 'Ndrina profile contributing a 'Canada' link
    to a Canada-based clan), and a NODE_OVERRIDES country change can retro-
    actively turn an existing link into a home-country one. Re-apply the same
    filter here so crimenet.json keeps step 4's invariant (a country is either
    the origin OR a foreign footprint, never both). Returns n_dropped."""
    n_dropped = 0
    for node in nodes:
        home = fold(node.get("country", ""))
        if not home or home == fold("Unknown"):
            continue
        kept = []
        for cl in node.get("country_links") or []:
            cn = (cl.get("country") if isinstance(cl, dict) else cl)
            if fold(cn) == home:
                n_dropped += 1
            else:
                kept.append(cl)
        node["country_links"] = kept
    if n_dropped:
        print(f"  home-country links: dropped {n_dropped} leak(s)")
    return n_dropped


def apply_country_link_blocklist(nodes, country_link_blocklist, name_index=None):
    """Drop country links whose evidence doesn't support a real footprint.
    Returns n_dropped.

    `name_index` (fold(any-name) → current standard_name) resolves blocklist
    keys through earlier renames, so an entry keyed on a pre-merge variant name
    still matches the node that now carries the link."""
    if not country_link_blocklist:
        return 0

    name_index = name_index or {}

    def canon_fold(nm):
        return fold(name_index.get(fold(nm), nm))

    # Build lookup: fold(canonical org) → {country.lower(), …}
    block = defaultdict(set)
    for org, countries in country_link_blocklist.items():
        fo = canon_fold(org)
        if fo:
            block[fo] |= {str(c).strip().lower() for c in countries}

    n_dropped = 0
    for node in nodes:
        fo = fold(node.get("standard_name", ""))
        blocked_countries = block.get(fo)
        if not blocked_countries:
            continue
        kept = []
        for cl in node.get("country_links") or []:
            cn = str((cl.get("country") if isinstance(cl, dict) else cl) or "").strip().lower()
            if cn in blocked_countries:
                n_dropped += 1
            else:
                kept.append(cl)
        node["country_links"] = kept

    if n_dropped:
        print(f"  COUNTRY_LINK_BLOCKLIST: dropped {n_dropped} link(s)")
    return n_dropped


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Apply all curated corrections to crimenet_raw.json and "
                    "write crimenet.json. No LLM calls.")
    ap.add_argument("--raw", "-r",
                    default=judge.root("data", "crimenet_raw.json"),
                    help="raw pipeline output (default data/crimenet_raw.json)")
    ap.add_argument("--cleanup", "-c",
                    default=judge.root("audit", "curated_corrections.py"),
                    help="curated_corrections.py to apply")
    ap.add_argument("--txts", "-t",
                    default=judge.root("data", "txts"),
                    help="txts/ directory (for BLOCKLIST split provenance)")
    ap.add_argument("--output", "-o",
                    default=judge.root("data", "crimenet.json"),
                    help="where to write the corrected dataset")
    ap.add_argument("--auto-dir", "-a",
                    default=judge.data_path(),
                    help="audit_data/ directory with LLM suggestion files "
                         "(default audit/audit_data/; pass empty string to disable)")
    ap.add_argument("--verdicts",
                    default=judge.data_path("6_llm_verdicts.json"),
                    help="audit 6 report; its HIGH-confidence rejections veto the "
                         "matching auto-suggestions (audit 6's second opinion made "
                         "effective). Pass empty string to disable.")
    args = ap.parse_args()

    raw_path = Path(args.raw)
    out_path = Path(args.output)
    txts_dir = Path(args.txts)

    if not raw_path.exists():
        print(f"Raw input not found: {raw_path}")
        sys.exit(2)

    # Load: auto-suggestions first, then manual overrides on top
    auto = {}
    if args.auto_dir:
        auto = load_auto_suggestions(args.auto_dir)
        if auto:
            auto_counts = ", ".join(
                f"{k}={len(v)}" for k, v in auto.items() if v)
            print(f"Auto-suggestions loaded: {auto_counts}")
        # Audit 6's high-confidence rejections veto matching auto-suggestions
        # (a confident-but-wrong auto suggestion the second opinion caught no
        # longer slips through just because the report wasn't reviewed by hand).
        vetoes = load_verdict_vetoes(args.verdicts)
        if vetoes:
            suppressed = apply_verdict_vetoes(auto, vetoes)
            if sum(suppressed.values()):
                print(f"Audit-6 veto: suppressed {sum(suppressed.values())} "
                      f"auto-suggestion(s) it high-confidence rejected "
                      f"({', '.join(f'{k}={v}' for k, v in suppressed.items() if v)})")
    manual = load_curated_data(args.cleanup)
    cd = merge_corrections(auto, manual)
    data = json.loads(raw_path.read_text("utf-8"))
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    n_nodes_start = len(nodes)
    n_edges_start = len(edges)
    print(f"Loaded {n_nodes_start} nodes, {n_edges_start} edges "
          f"from {raw_path.name}")

    # Build URL→org lookup for BLOCKLIST split
    url_to_canonical = build_url_to_org(txts_dir)

    # Apply corrections in order
    total_dropped_nodes = 0
    total_dropped_edges = 0
    total_merged = 0
    total_split = 0

    # 1. TO_BE_EXCLUDED
    nodes, edges, nd = apply_to_be_excluded(nodes, edges, cd["TO_BE_EXCLUDED"])
    total_dropped_nodes += nd

    # 2. KNOWN_DUPLICATES
    nodes, edges, nm = apply_known_duplicates(nodes, edges, cd["KNOWN_DUPLICATES"])
    total_merged += nm

    # 3. BLOCKLIST
    nodes, edges, ns = apply_blocklist(nodes, edges, cd["BLOCKLIST"], url_to_canonical)
    total_split += ns

    # 4. NODE_OVERRIDES
    apply_node_overrides_func(nodes, cd["NODE_OVERRIDES"])

    # Name index over the CURRENT nodes (after merges / splits / overrides), so
    # the edge & country blocklists resolve keys that may be pre-merge variant
    # names to the node/edge endpoint that now carries them. standard_names take
    # priority over aliases on collision.
    name_index = {}
    for n in nodes:
        name_index[fold(n["standard_name"])] = n["standard_name"]
    for n in nodes:
        sn = n["standard_name"]
        for nm in (n.get("aliases") or []) + (n.get("original_text_names") or []):
            name_index.setdefault(fold(nm), sn)

    # 5. EDGE_BLOCKLIST
    edges, nd_edges = apply_edge_blocklist(edges, cd["EDGE_BLOCKLIST"], name_index)
    total_dropped_edges += nd_edges

    # 6. COUNTRY_LINK_BLOCKLIST
    apply_country_link_blocklist(nodes, cd["COUNTRY_LINK_BLOCKLIST"], name_index)

    # 6b. Re-apply step 4's home-country leak filter (merges/overrides above can
    # reintroduce a footprint link to the node's own origin country).
    strip_home_country_links(nodes)

    # A node is "profiled" iff it has its own article (own_sources). Merges and
    # splits move own_sources around, so recompute the flag here rather than
    # trust the value carried through the corrections (step 4's invariant).
    for n in nodes:
        n["profiled"] = bool(n.get("own_sources"))

    # Drop any exact-duplicate edges that re-pointing (merge/split) produced.
    seen_edge_keys, deduped = set(), []
    for e in edges:
        k = (fold(e.get("source", "")), fold(e.get("target", "")),
             (e.get("relationship") or ""), e.get("evidence_quote") or "",
             tuple(e.get("source_urls") or []), tuple(e.get("descriptions") or []))
        if k in seen_edge_keys:
            continue
        seen_edge_keys.add(k)
        deduped.append(e)
    if len(deduped) != len(edges):
        print(f"  edge dedup: removed {len(edges) - len(deduped)} duplicate edge(s)")
    edges = deduped

    # Recompute metadata
    profiled_ct = sum(1 for n in nodes if n.get("profiled"))
    unprofiled_ct = len(nodes) - profiled_ct
    defunct_ct = sum(1 for n in nodes if n.get("is_defunct") is True)

    # Sort
    nodes.sort(key=lambda n: n["standard_name"].lower())
    edges.sort(key=lambda e: (e["source"].lower(), e["target"].lower(),
                               e["relationship"]))

    out_path.write_text(json.dumps({
        "metadata": {
            "n_nodes": len(nodes), "n_edges": len(edges),
            "n_profiled": profiled_ct, "n_unprofiled": unprofiled_ct,
            "n_defunct": defunct_ct,
            "corrections_applied": {
                "nodes_dropped": total_dropped_nodes,
                "nodes_merged": total_merged,
                "nodes_split": total_split,
                "edges_dropped": total_dropped_edges,
            },
            "generated": datetime.now().isoformat(timespec="seconds"),
        },
        "nodes": nodes,
        "edges": edges,
    }, ensure_ascii=False, indent=2), "utf-8")

    print(f"\n  Final: {len(nodes)} nodes ({profiled_ct} profiled, "
          f"{unprofiled_ct} unprofiled, {defunct_ct} defunct), "
          f"{len(edges)} edges")
    print(f"Saved → {out_path}")

    # Sync README stats (best-effort)
    _sync_readme_stats(out_path)


def _sync_readme_stats(output_path):
    """Best-effort: when we just wrote data/crimenet.json, refresh the headline
    counts in README.md. Never fails — a README that can't be updated is not
    an error."""
    repo_root = Path(__file__).resolve().parent.parent
    canonical = repo_root / "data" / "crimenet.json"
    try:
        if Path(output_path).resolve() != canonical.resolve():
            return
        updater = repo_root / "tools" / "update_readme_stats.py"
        if not updater.exists():
            return
        subprocess.run([sys.executable, str(updater)], check=False)
    except Exception as e:
        print(f"  (README stats not updated: {e})")


if __name__ == "__main__":
    main()
