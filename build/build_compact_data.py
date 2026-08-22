#!/usr/bin/env python3
"""Build a compact client-side data file for the browse page's in-panel previews.

Produces app/data/compact.json — a single JSON file with org metadata, country listings,
and sorted name lists. Edges are excluded (the connection finder uses
crimenet_adj.json + evidence shards directly).

Usage:
    python build_compact_data.py --input data/crimenet.json --output app/data/compact.json
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

# Clean up <br> → ", " substitution artifacts from step 1's infobox extraction:
# get_text(" ", strip=True) inserts spaces around the literal ", " commas from
# Wikipedia HTML, producing "California , Arizona" and "Region : , Country".
# Safe because legitimate prose never has whitespace before a comma or colon
# (English/Italian/Spanish/etc.).
_FIX_ARTIFACTS = re.compile(r"\s+,\s+")
_FIX_ARTIFACTS_REPL = ", "
_FIX_COLON = re.compile(r"\s+:")
_FIX_COLON_REPL = ":"
_FIX_COLON_COMMA = re.compile(r":,\s*")
_FIX_COLON_COMMA_REPL = ": "


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="data/crimenet.json", type=Path)
    ap.add_argument("--output", default="app/data/compact.json", type=Path)
    args = ap.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    # Degree per org
    degree: dict[str, int] = defaultdict(int)
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if s:
            degree[s] += 1
        if t:
            degree[t] += 1

    # Build org dicts indexed by standard_name
    orgs: dict[str, dict] = {}
    for n in nodes:
        name = n.get("standard_name")
        if not name:
            continue
        # Prune fields we don't need client-side
        cleaned_links = []
        for fp in (n.get("country_links") or []):
            if isinstance(fp, dict):
                cleaned_links.append({
                    **fp,
                    "evidence_quote": _FIX_COLON_COMMA.sub(_FIX_COLON_COMMA_REPL, _FIX_COLON.sub(_FIX_COLON_REPL, _FIX_ARTIFACTS.sub(_FIX_ARTIFACTS_REPL, fp.get("evidence_quote", "") or ""))),
                    "context": _FIX_COLON_COMMA.sub(_FIX_COLON_COMMA_REPL, _FIX_COLON.sub(_FIX_COLON_REPL, _FIX_ARTIFACTS.sub(_FIX_ARTIFACTS_REPL, fp.get("context", "") or ""))),
                })
            else:
                cleaned_links.append(fp)
        orgs[name] = {
            "aliases": n.get("aliases") or [],
            "description": (n.get("description") or "").strip(),
            "country": n.get("country"),
            "country_links": cleaned_links,
            "time_period": n.get("time_period"),
            "is_defunct": n.get("is_defunct"),
            "founded_year": n.get("founded_year"),
            "dissolved_year": n.get("dissolved_year"),
            "profiled": n.get("profiled", False),
            "own_sources": [s for s in (n.get("own_sources") or []) if s.get("url")],
            "mentioned_in": [s for s in (n.get("mentioned_in") or []) if s.get("url")][:5],
            "degree": degree.get(name, 0),
        }

    # Countries: based_here + footprints_here
    based_by_country: dict[str, list[str]] = defaultdict(list)
    footprints_by_country: dict[str, list[dict]] = defaultdict(list)

    for n in nodes:
        name = n.get("standard_name")
        if not name:
            continue
        c = n.get("country")
        if c and c != "Unknown":
            based_by_country[c].append(name)

        origin = n.get("country")
        home = origin if origin and origin != "Unknown" else None
        for fp in n.get("country_links") or []:
            dest = ((fp.get("country") if isinstance(fp, dict) else fp) or "").strip()
            if not dest:
                continue
            if home and dest == home:
                continue
            footprints_by_country[dest].append({
                "org": name,
                "origin": home,
                "context": _FIX_COLON_COMMA.sub(_FIX_COLON_COMMA_REPL, _FIX_COLON.sub(_FIX_COLON_REPL, _FIX_ARTIFACTS.sub(_FIX_ARTIFACTS_REPL, (fp.get("context") if isinstance(fp, dict) else "") or ""))),
                "quote": _FIX_COLON_COMMA.sub(_FIX_COLON_COMMA_REPL, _FIX_COLON.sub(_FIX_COLON_REPL, _FIX_ARTIFACTS.sub(_FIX_ARTIFACTS_REPL, (fp.get("evidence_quote") if isinstance(fp, dict) else "") or ""))),
                "source_url": (fp.get("source_url") if isinstance(fp, dict) else "") or "",
                "source_title": (fp.get("source_title") if isinstance(fp, dict) else "") or "",
            })

    # Sort org names by degree desc then name
    org_names = sorted(orgs, key=lambda n: (-orgs[n]["degree"], n.lower()))

    # Countries: union of based + footprint destinations
    all_countries = set(based_by_country) | set(footprints_by_country)
    country_total: dict[str, int] = {}
    for c in all_countries:
        country_total[c] = len(based_by_country.get(c, [])) + len(footprints_by_country.get(c, []))
    country_names = sorted(all_countries, key=lambda c: (-country_total[c], c.lower()))

    # Build country dicts
    countries: dict[str, dict] = {}
    for c in all_countries:
        countries[c] = {
            "total": country_total[c],
            "based_here": sorted(
                based_by_country.get(c, []),
                key=lambda n: (-orgs[n]["degree"], n.lower())),
            "footprints_here": sorted(
                footprints_by_country.get(c, []),
                key=lambda f: (-orgs[f["org"]]["degree"], f["org"].lower())),
        }

    compact = {
        "orgs": orgs,
        "countries": countries,
        "org_names": org_names,
        "country_names": country_names,
        "total_orgs": len(org_names),
        "total_countries": len(country_names),
        "total_edges": len(edges),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(compact, ensure_ascii=False), encoding="utf-8")

    size_kb = args.output.stat().st_size / 1024
    print(f"Wrote {len(org_names):,} orgs, {len(country_names)} countries, "
          f"{len(edges):,} edges → {args.output} ({size_kb:,.0f} KB)")


if __name__ == "__main__":
    main()
