"""
build_evidence_shards.py

Pre-splits per-org evidence (description + incident edges) into a fixed
number of static JSON shards, keyed by FNV-1a hash of the organization
name. The connection finder fetches the single shard it needs at runtime
by hashing the org name client-side — no manifest or server-side lookup
is required.

The bucket count must match EVIDENCE_BUCKETS in build_static_pages.py.

Output: <output>/000.json … NNN.json. Each file:
  { "<org standard_name>": { "description": str|null, "edges": [ <edge>, … ] }, … }

Usage:
    python build_evidence_shards.py --input ../data/crimenet.json --output ../app/data/evidence
"""

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path

DEFAULT_BUCKETS = 128  # MUST match EVIDENCE_BUCKETS in build_static_pages.py


def fnv1a(name: str) -> int:
    """32-bit FNV-1a over the UTF-8 bytes of the name. Reproduced in the
    connection-finder JS with TextEncoder so both sides agree byte-for-byte."""
    h = 0x811C9DC5
    for b in name.encode("utf-8"):
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def bucket_index(name: str, n_buckets: int) -> int:
    return fnv1a(name) % n_buckets


def build(input_path: Path, output_dir: Path, n_buckets: int) -> None:
    data = json.loads(input_path.read_text("utf-8"))
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    # Node description by standard_name (only keep non-empty ones).
    desc = {}
    for nd in nodes:
        name = nd.get("standard_name")
        if not name:
            continue
        d = nd.get("description")
        desc[name] = d.strip() if isinstance(d, str) and d.strip() else None

    # Incident edges per org — every edge that touches the org in either
    # direction. The frontend derives `_other` itself, so edges are stored
    # verbatim.
    incident = defaultdict(list)
    skipped = 0
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if not s or not t or s == t:
            skipped += 1
            continue
        incident[s].append(e)
        incident[t].append(e)

    # Union of every org that has a description and/or any edge.
    org_names = set(desc) | set(incident)

    buckets = defaultdict(dict)
    for name in org_names:
        entry = {
            "description": desc.get(name),
            "edges": incident.get(name, []),
        }
        # Skip orgs with nothing to lazy-load at all.
        if entry["description"] is None and not entry["edges"]:
            continue
        buckets[bucket_index(name, n_buckets)][name] = entry

    output_dir.mkdir(parents=True, exist_ok=True)
    # Clear stale shard files so a shrunk dataset can't leave orphans behind.
    for old in output_dir.glob("*.json"):
        old.unlink()

    width = max(3, len(str(n_buckets - 1)))
    total_orgs = 0
    sizes = []
    for i in range(n_buckets):
        payload = buckets.get(i, {})
        total_orgs += len(payload)
        fp = output_dir / f"{i:0{width}d}.json"
        fp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), "utf-8")
        sizes.append(fp.stat().st_size)

    nonempty = sum(1 for s_ in sizes if s_ > 2)  # "{}" is 2 bytes
    print(f"✓ {total_orgs} orgs → {n_buckets} buckets in {output_dir}/")
    print(f"  {nonempty} non-empty | largest {max(sizes)/1024:.1f} KB | "
          f"avg {sum(sizes)/len(sizes)/1024:.1f} KB | skipped {skipped} bad edges")
    if max(sizes) > 500 * 1024:
        print("  ⚠ a bucket exceeds 500 KB raw — consider raising --buckets")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", "-i", default="data/crimenet.json")
    ap.add_argument("--output", "-o", default="app/data/evidence")
    ap.add_argument("--buckets", "-b", type=int, default=DEFAULT_BUCKETS,
                    help=f"number of shard files (default {DEFAULT_BUCKETS}; "
                         f"must match EVIDENCE_BUCKETS in build_static_pages.py)")
    args = ap.parse_args()
    build(Path(args.input), Path(args.output), args.buckets)


if __name__ == "__main__":
    main()