"""
merge_network.py
Merge all per-folder extracted.json into a single global_network.json.
Deduplicates nodes (by standard_name) and edges (by source+target+relationship+detail).

Usage:
    python 3_merge_network.py --dir ./txts --output global_network.json
    python 3_merge_network.py --dir ./txts --output global_network.json --stats
"""

import json
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any
from collections import Counter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def safe_str(value) -> str:
    """Coerce None to '' so len() and string ops never crash."""
    return value if isinstance(value, str) else ""


def merge_nodes(all_nodes: List[Dict]) -> List[Dict]:
    node_map = {}

    for node in all_nodes:
        name = safe_str(node.get("standard_name")).strip()
        if not name:
            continue
        key = normalize_name(name)

        if key not in node_map:
            node_map[key] = {
                "standard_name": name,
                "original_text_names": set(),
                "aliases": set(),
                "type": node.get("type") or "criminal_organization",
                "context": safe_str(node.get("context")),
                "time_period": node.get("time_period") or None,
                "source_articles": set(),
                "wikipedia_urls": set(),
            }

        entry = node_map[key]

        otn = safe_str(node.get("original_text_name"))
        if otn:
            entry["original_text_names"].add(otn)

        for alias in node.get("aliases") or []:
            if alias and isinstance(alias, str):
                entry["aliases"].add(alias)

        new_ctx = safe_str(node.get("context"))
        if len(new_ctx) > len(entry["context"]):
            entry["context"] = new_ctx

        new_tp = safe_str(node.get("time_period"))
        old_tp = safe_str(entry.get("time_period"))
        if len(new_tp) > len(old_tp):
            entry["time_period"] = new_tp

        src = safe_str(node.get("_source_article"))
        if src:
            entry["source_articles"].add(src)

        url = safe_str(node.get("_wikipedia_url"))
        if url:
            entry["wikipedia_urls"].add(url)

    result = []
    for entry in node_map.values():
        tp = entry["time_period"]
        result.append({
            "standard_name": entry["standard_name"],
            "original_text_names": sorted(entry["original_text_names"]),
            "aliases": sorted(entry["aliases"]),
            "type": entry["type"],
            "context": entry["context"],
            "time_period": tp if tp else None,
            "source_articles": sorted(entry["source_articles"]),
            "wikipedia_urls": sorted(entry["wikipedia_urls"]),
        })

    return sorted(result, key=lambda x: x["standard_name"].lower())


def merge_edges(all_edges: List[Dict]) -> List[Dict]:
    edge_map = {}

    for edge in all_edges:
        source = normalize_name(safe_str(edge.get("source")))
        target = normalize_name(safe_str(edge.get("target")))
        rel = safe_str(edge.get("relationship")).strip().lower()
        detail = safe_str(edge.get("detail")).strip().lower()

        if not source or not target or not rel:
            continue

        key = (source, target, rel, detail)

        if key not in edge_map:
            edge_map[key] = {
                "source": safe_str(edge.get("source")).strip(),
                "target": safe_str(edge.get("target")).strip(),
                "relationship": rel,
                "detail": edge.get("detail") if edge.get("detail") else None,
                "context": safe_str(edge.get("context")),
                "time_period": edge.get("time_period") or None,
                "wikipedia_urls": set(),
            }

        entry = edge_map[key]

        new_ctx = safe_str(edge.get("context"))
        if len(new_ctx) > len(entry["context"]):
            entry["context"] = new_ctx

        new_tp = safe_str(edge.get("time_period"))
        old_tp = safe_str(entry.get("time_period"))
        if len(new_tp) > len(old_tp):
            entry["time_period"] = new_tp

        # URL comes from the folder's url.txt, tagged during loading
        url = safe_str(edge.get("_wikipedia_url"))
        if url:
            entry["wikipedia_urls"].add(url)

    result = []
    for entry in edge_map.values():
        tp = entry["time_period"]
        result.append({
            "source": entry["source"],
            "target": entry["target"],
            "relationship": entry["relationship"],
            "detail": entry["detail"],
            "context": entry["context"],
            "time_period": tp if tp else None,
            "wikipedia_urls": sorted(entry["wikipedia_urls"]),
        })

    return sorted(result, key=lambda x: (x["source"].lower(), x["target"].lower()))


def compute_stats(nodes, edges):
    type_counts = Counter(n["type"] for n in nodes)
    rel_counts = Counter(e["relationship"] for e in edges)
    detail_counts = Counter(e["detail"] for e in edges if e.get("detail"))

    edge_count = Counter()
    for e in edges:
        edge_count[e["source"]] += 1
        edge_count[e["target"]] += 1

    most_mentioned = sorted(nodes, key=lambda n: len(n.get("source_articles", [])), reverse=True)[:10]

    return {
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "node_types": dict(type_counts.most_common()),
        "relationship_types": dict(rel_counts.most_common()),
        "detail_types": dict(detail_counts.most_common()),
        "most_mentioned": [
            {"name": n["standard_name"], "articles": len(n.get("source_articles", []))}
            for n in most_mentioned
        ],
        "most_connected": [
            {"name": name, "edges": count}
            for name, count in edge_count.most_common(20)
        ],
    }


def merge(txts_dir: str, output_file: str, show_stats: bool = False):
    root = Path(txts_dir)
    folders = sorted(f for f in root.iterdir() if f.is_dir())

    all_nodes, all_edges = [], []
    loaded, missing = 0, 0

    for folder in folders:
        json_path = folder / "extracted.json"
        if not json_path.exists():
            missing += 1
            continue

        try:
            data = json.loads(json_path.read_text("utf-8"))
            url_path = folder / "url.txt"
            url = url_path.read_text("utf-8").strip() if url_path.exists() else ""

            for node in data.get("nodes", []):
                node["_source_article"] = folder.name
                node["_wikipedia_url"] = url

            for edge in data.get("edges", []):
                edge["_wikipedia_url"] = url

            all_nodes.extend(data.get("nodes", []))
            all_edges.extend(data.get("edges", []))
            loaded += 1
        except Exception as e:
            log.warning(f"  Error loading {json_path}: {e}")

    log.info(f"Loaded {loaded} files ({missing} missing)")
    log.info(f"Raw: {len(all_nodes)} nodes, {len(all_edges)} edges")

    nodes = merge_nodes(all_nodes)
    edges = merge_edges(all_edges)
    log.info(f"After dedup: {len(nodes)} nodes, {len(edges)} edges")

    output = {
        "metadata": {
            "description": "Global Criminal Network — extracted from Wikipedia",
            "source_articles": loaded,
            "total_nodes": len(nodes),
            "total_edges": len(edges),
        },
        "nodes": nodes,
        "edges": edges,
    }

    Path(output_file).write_text(json.dumps(output, ensure_ascii=False, indent=2), "utf-8")
    log.info(f"Saved → {output_file}")

    if show_stats:
        s = compute_stats(nodes, edges)
        print(f"\n{'='*60}\n  NETWORK STATISTICS\n{'='*60}")
        print(f"  Nodes: {s['total_nodes']}  |  Edges: {s['total_edges']}\n")
        print(f"  Node types:")
        for t, c in s["node_types"].items():
            print(f"    {t:35s} {c:5d}")
        print(f"\n  Relationship types:")
        for t, c in s["relationship_types"].items():
            print(f"    {t:35s} {c:5d}")
        if s["detail_types"]:
            print(f"\n  Detail types (for 'other'):")
            for t, c in s["detail_types"].items():
                print(f"    {t:35s} {c:5d}")
        print(f"\n  Most mentioned:")
        for item in s["most_mentioned"]:
            print(f"    {item['name']:40s} ({item['articles']} articles)")
        print(f"\n  Most connected:")
        for item in s["most_connected"]:
            print(f"    {item['name']:40s} ({item['edges']} edges)")
        print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge extracted JSONs into global network")
    parser.add_argument("--dir", "-d", required=True)
    parser.add_argument("--output", "-o", default="global_network.json")
    parser.add_argument("--stats", "-s", action="store_true")
    args = parser.parse_args()
    merge(args.dir, args.output, show_stats=args.stats)