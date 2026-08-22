"""
tools/coverage_statistics.py

Read a crimenet snapshot (default: data/crimenet.json) and write a JSON
report of every organization with no dedicated Wikipedia article — i.e.
nodes where own_sources is empty but mentioned_in is not. These are the
graph's coverage gaps: each org might warrant adding its Wikipedia article
to page_hyperlinks.csv if one exists.

This is NOT part of the audit pipeline — it is an optional statistics
report. It does not produce corrections and makes no LLM calls.

The report has a `summary` block (totals + percentage) and an `orgs` list
sorted by mention count descending, so the orgs most worth chasing up appear
first. Each org keeps the title + url of every article that mentions it.

Usage:
    python tools/coverage_statistics.py
    python tools/coverage_statistics.py --input data/crimenet.json
"""

import json
import sys
import argparse
from pathlib import Path

# judge.py lives in audit/; make it importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "audit"))
import judge


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default=judge.root("data", "crimenet.json"))
    default_out = str(Path(__file__).resolve().parent / "data" / "coverage_statistics.json")
    parser.add_argument("--output", "-o", default=default_out)
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        print(f"Input not found: {in_path}")
        return

    data = json.loads(in_path.read_text("utf-8"))
    nodes = data.get("nodes", [])

    mentioned_only = [
        n for n in nodes
        if not (n.get("own_sources") or []) and (n.get("mentioned_in") or [])
    ]

    # Most-mentioned first (most worth investigating), then alphabetical.
    mentioned_only.sort(
        key=lambda n: (-len(n.get("mentioned_in") or []),
                       n["standard_name"].lower())
    )

    def norm_mention(src):
        """Normalize a mentioned_in entry to {title, url}, tolerating bare
        string URLs from older snapshots."""
        if isinstance(src, dict):
            return {"title": src.get("title", ""), "url": src.get("url", "")}
        return {"title": "", "url": str(src)}

    total = len(nodes)
    n_gaps = len(mentioned_only)
    pct = round(100 * n_gaps / total, 1) if total else 0.0

    report = {
        "summary": {
            "total_nodes": total,
            "coverage_gaps": n_gaps,
            "pct": pct,
        },
        "orgs": [
            {
                "name": node["standard_name"],
                "mention_count": len(node.get("mentioned_in") or []),
                "mentioned_in": [norm_mention(s)
                                 for s in (node.get("mentioned_in") or [])],
            }
            for node in mentioned_only
        ],
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    print(f"Saved → {out_path}")
    print(f"  {n_gaps} of {total} orgs ({pct}%) are only mentioned, "
          f"never have their own article")


if __name__ == "__main__":
    main()
