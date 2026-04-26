"""
compare_articles.py
Compare two articles CSVs and print the differences.

Usage:
    python compare_articles.py --a articles.csv --b articles2.csv
"""

import csv
import re
import argparse
from pathlib import Path


def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def extract_oldid(url):
    m = re.search(r"oldid=(\d+)", url)
    return int(m.group(1)) if m else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", default="articles.csv",  help="First (reference) CSV")
    parser.add_argument("--b", default="articles2.csv", help="Second CSV to compare against A")
    args = parser.parse_args()

    rows_a = load_csv(args.a)
    rows_b = load_csv(args.b)

    map_a = {r["folder_name"]: r for r in rows_a}
    map_b = {r["folder_name"]: r for r in rows_b}

    keys_a = set(map_a)
    keys_b = set(map_b)

    only_in_a = keys_a - keys_b
    only_in_b = keys_b - keys_a
    common    = keys_a & keys_b

    print(f"\nFile A: {args.a}  ({len(rows_a)} rows)")
    print(f"File B: {args.b}  ({len(rows_b)} rows)")
    print(f"Common: {len(common)}")

    # Articles missing from B
    if only_in_a:
        print(f"\n--- Only in A ({len(only_in_a)}) ---")
        for k in sorted(only_in_a):
            print(f"  {k}")

    # Articles new in B
    if only_in_b:
        print(f"\n--- Only in B ({len(only_in_b)}) ---")
        for k in sorted(only_in_b):
            print(f"  {k}")

    # Title or URL differences for shared articles
    title_diffs = []
    oldid_diffs = []
    for k in sorted(common):
        a, b = map_a[k], map_b[k]
        if a["title"] != b["title"]:
            title_diffs.append((k, a["title"], b["title"]))

        oldid_a = extract_oldid(a["url"])
        oldid_b = extract_oldid(b["url"])
        if oldid_a != oldid_b:
            oldid_diffs.append((k, oldid_a, oldid_b))

    if title_diffs:
        print(f"\n--- Title differences ({len(title_diffs)}) ---")
        for k, ta, tb in title_diffs:
            print(f"  {k}")
            print(f"    A: {ta}")
            print(f"    B: {tb}")

    if oldid_diffs:
        print(f"\n--- oldid differences ({len(oldid_diffs)}) ---")
        same_oldid     = sum(1 for _, oa, ob in oldid_diffs if oa == ob)
        b_newer        = sum(1 for _, oa, ob in oldid_diffs if oa and ob and ob > oa)
        a_newer        = sum(1 for _, oa, ob in oldid_diffs if oa and ob and oa > ob)
        missing_either = sum(1 for _, oa, ob in oldid_diffs if oa is None or ob is None)

        print(f"  B has newer oldid:  {b_newer}")
        print(f"  A has newer oldid:  {a_newer}")
        print(f"  Missing in either:  {missing_either}")

    if not (only_in_a or only_in_b or title_diffs or oldid_diffs):
        print("\nFiles are identical.")


if __name__ == "__main__":
    main()