"""
run_pipeline.py — run the CRIMENET pipeline end to end, in order.
Stops at the first step that fails so a broken step never feeds the next.

Usage:
    python run_pipeline.py          (from pipeline/)
    python pipeline/run_pipeline.py (from repo root)
"""

import subprocess
import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PIPELINE_DIR.parent

STEPS = [
    [sys.executable, "0_urls_to_articles.py",  "--input", str(REPO_ROOT / "data" / "page_hyperlinks.csv"), "--output", str(REPO_ROOT / "data" / "articles.csv")],
    [sys.executable, "1_fetch_wikipedia.py",   "--csv",   str(REPO_ROOT / "data" / "articles.csv"),        "--output", str(REPO_ROOT / "data" / "txts")],
    [sys.executable, "2_extract_network.py",   "--dir",   str(REPO_ROOT / "data" / "txts")],
    [sys.executable, "3_enrich_nodes.py",      "--dir",   str(REPO_ROOT / "data" / "txts")],
    [sys.executable, "4_merge.py",          "--dir",   str(REPO_ROOT / "data" / "txts"),                "--output", str(REPO_ROOT / "data" / "crimenet_raw.json")],
]

if __name__ == "__main__":
    for i, cmd in enumerate(STEPS):
        # Prepend pipeline/ dir so scripts resolve regardless of cwd
        cmd[1] = str(PIPELINE_DIR / cmd[1])
        print(f"\n=== STEP {i}: {' '.join(cmd)} ===")
        if subprocess.run(cmd).returncode != 0:
            print(f"\nStep {i} failed — stopping.")
            sys.exit(1)

    print("\nPipeline complete → crimenet_raw.json")