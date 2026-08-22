"""
Shared data loading and preprocessing for CRIMENET analysis scripts.

Usage:
    from common import load_and_preprocess
    data, P = load_and_preprocess()
    # P.orgs, P.defunct, P.coop, P.conflict, P.all_edges,
    # P.pair_counts, P.conflict_pair_counts, P.org_country
"""

import json
from collections import defaultdict, Counter
from dataclasses import dataclass
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
IMAGES_DIR = Path(__file__).resolve().parent / "images"
CRIMENET_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "crimenet.json"

# ── Plot palette ────────────────────────────────────────────────────────
# Green marks cooperation; red marks conflict; blue is a neutral accent
# (mean lines), grey is secondary text.
PLOT_COLOR     = "#2db87d"   # primary green (cooperation)
COLOR_COOP     = "#2db87d"   # cooperation
COLOR_CONFLICT = "#d63b3b"   # conflict
COLOR_ACCENT   = "#2D6E8E"   # neutral blue accent (mean lines, etc.)
COLOR_MUTED    = "#6B7280"   # secondary annotation text

# Light→dark ramps for ranked bars (so the longest bar reads as the deepest).
GREEN_RAMP = ["#B8ECD4", "#5CCB9A", "#2DB87D", "#1A6D4A"]
RED_RAMP   = ["#F4B5B5", "#E57373", "#D63B3B", "#A81E1E"]

PLOT_DPI = 160


def gradient(n, ramp=None):
    """Return n colors interpolated along a light→dark ramp, for ranked bars."""
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("ramp", ramp or GREEN_RAMP)
    if n <= 1:
        return [cmap(1.0)]
    return [cmap(i / (n - 1)) for i in range(n)]


@dataclass
class Preprocessed:
    orgs: set           # EVERY organization (defunct included) — the analysis universe
    defunct: set        # informational only; NOT filtered out of the analysis
    coop: dict          # org -> set of orgs it cooperates with
    conflict: dict      # org -> set of orgs it conflicts with
    other: dict         # org -> set of orgs it has "other" edges with
    all_edges: dict     # org -> set of orgs it has ANY edge with
    pair_counts: Counter  # (a, b) sorted -> number of cooperation statements
    conflict_pair_counts: Counter  # (a, b) sorted -> number of conflict statements
    other_pair_counts: Counter     # (a, b) sorted -> number of "other" statements
    org_country: dict   # org -> country (empty string if unknown)
    nodes: list
    edges: list


def load_and_preprocess(crimenet_path=None):
    """
    Load crimenet.json and return (raw_data, Preprocessed).

    Parameters
    ----------
    crimenet_path : str or Path, optional
        Path to crimenet.json. Defaults to ../../data/crimenet.json
        relative to this file.

    Returns
    -------
    (data, Preprocessed)
    """
    path = Path(crimenet_path) if crimenet_path else CRIMENET_PATH

    with open(path) as f:
        data = json.load(f)

    nodes = data["nodes"]
    edges = data["edges"]

    defunct = {n["standard_name"] for n in nodes if n.get("is_defunct") is True}
    # The analysis universe is EVERY organization. Defunct groups are kept:
    # excluding them previously dropped ~26% of edges and the most-connected
    # historical cartels (Los Zetas, Medellín, Cali, Beltrán-Leyva). `defunct`
    # is retained for reporting only, never as a filter.
    orgs = {n["standard_name"] for n in nodes}

    coop = defaultdict(set)
    conflict = defaultdict(set)
    other = defaultdict(set)
    all_edges = defaultdict(set)
    pair_counts = Counter()
    conflict_pair_counts = Counter()
    other_pair_counts = Counter()

    for e in edges:
        s, t = e["source"], e["target"]
        rel = e["relationship"]
        all_edges[s].add(t)
        all_edges[t].add(s)
        if rel == "cooperation":
            coop[s].add(t)
            coop[t].add(s)
            pair_counts[tuple(sorted([s, t]))] += 1
        elif rel == "conflict":
            conflict[s].add(t)
            conflict[t].add(s)
            conflict_pair_counts[tuple(sorted([s, t]))] += 1
        elif rel == "other":
            other[s].add(t)
            other[t].add(s)
            other_pair_counts[tuple(sorted([s, t]))] += 1

    org_country = {}
    for n in nodes:
        org_country[n["standard_name"]] = n.get("country", "")

    return data, Preprocessed(
        orgs=orgs,
        defunct=defunct,
        coop=dict(coop),
        conflict=dict(conflict),
        other=dict(other),
        all_edges=dict(all_edges),
        pair_counts=pair_counts,
        conflict_pair_counts=conflict_pair_counts,
        other_pair_counts=other_pair_counts,
        org_country=org_country,
        nodes=nodes,
        edges=edges,
    )


# ── Valid network types ──────────────────────────────────────────────────
VALID_NETWORK_TYPES = frozenset({
    "cooperation", "conflict", "other",
    "conflict_and_cooperation", "all",
})


def build_graph(P, network_type):
    """Build an nx.Graph filtered to `network_type`, with optional pair_counts.

    Returns (G, pair_counts) where pair_counts may be None for mixed networks
    (cooperation+conflict counts are semantically incompatible).
    """
    import networkx as nx

    G = nx.Graph()
    for n in P.orgs:
        G.add_node(n)

    pc = None

    if network_type == "cooperation":
        for (a, b), count in P.pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        pc = P.pair_counts
    elif network_type == "conflict":
        for (a, b), count in P.conflict_pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        pc = P.conflict_pair_counts
    elif network_type == "other":
        for (a, b), count in P.other_pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        pc = P.other_pair_counts
    elif network_type == "conflict_and_cooperation":
        for (a, b), count in P.pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        for (a, b), count in P.conflict_pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        # Mixed semantics — unweighted
    elif network_type == "all":
        for (a, b), count in P.pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        for (a, b), count in P.conflict_pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        for (a, b), count in P.other_pair_counts.items():
            if a in P.orgs and b in P.orgs:
                G.add_edge(a, b)
        # Mixed semantics — unweighted

    return G, pc


def edge_matches_network_type(edge, network_type):
    """Check whether a crimenet edge matches the given network type."""
    rel = edge.get("relationship", "")
    if network_type == "cooperation":
        return rel == "cooperation"
    if network_type == "conflict":
        return rel == "conflict"
    if network_type == "other":
        return rel == "other"
    if network_type == "conflict_and_cooperation":
        return rel in ("cooperation", "conflict")
    if network_type == "all":
        return True
    return False


# ── LLM prompt per network type ──────────────────────────────────────────

NETWORK_SYSTEM_PROMPTS = {
    "cooperation": (
        "You are profiling communities of criminal organizations from CRIMENET, a "
        "knowledge graph of global organized crime built from Wikipedia.\n\n"
        "Each community was detected by Infomap, an algorithm that identifies clusters "
        "of densely interconnected nodes. Organizations that cooperate frequently end up "
        "in the same community.\n\n"
        "You are given every organization in the community (name, description, country, "
        "and degree centrality) and every known cooperation tie between them, with a "
        "description of each tie.\n\n"
        "Write a concise profile of this community. What ties these organizations "
        "together? What is the nature of their cooperation? Produce:\n"
        '  \"title\": a short descriptive label (4-8 words).\n'
        '  \"summary\": a clear, informative paragraph (4-6 sentences) that profiles '
        "this community.\n\n"
        "Return a JSON object with {title, summary}."
    ),
    "conflict": (
        "You are profiling communities of criminal organizations from CRIMENET, a "
        "knowledge graph of global organized crime built from Wikipedia.\n\n"
        "Each community was detected by Infomap, an algorithm that identifies clusters "
        "of densely interconnected nodes. In this network, edges represent conflict "
        "or rivalry — organizations that are in conflict with the same groups end up "
        "in the same community.\n\n"
        "You are given every organization in the community (name, description, country, "
        "and degree centrality) and every known conflict or rivalry tie between them, "
        "with a description of each tie.\n\n"
        "Write a concise profile of this community. What shared rivalries or conflicts "
        "bind these organizations? What is the nature of their antagonistic relationships? "
        "Produce:\n"
        '  \"title\": a short descriptive label (4-8 words).\n'
        '  \"summary\": a clear, informative paragraph (4-6 sentences) that profiles '
        "this community.\n\n"
        "Return a JSON object with {title, summary}."
    ),
    "other": (
        "You are profiling communities of criminal organizations from CRIMENET, a "
        "knowledge graph of global organized crime built from Wikipedia.\n\n"
        "Each community was detected by Infomap, an algorithm that identifies clusters "
        "of densely interconnected nodes. This network captures constituent, kinship, "
        "and other non-cooperative, non-conflict relationships between organizations.\n\n"
        "You are given every organization in the community (name, description, country, "
        "and degree centrality) and every known tie between them, with a description "
        "of each tie.\n\n"
        "Write a concise profile of this community. What structural or organizational "
        "relationships define this group? Produce:\n"
        '  \"title\": a short descriptive label (4-8 words).\n'
        '  \"summary\": a clear, informative paragraph (4-6 sentences) that profiles '
        "this community.\n\n"
        "Return a JSON object with {title, summary}."
    ),
    "conflict_and_cooperation": (
        "You are profiling communities of criminal organizations from CRIMENET, a "
        "knowledge graph of global organized crime built from Wikipedia.\n\n"
        "Each community was detected by Infomap, an algorithm that identifies clusters "
        "of densely interconnected nodes. This network combines both cooperation ties "
        "and conflict/rivalry ties. Each edge is labeled with its type.\n\n"
        "You are given every organization in the community (name, description, country, "
        "and degree centrality) and every known tie between them (cooperation or "
        "conflict), with a description of each tie.\n\n"
        "Write a concise profile of this community. How do cooperation and conflict "
        "shape this group? Produce:\n"
        '  \"title\": a short descriptive label (4-8 words).\n'
        '  \"summary\": a clear, informative paragraph (4-6 sentences) that profiles '
        "this community.\n\n"
        "Return a JSON object with {title, summary}."
    ),
    "all": (
        "You are profiling communities of criminal organizations from CRIMENET, a "
        "knowledge graph of global organized crime built from Wikipedia.\n\n"
        "Each community was detected by Infomap, an algorithm that identifies clusters "
        "of densely interconnected nodes. This network captures the full spectrum of "
        "relationships: cooperation, conflict/rivalry, and constituent/kinship/other "
        "ties. Each edge is labeled with its type.\n\n"
        "You are given every organization in the community (name, description, country, "
        "and degree centrality) and every known tie between them, with a description "
        "of each tie.\n\n"
        "Write a concise profile of this community. What ties these organizations "
        "together across cooperation, conflict, and organizational relationships? "
        "Produce:\n"
        '  \"title\": a short descriptive label (4-8 words).\n'
        '  \"summary\": a clear, informative paragraph (4-6 sentences) that profiles '
        "this community.\n\n"
        "Return a JSON object with {title, summary}."
    ),
}


def print_header(title):
    """Print a formatted section header."""
    width = 72
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def print_dataset_summary(P, data):
    """Print basic dataset statistics."""
    print(f"Loaded {len(data['nodes']):,} nodes, {len(data['edges']):,} edges")
    print(f"Organizations: {len(P.orgs):,}  (including {len(P.defunct):,} defunct)")
    print(f"Cooperation statements: {sum(P.pair_counts.values()):,}")
    print(f"  Unique pairs: {len(P.pair_counts):,}")
    multi_coop = sum(1 for c in P.pair_counts.values() if c > 1)
    print(f"  Multiplicity >1: {multi_coop:,} pairs")
    print(f"Conflict edges:       {sum(len(v) for v in P.conflict.values()) // 2:,}")
    print(f"Conflict statements:  {sum(P.conflict_pair_counts.values()):,}")
    print(f"  Unique conflict pairs: {len(P.conflict_pair_counts):,}")
    multi_conf = sum(1 for c in P.conflict_pair_counts.values() if c > 1)
    print(f"  Multiplicity >1: {multi_conf:,}")
    print(f"Orgs with >=1 cooperation edge: {len([k for k,v in P.coop.items() if v]):,}")
    print(f"Orgs with >=1 conflict edge:    {len([k for k,v in P.conflict.items() if v]):,}")
    with_country = sum(1 for c in P.org_country.values() if c)
    without = sum(1 for c in P.org_country.values() if not c)
    print(f"Active orgs with country: {with_country:,}")
    print(f"Active orgs without country: {without:,}")


def ensure_data_dir():
    """Create data/ directory if it doesn't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR


def ensure_images_dir():
    """Create images/ directory if it doesn't exist."""
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    return IMAGES_DIR


def setup_plot():
    """Configure matplotlib for headless rendering with a consistent,
    high-visibility style: large fonts, no top/right spines, light x-grid."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "figure.facecolor": "white",
        "savefig.facecolor": "white",
        "savefig.bbox": "tight",
        "savefig.dpi": PLOT_DPI,
        "font.family": "sans-serif",
        "font.size": 13,
        "text.color": "#222222",
        # Titles & axis labels, large and bold so they read at a glance.
        "axes.titlesize": 18,
        "axes.titleweight": "bold",
        "axes.titlepad": 14,
        "axes.labelsize": 14,
        "axes.labelweight": "semibold",
        "axes.labelpad": 8,
        "axes.labelcolor": "#222222",
        # Frame: keep only left + bottom.
        "axes.edgecolor": "#333333",
        "axes.linewidth": 1.3,
        "axes.spines.top": False,
        "axes.spines.right": False,
        # Light x-grid behind the bars for length comparison.
        "axes.axisbelow": True,
        "axes.grid": True,
        "axes.grid.axis": "x",
        "grid.color": "#e8e8e8",
        "grid.linewidth": 0.9,
        # Ticks: readable labels, no clutter marks on the category axis.
        "xtick.labelsize": 12,
        "ytick.labelsize": 12,
        "xtick.color": "#333333",
        "ytick.color": "#333333",
        "xtick.major.size": 5,
        "ytick.major.size": 0,
        "legend.fontsize": 12,
        "legend.frameon": False,
    })
    return plt
