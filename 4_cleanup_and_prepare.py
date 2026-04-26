"""
4_cleanup_and_prepare.py
Takes global_network.json → crimenet.json

The LLM extraction in 2_extract_network.py emits canonical types,
canonical relationships, and a fixed detail vocabulary directly. This
script handles only what the LLM cannot:

  - Cross-article dedup (KNOWN_DUPLICATES)
  - Generic-umbrella-name filtering (Russian organized crime, etc.)
  - Per-org type overrides (NODE_TYPE_OVERRIDES)
  - Hand-curated exclusions (TO_BE_EXCLUDED)
  - URL sanitization, source splitting
  - Betweenness centrality

Hand-curated data lives in cleanup_data.py. Edit that file to add new
duplicates, exclusions, or type overrides — no code changes needed.

A small safety net catches occasional LLM slips (synonyms or stray
non-canonical values).

Usage:
    python 4_cleanup_and_prepare.py --input global_network.json
    python 4_cleanup_and_prepare.py --input global_network.json --stats
"""

import json
import re
import argparse
import logging
from pathlib import Path
from collections import Counter

try:
    import networkx as nx
except ImportError:
    raise ImportError("Install networkx: pip install networkx")

try:
    from cleanup_data import KNOWN_DUPLICATES, TO_BE_EXCLUDED, NODE_TYPE_OVERRIDES
except ImportError:
    raise ImportError(
        "cleanup_data.py not found. It should sit next to this script and "
        "define KNOWN_DUPLICATES, TO_BE_EXCLUDED, and NODE_TYPE_OVERRIDES."
    )

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# CANONICAL VOCABULARY
# (mirrors the prompt in 2_extract_network.py — keep in sync)
# ═══════════════════════════════════════════════════════════════════

CANONICAL_NODE_TYPES = {
    "cartel", "mafia", "gang", "motorcycle_club",
    "faction", "clan", "triad", "militia", "terrorist_organization",
}

CANONICAL_DETAILS = {
    "splinter", "armed_wing", "successor", "merger",
    "faction_of", "support_club", "reformation",
    "founded_by_members_of", "evolved_into", "other",
}

VALID_RELATIONSHIPS = {"alliance", "rivalry", "other"}


# ═══════════════════════════════════════════════════════════════════
# SAFETY NETS
# Small maps to catch occasional LLM slips. If a value isn't here and
# isn't canonical, it's replaced with a default and logged.
# ═══════════════════════════════════════════════════════════════════

NODE_TYPE_FALLBACK = {
    "crime_family": "mafia",
    "crime_syndicate": "mafia",
    "criminal_organization": "other",
    "organized_crime_group": "other",
    "crew": "gang",
    "yakuza": "mafia",
    "secret_society": "other",
}

# "other" edges whose detail value really means alliance.
DETAIL_TO_ALLIANCE = {
    "alliance", "allied_with", "aligned_with",
    "collaboration", "cooperation", "cooperated_with",
    "partnership", "partnered_with", "joint_operation",
    "ties", "ties_to", "linked_to", "connected_to",
    "associated_with", "association",
}

# "other" edges whose detail value really means rivalry.
DETAIL_TO_RIVALRY = {
    "rivalry", "conflict", "feud", "confrontation",
    "competition", "war", "armed_conflict",
    "targeted", "targeted_by", "retaliation",
    "alliance_turned_rivalry",
}

DETAIL_FALLBACK = {
    "member_of": "faction_of", "part_of": "faction_of",
    "branch_of": "faction_of", "subgroup_of": "faction_of",
    "chapter_of": "faction_of", "affiliated_with": "faction_of",

    "absorbed": "merger", "absorbed_by": "merger",
    "merged_into": "merger", "merged_with": "merger",

    "split_off": "splinter", "broke_off": "splinter",
    "offshoot": "splinter",

    "predecessor": "successor", "took_over": "successor",
    "replaced": "successor", "replaced_by": "successor",

    "founded_by": "founded_by_members_of",
    "formed_by": "founded_by_members_of",

    "paramilitary_wing": "armed_wing",
    "enforcement_wing": "armed_wing",

    "puppet_club": "support_club",
    "prospect_club": "support_club",
}


# ═══════════════════════════════════════════════════════════════════
# GENERIC NODE FILTER
# ═══════════════════════════════════════════════════════════════════

GENERIC_SUFFIXES = [
    r"\bmafia$", r"\borganized crime$", r"\borganised crime$",
    r"\bcrime groups?$", r"\bcriminal organizations?$", r"\bcriminal groups?$",
    r"\bdrug cartels?$", r"\bgangs?$", r"\bcriminal networks?$",
    r"\bcrime syndicate$", r"\bunderworld$", r"\bcriminal underworld$", r"\bmob$",
]

GENERIC_PREFIXES = [
    "african", "albanian", "american", "armenian", "australian", "azerbaijani",
    "azeri", "balkan", "bangladeshi", "belarusian", "bolivian", "bosnian",
    "brazilian", "british", "bulgarian", "burmese", "cambodian", "canadian",
    "caribbean", "central american", "chechen", "chilean", "chinese",
    "colombian", "corsican", "croatian", "cuban", "czech", "dominican",
    "dutch", "east asian", "east european", "ecuadorian", "egyptian",
    "estonian", "european", "filipino", "french", "galician", "georgian",
    "german", "greek", "guatemalan", "haitian", "honduran", "hungarian",
    "indian", "indonesian", "iranian", "iraqi", "irish", "israeli",
    "italian", "jamaican", "japanese", "kazakh", "korean", "kurdish",
    "kyrgyz", "latin american", "latvian", "lebanese", "libyan",
    "lithuanian", "macedonian", "malaysian", "mexican", "moldovan",
    "montenegrin", "moroccan", "nigerian", "north african", "pakistani",
    "palestinian", "paraguayan", "peruvian", "polish", "portuguese",
    "puerto rican", "romanian", "russian", "salvadoran", "saudi",
    "scandinavian", "serbian", "singaporean", "slovak", "south african",
    "south american", "southeast asian", "spanish", "swedish", "swiss",
    "syrian", "taiwanese", "tajik", "thai", "trinidadian", "tunisian",
    "turkish", "turkmen", "ukrainian", "uruguayan", "uzbek", "venezuelan",
    "vietnamese", "west african", "yugoslav",
]

GENERIC_BLOCKLIST = {
    "organized crime", "organised crime", "transnational organized crime",
    "international organized crime", "drug trafficking organizations",
    "drug trafficking", "narcotrafficking", "latin american drug cartels",
    "european organized crime", "asian organized crime", "african organized crime",
    "mafia", "la mafia", "drug cartels", "motorcycle gangs", "drug cartel",
    "criminal organizations", "criminal organization",
    "colombian cartels", "colombian drug cartels", "colombian criminal networks",
    "colombian criminal organizations", "colombian mafia",
    "mexican cartels", "mexican drug cartels",
    "african american gangs", "african criminal networks",
    "chinese gangs", "east asian gangs",
    "dominican criminal groups", "dutch criminal groups",
    "indian gangs", "irish gangs", "italian gangs", "korean gangs",
    "korean criminal groups", "korean criminal organizations",
    "nigerian crime groups", "nigerian organized crime",
    "north african gangs", "puerto rican gangs",
    "russian crime groups", "russian criminal networks", "russian gangs",
    "russian organized crime", "swedish criminal networks",
    "turkish crime groups", "turkish gangs",
    "vietnamese crime groups", "vietnamese gangs",
    "south american drug cartels", "italian crime groups",
    "dutch organized crime", "british underworld",
    "balkan organized crime groups",
}

GENERIC_SAFELIST = {
    "mexican mafia", "new mexican mafia", "irish mob", "dixie mafia",
    "jewish mafia", "cornbread mafia", "black mafia", "black mafia family",
    "thai mafia", "american mafia", "albanian mafia", "serbian mafia",
    "corsican mafia", "israeli mafia", "chechen mafia", "bulgarian mafia",
    "montenegrin mafia", "azerbaijani mafia", "georgian mafia", "armenian mafia",
    "iranian mafia", "lebanese mafia", "kurdish mafia", "turkish mafia",
    "ukrainian mafia", "romanian mafia", "russian mafia", "russian mob",
    "nigerian mafia", "pakistani mafia", "moroccan mafia", "cuban mafia",
    "greek mafia", "indian mafia", "irish mafia", "italian mafia", "italian mob",
    "polish mob", "portuguese mafia", "slovak mafia", "canadian mafia",
    "balkan mafia", "yugoslav mafia", "north macedonian mafia",
    "galician mafia", "red mafia", "new mafia", "dz mafia",
    "axe gang", "31 gang", "856 gang", "b13 gang", "fk gang", "fob gang",
    "k&a gang", "lvm gang", "lal gang", "mbm gang", "sza gang", "sin ma gang",
    "bosnian drug cartel", "cuban drug cartel",
    "georgian organized crime", "israeli organized crime",
}

_SUFFIX_PATTERNS = [re.compile(s, re.IGNORECASE) for s in GENERIC_SUFFIXES]
_PREFIX_SET = set(GENERIC_PREFIXES)


def is_generic_node(name):
    lower = name.strip().lower()
    if lower in GENERIC_SAFELIST:
        return False
    if lower in GENERIC_BLOCKLIST:
        return True
    for pattern in _SUFFIX_PATTERNS:
        if pattern.search(lower):
            prefix_part = pattern.sub("", lower).strip().lower()
            if prefix_part in _PREFIX_SET:
                return True
    return False


# ═══════════════════════════════════════════════════════════════════
# URL HELPERS
# ═══════════════════════════════════════════════════════════════════

def is_valid_wiki_url(url):
    return url and ("wikipedia.org/" in url)


def extract_wiki_title(url):
    if not url:
        return None
    match = re.search(r'title=([^&]+)', url)
    if match:
        raw = match.group(1)
    elif '/wiki/' in url:
        raw = url.split('/wiki/')[-1].split('?')[0].split('#')[0]
    else:
        return None
    try:
        from urllib.parse import unquote
        raw = unquote(raw)
    except ImportError:
        raw = raw.replace('%27', "'").replace('%28', '(').replace('%29', ')')
    return raw.replace('_', ' ')


def split_node_sources(node_name, aliases, urls):
    name_lower = node_name.strip().lower()
    alias_set = {a.strip().lower() for a in aliases} if aliases else set()
    alias_set.add(name_lower)

    GENERIC_SOURCE_TITLES = {
        "mafia", "gang", "cartel", "triad", "organized crime", "crime family",
        "death squad", "irish mob", "bloods", "crips", "yakuza",
    }

    SOURCE_OVERRIDES = {
        "cosa nostra": {"url": "https://en.wikipedia.org/w/index.php?title=Sicilian_Mafia&oldid=1343334461", "title": "Sicilian Mafia"},
        "bloods": {"url": "https://en.wikipedia.org/wiki/Bloods", "title": "Bloods"},
        "evil corp": {"url": "https://en.wikipedia.org/wiki/Evil_Corp", "title": "Evil Corp"},
        "joe boys": {"url": "https://en.wikipedia.org/wiki/Joe_Boys", "title": "Joe Boys"},
        "gallo crew": None,
        "wo group": None,
    }

    if name_lower in SOURCE_OVERRIDES:
        own_source = SOURCE_OVERRIDES[name_lower]
        mentioned_in = [{"url": u, "title": extract_wiki_title(u) or "Wikipedia"}
                        for u in urls if not own_source or u != own_source["url"]]
        return own_source, mentioned_in

    own_source = None
    mentioned_in = []

    for url in urls:
        title = extract_wiki_title(url)
        if not title:
            mentioned_in.append({"url": url, "title": "Wikipedia"})
            continue
        title_lower = title.strip().lower()

        if title_lower in GENERIC_SOURCE_TITLES:
            mentioned_in.append({"url": url, "title": title})
            continue

        is_own = False
        if title_lower == name_lower:
            is_own = True
        elif title_lower in alias_set:
            is_own = True
        elif name_lower in title_lower and len(name_lower) > len(title_lower) * 0.5:
            is_own = True

        if is_own and own_source is None:
            own_source = {"url": url, "title": title}
        else:
            mentioned_in.append({"url": url, "title": title})

    return own_source, mentioned_in


# ═══════════════════════════════════════════════════════════════════
# BETWEENNESS CENTRALITY
# ═══════════════════════════════════════════════════════════════════

def compute_betweenness(node_names, edge_list):
    connected = set()
    for e in edge_list:
        s, t = e.get("source", ""), e.get("target", "")
        if s in node_names and t in node_names and s != t:
            connected.add(s)
            connected.add(t)

    G = nx.Graph()
    G.add_nodes_from(connected)
    for e in edge_list:
        s, t = e.get("source", ""), e.get("target", "")
        if s in connected and t in connected:
            G.add_edge(s, t)

    n = len(G.nodes)
    if n < 2:
        return {name: 0.0 for name in node_names}

    log.info(f"  Betweenness centrality ({n} connected nodes, {G.number_of_edges()} edges)…")
    bc = nx.betweenness_centrality(G, normalized=True)

    for name in node_names:
        bc.setdefault(name, 0.0)

    top5 = sorted(bc.items(), key=lambda x: -x[1])[:5]
    log.info(f"  Top 5: {[(n, round(v, 4)) for n, v in top5]}")
    return bc


# ═══════════════════════════════════════════════════════════════════
# DEDUP & NORMALIZATION
# ═══════════════════════════════════════════════════════════════════

def normalize(name):
    return re.sub(r"\s+", " ", name.strip().lower())


def build_dedup_map(nodes):
    variant_to_canonical = {}
    for canonical, variants in KNOWN_DUPLICATES.items():
        for v in variants:
            variant_to_canonical[v.strip().lower()] = canonical
        variant_to_canonical[canonical.strip().lower()] = canonical

    merge_map = {}
    group_tracker = {}

    for node in nodes:
        name = node["standard_name"]
        lower = name.strip().lower()
        if lower in variant_to_canonical:
            canonical = variant_to_canonical[lower]
            if name != canonical:
                merge_map[name] = canonical
                if canonical not in group_tracker:
                    group_tracker[canonical] = {canonical}
                group_tracker[canonical].add(name)

    groups = [v for v in group_tracker.values() if len(v) > 1]
    return merge_map, groups


def normalize_node_type(t):
    t = (t or "").strip().lower()
    if t in CANONICAL_NODE_TYPES or t == "other":
        return t
    return NODE_TYPE_FALLBACK.get(t, "other")


def normalize_detail(d):
    if not d:
        return None
    d = d.strip().lower()
    if d in CANONICAL_DETAILS:
        return d
    return DETAIL_FALLBACK.get(d, d)


# ═══════════════════════════════════════════════════════════════════
# CLEANUP PIPELINE
# ═══════════════════════════════════════════════════════════════════

def cleanup(data, show_stats=False):
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])
    log.info(f"Input: {len(nodes)} nodes, {len(edges)} edges")
    log.info(f"Curated data: {len(KNOWN_DUPLICATES)} duplicate groups, "
             f"{len(TO_BE_EXCLUDED)} exclusions, {len(NODE_TYPE_OVERRIDES)} type overrides")

    # 1. Normalize node types (catch LLM slips)
    type_remapped = 0
    non_canonical = Counter()
    for node in nodes:
        old = node.get("type", "")
        new = normalize_node_type(old)
        if old != new:
            type_remapped += 1
            non_canonical[old] += 1
        node["type"] = new
    if type_remapped:
        log.info(f"Node type normalization: {type_remapped} non-canonical values remapped")
        if show_stats:
            log.info(f"  Top non-canonical types: {non_canonical.most_common(10)}")

    # 2. Hand-curated exclusions
    before = len(nodes)
    nodes = [n for n in nodes
             if n["standard_name"].strip().lower() not in TO_BE_EXCLUDED]
    removed = before - len(nodes)
    if removed:
        log.info(f"Removed {removed} non-criminal entities")

    # 3. Sanitize string-null values that the LLM occasionally emits
    null_strings = {"null", "none", "unknown", "n/a", ""}
    null_fixed = 0
    for node in nodes:
        tp = node.get("time_period")
        if isinstance(tp, str) and tp.strip().lower() in null_strings:
            node["time_period"] = None
            null_fixed += 1
    for edge in edges:
        tp = edge.get("time_period")
        if isinstance(tp, str) and tp.strip().lower() in null_strings:
            edge["time_period"] = None
            null_fixed += 1
        d = edge.get("detail")
        if isinstance(d, str) and d.strip().lower() in null_strings:
            edge["detail"] = None
    if null_fixed:
        log.info(f"String-null sanitization: {null_fixed} fields fixed")

    # 4. Sanitize relationships and reclassify "other" edges that should be
    #    alliance/rivalry (safety net for LLM slips)
    invalid_rels = 0
    reclassified = 0
    detail_remapped = 0
    for edge in edges:
        rel = (edge.get("relationship") or "").strip().lower()

        if rel not in VALID_RELATIONSHIPS:
            edge["relationship"] = "other"
            invalid_rels += 1
            rel = "other"

        detail = (edge.get("detail") or "").strip().lower()

        if rel == "other" and detail:
            if detail in DETAIL_TO_ALLIANCE:
                edge["relationship"] = "alliance"
                edge["detail"] = None
                reclassified += 1
                continue
            elif detail in DETAIL_TO_RIVALRY:
                edge["relationship"] = "rivalry"
                edge["detail"] = None
                reclassified += 1
                continue

        if edge.get("detail"):
            old = edge["detail"]
            new = normalize_detail(old)
            if old != new:
                detail_remapped += 1
            edge["detail"] = new

    if invalid_rels:
        log.info(f"Relationship sanitization: {invalid_rels} invalid values fixed")
    if reclassified:
        log.info(f"Detail reclassification: {reclassified} edges moved to alliance/rivalry")
    if detail_remapped:
        log.info(f"Detail normalization: {detail_remapped} non-canonical values remapped")

    # 5. Cross-article deduplication
    merge_map, groups = build_dedup_map(nodes)
    if groups:
        log.info(f"Dedup: {len(groups)} duplicate groups:")
        for group in groups:
            canonical = min(group, key=len)
            log.info(f"  '{canonical}' ← {group - {canonical}}")

    final_nodes = {}
    for node in nodes:
        name = node["standard_name"]
        canonical = merge_map.get(name, name)
        node["standard_name"] = canonical
        key = normalize(canonical)
        if key not in final_nodes:
            final_nodes[key] = node
        else:
            existing = final_nodes[key]
            existing["aliases"] = sorted(
                set(existing.get("aliases", [])) | set(node.get("aliases", []))
                | ({name} if name != canonical else set()))
            existing["wikipedia_urls"] = sorted(
                set(existing.get("wikipedia_urls", [])) | set(node.get("wikipedia_urls", [])))
            existing["source_articles"] = sorted(
                set(existing.get("source_articles", [])) | set(node.get("source_articles", [])))
            if len(node.get("context", "") or "") > len(existing.get("context", "") or ""):
                existing["context"] = node["context"]
            if len(node.get("time_period") or "") > len(existing.get("time_period") or ""):
                existing["time_period"] = node["time_period"]
    nodes = list(final_nodes.values())

    # 6. Per-organization type overrides (curated, post-corpus knowledge)
    overrides_applied = 0
    for node in nodes:
        override = NODE_TYPE_OVERRIDES.get(node["standard_name"].strip().lower())
        if override:
            node["type"] = override
            overrides_applied += 1
    if overrides_applied:
        log.info(f"Type overrides applied: {overrides_applied}")

    # 7. Re-target edges to canonical names and dedup post-merge
    for edge in edges:
        edge["source"] = merge_map.get(edge["source"], edge["source"])
        edge["target"] = merge_map.get(edge["target"], edge["target"])

    edges = [e for e in edges if normalize(e["source"]) != normalize(e["target"])]

    edge_map = {}
    for edge in edges:
        key = (normalize(edge["source"]), normalize(edge["target"]),
               edge.get("relationship", ""), edge.get("detail") or "")
        if key not in edge_map:
            edge_map[key] = edge
        else:
            existing = edge_map[key]
            if len(edge.get("context", "") or "") > len(existing.get("context", "") or ""):
                existing["context"] = edge["context"]
            if len(edge.get("time_period") or "") > len(existing.get("time_period") or ""):
                existing["time_period"] = edge["time_period"]
            existing["wikipedia_urls"] = sorted(
                set(existing.get("wikipedia_urls", [])) | set(edge.get("wikipedia_urls", []))
            )
    edges = list(edge_map.values())

    # 8. URL sanitization
    bad = 0
    for node in nodes:
        orig = node.get("wikipedia_urls", [])
        clean = [u for u in orig if is_valid_wiki_url(u)]
        if len(clean) < len(orig):
            bad += len(orig) - len(clean)
        node["wikipedia_urls"] = clean
    for edge in edges:
        orig = edge.get("wikipedia_urls", [])
        clean = [u for u in orig if is_valid_wiki_url(u)]
        if len(clean) < len(orig):
            bad += len(orig) - len(clean)
        edge["wikipedia_urls"] = clean
    if bad:
        log.info(f"URL sanitization: removed {bad} broken URLs")

    log.info(f"After cleanup: {len(nodes)} nodes, {len(edges)} edges")

    if show_stats:
        tc = Counter(n["type"] for n in nodes)
        rc = Counter(e["relationship"] for e in edges)
        dc = Counter(e["detail"] for e in edges if e.get("detail"))
        print(f"\n{'='*60}\n  CLEANED NETWORK\n{'='*60}")
        print(f"  Nodes: {len(nodes)}  |  Edges: {len(edges)}\n")
        print(f"  Node types ({len(tc)}):")
        for t, c in tc.most_common():
            print(f"    {t:35s} {c:5d}")
        print(f"\n  Relationships:")
        for t, c in rc.most_common():
            print(f"    {t:35s} {c:5d}")
        if dc:
            print(f"\n  Detail types ({len(dc)}):")
            for t, c in dc.most_common():
                print(f"    {t:35s} {c:5d}")
        print("=" * 60)

    return nodes, edges


# ═══════════════════════════════════════════════════════════════════
# BUILD crimenet.json
# ═══════════════════════════════════════════════════════════════════

# Only the 9 canonical types end up in the visualization.
SPECIFIC_ORG_TYPES = CANONICAL_NODE_TYPES

# Edge types are renamed for the visualization JSON.
SPECIFIC_EDGE_MAP = {"alliance": "allied_with", "rivalry": "rivals_with"}


def build_specific(nodes, edges):
    org_names = set()
    entities = []
    filtered = []

    for n in nodes:
        if n["type"] not in SPECIFIC_ORG_TYPES:
            continue
        if is_generic_node(n["standard_name"]):
            filtered.append(n["standard_name"])
            continue
        org_names.add(n["standard_name"])
        own_src, mentioned = split_node_sources(
            n["standard_name"], n.get("aliases", []), n.get("wikipedia_urls", []),
        )
        entities.append({
            "name": n["standard_name"],
            "type": n["type"],
            "descriptions": [n["context"]] if n.get("context") else [],
            "time_period": n.get("time_period") or None,
            "own_source": own_src,
            "mentioned_in": mentioned,
        })

    if filtered:
        log.info(f"Filtered {len(filtered)} generic nodes:")
        for name in sorted(filtered):
            log.info(f"    ✗ {name}")

    spec_edges = [e for e in edges
                  if e["relationship"] in SPECIFIC_EDGE_MAP
                  and e["source"] in org_names and e["target"] in org_names]

    alliance_edges = [e for e in spec_edges if e["relationship"] == "alliance"]
    rivalry_edges = [e for e in spec_edges if e["relationship"] == "rivalry"]

    log.info("Computing betweenness (alliance only):")
    bc_alliance = compute_betweenness(org_names, alliance_edges)
    log.info("Computing betweenness (rivalry only):")
    bc_rivalry = compute_betweenness(org_names, rivalry_edges)
    log.info("Computing betweenness (combined):")
    bc_combined = compute_betweenness(org_names, spec_edges)

    for ent in entities:
        name = ent["name"]
        ent["betweenness_alliance"] = round(bc_alliance.get(name, 0.0), 6)
        ent["betweenness_rivalry"] = round(bc_rivalry.get(name, 0.0), 6)
        ent["betweenness_combined"] = round(bc_combined.get(name, 0.0), 6)

    relations = []
    for e in spec_edges:
        relations.append({
            "source": e["source"],
            "target": e["target"],
            "type": SPECIFIC_EDGE_MAP[e["relationship"]],
            "descriptions": [e["context"]] if e.get("context") else [],
            "time_period": e.get("time_period") or None,
            "sources": [
                {"url": url, "title": extract_wiki_title(url) or "Wikipedia"}
                for url in e.get("wikipedia_urls", [])
            ],
        })

    return {"entities": entities, "relations": relations}


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Cleanup and build crimenet.json")
    parser.add_argument("--input", "-i", default="global_network.json")
    parser.add_argument("--stats", "-s", action="store_true")
    args = parser.parse_args()

    data = json.loads(Path(args.input).read_text("utf-8"))
    nodes, edges = cleanup(data, show_stats=args.stats)

    specific = build_specific(nodes, edges)
    Path("crimenet.json").write_text(
        json.dumps(specific, ensure_ascii=False, indent=2), "utf-8"
    )
    log.info(f"Output: {len(specific['entities'])} entities, "
             f"{len(specific['relations'])} relations → crimenet.json")


if __name__ == "__main__":
    main()