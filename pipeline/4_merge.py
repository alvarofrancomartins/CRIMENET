"""
4_merge.py

Merge every folder's article_graph.json into one graph, fold variant
names together using a deduplication map built AUTOMATICALLY from the
org_profile.json files (step 3), attach each profile's authoritative
fields to its node, and normalize country names via countries.py.

    txts/<folder>/article_graph.json   (per-article nodes + edges, step 2)
    txts/<folder>/org_profile.json     (per-org enrichment, step 3)
        → crimenet_raw.json

This step does NOT apply any hand-curated corrections. It produces a raw
dataset. Run audit/7_apply_corrections.py afterward to produce the final
crimenet.json.

== How a mention is matched to a profiled org ==

Each org_profile.json declares a canonical name and a full alias list.
We turn all profiles into two lookups:
  - exact-fold map: fold(name) → canonical, where fold() NFKD-strips
    accents, lowercases, keeps alphanumerics only. "Beltrán-Leyva
    Cartel" == "Beltran Leyva Cartel".
  - core map: significant-token set → canonical, where generic org-type
    words (crime, family, mafia, clan, …) are stripped first. Lets
    "Balistrieri family" match a profile's "Balistrieri crime family".

A mention NODE is resolved using ALL the names it is known by — its
standard_name, its own extracted aliases, and its original_text_name —
not just the standard_name. Resolution order:
  1. exact-fold on each of the node's names (standard_name first);
  2. core match on each of the node's names;
  3. otherwise the node stays itself (unprofiled).

This two-signal, multi-name strategy is what catches the hard cases:
  - "Black Disciple Nation" carries "Black Disciples" in its own aliases,
    so it folds into the profiled "Black Disciples" by exact-fold on an
    alias — even though its standard_name and the singular/plural core
    don't match.
  - "Balistrieri family" has no helpful alias but core-matches the
    profile's "Balistrieri crime family".

Every resolution a node's standard_name and aliases imply is fed back
into an augmented map, so EDGES pointing at any of those names follow the
same merge (an edge to "Black Disciple Nation" resolves to "Black
Disciples" even though the edge only carries the bare name).

Safety: core matching only fires when exact found nothing, never
overrides a real canonical, requires a ≥4-char core, and refuses cores
claimed by two different profiled orgs (reported as conflicts). Every
cross-name merge is printed for auditing.

== Country normalization ==

Country names are normalized via pipeline/lib/countries.py at merge time.
crimenet_raw.json carries clean, canonical country data; no further
country filtering is needed elsewhere.

== Edges ==

One edge per distinct extracted statement, each with its evidence_quote
and source_url. Endpoints canonicalized through the augmented map (+ core
fallback); self-loops and edges to dropped/missing nodes removed; byte-
identical statements from the same source collapse.

Usage:
    python 4_merge.py --dir ../data/txts --output ../data/crimenet_raw.json
"""

import json
import re
import sys
import unicodedata
import argparse
from pathlib import Path
from collections import defaultdict, Counter
from urllib.parse import unquote

from lib.countries import normalize_country

# Generic org-type words stripped before core matching. Scoped to the
# Mafia/gang naming pattern where "X family"/"X crime family"/"X Mafia"
# are the same org. Excludes identity-bearing words (cartel, gang, triad,
# outfit) so core matching can't over-collapse distinct cartels/gangs.
GENERIC_TOKENS = {
    "the", "crime", "family", "families", "mafia", "clan", "clans",
    "organization", "organisation", "syndicate", "mob", "cosca", "cosche",
    "borgata", "ndrina", "ndrine",
    # Motorcycle-club vocabulary: "X MC" / "X Motorcycle Club" / "X MCC"
    # are the same org, so these are stripped before core matching.
    "mc", "mcc", "motorcycle", "club",
}

MIN_CORE_CHARS = 4


# ── normalization ──────────────────────────────────────────────────────

def norm(name):
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def fold(s):
    """NFKD-decompose, drop combining marks, lowercase, alphanumeric-only."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return "".join(c for c in ascii_str.lower() if c.isalnum())

GENERIC_FOLDED = {fold(g) for g in GENERIC_TOKENS}


# Aliases too generic to safely merge on — they describe a TYPE of org,
# not a specific one. Even if step 3 lets one slip through, it must never
# become a matching key (otherwise a node literally named "mafia" would
# fold into whichever profile listed "mafia" as an alias). Compared under
# fold().
GENERIC_ALIAS_BLOCK = {
    fold(x) for x in {
        "mafia", "the mob", "mob", "camorra", "cartel", "gang", "triad",
        "the syndicate", "syndicate", "organized crime", "organised crime",
        "the family", "the firm", "the outfit",
        "the mafia", "drug cartel", "crime family", "crime syndicate",
        "the honoured society", "the honored society", "onorata societa",
    }
}


def sig_tokens(name):
    """Significant (non-generic) folded tokens of a name, as a frozenset."""
    out = set()
    for tok in re.split(r"[\s\-_/]+", (name or "").strip()):
        ft = fold(tok)
        if ft and ft not in GENERIC_FOLDED:
            out.add(ft)
    return frozenset(out)


def mc_variants(name):
    """MC ↔ Motorcycle Club spelling variants of a name (incl. itself).

    'Devils Diciples Motorcycle Club' → also 'Devils Diciples MC'
    'Bandidos MC'                     → also 'Bandidos Motorcycle Club'
    Only fires for names that actually carry one of these suffixes, so
    non-MC orgs are unaffected. Returns a list, original first."""
    n = (name or "").strip()
    if not n:
        return []
    out = [n]
    # full form → MC
    m = re.sub(r"\bmotorcycle\s+club\b", "MC", n, flags=re.IGNORECASE)
    if m != n:
        out.append(m)
    # MC (trailing) → full form
    m = re.sub(r"\bMC\b\s*$", "Motorcycle Club", n, flags=re.IGNORECASE)
    if m != n:
        out.append(m)
    # dedupe by fold, preserve order
    seen, dedup = set(), []
    for v in out:
        if fold(v) not in seen:
            seen.add(fold(v))
            dedup.append(v)
    return dedup


def extract_wiki_title(url):
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


def year_int(y):
    if not y:
        return None
    m = re.search(r"\b(1[0-9]{3}|20[0-9]{2})\b", str(y))
    return int(m.group(1)) if m else None


# ── loading ────────────────────────────────────────────────────────────

def load_graphs(txts_dir):
    all_nodes, all_edges, loaded = [], [], 0
    for folder in sorted(p for p in txts_dir.iterdir() if p.is_dir()):
        path = folder / "article_graph.json"
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text("utf-8"))
        except Exception as e:
            print(f"  error loading {path}: {e}")
            continue
        all_nodes.extend(data.get("nodes", []))
        all_edges.extend(data.get("edges", []))
        loaded += 1
    return all_nodes, all_edges, loaded


def load_profiles(txts_dir):
    """Load every org_profile.json, skipping non-org records (those step 3
    wrote with canonical_name == null)."""
    profiles = []
    for folder in sorted(p for p in txts_dir.iterdir() if p.is_dir()):
        path = folder / "org_profile.json"
        if not path.exists():
            continue
        try:
            prof = json.loads(path.read_text("utf-8"))
        except Exception as e:
            print(f"  error loading {path}: {e}")
            continue
        if isinstance(prof, dict) and (prof.get("canonical_name") or "").strip():
            profiles.append(prof)
    return profiles


# ── matching maps ──────────────────────────────────────────────────────

def unify_mutual_profiles(profiles):
    """Merge profiles that are the same org named differently across
    languages, BEFORE the maps are built.

    The signal is MUTUAL canonical reference: profile A lists B's canonical
    name as one of its aliases AND profile B lists A's canonical name as one
    of its aliases. That two-way pointing is strong evidence they're the same
    org (the cross-language case: "Jalisco New Generation Cartel" en ↔
    "Cártel de Jalisco Nueva Generación" es). A ONE-way reference is NOT
    enough — that's the hijack case the conflict logic rightly avoids.

    For each connected cluster of mutually-referencing profiles, one canonical
    is chosen (the longest name, as a stable, language-neutral tie-break) and
    every other profile in the cluster has its canonical_name rewritten to it,
    with its former canonical preserved as an alias. Downstream grouping then
    folds the whole cluster into one node with multiple own_sources.

    Returns the profiles list with canonical_names unified in place.
    """
    if not profiles:
        return profiles

    canon_folds = {fold(p.get("canonical_name", "")) for p in profiles}
    canon_folds.discard("")

    # refs[i] = set of canon-folds that profile i lists as aliases
    refs = []
    for p in profiles:
        s = set()
        for a in p.get("aliases") or []:
            fa = fold(a)
            if fa in canon_folds:
                s.add(fa)
        refs.append(s)

    fold_to_idxs = defaultdict(list)
    for i, p in enumerate(profiles):
        fold_to_idxs[fold(p.get("canonical_name", ""))].append(i)

    # Union-Find over profiles linked by MUTUAL canonical references.
    parent = list(range(len(profiles)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i, p in enumerate(profiles):
        fi = fold(p.get("canonical_name", ""))
        for fj in refs[i]:                      # i references j's canonical
            for j in fold_to_idxs.get(fj, []):
                if j == i:
                    continue
                if fi in refs[j]:               # and j references i's canonical
                    union(i, j)

    clusters = defaultdict(list)
    for i in range(len(profiles)):
        clusters[find(i)].append(i)

    unified = 0
    for members in clusters.values():
        if len(members) < 2:
            continue
        names = [profiles[i].get("canonical_name", "").strip() for i in members]
        # Stable, language-neutral choice: longest name, then alphabetical.
        chosen = sorted(names, key=lambda n: (-len(n), n.lower()))[0]
        for i in members:
            old = profiles[i].get("canonical_name", "").strip()
            if fold(old) == fold(chosen):
                continue
            al = list(profiles[i].get("aliases") or [])
            if old and fold(old) not in {fold(x) for x in al}:
                al.append(old)
            profiles[i]["aliases"] = al
            profiles[i]["canonical_name"] = chosen
            unified += 1
        print(f"  unified cross-language org: {sorted(set(names))} → '{chosen}'")

    if unified:
        print(f"Cross-language unification: {unified} profile(s) merged "
              f"via mutual canonical reference")
    return profiles


def build_maps(profiles):
    """Returns (canon_map, sig_map, canonical_folds, conflicts).

    Aliases that are BLOCKLISTED against their own canonical are not
    registered as keys (a forbidden alias→canonical entry would otherwise
    survive into the edge resolver, which walks canon_map directly). The
    blocklist is still ALSO enforced at resolution time in resolve_node,
    so exact/alias/core matching is covered uniformly.
    """
    canon_map = {}
    canonical_by_fold = {}
    sig_map = {}
    ambiguous_sigs = set()
    conflicts = []

    for p in profiles:
        c = (p.get("canonical_name") or "").strip()
        fc = fold(c)
        if not fc:
            continue
        canon_map[fc] = c
        canonical_by_fold.setdefault(fc, c)
        # register MC ↔ Motorcycle Club variants of the canonical name
        for v in mc_variants(c):
            canon_map.setdefault(fold(v), c)
        s = sig_tokens(c)
        if s:
            prev = sig_map.get(s)
            if prev is None:
                sig_map[s] = c
            elif fold(prev) != fold(c):
                ambiguous_sigs.add(s)

    for p in profiles:
        c = (p.get("canonical_name") or "").strip()
        if not c:
            continue
        for a in p.get("aliases") or []:
            fa = fold(a)
            if fa in GENERIC_ALIAS_BLOCK:
                continue  # never merge on a generic category word
            if fa and fa != fold(c):
                registered = False
                if fa in canonical_by_fold and fold(canonical_by_fold[fa]) != fold(c):
                    conflicts.append((a, canonical_by_fold[fa], c, "alias is another org's canonical"))
                else:
                    existing = canon_map.get(fa)
                    if existing is None:
                        canon_map[fa] = c
                        registered = True
                    elif fold(existing) != fold(c):
                        conflicts.append((a, existing, c, "alias claimed by two orgs"))
                    else:
                        registered = True  # already maps to this canonical
                # Register MC ↔ Motorcycle Club variants only when the alias
                # itself mapped cleanly to c — a conflicting alias must not
                # sneak back in through its spelling variant.
                if registered:
                    for v in mc_variants(a):
                        canon_map.setdefault(fold(v), c)
            s = sig_tokens(a)
            if s:
                prev = sig_map.get(s)
                if prev is None:
                    sig_map[s] = c
                elif fold(prev) != fold(c):
                    ambiguous_sigs.add(s)

    for s in ambiguous_sigs:
        sig_map.pop(s, None)

    return canon_map, sig_map, set(canonical_by_fold.keys()), conflicts


def make_resolvers(canon_map, sig_map, canonical_folds):
    def resolve_exact(name):
        """Follow the fold map to a fixpoint; return canonical or None."""
        cur = (name or "").strip()
        for _ in range(6):
            nxt = canon_map.get(fold(cur))
            if nxt is None:
                break
            if fold(nxt) == fold(cur):
                cur = nxt
                break
            cur = nxt
        return cur if fold(cur) in canonical_folds else None

    def resolve_core(name):
        s = sig_tokens(name)
        if s and len("".join(sorted(s))) >= MIN_CORE_CHARS:
            c = sig_map.get(s)
            if c:
                return resolve_exact(c) or c
        return None

    def resolve_node(names):
        """names = [standard_name, *aliases, original_text_name].
        Returns (canonical, method)."""
        primary = names[0] if names else ""
        # Exact/alias matching.
        for i, nm in enumerate(names):
            c = resolve_exact(nm)
            if c:
                return c, ("exact" if i == 0 else "alias")
        # Fuzzy core matching.
        for nm in names:
            c = resolve_core(nm)
            if c:
                return c, "core"
        return primary, "self"

    return resolve_exact, resolve_core, resolve_node


def node_names(node):
    """Distinct names a node is known by: standard_name, aliases,
    original_text_name — deduped by fold, standard_name first."""
    raw = [(node.get("standard_name") or "").strip()]
    for a in node.get("aliases") or []:
        if isinstance(a, str) and a.strip():
            raw.append(a.strip())
    otn = (node.get("original_text_name") or "").strip()
    if otn:
        raw.append(otn)
    seen, out = set(), []
    for n in raw:
        if n and fold(n) not in seen:
            seen.add(fold(n))
            out.append(n)
    return out

# ── profile merge (multi-language) ─────────────────────────────────────

def merge_profiles(canonical_display, profiles):
    aliases, seen = [], {fold(canonical_display)}
    by_country = {}            # canon.lower() → strongest country-link record
    descriptions, periods = [], []
    any_defunct = False        # some profile says defunct
    any_active = False         # some profile says active
    founded, dissolved = [], []
    country = "Unknown"
    own_sources = []

    for p in profiles:
        for nm in [p.get("canonical_name", "")] + (p.get("aliases") or []):
            nm = (nm or "").strip()
            if nm and fold(nm) not in seen:
                seen.add(fold(nm))
                aliases.append(nm)
        p_url = p.get("source_url") or ""
        p_title = extract_wiki_title(p_url) if p_url else ""
        for rec in p.get("country_links") or []:
            # country_links entries are records {country, evidence_quote,
            # context}. Older string entries are tolerated for back-compat.
            if isinstance(rec, dict):
                name = str(rec.get("country", "")).strip()
            else:
                name = str(rec).strip()
                rec = {"country": name, "evidence_quote": "", "context": ""}
            # Drop anything that isn't a real country (subnational regions,
            # continents, …) and fold alias spellings to one canonical name, so
            # the footprint data is clean and de-duplicated at the source.
            canon = normalize_country(name)
            if not canon:
                continue
            key = canon.lower()
            # Rebuild the record explicitly so only the carried fields survive
            # (drops any stale `kind` left in older profile data).
            # Attach the source that produced each footprint quote.
            new_rec = {"country": canon,
                       "evidence_quote": rec.get("evidence_quote", "") or "",
                       "context": rec.get("context", "") or "",
                       "source_url": p_url,
                       "source_title": p_title}
            cur = by_country.get(key)
            if cur is None:
                by_country[key] = [new_rec]
            else:
                # Accumulate distinct evidence quotes per country so
                # multi-language profiles both contribute visible entries.
                seen_quotes = {r.get("evidence_quote", "") for r in cur}
                if new_rec.get("evidence_quote", "") not in seen_quotes:
                    cur.append(new_rec)
        if p.get("description"):
            descriptions.append(p["description"])
        if p.get("time_period") and p["time_period"] != "Unknown":
            periods.append(p["time_period"])
        d = p.get("is_defunct")
        if d is True:
            any_defunct = True
        elif d is False:
            any_active = True
        # "unknown" / anything else contributes no signal
        fy, dy = year_int(p.get("founded_year")), year_int(p.get("dissolved_year"))
        if fy:
            founded.append(fy)
        if dy:
            dissolved.append(dy)
        if country == "Unknown" and p.get("country") and p["country"] != "Unknown":
            # Normalize the origin to its canonical spelling; if it isn't a real
            # country (a region slipped in), leave it Unknown rather than keep it.
            country = normalize_country(p["country"]) or "Unknown"
        url = p.get("source_url")
        if url:
            own_sources.append({"url": url, "title": extract_wiki_title(url)})

    # The home country is identity, not a footprint (step 3's NOT-HOME rule).
    # Drop it from country_links even when the LLM emitted it anyway, so the
    # invariant holds regardless of model compliance or spelling mismatch.
    home_key = country.lower() if country and country != "Unknown" else None
    # Flatten by_country lists back to records, deduplicated by evidence_quote
    country_links = []
    for k, recs in by_country.items():
        if k == home_key:
            continue
        country_links.extend(recs)
    country_links.sort(key=lambda rec: rec["country"].lower())

    aliases.sort(key=lambda x: x.lower())
    return {
        "standard_name": canonical_display,
        "aliases": aliases,
        "description": max(descriptions, key=len) if descriptions else "",
        "country": country,
        "country_links": country_links,
        "time_period": max(periods, key=len) if periods else "Unknown",
        "is_defunct": (True if (any_defunct or dissolved)
                       else False if any_active
                       else "unknown"),
        "founded_year": str(min(founded)) if founded else None,
        "dissolved_year": str(max(dissolved)) if dissolved else None,
        "own_sources": own_sources,
    }


# ── core ───────────────────────────────────────────────────────────────

def build(txts_dir):
    raw_nodes, raw_edges, n_files = load_graphs(txts_dir)
    profiles = load_profiles(txts_dir)
    print(f"Loaded {n_files} article graphs | {len(profiles)} org profiles")
    print(f"Raw: {len(raw_nodes)} nodes, {len(raw_edges)} edges")

    profiles = unify_mutual_profiles(profiles)

    canon_map, sig_map, canonical_folds, conflicts = build_maps(profiles)
    resolve_exact, resolve_core, resolve_node = make_resolvers(
        canon_map, sig_map, canonical_folds)
    print(f"Dedup map: {len(canon_map)} exact + {len(sig_map)} core entries")
    if conflicts:
        print(f"⚠ {len(conflicts)} alias conflict(s) NOT auto-merged "
              f"(resolve in KNOWN_DUPLICATES if same org):")
        for alias, a, b, why in conflicts[:25]:
            print(f"    '{alias}': '{a}' vs '{b}'  [{why}]")
        if len(conflicts) > 25:
            print(f"    ... and {len(conflicts) - 25} more")

    # Profiles keyed by fold(resolved canonical), merged across languages.
    prof_groups = defaultdict(list)
    for p in profiles:
        rc = resolve_exact(p.get("canonical_name", "")) or (p.get("canonical_name") or "").strip()
        if rc:
            prof_groups[fold(rc)].append((rc, p))
    merged_profiles = {}
    for fk, items in prof_groups.items():
        rc = items[0][0]
        merged_profiles[fk] = merge_profiles(rc, [it[1] for it in items])

    # Resolve every node (using all its names). Accumulate group data and
    # an augmented name→canonical map for edge resolution.
    augmented = dict(canon_map)
    alias_merges, core_merges = {}, {}
    groups = defaultdict(lambda: {
        "display": Counter(), "original_text_names": set(), "aliases": set(),
        "descriptions": [], "time_periods": [], "source_urls": set(),
    })

    for node in raw_nodes:
        names = node_names(node)
        if not names:
            continue
        canonical, method = resolve_node(names)
        if fold(names[0]) != fold(canonical):
            if method == "alias":
                alias_merges[names[0]] = canonical
            elif method == "core":
                core_merges[names[0]] = canonical

        # feed every name this node is known by into the edge map
        for nm in names:
            f = fold(nm)
            if not f:
                continue
            if f in canon_map:
                continue
            if f in augmented and fold(augmented[f]) != fold(canonical):
                continue
            augmented[f] = canonical

        g = groups[fold(canonical)]
        g["display"][canonical] += 1
        for otn in [(node.get("original_text_name") or "").strip()]:
            if otn:
                g["original_text_names"].add(otn)
        for a in node.get("aliases") or []:
            if isinstance(a, str) and a.strip():
                g["aliases"].add(a.strip())
        if node.get("context"):
            g["descriptions"].append(node["context"].strip())
        if node.get("time_period"):
            g["time_periods"].append(node["time_period"].strip())
        url = (node.get("source_url") or "").strip()
        if url:
            g["source_urls"].add(url)

    if alias_merges or core_merges:
        print(f"Cross-name merges: {len(alias_merges)} via alias, "
              f"{len(core_merges)} via core (audit these):")
        shown = 0
        for mention, canonical in sorted(alias_merges.items()):
            print(f"    [alias] '{mention}' → '{canonical}'")
            shown += 1
            if shown >= 20:
                break
        shown = 0
        for mention, canonical in sorted(core_merges.items()):
            print(f"    [core]  '{mention}' → '{canonical}'")
            shown += 1
            if shown >= 20:
                break

    # Build final nodes.
    nodes = []
    final_name_by_fold = {}
    profiled_ct = unprofiled_ct = defunct_ct = 0

    for fk, g in groups.items():
        prof = merged_profiles.get(fk)
        canonical_display = prof["standard_name"] if prof else g["display"].most_common(1)[0][0]

        own_urls = {s["url"] for s in (prof["own_sources"] if prof else [])}
        mentioned_in = [
            {"url": u, "title": extract_wiki_title(u)}
            for u in sorted(g["source_urls"]) if u not in own_urls
        ]

        if prof:
            # Aliases come ONLY from the profile (step 3, the org's own
            # authoritative article). Mention-derived aliases in g["aliases"]
            # were already used for MATCHING (folding mentions into this
            # node), but they are deliberately NOT stored here — they are
            # frequently noise scraped from other orgs' articles (e.g. a
            # passing "the Calabrian mafia" or "the camorra" near a mention
            # of 'Ndrangheta), and unioning them produces wrong aliases.
            alias_set, seen = [], {fold(canonical_display)}
            for a in prof["aliases"]:
                if a and fold(a) not in seen:
                    seen.add(fold(a))
                    alias_set.append(a)
            clinks = prof["country_links"]
            node = {
                "standard_name": canonical_display,
                "aliases": sorted(alias_set, key=lambda x: x.lower()),
                "original_text_names": sorted(g["original_text_names"]),
                "description": prof["description"],
                "country": prof["country"],
                "country_links": clinks,
                "time_period": prof["time_period"],
                "is_defunct": prof["is_defunct"],
                "founded_year": prof["founded_year"],
                "dissolved_year": prof["dissolved_year"],
                "profiled": True,
                "own_sources": prof["own_sources"],
                "mentioned_in": mentioned_in,
            }
            profiled_ct += 1
            if prof["is_defunct"] is True:
                defunct_ct += 1
        else:
            # No own article — every attribute here is a guess scraped from
            # passing mentions in OTHER orgs' articles, so it's low-quality
            # by nature. We keep identity (names + aliases, useful for search
            # and for matching) but do NOT fabricate confident attributes:
            #   - description: dropped (a one-sentence mention can't profile
            #     an org, and the longest mention was often about the
            #     relationship, not the org).
            #   - time_period / country: left empty / Unknown.
            node = {
                "standard_name": canonical_display,
                "aliases": sorted(g["aliases"], key=lambda x: x.lower()),
                "original_text_names": sorted(g["original_text_names"]),
                "description": None,
                "country": None,
                "country_links": [],
                "time_period": None,
                "is_defunct": "unknown",
                "founded_year": None,
                "dissolved_year": None,
                "profiled": False,
                "own_sources": [],
                "mentioned_in": mentioned_in,
            }
            unprofiled_ct += 1

        nodes.append(node)
        final_name_by_fold[fk] = canonical_display

    # Edge endpoint resolution: augmented map (with chain) then core,
    # constrained to names that survive as nodes.
    def resolve_endpoint(name):
        cur = (name or "").strip()
        for _ in range(6):
            nxt = augmented.get(fold(cur))
            if nxt is None:
                break
            if fold(nxt) == fold(cur):
                cur = nxt
                break
            cur = nxt
        if fold(cur) in final_name_by_fold:
            return fold(cur)
        c = resolve_core(cur)
        if c and fold(c) in final_name_by_fold:
            return fold(c)
        return fold(cur)

    edge_map = {}
    dropped_edges = 0
    for edge in raw_edges:
        s_raw = (edge.get("source") or "").strip()
        t_raw = (edge.get("target") or "").strip()
        rel = (edge.get("relationship") or "").strip().lower()
        if not s_raw or not t_raw or not rel:
            continue
        sf, tf = resolve_endpoint(s_raw), resolve_endpoint(t_raw)
        if sf == tf or sf not in final_name_by_fold or tf not in final_name_by_fold:
            dropped_edges += 1
            continue
        s, t = final_name_by_fold[sf], final_name_by_fold[tf]
        desc = (edge.get("context") or "").strip()
        url = (edge.get("source_url") or "").strip()
        raw_q = edge.get("evidence_quote")
        quote = raw_q.strip() or None if isinstance(raw_q, str) else None
        key = (sf, tf, rel, desc, url)
        if key not in edge_map:
            tp = (edge.get("time_period") or "").strip()
            edge_map[key] = {
                "source": s, "target": t, "relationship": rel,
                "descriptions": [desc] if desc else [],
                "time_periods": [tp] if tp else [],
                "evidence_quote": quote,
                "source_urls": [url] if url else [],
            }
        elif quote and not edge_map[key].get("evidence_quote"):
            edge_map[key]["evidence_quote"] = quote
    edges = list(edge_map.values())

    nodes.sort(key=lambda n: n["standard_name"].lower())
    edges.sort(key=lambda e: (e["source"].lower(), e["target"].lower(), e["relationship"]))

    print(f"Dropped {dropped_edges} dangling/self-loop edge(s)")
    print(f"Nodes: {len(nodes)} ({profiled_ct} profiled, {unprofiled_ct} unprofiled, "
          f"{defunct_ct} defunct)")
    print(f"Edges: {len(edges)}")
    return nodes, edges, profiled_ct, unprofiled_ct, defunct_ct


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", "-d", default="./txts")
    parser.add_argument("--output", "-o", default="crimenet_raw.json")
    args = parser.parse_args()

    txts_dir = Path(args.dir)
    if not txts_dir.exists():
        print(f"Directory not found: {txts_dir}")
        return

    nodes, edges, profiled, unprofiled, defunct = build(txts_dir)

    Path(args.output).write_text(json.dumps({
        "metadata": {
            "n_nodes": len(nodes), "n_edges": len(edges),
            "n_profiled": profiled, "n_unprofiled": unprofiled,
            "n_defunct": defunct,
        },
        "nodes": nodes,
        "edges": edges,
    }, ensure_ascii=False, indent=2), "utf-8")
    print(f"Saved → {args.output}")


if __name__ == "__main__":
    main()