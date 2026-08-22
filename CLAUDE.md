# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

CRIMENET is a knowledge graph of global organized crime (4,505 orgs, 10,935 relationships) extracted from 1,418 Wikipedia articles via DeepSeek LLM. Every edge carries a verbatim evidence quote, a description, a time period, and a versioned source URL. The web app is fully static, served from `/crimenet/` at alvarofrancomartins.com/crimenet.

The pipeline fetches 1,491 articles (`data/articles.csv` + `data/txts/`), but some — concept articles, non-org subjects — yield no nodes. The 1,418 figure is the number of articles that actually contributed data to the graph, computed as unique article URLs across all nodes' `own_sources` and `mentioned_in`. This is the real source footprint and the number used everywhere in the app and docs.

## Key commands

```bash
# One-time
pip install -r requirements.txt
export DEEPSEEK_API_KEY="sk-..."

# Full pipeline (steps 0→4, stops on first failure, ends at data/crimenet_raw.json — does NOT build app)
cd pipeline && python run_pipeline.py && cd ..

# Individual pipeline steps (from pipeline/ dir, all re-runnable independently)
python 1_fetch_wikipedia.py --csv ../data/articles.csv --output ../data/txts           # re-fetch all
python 1_fetch_wikipedia.py -F "Jalisco" --csv ../data/articles.csv --output ../data/txts --force  # single article
python 2_extract_network.py --dir ../data/txts --force-failed                           # retry partials only
python 4_merge.py --dir ../data/txts --output ../data/crimenet_raw.json         # rebuild dataset (no API calls)

# Build app assets from data/crimenet.json (ORDER MATTERS)
python build/build_compact_data.py        --input data/crimenet.json --output app/data/compact.json
python build/build_adjacency.py        --input data/crimenet.json --output app/data/crimenet_adj.json
python build/build_evidence_shards.py  --input data/crimenet.json --output app/data/evidence
python build/build_relationship_summaries.py --input data/crimenet.json --output app/data/relationship_summaries
python build/build_communities_data.py --input data/crimenet.json --output app/data/communities.json --workers 10
python build/build_bridges_data.py --input data/crimenet.json --communities app/data/communities.json --output app/data/bridges.json
python build/build_triadic_data.py --input data/crimenet.json --output app/data/triadic_signals.json
python build/build_centrality.py         --input data/crimenet.json --output app/data/centrality.json
python build/build_static_pages.py     --input data/crimenet.json --output app --base-url https://www.alvarofrancomartins.com/crimenet

# Local preview (must use HTTP — fetch() fails on file://)
python -m http.server 8000   # then open localhost:8000/app/index.html

# Audit (1. audit 0–5 → 2. review → 3. apply)
python audit/0_review_wrong_merges.py && python audit/1_review_missed_merges.py && python audit/2_review_edges.py && python audit/3_review_country_links.py && python audit/4_review_umbrella_orgs.py && python audit/5_review_non_criminal_orgs.py
python audit/6_review_suggestions.py   # LLM second opinion → audit_data/6_llm_verdicts.json (run before apply)
python audit/7_apply_corrections.py      # auto-applies confident audit suggestions + manual overrides → data/crimenet.json
#    If you spot an error, add a fix to audit/curated_corrections.py — it always wins over auto.
```

## Architecture

### Pipeline (pipeline/) — Wikipedia → crimenet_raw.json

Each step writes to a distinct output; any step can be re-run independently without invalidating others. Steps 1–4 are resumable (skip existing unless `--force`). Step 2 runs 50 parallel workers against DeepSeek.

- **Step 0**: plain URLs → versioned URLs with `oldid` (articles.csv)
- **Step 1**: fetch Wikipedia HTML via MediaWiki API, extract clean body text + infobox → `data/txts/<folder>/{content.txt,url.txt}`
- **Step 2**: LLM extracts org nodes + edges per article → `article_graph.json` (chunked by ~2500 words, infobox appended to every chunk)
- **Step 3**: LLM enriches profiled orgs (description, aliases, country, country_links, time_period, is_defunct) → `org_profile.json`. Two passes: Pass 1 profiles the subject org; Pass 2 extracts country footprints
- **Step 4**: merges all graph fragments, runs auto-dedup (fuzzy + containment), attaches org profiles, normalizes countries via `lib/countries.py` → `data/crimenet_raw.json`

Step 4 does NOT apply any hand-curated corrections. Run `audit/7_apply_corrections.py` afterward to produce the final `data/crimenet.json`.

**Critical:** Confident audit suggestions are auto-applied by `7_apply_corrections.py`. For manual fixes, add entries to `audit/curated_corrections.py` — they always win over auto-suggestions on conflict. The `6_review_suggestions.py` step is a second opinion that writes `audit_data/6_llm_verdicts.json` and never edits `curated_corrections.py`; `7_apply_corrections.py` reads that report and lets its high-confidence rejections veto the matching auto wrong-merge (BLOCKLIST) and duplicate (KNOWN_DUPLICATES) suggestions only.

### Build (build/) — crimenet.json → static app/

**Build order matters.** `build_compact_data.py` generates a compact client-side data file (`app/data/compact.json`) that the browse page loads for in-panel org/country previews. `build_static_pages.py` no longer generates individual org or country pages — clicking an org or country in the browse panel renders its details directly in the right panel.

- `build_compact_data.py` → `app/data/compact.json` (org metadata + country listings for client-side rendering)
- `build_adjacency.py` → `app/data/crimenet_adj.json` (topology for the connection finder autocomplete)
- `build_evidence_shards.py` → `app/data/evidence/NNN.json` (128 shards by FNV-1a of org name)
- `build_relationship_summaries.py` → `app/data/relationship_summaries/NNN.json` (128 shards by FNV-1a of pair key; LLM-generated paragraph summaries for every directly-connected pair)
- `build_communities_data.py` → `app/data/communities.json` (self-contained: Infomap community detection + DeepSeek characterization with title, full summary, and short summary; first run requires DEEPSEEK_API_KEY, subsequent runs cache by frozenset(org_names) and make zero API calls when the partition is unchanged)
- `build_bridges_data.py` → `app/data/bridges.json` (self-contained: top 50 bridge organizations by cross-community cooperation edges; reads crimenet.json + communities.json)
- `build_triadic_data.py` → `app/data/triadic_signals.json` (self-contained: triadic closure candidate pairs from common cooperation partners and common adversaries; reads crimenet.json directly)
- `build_centrality.py` → `app/data/centrality.json` (network centrality metrics: degree, betweenness, PageRank on full, cooperation, and conflict graphs)
- `build_static_pages.py` → `app/index.html` + `app/css/main.css` + sitemap.xml + copies `crimenet.json` into `app/`

**`build_static_pages.py` IS the source** for `app/index.html`, `app/css/main.css`, and `app/sitemap.xml`. **Never edit files in `app/` that are generated by the build** — changes will be lost on the next rebuild. It overwrites `index.html`, `css/main.css`, and `sitemap.xml` in place.

`app/browse.html`, `app/footprints.html`, `app/knowledge_graph.html`, and `app/about.html` are **hand-authored source files** (connection finder, D3 world map, three.js 3D network, and the about page). They live in `app/` root so the build's `shutil.copyfile` of `crimenet.json` lands next to them. They are NOT generated. about.html's stat cards and the `app/js/ask_ai.js` Ask AI system prompt's headline-count line both carry the same `crimenet-stats` markers as the README (in ask_ai.js the markers are `//` JS comments), so `tools/update_readme_stats.py` keeps all three in sync (run by `7_apply_corrections.py`). Do NOT hand-edit those marked lines.

### Connection finder

The connection finder lives on `app/browse.html` (a hand-authored page). It fetches connection evidence from `evidence/` shards and per-pair relationship summaries from `relationship_summaries/` shards, and renders Relationship Summary + Direct linkages with Source/Time/Quote pills.

### Ask CRIMENET AI

A natural language query interface on `app/ask.html`. Users ask questions about organized crime in plain English; the LLM calls tools that query the static data files, and the agent loop runs entirely in the browser. Only the DeepSeek API call goes through a Netlify Function proxy (`netlify/functions/crimenet-ask.js`, lives in the Hugo site repo). 13 tools: `get_organization`, `find_by_type`, `get_connections`, `get_relationship_summary`, `find_by_country`, `find_by_countries`, `find_paths`, `find_cooperation_routes`, `get_network_neighborhood`, `get_community`, `get_triadic_signals`, `get_bridges`, `get_centrality`. Code lives in `app/js/ask_ai.js` (tool definitions, implementations, agent loop, system prompt, UI wiring). Every answer cites the underlying Wikipedia sources so claims remain auditable. `app/crimenet_app_notes.md` is the human-readable reference for how Ask CRIMENET AI works — update it whenever `app/ask.html` or `app/js/ask_ai.js` changes.

**When to use what.** For specific, structured queries — looking up a single org, finding all edges between two specific orgs, seeing which orgs operate in a given country, browsing community clusters, or exploring triadic signals — use the dedicated tabs on `browse.html` and `index.html`. These render the data directly with no LLM involvement. For complex questions that combine multiple pieces of information — tracing indirect relationships through intermediaries, comparing orgs across countries, ranking by network influence, or asking about structural patterns — use Ask CRIMENET AI. The AI queries the graph through tools and synthesizes an answer. As with any LLM response, verify claims against the source evidence the answer cites.

### App pages — cross-linking

index.html has a two-panel dashboard: the left panel toggles between Organizations and Countries; clicking any name renders its full detail in the right panel. `browse.html` hosts the standalone connection finder.

## Key conventions

### Browse page layout

The browse page uses a flex-column wrapper (`.browse-wrap` with `height:100vh`) so the two-panel grid fills the viewport exactly. Each panel (`.browse-panel` with `overflow:hidden`) contains a flex layout where `.panel-scroll` (`flex:1`) is the independently-scrollable area. The grid columns are `1fr 2.2fr` (Browse | Detail). The header (`.browse-head`) is a flex row with the title+description on the left and stats on the right.

### Unicode / text quality

Step 1 strips zero-width Unicode format characters (category Cf: U+200B–U+200F, U+2028–U+202E, U+2060–U+206F, U+FEFF) and replaces NBSP (U+00A0) with regular space in `_clean_inline()`. This applies to infobox keys, values, captions, and subheaders — all paths. Citation brackets in 5 languages are also stripped.

### Wikipedia infobox extraction

BeautifulSoup's `class_` lambdas receive a **string** (space-separated classes), NOT a list. Use substring checks like `"infobox" in c.lower()` — never iterate `c` as if it were a list.

### Country normalization

`pipeline/lib/countries.py` is the single source of truth via `normalize_country()`: canonical allowlist + alias→canonical folding. Step 4 applies this at merge time, so `crimenet.json` is clean at the source. Do NOT add display-time country filtering elsewhere.

### Git

The hand-authored `app/` pages (browse.html, footprints.html, knowledge_graph.html, about.html), evidence shards, adjacency, compact.json, and the copied `crimenet.json` are committed to the repo and are fully regenerable from `data/crimenet.json` via the build steps.

### API key

DeepSeek API key goes in the `DEEPSEEK_API_KEY` env var. Steps 2 and 3 and all audit scripts use it (`pipeline/2_extract_network.py` loads it via `load_key()` from the environment).


### Writing

Your writing should be concise and only the necessary. Never use dashes.