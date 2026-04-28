# CRIMENET — Global Criminal Network Database

Open-source database and interactive visualization of alliances and rivalries between criminal organizations worldwide, extracted from Wikipedia with an LLM pipeline.

**1,890 organizations. 3,354 relationships. 771 source articles.**

- Live visualization: <a href="https://www.alvarofrancomartins.com/crimenet" target="_blank">alvarofrancomartins.com/crimenet</a>

<p align="center">
  <a href="https://www.alvarofrancomartins.com/crimenet">
    <img src="featured.png" alt="CRIMENET visualization" width="1000">
  </a>
</p>

## Repository layout

```
.
├── 0_urls_to_articles.py        # Step 0: page_hyperlinks.csv → articles.csv (versioned URLs)
├── 1_fetch_wikipedia.py         # Step 1: Wikipedia text + infobox → txts/
├── 2_extract_network.py         # Step 2: LLM extraction → txts/<slug>/extracted.json
├── 3_merge_network.py           # Step 3: merge per-article JSONs → global_network.json
├── 4_cleanup_and_prepare.py     # Step 4: cleanup + centrality → crimenet.json
├── 5_dedup_edges_with_llm.py    # Step 5: collapse duplicate pairs in crimenet.json
├── cleanup_data.py              # Hand-curated duplicates, exclusions, type overrides
├── index.html                   # D3.js force-directed visualization
├── page_hyperlinks.csv          # Input: one Wikipedia URL per row
├── articles.csv                 # Generated: title, folder_name, versioned URL
├── global_network.json          # Raw merged network (pre-cleanup)
├── crimenet.json                # Final network (cleaned, deduplicated, with centrality)
├── deepseek_api_key.txt         # API key (not committed)
├── txts/                        # One folder per article (content.txt, url.txt, extracted.json)
├── notebooks/                   # Analysis notebooks for the report
├── report/                      # Technical results report
└── README.md
```

## Pipeline

Run the six numbered scripts in order.

```bash
python 0_urls_to_articles.py --input page_hyperlinks.csv --output articles.csv
python 1_fetch_wikipedia.py --csv articles.csv --output ./txts
python 2_extract_network.py --dir ./txts
python 3_merge_network.py --dir ./txts --output global_network.json --stats
python 4_cleanup_and_prepare.py --input global_network.json --output crimenet.json
python 5_dedup_edges_with_llm.py --input crimenet.json
```

### Step 0: URLs → `articles.csv`

`0_urls_to_articles.py` reads plain Wikipedia URLs from `page_hyperlinks.csv` (one URL per row), queries the Wikipedia API for the current revision ID of each, and writes `articles.csv` with title, folder name, and versioned URL (`?oldid=...`).

`page_hyperlinks.csv` format:

```csv
url
https://en.wikipedia.org/wiki/Sinaloa_Cartel
https://it.wikipedia.org/wiki/Cosa_nostra
```

Both English and Italian Wikipedia URLs are supported. Language is detected from the domain. The script is resumable: if it crashes, re-run and it picks up where it left off. Titles that fail to resolve to a revision after retries are listed at the end of the run and never silently saved. Duplicate URLs (or URLs that resolve to the same Wikipedia title) are deduplicated automatically.

Folder names are sanitized to be filesystem-safe (path separators in titles like `CBL/BFL` become `CBL_BFL`), and URLs are properly percent-encoded so that titles with apostrophes, slashes, or accented characters work correctly downstream.

### Step 1: Fetch Wikipedia text → `txts/`

`1_fetch_wikipedia.py` reads `articles.csv` and, for each article, makes two calls to the MediaWiki Action API, **both pinned to the recorded revision (`oldid`)**:

- `action=query&prop=extracts&explaintext=1` — clean, well-structured plain text body.
- `action=parse&prop=text` — rendered HTML, parsed with BeautifulSoup to extract the infobox table (aliases, allies, rivals, years active). The infobox is appended to the article text as key-value pairs.

Two calls are needed because MediaWiki's plain-text endpoint produces well-structured texts but strips out infobox tables, while the HTML endpoint preserves the infobox but yields lower-quality body text. Pinning both calls to the same `oldid` means the text used for extraction is exactly the version the URL points to, regardless of later edits to the article. Writes `txts/<slug>/content.txt` and `txts/<slug>/url.txt`. Resumes automatically. Use `--force` to re-fetch all.

### Step 2: LLM extraction → `txts/<slug>/extracted.json`

Add a DeepSeek API key in `deepseek_api_key.txt` in the project root.

`2_extract_network.py` sends each article to the DeepSeek API with a structured prompt that enforces canonical output directly. Extracts:

- **Nodes**: standardized name, aliases, type, context, time period. The `type` is constrained to one of 9 canonical values (`cartel`, `mafia`, `gang`, `motorcycle_club`, `clan`, `triad`, `militia`, `faction`, `terrorist_organization`) or `other`.
- **Edges**: source, target, relationship (`alliance`, `rivalry`, or `other`), optional detail, context, time period. The prompt explicitly maps both cooperation and structural relationships to `alliance` (sub-groups, factions, splinters, successors, mergers, support clubs, etc.), maps conflict to `rivalry`, and reserves `other` for the rare cases where the relationship is genuinely neither cooperative nor hostile.

Constraining the schema in the prompt instead of post-processing means most cleanup work in step 4 becomes unnecessary.

Articles longer than 2,500 words are chunked, with the article's opening paragraph passed as context to every chunk for entity resolution. Runs in parallel across 50 workers by default. On JSON parse errors or `finish_reason=length` truncations, retries with double `max_tokens` and a "be concise" nudge. Partial failures (some chunks succeeded, others failed) are flagged loudly and the output JSON is marked `incomplete`.

- `--force`: re-extract everything.
- `--force-failed`: retry only folders with missing, broken, or partial `extracted.json`.
- `--workers N`: override default parallelism.

### Step 3: Merge → `global_network.json`

`3_merge_network.py` combines all `extracted.json` files into a single `global_network.json`. Nodes are deduplicated by name (case-insensitive), with aliases, descriptions, and source references merged across duplicates. Edges are deduplicated by `(source, target, relationship, detail)`. Every node and edge is tagged with the source article's versioned Wikipedia URL.

### Step 4: Cleanup → `crimenet.json`

`4_cleanup_and_prepare.py` applies the work that can't be done at the LLM level:

- **Cross-article deduplication**: a curated dictionary maps known variant spellings to their canonical name (e.g., `"Medellin Cartel"` → `"Medellín Cartel"`, `"FARC-EP"` → `"FARC"`). No fuzzy matching.
- **Per-organization type overrides**: organizations the LLM consistently mistypes get their type forced to the correct value.
- **Hand-curated exclusions**: non-criminal entities (governments, NGOs) that slip through extraction get removed.
- **Generic node filtering**: umbrella terms (`"Russian organized crime"`, `"Colombian drug cartels"`) are removed; specific organizations with similar names are preserved via a safelist.
- **Type-name safety net**: a small fallback map catches occasional LLM slips on type names (e.g., `crime_family` → `mafia`, `crew` → `gang`). Relationship and detail values are passed through unchanged — the prompt constrains those upstream.
- **Source URL splitting**: each organization's URLs are split into `own_source` (the page about the organization) and `mentioned_in` (other articles referencing it).
- **Network setup**: Create the network and compute the betwenness centrality three ways (alliance-only, rivalry-only, combined) using NetworkX, so the (D3.js) visualization can resize nodes correctly for each filter.

Hand-curated data lives in `cleanup_data.py` (`KNOWN_DUPLICATES`, `TO_BE_EXCLUDED`, `NODE_TYPE_OVERRIDES`). Edit that file to add new cases — no code changes needed.

Outputs `crimenet.json` by default. Use `--output` to write to a different filename.

### Step 5: Edge deduplication → `crimenet.json` (in place)

`5_dedup_edges_with_llm.py` collapses cases where the same pair of organizations has multiple edges (typically because they appeared in several articles, or because their relationship changed over time). For each pair `(A, B)`:

- If every edge has a parseable date → keep the one with the latest end date.
- If some edges have dates and others don't → ask the LLM to pick the most current.
- If no edges have dates → ask the LLM to pick the most current.

Direction is treated as undirected: `(A, B)` and `(B, A)` are the same pair, and an alliance and a rivalry between the same pair compete (latest wins). Sources and descriptions from dropped edges are merged into the survivor so no provenance is lost. The original `crimenet.json` is backed up to `crimenet.json.bak` before overwriting.

`--dry-run` runs the deduplication and logs every decision without writing the output file. Useful for inspecting the LLM's choices before committing.

### Visualize

```bash
python -m http.server 8000
```

Then open [http://localhost:8000/index.html](http://localhost:8000/index.html). `index.html` expects `crimenet.json` in the same folder. The D3.js graph supports alliance/rivalry filtering, search by organization (dropdown ranked by betweenness), neighbor isolation on click, a side panel with organization details and edge evidence, and adjustable force parameters.

### Next steps

Implement a workflow for correcting LLM extraction mistakes, such as misclassified edges or spurious nodes.