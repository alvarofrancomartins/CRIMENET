"""
Infomap community detection on any edge-type network, followed by LLM
characterization of each community (title + summary via DeepSeek).

Saves:
    data/communities_{network_type}.json      — full community assignments + characterizations
    images/community_sizes_{network_type}.png — histogram + top-20 bar chart

Robust, resumable characterization
----------------------------------
The Infomap partition is deterministic (sorted node ids + a fixed seed), so the
same communities with the same membership are produced every run. LLM
characterization is cached PER COMMUNITY, keyed on the community's exact org set
(not on a fragile size signature) and gated by (PROMPT_VERSION, network_type):

  * A re-run reuses any community whose org set is unchanged AND already carries a
    real (non-"?") title from a run with the same PROMPT_VERSION and network_type.
    Only NEW, CHANGED, or previously-FAILED communities are sent to the LLM, so an
    unchanged partition makes zero API calls.
  * A failed call leaves the title empty/"?", which is never treated as cached, so
    the NEXT run retries it. Failures are no longer baked in permanently.
  * Results are flushed to communities.json after every successful call, so a run
    that is interrupted (or partly rate-limited) can simply be re-run to finish
    the rest.

Characterization runs at low concurrency by default (robustness over speed) with a
sequential cleanup pass for any straggler, and each call has a generous retry
budget for 429 / 5xx / network errors plus output-token escalation. Bump
PROMPT_VERSION when the prompt construction changes to force a full recharacterize.

Usage:
    python 4_communities.py --network-type cooperation   # default
    python 4_communities.py -n conflict                   # conflict communities
    python 4_communities.py -n all                        # all edge types
    python 4_communities.py --workers 1                   # fully sequential
    python 4_communities.py --force                       # ignore cache, recharacterize all
"""

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import networkx as nx
import requests
from requests.adapters import HTTPAdapter
from infomap import Infomap

from common import (load_and_preprocess, print_header, print_dataset_summary,
                    ensure_data_dir, ensure_images_dir, setup_plot,
                    PLOT_DPI, gradient, GREEN_RAMP, COLOR_CONFLICT, COLOR_MUTED,
                    build_graph, edge_matches_network_type,
                    NETWORK_SYSTEM_PROMPTS, VALID_NETWORK_TYPES)

# ── LLM config ─────────────────────────────────────────────────────────────
API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
MAX_TOKENS = 8192          # first-attempt output cap
MAX_TOKENS_RETRY = 16384   # escalate on a length-truncation retry
RETRIES = 5                # content-level attempts (parse errors / truncation)
MAX_RL_RETRIES = 8         # 429 / 5xx / network backoff budget, separate from RETRIES
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 180
TEMPERATURE = 0.0          # deterministic titles → reproducible across runs
TOP_N = 20
DEFAULT_WORKERS = 4        # robustness over speed; the 50-worker burst is what failed

# Bump when the prompt construction changes so cached results from an older prompt
# format are invalidated and every community is recharacterized.
PROMPT_VERSION = 3

session = requests.Session()
session.mount("https://", HTTPAdapter(pool_connections=20, pool_maxsize=20))


def _post_with_rate_limit(headers, payload):
    """POST to DeepSeek, retrying 429 / 5xx / network errors on their own backoff
    budget so a burst of rate-limit or transient server errors does not consume the
    content-retry attempts. Returns the final Response, or None if every attempt
    raised a network-level error."""
    r = None
    for rl in range(MAX_RL_RETRIES):
        try:
            r = session.post(API_URL, headers=headers, json=payload,
                             timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except requests.exceptions.RequestException:
            time.sleep(min(60, 5 * (2 ** rl)))
            r = None
            continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(60, 5 * (2 ** rl)))
            continue
        return r
    return r


def call_llm(api_key, system, user, temperature=TEMPERATURE):
    """One DeepSeek chat call returning the parsed JSON dict, or None after the full
    retry budget is spent. 429 / 5xx / network errors retry on _post_with_rate_limit's
    budget; JSON-parse errors and length-truncations retry here, escalating the output
    cap and nudging for terser output."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    for attempt in range(1, RETRIES + 1):
        max_tokens = MAX_TOKENS if attempt == 1 else MAX_TOKENS_RETRY
        prompt = user
        if attempt > 1:
            prompt += ("\n\nIMPORTANT: a previous attempt failed or was truncated. "
                       "Return STRICT JSON {\"title\": ..., \"summary\": ...} and keep "
                       "the summary to 4-6 sentences.")
        payload = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        try:
            r = _post_with_rate_limit(headers, payload)
            if r is None:
                time.sleep(3 * attempt)
                continue
            r.raise_for_status()
            choice = r.json()["choices"][0]
            if choice.get("finish_reason") == "length" and attempt < RETRIES:
                time.sleep(2)
                continue
            raw = choice["message"]["content"].strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            return json.loads(raw)
        except (json.JSONDecodeError, requests.exceptions.RequestException,
                KeyError, TypeError, IndexError):
            time.sleep(3 * attempt)
    return None


# ── Per-community cache helpers ──────────────────────────────────────────────

def real_title(t):
    """True only for a usable characterization title (not None, empty, or '?')."""
    return isinstance(t, str) and t.strip() not in ("", "?")


def load_prev_characterizations(path, prompt_version, network_type):
    """Map frozenset(org_names) → (title, summary) from a prior communities.json,
    used ONLY when its stored prompt_version AND network_type match the current run.
    Keying on the org set (not the Infomap id, which can renumber) makes reuse robust
    to anything except an actual change in a community's membership. Failed/empty
    titles are skipped so they are always retried."""
    if not os.path.exists(path):
        return {}
    try:
        prev = json.loads(open(path, encoding="utf-8").read())
    except Exception:
        return {}
    if prev.get("prompt_version") != prompt_version:
        return {}
    if prev.get("network_type") != network_type:
        return {}
    out = {}
    for info in (prev.get("communities") or {}).values():
        if real_title(info.get("title")):
            key = frozenset(info.get("orgs") or [])
            out[key] = (info["title"], info.get("summary") or "")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--network-type", "-n", default="cooperation",
                    choices=sorted(VALID_NETWORK_TYPES),
                    help="edge types to include (default: cooperation)")
    ap.add_argument("--workers", "-w", type=int, default=DEFAULT_WORKERS,
                    help=f"parallel DeepSeek workers for characterization "
                         f"(default {DEFAULT_WORKERS}; use 1 for fully sequential)")
    ap.add_argument("--force", "-f", action="store_true",
                    help="ignore the cache and recharacterize every community")
    args = ap.parse_args()

    nt = args.network_type
    nt_label = nt.replace("_", " ").title()
    print_header(f"Communities — Infomap Clustering ({nt_label} Network)")
    data, P = load_and_preprocess()
    print_dataset_summary(P, data)

    system_prompt = NETWORK_SYSTEM_PROMPTS[nt]

    out = ensure_data_dir()

    # ── Build graph (orgs + edges filtered by network type) ──────────────────
    G, _pair_counts = build_graph(P, nt)

    # ── Graph node attributes ────────────────────────────────────────────────
    for n in data["nodes"]:
        name = n.get("standard_name")
        if not name or name not in G:
            continue
        G.nodes[name].update({
            "profiled": n.get("profiled", False),
            "country": n.get("country") or "",
            "description": n.get("description") or "",
            "is_defunct": n.get("is_defunct", "unknown"),
        })

    print(f"\n{nt_label} graph: {G.number_of_nodes():,} nodes, "
          f"{G.number_of_edges():,} edges")

    # ── Infomap community detection ──────────────────────────────────────────
    # Infomap natively handles disconnected components. Every node gets a
    # community assignment. Sort the node list before assigning ids so the
    # partition is fully reproducible (sorted + explicit seed).
    components = list(nx.connected_components(G))
    sizes_comp = sorted((len(c) for c in components), reverse=True)
    lcc_size = sizes_comp[0]
    print(f"Connected components: {len(components):,}")
    print(f"  LCC: {lcc_size:,} nodes; non-LCC: "
          f"{G.number_of_nodes() - lcc_size:,} nodes across "
          f"{len(components) - 1:,} other components")

    id2name = dict(enumerate(sorted(G.nodes())))
    name2id = {v: k for k, v in id2name.items()}

    im = Infomap("--two-level --silent --seed 42")
    for e in data["edges"]:
        if not edge_matches_network_type(e, nt):
            continue
        s, t = e["source"], e["target"]
        if not s or not t or s not in name2id or t not in name2id:
            continue
        im.add_link(name2id[s], name2id[t])
    im.run()

    communities: dict = {}
    for node in im.tree:
        if node.is_leaf:
            communities.setdefault(node.module_id, []).append(id2name[node.node_id])

    # Sort by size descending
    cid2nodes = {str(cid): sorted(nodes) for cid, nodes in communities.items()}
    sorted_cids = sorted(cid2nodes, key=lambda c: len(cid2nodes[c]), reverse=True)
    cid2nodes = {c: cid2nodes[c] for c in sorted_cids}

    sizes = sorted((len(v) for v in cid2nodes.values()), reverse=True)
    print(f"\n{len(cid2nodes)} communities")
    print(f"Size range: {sizes[-1]} – {sizes[0]}, mean: {sum(sizes)/len(sizes):.1f}, "
          f"median: {sizes[len(sizes)//2]}")
    num_singletons = sum(1 for s in sizes if s == 1)
    print(f"Singletons: {num_singletons}")
    print(f"Top 10 sizes: {sizes[:10]}")

    cids = list(cid2nodes.keys())
    org2cid = {name: cid for cid, names in cid2nodes.items() for name in names}
    COMMUNITIES_PATH = str(out / f"communities_{nt}.json")

    # ── Per-community evidence for prompts (degree, descriptions, internal ties) ──
    # Collect ALL unique descriptions per pair (a pair documented across
    # N articles carries more signal). For mixed networks, collect per edge type.
    edge_descs: dict = {}
    for e in data["edges"]:
        if not edge_matches_network_type(e, nt):
            continue
        s, t = e.get("source"), e.get("target")
        if not s or not t:
            continue
        key = tuple(sorted([s, t]))
        descs = e.get("descriptions") or []
        edge_descs.setdefault(key, [])
        for d in descs:
            if d not in edge_descs[key]:
                edge_descs[key].append(d)

    # For mixed networks, also track the relationship type per pair
    edge_rels: dict = {}
    if nt in ("conflict_and_cooperation", "all"):
        for e in data["edges"]:
            if not edge_matches_network_type(e, nt):
                continue
            s, t = e.get("source"), e.get("target")
            if not s or not t:
                continue
            key = tuple(sorted([s, t]))
            edge_rels[key] = e.get("relationship", "?")

    top_orgs: dict = {}
    all_orgs: dict = {}
    internal_edges: dict = {}
    for cid in cids:
        names = cid2nodes[cid]
        sub = G.subgraph(names)
        ranked = sorted(sub.degree(), key=lambda x: (-x[1], x[0]))
        orgs = []
        for name, deg in ranked:
            node = G.nodes[name]
            orgs.append({"name": name, "degree": deg,
                         "country": node.get("country", ""),
                         "description": node.get("description", "")})
        all_orgs[cid] = orgs
        top_orgs[cid] = orgs[:TOP_N]

        nameset = set(names)
        edges_list = []
        for u, v in G.edges():
            if u in nameset and v in nameset:
                key = tuple(sorted([u, v]))
                descs = edge_descs.get(key, [])
                rel = edge_rels.get(key, "") if edge_rels else ""
                if descs:
                    for d in descs:
                        edges_list.append((u, v, d, rel))
                else:
                    edges_list.append((u, v, "", rel))
        internal_edges[cid] = edges_list

    # Edge-type label for prompts
    if nt == "cooperation":
        edge_label = "cooperation ties"
    elif nt == "conflict":
        edge_label = "conflict ties"
    elif nt == "other":
        edge_label = "ties"
    elif nt == "conflict_and_cooperation":
        edge_label = "ties (cooperation or conflict)"
    else:
        edge_label = "ties (cooperation, conflict, or other)"

    def build_prompt(cid):
        orgs = all_orgs[cid]
        edges = internal_edges[cid]
        lines = [f"Community {cid} — {len(orgs)} organizations, "
                 f"{len(edges)} {edge_label}", "\nOrganizations:"]
        for org in orgs:
            desc = org["description"] if org["description"] else "(no description)"
            lines.append(f"  - {org['name']} (degree={org['degree']}, "
                         f"country={org.get('country', '?')}): {desc}")
        if edges:
            lines.append(f"\n{edge_label.capitalize()} within this community:")
            for item in edges:
                u, v, desc = item[0], item[1], item[2]
                rel_prefix = f"[{item[3]}] " if len(item) > 3 and item[3] else ""
                if desc:
                    lines.append(f"  {rel_prefix}{u} — {v}: {desc}")
                else:
                    lines.append(f"  {rel_prefix}{u} — {v}")
        return "\n".join(lines)

    # ── Build communities.json skeleton, reusing cached characterizations ──
    prev = {} if args.force else load_prev_characterizations(COMMUNITIES_PATH,
                                                              PROMPT_VERSION, nt)
    communities_data = {
        "prompt_version": PROMPT_VERSION,
        "network_type": nt,
        "communities": {},
        "org2community": org2cid,
    }
    need = []
    for cid in cids:
        names = cid2nodes[cid]
        title, summary = prev.get(frozenset(names), (None, None))
        communities_data["communities"][cid] = {
            "size": len(names),
            "orgs": names,
            "title": title,
            "summary": summary,
            "top_orgs": [o["name"] for o in top_orgs[cid]],
        }
        if not real_title(title):
            need.append(cid)

    def flush():
        with open(COMMUNITIES_PATH, "w", encoding="utf-8") as f:
            json.dump(communities_data, f, ensure_ascii=False, indent=2)

    flush()
    reused = len(cids) - len(need)
    print(f"\nCharacterization: {reused} reused from cache, {len(need)} to characterize "
          f"(prompt_version={PROMPT_VERSION}, network_type={nt})")

    # ── Characterize the communities that need it ──────────────────────────
    def characterize_batch(todo, api_key, workers, label):
        if not todo:
            return []
        print(f"  {label}: {len(todo)} community(ies), {workers} worker(s)")
        still = []

        def do(cid):
            return cid, call_llm(api_key, system_prompt, build_prompt(cid))

        n = len(todo)
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(do, cid): cid for cid in todo}
            for fut in as_completed(futs):
                cid = futs[fut]
                done += 1
                try:
                    _, res = fut.result()
                except Exception:
                    res = None
                if res and real_title(res.get("title")):
                    communities_data["communities"][cid]["title"] = res["title"].strip()
                    communities_data["communities"][cid]["summary"] = (res.get("summary") or "").strip()
                    flush()
                    print(f"    [{done}/{n}] community {cid} "
                          f"(size {communities_data['communities'][cid]['size']}): "
                          f"{res['title'].strip()}")
                else:
                    still.append(cid)
                    print(f"    [{done}/{n}] community {cid}: FAILED (will retry)")
        return still

    if need:
        api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "DEEPSEEK_API_KEY not set — required to characterize communities. "
                "Set the env var and re-run (already-characterized communities are "
                "cached, so a re-run only does what is missing)."
            )
        remaining = characterize_batch(need, api_key, max(1, args.workers),
                                       "pass 1 (concurrent)")
        if remaining:
            remaining = characterize_batch(remaining, api_key, 1,
                                           "pass 2 (sequential retry)")
        if remaining:
            print(f"  WARNING: {len(remaining)} community(ies) still uncharacterized "
                  f"after retries: {remaining}")
            print(f"  Re-run `python 4_communities.py -n {nt}` to retry only these "
                  f"(everything else is cached).")
        else:
            print("  All communities characterized.")
    else:
        print("  Nothing to characterize (cache covered every community).")

    flush()
    print(f"Saved {len(cids)} communities + {len(org2cid)} orgs → {COMMUNITIES_PATH}")

    # ── Print summary table (top 20) ───────────────────────────────────────
    print(f"\n{'#':>3s}  {'Size':>4s}  {'Title':<48s}  Top Organizations")
    print("-" * 120)
    for i, cid in enumerate(cids[:20]):
        info = communities_data["communities"][cid]
        tops = info["top_orgs"][:4]
        top_str = ", ".join(tops)
        if len(info["top_orgs"]) > 4:
            top_str += f" … +{len(info['top_orgs']) - 4}"
        print(f"{i+1:3d}  {info['size']:4d}  {(info['title'] or '?'):<48s}  {top_str[:60]}")

    # ── Community size distribution plot ───────────────────────────────────
    plt = setup_plot()

    sizes_list = [communities_data["communities"][cid]["size"] for cid in cids]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7.5))

    ax1.grid(axis="x", visible=False)
    ax1.grid(axis="y", color="#e8e8e8", linewidth=0.9)
    ax1.hist(sizes_list, bins=30, edgecolor="white", linewidth=0.8, color=GREEN_RAMP[1])
    ax1.set_xlabel("Community size")
    ax1.set_ylabel("Count")
    ax1.set_title("Community Size Distribution")
    mean_size = sum(sizes_list) / len(sizes_list)
    ax1.axvline(mean_size, color=COLOR_CONFLICT, linestyle="--", linewidth=2.2,
                label=f"Mean: {mean_size:.1f}")
    ax1.legend()

    top20_cids = cids[:20]
    vals = [communities_data["communities"][c]["size"] for c in reversed(top20_cids)]
    ax2.barh(range(20), vals, color=gradient(len(vals), GREEN_RAMP))
    ax2.set_yticks(range(20))
    ax2.set_yticklabels(
        [(communities_data["communities"][c]["title"] or "?")[:42]
         for c in reversed(top20_cids)], fontsize=10)
    ax2.set_xlabel("Size")
    ax2.set_title("Top 20 Communities by Size")
    for i, v in enumerate(vals):
        ax2.text(v + max(vals) * 0.01, i, str(v), va="center", fontsize=9.5,
                 color=COLOR_MUTED, fontweight="bold")

    plt.tight_layout()
    img_dir = ensure_images_dir()
    chart_path = img_dir / f"community_sizes_{nt}.png"
    plt.savefig(str(chart_path), dpi=PLOT_DPI, bbox_inches="tight")
    print(f"Saved chart → {chart_path}")

    print(f"\nTotal communities: {len(sizes_list)}")
    print(f"Size range: {min(sizes_list)} – {max(sizes_list)}")
    print(f"Mean: {mean_size:.1f}  Median: {sorted(sizes_list)[len(sizes_list)//2]}")
    print(f"Singletons (size=1): {sum(1 for s in sizes_list if s == 1)}")
    print(f"Large communities (size >= 50): {sum(1 for s in sizes_list if s >= 50)}")
    print(f"Communities with >= 10 orgs: {sum(1 for s in sizes_list if s >= 10)}")

    print("\nDone.")


if __name__ == "__main__":
    main()
