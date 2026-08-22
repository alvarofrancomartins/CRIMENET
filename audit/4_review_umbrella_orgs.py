"""
4_review_umbrella_orgs.py

Audit every organization in the graph: is it a real single criminal
organization, or an umbrella term / collective category?

Every node — profiled or unprofiled — is sent to DeepSeek for judgment.
The LLM sees the node's full profile (name, description, aliases, country,
time period) and decides whether it names a specific group or a generic
label that lumps together multiple distinct entities.

It writes:

  4_umbrella_orgs.py — a TO_BE_EXCLUDED set of the orgs the model judged to be
  umbrella terms / collective categories, each annotated with the model's reason,
  confidence, and source URLs. Low-confidence ones follow in a same-shape
  TO_BE_EXCLUDED_LOW_CONFIDENCE block. 7_apply_corrections.py auto-applies the
  confident (high/medium) entries and drops those nodes and every edge touching
  them.

The run prints total API calls + tokens + rough cost.

Needs the DEEPSEEK_API_KEY environment variable set.

Usage:
    python 4_review_umbrella_orgs.py -w 50
"""

import json
import argparse
from collections import Counter
from pathlib import Path

import judge

UMBRELLA_SYSTEM = """You are auditing a database of criminal organizations. Some entries are NOT real single organizations but are umbrella terms, collective categories, or geographic groupings — the Wikipedia article describes a phenomenon, not a specific group.

You are given an organization's full profile: its name, description, aliases, country, and time period. Decide ONE thing: is this a real single criminal organization, or is it an umbrella term / collective category that lumps together multiple distinct groups?

Return ONLY a JSON object:
{
  "is_umbrella": true | false,
  "confidence": "high" | "medium" | "low",
  "reason": "one short sentence"
}

What counts as an UMBRELLA (is_umbrella=true):
- A Wikipedia article about "organised crime in [country]" — describing the phenomenon in that country, not a single group.
- A name that is a general label for many unrelated groups: "[Nationality] mafia" when the article describes it as "a term for various criminal organizations", "a collective of", "various groups", "a colloquial term", etc.
- A geographic grouping that collects distinct organizations solely by region or ethnicity, with no single structure or leadership.

What counts as a REAL ORGANIZATION (is_umbrella=false):
- A specific criminal organization with an identifiable structure, membership, leadership, or history — even if it has many factions or chapters (e.g., Sicilian Mafia / Cosa Nostra, American Mafia, Mexican Mafia).
- A specific gang, cartel, crime family, syndicate, or brotherhood — even if loosely structured.
- A named criminal group with a specific founding story, founder, or territory.

BE CONSERVATIVE. This list is used to DELETE organizations from the database. Only set is_umbrella=true when the evidence clearly shows the article is about a general phenomenon, not a specific group. If you are unsure, set is_umbrella=false with confidence "low" so a human reviews it (it will NOT be auto-applied).

Key signal: read the DESCRIPTION carefully. If it says things like "a term for", "various criminal organizations", "a collective of", "a colloquial term for", "a category of", "refers to various", the entry is almost certainly an umbrella term. If it describes a SPECIFIC group — when and where it was founded, who founded it, what it does specifically — it is a real organization."""


def parse_verdict(v):
    """llm_call result → (is_umbrella|None, confidence, reason). None = error."""
    if not isinstance(v, dict):
        return None, "low", "API/parse error — check manually"
    return (bool(v.get("is_umbrella", False)),
            str(v.get("confidence", "low")).lower(),
            v.get("reason", ""))


def select_candidates(nodes):
    """Return every node as a candidate for LLM judgment.  No pre-filtering —
    the LLM is better at spotting unexpected umbrella terms than pattern
    matching is."""
    candidates = [(n, "all-nodes") for n in nodes if (n.get("standard_name") or "").strip()]
    return candidates, []  # second element kept for backward compat with caller


def describe_org(node):
    """Full evidence for one organization, from its crimenet.json record."""
    return {
        "name": (node.get("standard_name") or "").strip(),
        "description": (node.get("description") or "").strip(),
        "aliases": node.get("aliases") or [],
        "country": (node.get("country") or "").strip(),
        "time_period": (node.get("time_period") or "").strip(),
        "is_defunct": node.get("is_defunct", "unknown"),
    }


def build_prompt(org, contexts=None):
    """Build a user prompt showing the LLM everything we know about this org.
    For a mention-only node (no description), `contexts` carries the passage(s)
    where it is named in the articles that reference it, so the model judges
    from real text rather than the bare name."""
    d = describe_org(org)
    aliases = ", ".join(d["aliases"][:12]) or "(none listed)"
    if contexts:
        joined = "\n---\n".join(contexts)
        ctx_block = ("\nPassages from the article(s) that mention this entry "
                     "(use these to judge what it actually is):\n"
                     f'"""\n{joined}\n"""\n')
    else:
        ctx_block = ""
    return (
        f"Organization: {d['name']}\n"
        f"  country: {d['country'] or '(unknown)'}\n"
        f"  time period: {d['time_period'] or '(unknown)'}\n"
        f"  aliases: {aliases}\n"
        f"  defunct status: {d['is_defunct']}\n"
        f"  description: {d['description'] or '(no description on file — mention-only node)'}\n"
        f"{ctx_block}\n"
        f"Is '{d['name']}' a real single criminal organization, or is it an "
        f"umbrella term / collective category that lumps together multiple "
        f"distinct groups?"
    )


def main():
    ap = argparse.ArgumentParser(
        description="Audit organizations for umbrella terms / collective "
                    "categories: DeepSeek judges whether each candidate is a "
                    "real single org or an umbrella term, and writes a "
                    "ready-to-paste TO_BE_EXCLUDED set. "
                    "Needs DEEPSEEK_API_KEY.")
    ap.add_argument("--input", "-i", default=judge.root("data", "crimenet_raw.json"))
    ap.add_argument("--output", "-o", default=judge.data_path("4_umbrella_orgs.py"))
    ap.add_argument("--txts", default=judge.root("data", "txts"),
                    help="dir of <page>/{url.txt,content.txt} for mention context")
    ap.add_argument("--no-context", action="store_true",
                    help="judge from the profile alone (skip article mention context)")
    ap.add_argument("--limit", type=int, default=0,
                    help="only judge the first N candidates (0 = all; for a quick test)")
    ap.add_argument("--workers", "-w", type=int, default=judge.DEFAULT_WORKERS,
                    help="parallel DeepSeek API workers (default 50)")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"Input not found: {in_path}")
        return

    data = json.loads(in_path.read_text("utf-8"))
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])
    degree = judge.compute_degree(edges)

    # ── Find candidates ───────────────────────────────────────────────────
    candidates, _ = select_candidates(nodes)
    if args.limit:
        candidates = candidates[:args.limit]

    print(f"Found {len(candidates)} node(s) to judge ({args.workers} workers).")
    if not candidates:
        lines = [
            "# 4_umbrella_orgs.py",
            "# Auto-generated by 4_review_umbrella_orgs.py — no candidates matched.",
            "#",
            "# No organizations matched the umbrella-term name patterns.",
            "# If you expected candidates, check select_candidates() patterns.",
            "",
            "TO_BE_EXCLUDED = set()",
            "",
            "TO_BE_EXCLUDED_LOW_CONFIDENCE = set()",
            "",
        ]
        Path(args.output).write_text("\n".join(lines), encoding="utf-8")
        print(f"Saved → {args.output}")
        return

    # ── Attach mention context to description-less (mention-only) nodes ────
    # so the model judges them from real article text, not the bare name.
    ctx_of = {}
    if not args.no_context:
        url_index = judge.build_url_index(args.txts)
        cache = {}
        for org, _tag in candidates:
            if (org.get("description") or "").strip():
                continue
            ctxs = judge.node_mention_contexts(org, url_index, cache)
            if ctxs:
                ctx_of[org["standard_name"]] = ctxs
        print(f"  attached mention context to {len(ctx_of)} description-less node(s).")

    # ── Run the LLM pass (only if there are candidates needing judgment) ────
    counts = Counter()
    flagged, low_conf = [], []

    if candidates:
        def line_fmt(done, n, item, v):
            org, tag = item
            is_umb, conf, reason = parse_verdict(v)
            flag = "err  " if is_umb is None else ("UMB  " if is_umb else "ok   ")
            d = degree.get(org.get("standard_name", ""), 0)
            return (f"  [{done}/{n}] {flag} (d={d:3d}, {conf:6s}) "
                    f"{org['standard_name'][:58]:58s} {reason[:40]}")

        verdicts = judge.run_pass(
            candidates,                                  # pass (node, tag) tuples
            lambda item: build_prompt(item[0], ctx_of.get(item[0]["standard_name"])),
            args.workers,
            line_fmt,
            system=UMBRELLA_SYSTEM,
        )

        # ── Classify ──────────────────────────────────────────────────────
        for (org, tag), v in zip(candidates, verdicts):
            is_umb, conf, reason = parse_verdict(v)
            name = org.get("standard_name", "").strip()
            if is_umb is None:
                counts["errors"] += 1
            elif is_umb:
                counts["umbrella"] += 1
            else:
                counts["real_org"] += 1

            if not name:
                continue

            entry = {
                "name": name,
                "confidence": conf,
                "reason": reason,
                "tag": tag,
                "description": (org.get("description") or "")[:200],
                "country": org.get("country") or "",
                "degree": degree.get(name, 0),
                "sources": judge.urls(org.get("own_sources") or []),
            }
            if is_umb is True and conf in ("high", "medium"):
                flagged.append(entry)
            elif is_umb is None or (is_umb is True and conf == "low"):
                low_conf.append(entry)

    # ── Render output ─────────────────────────────────────────────────────
    def render_set(entries):
        """One TO_BE_EXCLUDED-like set as lines, ordered by degree desc."""
        lines = []
        for e in sorted(entries, key=lambda e: (-e["degree"], e["name"].lower())):
            lines.append(f'    {json.dumps(e["name"], ensure_ascii=False)},'
                         f'  # ({e["confidence"]}) {e["reason"]}')
            if e["description"]:
                d = e["description"]
                if len(d) > 120:
                    d = d[:120] + "…"
                lines.append(f'    #     desc:  "{d}"')
            lines.append(f'    #     tag:   {e["tag"]}')
            if e["sources"]:
                lines.append(f'    #     own:   {" | ".join(e["sources"])}')
            lines.append("")
        return lines

    header = [
        "# 4_umbrella_orgs.py",
        "# Auto-generated by 4_review_umbrella_orgs.py — DeepSeek judged every node",
        "# in the graph; below are the ones it judged to be UMBRELLA TERMS or",
        "# COLLECTIVE CATEGORIES, not real single criminal organizations.",
        "#",
        "# REVIEW, then paste the good entries into curated_corrections.py TO_BE_EXCLUDED.",
        "# 7_apply_corrections.py drops those nodes and every edge touching them.",
        "#",
        f"# {len(candidates)} node(s) judged | {len(flagged)} umbrella(s) "
        f"(high/medium), {len(low_conf)} low-confidence | "
        f"{counts.get('real_org', 0)} real org(s), {counts.get('errors', 0)} error(s)",
        "#",
        f"# Cost: {judge.usage_report()}",
        "",
    ]

    out_lines = list(header)
    if flagged:
        out_lines.append("TO_BE_EXCLUDED = {")
        out_lines.extend(render_set(flagged))
        out_lines.append("}")
    else:
        out_lines.append("TO_BE_EXCLUDED = set()")  # empty set, not {} (a dict)
    out_lines.append("")

    if low_conf:
        out_lines += [
            "# ══════════════════════════════════════════════════════════════════",
            "# LOW-CONFIDENCE — DeepSeek was unsure (same format as above). Review,",
            "# and move any that are correct up into TO_BE_EXCLUDED (or straight into",
            "# curated_corrections.py). NOT applied while named like this.",
            "# ══════════════════════════════════════════════════════════════════",
            "",
            "TO_BE_EXCLUDED_LOW_CONFIDENCE = {",
        ]
        out_lines.extend(render_set(low_conf))
        out_lines.append("}")
        out_lines.append("")

    Path(args.output).write_text("\n".join(out_lines), encoding="utf-8")

    print(f"\n{counts.get('umbrella', 0)} umbrella(s), "
          f"{counts.get('real_org', 0)} real org(s), "
          f"{counts.get('errors', 0)} error(s); "
          f"{len(flagged)} confident + {len(low_conf)} low-confidence written.")
    print(f"Cost: {judge.usage_report()}")
    print(f"Saved → {args.output}  (review, then paste into curated_corrections.py)")


if __name__ == "__main__":
    main()
