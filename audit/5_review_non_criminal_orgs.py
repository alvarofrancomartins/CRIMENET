"""
5_review_non_criminal_orgs.py

Audit every node: is it a CRIMINAL organization, or a NON-CRIMINAL entity that
does not belong in a database of organized crime?

CRIMENET's scope is criminal organizations: cartels, mafias, gangs, crime
families, syndicates, triads, prison gangs, outlaw motorcycle clubs, and the
armed groups that operate outside the law (terrorist groups, insurgencies,
guerrillas, paramilitaries, militias, warlord factions). Step 3 of the pipeline
only profiles the org each ARTICLE is about, and declines a non-org subject
there. But nodes that appear only as MENTIONS in other orgs' articles are never
run through that check, so legitimate state, political, religious, and civil
bodies slip in as empty mention nodes: national armies, federal police forces,
mainstream political parties, a church. They are real endpoints in the source
text, but they are not criminal organizations, so they pollute the graph.

This step closes that gap. Every node (profiled or unprofiled) is sent to
DeepSeek, which judges from the name and whatever profile exists whether the
entry is a criminal/armed group (keep) or a legitimate non-criminal entity
(drop). Non-state armed groups stay in scope; only state militaries, national
police/intelligence, government bodies, mainstream parties, religious orders,
NGOs, companies, universities, sports clubs, and media outlets are flagged.

It writes:

  5_non_criminal_orgs.py — a TO_BE_EXCLUDED set of the entries the model judged
  to be non-criminal entities, each annotated with the model's reason,
  confidence, and source URLs. Low-confidence ones follow in a same-shape
  TO_BE_EXCLUDED_LOW_CONFIDENCE block. 7_apply_corrections.py auto-applies the
  confident (high/medium) entries and drops those nodes and every edge touching
  them. It writes to the SAME TO_BE_EXCLUDED set as 4_review_umbrella_orgs.py;
  apply_corrections unions the two files.

This is the lever for CRIMENET's "criminal organizations only" scope. To later
WIDEN the scope (let governments and politics in as first-class nodes), skip
this step or clear its output file; the umbrella audit (step 4) is unaffected.

The run prints total API calls + tokens + rough cost.

Needs the DEEPSEEK_API_KEY environment variable set.

Usage:
    python 5_review_non_criminal_orgs.py -w 50
"""

import json
import argparse
from collections import Counter
from pathlib import Path

import judge

NONCRIMINAL_SYSTEM = """You are auditing a database of CRIMINAL organizations. Every node is supposed to be a criminal organization: a cartel, mafia, gang, crime family, syndicate, triad, yakuza group, prison gang, outlaw motorcycle club, street gang, drug-trafficking organization, or an armed group that operates outside the law (terrorist group, insurgency, guerrilla movement, paramilitary, militia, warlord or rebel faction, mercenary company).

Some entries are NOT criminal organizations at all. They were pulled into the database only because another group's article mentioned them. Decide ONE thing: is this entry a criminal/armed organization, or a NON-CRIMINAL entity that does not belong here?

NON-CRIMINAL entities (set is_criminal_org=false) are legitimate state, political, religious, or civil bodies, for example:
- A sovereign state's regular armed forces or national military (army, navy, air force, national guard, "Armed Forces of ...", "... Armed Forces").
- A national or federal police force, gendarmerie, intelligence agency, or security/customs service.
- A government body: a ministry, department, agency, court, legislature, or a sitting government.
- A mainstream political party that contests elections and is not itself an armed group.
- A religious body: a church, congregation, denomination, or religious order.
- A civil or commercial entity: an NGO, charity, company, university, sports club, trade union, or media outlet.

These ARE in scope (set is_criminal_org=true; do NOT exclude), even when they look political or militarized:
- Non-state armed groups: terrorist organizations, insurgencies, guerrilla movements, paramilitaries, militias, warlord and rebel factions, mercenary/private military companies that operate as armed groups.
- A criminal or armed group that also runs a political wing, charity, or social services: if it is primarily an armed or criminal organization, keep it.

You are given the entry's name and whatever profile exists (description, aliases, country, time period), and for mention-only entries WITHOUT a description you are also given the passage(s) from the article(s) that name it. Use those passages first: they tell you what the entry actually is (an entry listed among street gangs is a gang, not a religious body, even if its name sounds religious). When no passage is available, judge by the NAME when you recognize it (e.g. "Sudanese Armed Forces", "Chinese Communist Party", "National Police of Colombia", "Armed Forces of Ukraine" are clearly non-criminal state/political bodies).

Return ONLY a JSON object:
{
  "is_criminal_org": true | false,
  "entity_type": "what it actually is, a few words (e.g. 'national army', 'political party', 'drug cartel', 'guerrilla group')",
  "confidence": "high" | "medium" | "low",
  "reason": "one short sentence"
}

BE CONSERVATIVE. This list DELETES entries from the database. Only set is_criminal_org=false when you are confident the entry is a legitimate state, political, religious, or civil body, NOT a criminal or armed group. If the name is obscure, or you cannot tell what it is, set is_criminal_org=true with confidence "low" so it is kept and a human reviews it (low confidence is NOT auto-applied). When in doubt about a non-state armed or militant group, keep it."""


def parse_verdict(v):
    """llm_call result → (is_non_criminal|None, confidence, reason). None = error.
    Folds entity_type into the reason so the reviewer sees what the model thinks
    the entry actually is (e.g. "[national army] regular military of Ukraine")."""
    if not isinstance(v, dict):
        return None, "low", "API/parse error — check manually"
    is_org = bool(v.get("is_criminal_org", True))   # default True = keep (safe)
    conf = str(v.get("confidence", "low")).lower()
    reason = v.get("reason", "")
    etype = str(v.get("entity_type", "")).strip()
    if etype and not is_org:
        reason = f"[{etype}] {reason}".rstrip()
    return (not is_org), conf, reason


def select_candidates(nodes):
    """Return every node as a candidate for LLM judgment. No pre-filtering — the
    mention-only nodes that matter most carry no description to pattern-match on,
    so the LLM judges each from its name and whatever profile exists."""
    candidates = [(n, "all-nodes") for n in nodes if (n.get("standard_name") or "").strip()]
    return candidates, []  # second element kept for backward compat with caller


def describe_org(node):
    """Full evidence for one node, from its crimenet.json record."""
    return {
        "name": (node.get("standard_name") or "").strip(),
        "description": (node.get("description") or "").strip(),
        "aliases": node.get("aliases") or [],
        "country": (node.get("country") or "").strip(),
        "time_period": (node.get("time_period") or "").strip(),
        "is_defunct": node.get("is_defunct", "unknown"),
    }


def build_prompt(org, contexts=None):
    """Build a user prompt showing the LLM everything we know about this entry.
    For a mention-only node (no description), `contexts` carries the passage(s)
    where it is named in the articles that reference it, so the model judges
    from real text rather than guessing from the bare name."""
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
        f"Entry: {d['name']}\n"
        f"  country: {d['country'] or '(unknown)'}\n"
        f"  time period: {d['time_period'] or '(unknown)'}\n"
        f"  aliases: {aliases}\n"
        f"  defunct status: {d['is_defunct']}\n"
        f"  description: {d['description'] or '(no description on file — mention-only node)'}\n"
        f"{ctx_block}\n"
        f"Is '{d['name']}' a criminal or armed organization that belongs in a "
        f"database of organized crime, or a non-criminal state / political / "
        f"religious / civil body that should be excluded?"
    )


def main():
    ap = argparse.ArgumentParser(
        description="Audit nodes for NON-CRIMINAL entities (state armed forces, "
                    "national police, mainstream political parties, religious / "
                    "civil bodies) that slipped in as mention nodes: DeepSeek "
                    "judges each, and writes a ready-to-paste TO_BE_EXCLUDED set. "
                    "Needs DEEPSEEK_API_KEY.")
    ap.add_argument("--input", "-i", default=judge.root("data", "crimenet_raw.json"))
    ap.add_argument("--output", "-o", default=judge.data_path("5_non_criminal_orgs.py"))
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
            "# 5_non_criminal_orgs.py",
            "# Auto-generated by 5_review_non_criminal_orgs.py — no candidates.",
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
    # Without this the model judges those from the bare name and guesses wrong
    # (e.g. the street gang "Almighty Bishops" read as a "religious body").
    ctx_of = {}
    if not args.no_context:
        url_index = judge.build_url_index(args.txts)
        cache = {}
        for org, _tag in candidates:
            if (org.get("description") or "").strip():
                continue   # profiled node already carries its own text
            ctxs = judge.node_mention_contexts(org, url_index, cache)
            if ctxs:
                ctx_of[org["standard_name"]] = ctxs
        print(f"  attached mention context to {len(ctx_of)} description-less node(s).")

    # ── Run the LLM pass ──────────────────────────────────────────────────
    counts = Counter()
    flagged, low_conf = [], []

    def line_fmt(done, n, item, v):
        org, tag = item
        is_non, conf, reason = parse_verdict(v)
        flag = "err  " if is_non is None else ("DROP " if is_non else "ok   ")
        d = degree.get(org.get("standard_name", ""), 0)
        return (f"  [{done}/{n}] {flag} (d={d:3d}, {conf:6s}) "
                f"{org['standard_name'][:58]:58s} {reason[:40]}")

    verdicts = judge.run_pass(
        candidates,                                  # pass (node, tag) tuples
        lambda item: build_prompt(item[0], ctx_of.get(item[0]["standard_name"])),
        args.workers,
        line_fmt,
        system=NONCRIMINAL_SYSTEM,
    )

    # ── Classify ──────────────────────────────────────────────────────────
    for (org, tag), v in zip(candidates, verdicts):
        is_non, conf, reason = parse_verdict(v)
        name = org.get("standard_name", "").strip()
        if is_non is None:
            counts["errors"] += 1
        elif is_non:
            counts["non_criminal"] += 1
        else:
            counts["criminal_org"] += 1

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
            "sources": judge.urls((org.get("own_sources") or [])
                                  or (org.get("mentioned_in") or [])),
        }
        if is_non is True and conf in ("high", "medium"):
            flagged.append(entry)
        elif is_non is None or (is_non is True and conf == "low"):
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
            lines.append(f'    #     deg:   {e["degree"]}')
            if e["sources"]:
                lines.append(f'    #     src:   {" | ".join(e["sources"])}')
            lines.append("")
        return lines

    header = [
        "# 5_non_criminal_orgs.py",
        "# Auto-generated by 5_review_non_criminal_orgs.py — DeepSeek judged every",
        "# node; below are the ones it judged to be NON-CRIMINAL entities (state",
        "# armed forces, national police, mainstream political parties, religious /",
        "# civil bodies), NOT criminal or armed organizations.",
        "#",
        "# REVIEW, then paste the good entries into curated_corrections.py TO_BE_EXCLUDED.",
        "# 7_apply_corrections.py auto-applies the confident ones and drops those nodes",
        "# and every edge touching them (it unions this set with 4_umbrella_orgs.py).",
        "#",
        f"# {len(candidates)} node(s) judged | {len(flagged)} non-criminal "
        f"(high/medium), {len(low_conf)} low-confidence | "
        f"{counts.get('criminal_org', 0)} criminal org(s), {counts.get('errors', 0)} error(s)",
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

    print(f"\n{counts.get('non_criminal', 0)} non-criminal entity(ies), "
          f"{counts.get('criminal_org', 0)} criminal org(s), "
          f"{counts.get('errors', 0)} error(s); "
          f"{len(flagged)} confident + {len(low_conf)} low-confidence written.")
    print(f"Cost: {judge.usage_report()}")
    print(f"Saved → {args.output}  (review, then paste into curated_corrections.py)")


if __name__ == "__main__":
    main()
