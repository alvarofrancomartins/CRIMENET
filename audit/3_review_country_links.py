"""
3_review_country_links.py

Audit the COUNTRY attributions in crimenet_raw.json. Every node may carry
`country_links` — claims that the organization is tied to some country, each
with an `evidence_quote` from the source article. This tool asks DeepSeek, per
link, whether that quote shows the ORG ITSELF having a real footprint in/through
the country — presence (based / members / cells / territory), activity (an
attack/operation carried out there), supply (sources FROM it), or transit (routes
THROUGH it) — or whether the country is named only incidentally. It explicitly
rejects the common false positives the bulk extractor lets through: a single
member arrested/extradited/fled/died there, a country acting toward the org
(allying, aiding, designating, reporting on), and statistical/background mentions.

It writes a ready-to-paste removal list:

  3_country_link_blocklist.py — a `COUNTRY_LINK_BLOCKLIST` dict ({org: {country,
  ...}}) of the links the model judged UNSUPPORTED, ordered by org connection
  degree, each annotated with the model's reason + the evidence quote + the
  org's page URL so you can verify by hand. Low-confidence ones are listed
  (commented out) at the bottom for manual review. Paste the good entries into
  curated_corrections.py COUNTRY_LINK_BLOCKLIST; 7_apply_corrections.py then drops them from the graph
  (and the worldwide map). The LLM only ADVISES — you review first.

It is the smaller sibling of the all-edges audit (2_review_edges.py) and shares its
shape; the run prints total API calls + tokens + rough cost.

Needs the DEEPSEEK_API_KEY environment variable set.

Usage:
    python 3_review_country_links.py -w 50
"""

import json
import argparse
from collections import Counter, defaultdict
from pathlib import Path

import judge

COUNTRY_SYSTEM = """You are verifying COUNTRY attributions in a database of criminal organizations. You are given an organization and a claim that it is tied to a particular country, with a verbatim evidence quote (and a paraphrased context) from the source article. Decide whether the evidence shows the ORGANIZATION ITSELF having a real footprint in/through that country, or whether the country is named only incidentally.

A real footprint (supported = true) is ONE of these, past or present:
- presence — the org is based there, or has members, cells, chapters, a headquarters, bases, territory, offices, or assets there; it established or expanded itself there.
- activity — the org carries out an activity there: an attack, operation, killing, kidnapping, extortion, or local trafficking/selling on that country's soil.
- supply  — the org sources, buys, or imports goods / drugs / weapons / money FROM that country.
- transit — the org routes, ships, moves, or launders goods / money THROUGH that country.

It is NOT a footprint (supported = false) when the country is named only because of:
- A single INDIVIDUAL (a boss, member, or associate) who was arrested, captured, extradited, convicted, fled to, hid in, lived in, retired in, or died in the country. One person being there is not the organization operating there.
- An action a COUNTRY, its press, or its courts takes TOWARD the org: allying with / aiding / supporting it; designating, labelling, sanctioning, listing, or banning it; reporting on it; or the country being targeted/victimized by it — with no stated org operation there.
- A statistical, background, ideological, or ranking mention ("one of the top targeted countries"), a comparison, an unrelated event, or mere co-occurrence in a list.
- Inference from the org's name / ethnicity / reputation; a region or continent expanded into countries; or a presence that was only proposed, planned, attempted, rumored, or that failed.
- The country being the org's own home / origin.

Return ONLY a JSON object:
{
  "supported": true | false,
  "confidence": "high" | "medium" | "low",
  "reason": "one short sentence"
}

Rules:
- Judge ONLY from the evidence given — not outside knowledge. Weight the VERBATIM quote: the context is the extractor's paraphrase and may overstate (e.g. it may say "indicating presence" about a mere arrest) — trust the quote over the context.
- When the quote clearly matches an excluded pattern above (e.g. "X was arrested in Brazil", "Brazil aided X", "Allies: … Brazil", "media reported on X in Brazil"), set supported=false with confidence "high".
- If the evidence is too thin to tell either way, set supported=false with confidence "low" (flag for human review).

Examples:
- "boss X was captured/arrested in Brazil after fleeing" → {"supported": false, "confidence": "high", "reason": "a single member was arrested there, not the org operating"}
- "Allies: … Brazil" / "Brazil provided aid to X" → {"supported": false, "confidence": "high", "reason": "the country aided the org; not the org operating there"}
- "foreign media reported on X in Brazil" → {"supported": false, "confidence": "high", "reason": "media coverage, not an operation"}
- "X has a chapter / cells / territory / a base in Brazil" → {"supported": true, "confidence": "high", "reason": "established presence"}
- "X sourced its cocaine from Brazil" / "trafficking route … through Brazil" → {"supported": true, "confidence": "high", "reason": "supply/transit link"}"""


def parse_supported(v):
    """llm_call result → (supported|None, confidence, reason). None = error."""
    if not isinstance(v, dict):
        return None, "low", "API/parse error — check manually"
    return (bool(v.get("supported", False)),
            str(v.get("confidence", "low")).lower(),
            v.get("reason", ""))


def main():
    ap = argparse.ArgumentParser(
        description="Audit country_links in crimenet_raw.json: DeepSeek judges "
                    "whether each org→country claim is supported by its evidence "
                    "quote, and writes a ready-to-paste COUNTRY_LINK_BLOCKLIST. "
                    "Needs DEEPSEEK_API_KEY.")
    ap.add_argument("--input", "-i", default=judge.root("data", "crimenet_raw.json"))
    ap.add_argument("--output", "-o", default=judge.data_path("3_country_link_blocklist.py"))
    ap.add_argument("--limit", type=int, default=0,
                    help="only judge the first N links (0 = all; for a quick test)")
    ap.add_argument("--max-mentions", type=int, default=8,
                    help="max 'mentioned in' URLs to list per org (default 8)")
    ap.add_argument("--workers", "-w", type=int, default=judge.DEFAULT_WORKERS,
                    help="parallel DeepSeek API workers (default 50)")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"Input not found: {in_path}")
        return

    data = json.loads(in_path.read_text("utf-8"))
    nodes = data.get("nodes", [])
    degree = judge.compute_degree(data.get("edges", []))

    # One item per (node, country_link).
    items = []   # (node, link)
    for n in nodes:
        for link in n.get("country_links") or []:
            if (link.get("country") or "").strip():
                items.append((n, link))
    if args.limit:
        items = items[:args.limit]

    print(f"Found {len(items)} country link(s) to judge "
          f"({args.workers} workers).")
    if not items:
        judge.render_dict_file(args.output, "COUNTRY_LINK_BLOCKLIST",
                               ["# 3_country_link_blocklist.py",
                                "# No country links to judge.", ""], [])
        print(f"Saved → {args.output}")
        return

    def build_prompt(item):
        n, link = item
        desc = (n.get("description") or "").strip()
        if len(desc) > 700:
            desc = desc[:700] + "…"
        country = link.get("country", "")
        return (
            f"Organization: {n.get('standard_name', '')}\n"
            f"  home country: {n.get('country') or '(unknown)'}\n"
            f"  description: {desc or '(none)'}\n\n"
            f"Claimed country link: {country}\n"
            f"  evidence quote: {link.get('evidence_quote') or '(none)'}\n"
            f"  context: {link.get('context') or '(none)'}\n\n"
            f"Does the evidence support that this organization is really "
            f"linked to {country}?"
        )

    def line_fmt(done, n, item, v):
        node, link = item
        supported, conf, reason = parse_supported(v)
        flag = "err " if supported is None else ("ok  " if supported else "BAD ")
        return (f"  [{done}/{n}] {flag} {node.get('standard_name','')[:30]:30s} → "
                f"{link.get('country','')[:18]:18s} ({conf}) {reason[:42]}")

    verdicts = judge.run_pass(items, build_prompt, args.workers, line_fmt,
                              system=COUNTRY_SYSTEM)

    # Classify unsupported links into a CONFIDENT bucket (high/medium) and a
    # LOW-CONFIDENCE bucket (low conf + API errors). Both are rendered in the
    # SAME structured dict form so the uncertain ones are just as easy to check
    # and to promote if they turn out correct.
    counts = Counter()
    hi = defaultdict(list)   # org → [(country, conf, reason, quote)]
    lo = defaultdict(list)
    nodes_by_name = {}
    for (node, link), v in zip(items, verdicts):
        supported, conf, reason = parse_supported(v)
        country = link.get("country", "")
        org = node.get("standard_name", "")
        if supported is None:
            counts["errors"] += 1
        elif supported:
            counts["supported"] += 1
        else:
            counts["unsupported"] += 1
        if supported is False and conf in ("high", "medium"):
            nodes_by_name[org] = node
            hi[org].append((country, conf, reason, link.get("evidence_quote", "")))
        elif supported is None or conf == "low":
            nodes_by_name[org] = node
            lo[org].append((country, conf, reason, link.get("evidence_quote", "")))

    def sections_for(bucket):
        """Org sections (degree-ordered) for render, from an org→[…] bucket."""
        secs = []
        for org in sorted(bucket, key=lambda o: (-degree.get(o, 0), o.lower())):
            item_objs = []
            for country, conf, reason, quote in sorted(bucket[org], key=lambda x: x[0].lower()):
                comments = [f'        #     quote: "{quote}"'] if quote else []
                item_objs.append({"name": country,
                                  "note": f"# ({conf}) {reason}".rstrip(),
                                  "comments": comments})
            secs.append({
                "key": org,
                "header": judge.node_section_header(nodes_by_name[org], degree, args.max_mentions),
                "items": item_objs,
            })
        return secs

    n_hi = sum(len(v) for v in hi.values())
    n_lo = sum(len(v) for v in lo.values())
    lines = [
        "# 3_country_link_blocklist.py",
        "# Auto-generated by 3_review_country_links.py — DeepSeek judged each org→country",
        "# link; below are the ones it judged UNSUPPORTED (the country was only",
        "# mentioned incidentally). REVIEW, then paste the good entries into",
        "# curated_corrections.py COUNTRY_LINK_BLOCKLIST — 7_apply_corrections.py drops them from the graph",
        "# and the worldwide map. Each org header shows its page (own:) to verify.",
        "#",
        "# Format matches curated_corrections.COUNTRY_LINK_BLOCKLIST: {org: {country, ...}}.",
        f"# {n_hi} confident link(s) / {len(hi)} org(s); "
        f"{n_lo} low-confidence link(s) / {len(lo)} org(s).",
        "",
    ]
    lines += judge.dict_block_lines("COUNTRY_LINK_BLOCKLIST", sections_for(hi))
    if lo:
        lines += [
            "",
            "# ══════════════════════════════════════════════════════════════════",
            "# LOW-CONFIDENCE — DeepSeek was unsure (same format as above). Review,",
            "# and move any that are correct up into COUNTRY_LINK_BLOCKLIST (or",
            "# straight into curated_corrections.py). NOT applied while named like this.",
            "# ══════════════════════════════════════════════════════════════════",
            "",
        ]
        lines += judge.dict_block_lines("COUNTRY_LINK_BLOCKLIST_LOW_CONFIDENCE",
                                        sections_for(lo))
    Path(args.output).write_text("\n".join(lines), encoding="utf-8")

    print(f"\n{counts['unsupported']} unsupported, {counts['supported']} supported, "
          f"{counts['errors']} error(s); {n_hi} confident + {n_lo} low-confidence "
          f"link(s) written.")
    print(f"Cost: {judge.usage_report()}")
    print(f"Saved → {args.output}  (review, then paste into curated_corrections.py)")


if __name__ == "__main__":
    main()
