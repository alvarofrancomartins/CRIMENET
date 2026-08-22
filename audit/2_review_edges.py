"""
2_review_edges.py

Audit the RELATIONSHIP edges in crimenet_raw.json — the org→org connections. Each
edge claims a relationship between two organizations, carrying a description, an
`evidence_quote`, and `source_urls`. This tool asks DeepSeek, per edge, ONE
tractable question: do the two organizations have a real relationship of ANY
kind, or is the edge spurious (the two are only co-mentioned, the quote is about
some third party/event, the link is hallucinated)?

It deliberately does NOT re-judge the relationship TYPE (cooperation vs conflict
vs other). That sub-call is where the model fails: a single quote about a hit or
a forced resignation reads like "conflict" even when the two orgs are actually
cooperating against a third party, so re-typing produces confident-but-wrong
removals. Existence-only judging matches the country-links audit (which works)
and only removes edges that have no real relationship behind them at all.

To give the model the context a short quote lacks, each edge is judged together
with an EXCERPT of its source article — the passage around the evidence quote,
pulled from data/txts/<…>/content.txt via the edge's source_urls. The judge
separates a DIRECT tie between the two orgs from a SHARED CIRCUMSTANCE that only
co-mentions them (the same police operation, the same government designation, the
same supplier/patron/enemy, the same list or trait). It stays conservative on
real ties, but a recognizable co-mention pattern is treated as a CONFIDENT "no",
not a hedged "low" — so these clear cases actually reach the applied blocklist
instead of stalling in the low-confidence bucket.

It writes a ready-to-paste removal list:

  2_edge_blocklist.py — an `EDGE_BLOCKLIST` dict ({org: {other_org: {relationship,
  ...}}}) of the edges the model judged to have NO real relationship, grouped by
  the more-connected org and ordered by pair degree, each annotated with the
  model's reason, the evidence quote, and the source URL. Low-confidence ones
  follow in a same-shape `*_LOW_CONFIDENCE` block. Paste the good entries into
  curated_corrections.py EDGE_BLOCKLIST; 7_apply_corrections.py then drops those edges. The LLM only
  ADVISES — you review first.

The run prints total API calls + tokens + rough cost. Use --relationship and
--limit to audit a cheaper subset (e.g. just the vague "other" edges) instead of
all ≈15k at once.

Needs the DEEPSEEK_API_KEY environment variable set.

Usage:
    python 2_review_edges.py -w 50
"""

import re
import json
import argparse
from collections import Counter, defaultdict
from pathlib import Path

import judge

EDGE_SYSTEM = """You are verifying RELATIONSHIP edges in a database of criminal organizations. You are given two organizations (A and B), a short description of the claimed relationship, an evidence quote, and usually an excerpt of the source article around that quote. Decide ONE thing: do A and B have a REAL relationship WITH EACH OTHER?

The test is a DIRECT tie between A and B themselves, not a circumstance they merely share. A direct tie is any genuine org-to-org link: alliance, partnership, pact, joint operation, a commercial dealing (one supplies, buys from, or launders for the other), rivalry, war, violence between them, a truce, a merger, one being a faction/splinter/successor of the other, absorption, or turf they actually contest or co-run. If members of A act directly on or alongside B (even violently, even while jointly attacking a third party), that is a direct tie.

A SHARED CIRCUMSTANCE is NOT a relationship, even when it puts both names in one sentence. These patterns mean NO relationship:
- Co-mention only: both are named in the same article, list, or category, or called "similar" or "another group of the same kind", with no interaction between them.
- Common target of the authorities: both were arrested, indicted, prosecuted, sanctioned, designated, or named in the same police operation, investigation, or crackdown.
- Common third party: each is independently tied to the same supplier, patron, ally, enemy, or event, but nothing ties the two to each other (A deals with C and B deals with C is not an A to B edge).
- Shared trait: same ethnicity, era, city, ideology, or symbol, with no contact.
- Bystander: the quote is really about a third org, person, or event and one of the two is only incidentally present.

Do NOT judge whether the label is correct (cooperation vs conflict vs other). Only judge whether a real A to B relationship exists at all.

Return ONLY a JSON object:
{
  "connection": "direct_tie" | "shared_circumstance" | "co_mention" | "none",
  "supported": true | false,
  "confidence": "high" | "medium" | "low",
  "reason": "one short sentence naming the actual link in the evidence"
}

How to decide:
- connection="direct_tie"  -> supported=true. The evidence shows A and B themselves linked.
- connection is anything else -> supported=false. The only thing joining them is a shared circumstance, a co-mention, or nothing.
- EXCEPTION, infobox assertion: if the evidence quote is an infobox field row (it begins with "Allies:", "Rivals:", "Enemies:", "Affiliations:", or a translation such as "Aliados:", "Rivales:", "Alleati:", "Enemigos:"), the source is DELIBERATELY asserting an alliance, rivalry, or affiliation between A and B. Treat it as connection="direct_tie", supported=true, and KEEP it, even though no narrative interaction is described. The reader can open the cited revision and see the listing.

Confidence, calibrated:
- Judge from the evidence in front of you. You are given the article excerpt, so if within it the sole link between A and B is one of the shared-circumstance patterns above, that is a CLEAR finding, not a guess: set supported=false with "high" or "medium" confidence. A recognizable co-mention, same-operation, or same-third-party pattern is a confident NO. Do NOT downgrade to "low" because deleting feels risky.
- The supplied description and quote are the extractor's own read of the source. If they describe only co-occurrence, or state outright that no direct relationship is mentioned, weigh that as strong evidence for supported=false and raise your confidence accordingly.
- Reserve "low" for GENUINE ambiguity: the quote is fragmentary or cut off, the names are unclear, or a direct tie may plausibly exist just outside the evidence shown. Low-confidence verdicts are NOT auto-applied, so use low only when a human truly must look.
- Stay conservative on real ties: if a direct tie plausibly exists but you are unsure of its exact nature, keep the edge (supported=true)."""


def parse_edge(v):
    """llm_call result → (supported|None, confidence, reason). None = error.
    Folds the `connection` category into the reason so reviewers see WHY an edge
    was flagged (e.g. "[shared_circumstance] both swept in one operation")."""
    if not isinstance(v, dict):
        return None, "low", "API/parse error — check manually"
    supported = bool(v.get("supported", False))
    conf = str(v.get("confidence", "low")).lower()
    reason = v.get("reason", "")
    conn = str(v.get("connection", "")).strip().lower()
    if conn and conn != "direct_tie" and not supported:
        reason = f"[{conn}] {reason}".rstrip()
    return supported, conf, reason


# Infobox "Allies:" / "Rivals:" / "Enemies:" / "Affiliations:" rows (and their
# Spanish/Italian/Portuguese equivalents) are a DELIBERATE source assertion that
# the two orgs are allied/rival/affiliated. Step 2 mints them as edges by design,
# and a reader can open the cited revision to see the row, so they are KEPT even
# without narrative context. We detect them deterministically and auto-keep them
# (never judged), which guarantees they survive and saves ~3k API calls.
INFOBOX_REL = re.compile(
    r"^\s*(allies?|allied|aliados?|alli[ée]s|alleati|"
    r"rivals?|rivales?|rivali|rivais|"
    r"enem(?:y|ies)|enemigos?|nemici|inimigos?|"
    r"opponents?|adversaires|"
    r"affiliations?|affiliated|asociados?)\s*[:\-–—]", re.I)


def is_infobox_rel_edge(e):
    """True if the edge's only evidence is an infobox Allies/Rivals/Enemies row."""
    return bool(INFOBOX_REL.match((e.get("evidence_quote") or "").strip()))


# ── Source-article context ───────────────────────────────────────────────────
# An edge's source_urls match url.txt in the data/txts/<page>/ dirs 1:1, and the
# article body sits next to it in content.txt. We locate the evidence quote in
# that body and hand the judge the surrounding passage, so it sees who did what
# to whom — context a bare quote often drops.
def build_url_index(txts_dir):
    """{source url → content.txt Path} across every data/txts/<page>/ dir."""
    idx = {}
    base = Path(txts_dir)
    if not base.is_dir():
        return idx
    for d in base.iterdir():
        u, c = d / "url.txt", d / "content.txt"
        if u.exists() and c.exists():
            url = u.read_text("utf-8").strip()
            if url:
                idx.setdefault(url, c)
    return idx


def _normalize_with_map(text):
    """Lowercase, collapse every run of non-alphanumerics to one space, and keep
    a map from each normalized-char position back to its original index. Lets us
    match a quote against the article ignoring all punctuation/quote-glyph/
    whitespace differences, then recover the real offsets to slice."""
    chars, omap, prev_space = [], [], False
    for i, ch in enumerate(text):
        if ch.isalnum():
            chars.append(ch.lower())
            omap.append(i)
            prev_space = False
        elif not prev_space:
            chars.append(" ")
            omap.append(i)
            prev_space = True
    return "".join(chars), omap


def find_context(quote, content, window=1500):
    """A slice of `content` around where `quote` appears, or '' if not found.
    Quotes often join fragments with '…'/'...' and differ in apostrophes/quote
    glyphs, so we match on a punctuation-insensitive normalization of both and
    map the hit back to real offsets. Tries the longest fragment first, backing
    off to shorter probes if the full fragment isn't a verbatim span."""
    if not quote or not content:
        return ""
    norm_content, omap = _normalize_with_map(content)

    cands = []
    for f in re.split(r"\.\.\.|…", quote):
        nf = _normalize_with_map(f)[0].strip()
        if len(nf.split()) >= 4:
            cands.append(nf)
    if not cands:
        cands = [_normalize_with_map(quote)[0].strip()]

    for anchor in sorted(cands, key=len, reverse=True):
        words = anchor.split()
        for take in (14, 10, 6):
            probe = " ".join(words[:take])
            if len(probe) < 12:
                continue
            pos = norm_content.find(probe)
            if pos == -1:
                continue
            start = max(0, omap[pos] - window)
            end = min(len(content), omap[pos + len(probe) - 1] + 1 + window)
            excerpt = content[start:end].strip()
            if start > 0:
                excerpt = "… " + excerpt
            if end < len(content):
                excerpt = excerpt + " …"
            return excerpt
    return ""


def main():
    ap = argparse.ArgumentParser(
        description="Audit org→org relationship edges in crimenet_raw.json: DeepSeek "
                    "judges whether each edge reflects a REAL org-to-org "
                    "relationship (given the source-article context), and writes "
                    "a ready-to-paste EDGE_BLOCKLIST. Needs DEEPSEEK_API_KEY.")
    ap.add_argument("--input", "-i", default=judge.root("data", "crimenet_raw.json"))
    ap.add_argument("--output", "-o", default=judge.data_path("2_edge_blocklist.py"))
    ap.add_argument("--txts", default=judge.root("data", "txts"),
                    help="dir of <page>/{url.txt,content.txt} for article context")
    ap.add_argument("--relationship", choices=["cooperation", "conflict", "other", "all"],
                    default="all", help="only audit edges of this type (default all)")
    ap.add_argument("--limit", type=int, default=0,
                    help="only judge the first N edges (0 = all; for a quick test)")
    ap.add_argument("--window", type=int, default=1500,
                    help="chars of article context on each side of the quote (default 1500)")
    ap.add_argument("--no-context", action="store_true",
                    help="judge from the evidence quote alone (skip article excerpts)")
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

    # Short description per org name, to give the judge context on each side.
    desc_of = {}
    for n in nodes:
        sn = (n.get("standard_name") or "").strip()
        if sn:
            d = (n.get("description") or "").strip()
            desc_of[sn] = (d[:300] + "…") if len(d) > 300 else d

    edges_sel = [e for e in edges
                 if args.relationship == "all" or e.get("relationship") == args.relationship]

    # Auto-keep infobox Allies/Rivals/Enemies edges: a deliberate source
    # assertion, kept even without narrative context (the reader can open the
    # cited revision and see the infobox row). Never judged, never blocked.
    infobox_auto_kept = sum(1 for e in edges_sel if is_infobox_rel_edge(e))
    edges_sel = [e for e in edges_sel if not is_infobox_rel_edge(e)]
    if args.limit:
        edges_sel = edges_sel[:args.limit]

    if infobox_auto_kept:
        print(f"  auto-kept {infobox_auto_kept} infobox Allies/Rivals/Enemies "
              f"edge(s) (deliberate source assertion; not judged).")
    print(f"Found {len(edges_sel)} edge(s) to judge "
          f"(relationship {args.relationship}; {args.workers} workers).")
    if not edges_sel:
        judge.render_dict_file(args.output, "EDGE_BLOCKLIST",
                               ["# 2_edge_blocklist.py",
                                "# No edges to judge.", ""], [])
        print(f"Saved → {args.output}")
        return

    # Pre-attach the source-article excerpt to each edge (single-threaded, with a
    # content cache) so build_prompt just reads a precomputed string.
    excerpt_of = {}
    if not args.no_context:
        url_index = build_url_index(args.txts)
        content_cache = {}

        def content_for(path):
            if path not in content_cache:
                content_cache[path] = path.read_text("utf-8")
            return content_cache[path]

        n_ctx = 0
        for e in edges_sel:
            ex = ""
            quote = e.get("evidence_quote") or ""
            for u in e.get("source_urls") or []:
                path = url_index.get(u)
                if path:
                    ex = find_context(quote, content_for(path), args.window)
                    if ex:
                        break
            excerpt_of[id(e)] = ex
            if ex:
                n_ctx += 1
        print(f"  attached article context to {n_ctx}/{len(edges_sel)} edge(s) "
              f"(±{args.window} chars).")

    def pair_degree(e):
        return degree.get(e.get("source", ""), 0) + degree.get(e.get("target", ""), 0)

    def build_prompt(e):
        src, tgt = e.get("source", ""), e.get("target", "")
        descs = " ".join(e.get("descriptions") or []) or "(none)"
        tps = ", ".join(e.get("time_periods") or []) or "(unknown)"
        excerpt = excerpt_of.get(id(e), "")
        if excerpt:
            ctx_block = ("\nSource article excerpt (context around the quote):\n"
                         f'"""\n{excerpt}\n"""\n')
        else:
            ctx_block = "\n(No article excerpt available — judge from the quote.)\n"
        return (
            f"Organization A: {src}\n"
            f"  what it is: {desc_of.get(src) or '(no description on file)'}\n"
            f"Organization B: {tgt}\n"
            f"  what it is: {desc_of.get(tgt) or '(no description on file)'}\n\n"
            f"Claimed relationship between {src} and {tgt}.\n"
            f"  description: {descs}\n"
            f"  evidence quote: {e.get('evidence_quote') or '(none)'}\n"
            f"  time period(s): {tps}\n"
            f"{ctx_block}\n"
            f"Do {src} and {tgt} have a real org-to-org relationship of any kind, "
            f"based on this evidence?"
        )

    def line_fmt(done, n, e, v):
        supported, conf, reason = parse_edge(v)
        flag = "err " if supported is None else ("ok  " if supported else "BAD ")
        pair = f"{e.get('source','')[:22]} ~ {e.get('target','')[:22]}"
        return (f"  [{done}/{n}] {flag} {e.get('relationship','')[:11]:11s} "
                f"{pair[:47]:47s} ({conf}) {reason[:36]}")

    verdicts = judge.run_pass(edges_sel, build_prompt, args.workers, line_fmt,
                              system=EDGE_SYSTEM)

    # Classify edges with NO real relationship into a CONFIDENT bucket
    # (high/medium) and a LOW-CONFIDENCE bucket (low conf + API errors), each
    # collapsed per org pair (a pair may have several flagged edges → union
    # their relationship types). Both render in the SAME structured dict form so
    # the uncertain ones are just as easy to check and to promote if correct.
    counts = Counter()
    # Tally each (pair, relationship): a type is only confidently blocked when
    # EVERY judged edge of that type between the pair is spurious. The graph
    # stores one edge per statement, and apply_corrections drops by (pair, rel),
    # so blocking a type while a real same-type statement exists would delete it
    # too. Tracking the tally keeps that real edge.
    tally = {}   # (frozenset({src,tgt}), rel) → {total, bad, real, outer, …}
    for e, v in zip(edges_sel, verdicts):
        supported, conf, reason = parse_edge(v)
        src, tgt, rel = e.get("source", ""), e.get("target", ""), e.get("relationship", "")
        if supported is None:
            counts["errors"] += 1
        elif supported:
            counts["supported"] += 1
        else:
            counts["unsupported"] += 1
        if not src or not tgt:
            continue
        key = (frozenset((src, tgt)), rel)
        t = tally.get(key)
        if t is None:
            # outer = the more-connected org, so its edges group together.
            outer, inner = ((src, tgt) if degree.get(src, 0) >= degree.get(tgt, 0)
                            else (tgt, src))
            t = {"outer": outer, "inner": inner, "pdeg": pair_degree(e),
                 "total": 0, "bad": 0, "real": 0, "comment": None}
            tally[key] = t
        t["total"] += 1
        if supported is False and conf in ("high", "medium"):
            t["bad"] += 1
            if t["comment"] is None:
                t["comment"] = (conf, reason, e.get("evidence_quote", ""),
                                e.get("source_urls") or [])
        elif supported is True:
            t["real"] += 1
        else:  # API error or low confidence
            if t["comment"] is None:
                t["comment"] = (conf, reason, e.get("evidence_quote", ""),
                                e.get("source_urls") or [])

    # Fold the per-(pair,rel) tallies into pair-keyed buckets for rendering:
    #   confident  → every judged edge of this type is confidently spurious
    #   low-conf   → no real same-type edge, but not all judgments were confident
    #   keep       → at least one judged edge of this type is real (skip entirely)
    hi, lo = {}, {}     # frozenset({src,tgt}) → record
    for (pkey, rel), t in tally.items():
        if t["real"] > 0:
            continue
        bucket = hi if t["bad"] == t["total"] else lo
        rec = bucket.get(pkey)
        if rec is None:
            rec = {"outer": t["outer"], "inner": t["inner"], "rels": set(),
                   "pdeg": t["pdeg"], "comment": t["comment"]}
            bucket[pkey] = rec
        rec["rels"].add(rel)
        if rec["comment"] is None:
            rec["comment"] = t["comment"]

    def edge_block_lines(varname, bucket):
        """Render one EDGE_BLOCKLIST-shaped block from a pair bucket."""
        by_outer = defaultdict(list)
        for rec in bucket.values():
            by_outer[rec["outer"]].append(rec)
        out = [f"{varname} = {{"]
        for outer in sorted(by_outer, key=lambda o: (-degree.get(o, 0), o.lower())):
            out.append("")
            out.append(f'    # ── "{outer}"  (degree {degree.get(outer, 0)}) '
                       + "─" * max(0, 40 - len(outer)))
            out.append(f'    {json.dumps(outer, ensure_ascii=False)}: {{')
            for rec in sorted(by_outer[outer], key=lambda r: (-r["pdeg"], r["inner"].lower())):
                rels = "{" + ", ".join(json.dumps(r) for r in sorted(rec["rels"])) + "}"
                conf, reason, quote, urls = rec["comment"]
                out.append(f'        {json.dumps(rec["inner"], ensure_ascii=False)}: '
                           f'{rels},  # ({conf}) {reason}'.rstrip())
                if quote:
                    out.append(f'        #     quote: "{quote}"')
                if urls:
                    out.append("        #     src:   " + " | ".join(urls))
            out.append("    },")
        out.extend(["", "}", ""])
        return out

    lines = [
        "# 2_edge_blocklist.py",
        "# Auto-generated by 2_review_edges.py — DeepSeek judged each org→org edge; below",
        "# are the ones it judged to have NO real relationship between the two orgs",
        "# (spurious co-mention / evidence about a third party). REVIEW, then paste",
        "# the good entries into curated_corrections.py EDGE_BLOCKLIST — 7_apply_corrections.py drops them.",
        "#",
        "# Format matches curated_corrections.EDGE_BLOCKLIST: {org: {other_org: {relationship,",
        "# ...}}}. Symmetric; '*' would drop all types between the pair.",
        f"# relationship {args.relationship} | {len(hi)} confident edge-pair(s); "
        f"{len(lo)} low-confidence.",
        "",
    ]
    lines += edge_block_lines("EDGE_BLOCKLIST", hi)
    if lo:
        lines += [
            "",
            "# ══════════════════════════════════════════════════════════════════",
            "# LOW-CONFIDENCE — DeepSeek was unsure (same format as above). Review,",
            "# and move any that are correct up into EDGE_BLOCKLIST (or straight into",
            "# curated_corrections.py). NOT applied while named like this.",
            "# ══════════════════════════════════════════════════════════════════",
            "",
        ]
        lines += edge_block_lines("EDGE_BLOCKLIST_LOW_CONFIDENCE", lo)
    Path(args.output).write_text("\n".join(lines), encoding="utf-8")

    print(f"\n{counts['unsupported']} no-relationship, {counts['supported']} real, "
          f"{counts['errors']} error(s); {len(hi)} confident + {len(lo)} "
          f"low-confidence edge-pair(s) written.")
    print(f"Cost: {judge.usage_report()}")
    print(f"Saved → {args.output}  (review, then paste into curated_corrections.py)")


if __name__ == "__main__":
    main()
