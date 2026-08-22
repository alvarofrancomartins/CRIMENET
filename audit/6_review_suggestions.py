"""
6_review_suggestions.py

SECOND-OPINION REVIEW (run before 7_apply_corrections.py): DeepSeek re-reviews
every suggestion from audits 0-5 and writes a verdict report. It never edits
curated_corrections.py, but 7_apply_corrections.py reads the report and lets its
high-confidence rejections veto the matching identity auto-suggestions (the
wrong-merge BLOCKLIST and duplicate KNOWN_DUPLICATES only).

For each suggestion the LLM re-judges (using the SAME prompts the original
audits used) and only the ones it is high-confidence about are listed as
"approved" in the report. You still decide what goes into curated_corrections.py;
this just pre-screens thousands of suggestions so you don't have to.

It reads BOTH the confident block AND the *_LOW_CONFIDENCE block from each
audit output, so a good suggestion the first pass was unsure about can still
be promoted.

  • 0_node_blocklist.py            → BLOCKLIST              approved if "different orgs"
  • 1_known_duplicates.py     → KNOWN_DUPLICATES       approved if "same org"
  • 2_edge_blocklist.py       → EDGE_BLOCKLIST         approved if "no relationship"
  • 3_country_link_blocklist.py → COUNTRY_LINK_BLOCKLIST approved if "no link"
  • 4_umbrella_orgs.py        → TO_BE_EXCLUDED         approved if "is umbrella"
  • 5_non_criminal_orgs.py    → TO_BE_EXCLUDED         approved if "is non-criminal"

Evidence for re-judgement is enriched from crimenet.json (org descriptions,
edge evidence quotes, country link context) and, for edges, the source-article
passage from data/txts/.

The output is a JSON report:
  audit_data/6_llm_verdicts.json — per-audit lists of approved/rejected/error
  suggestions, each with the model's confidence and reason.

Needs the DEEPSEEK_API_KEY environment variable set.

Usage:
    python 6_review_suggestions.py -w 50
"""

import argparse
import ast
import importlib.util
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import judge

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))


def _load_mod(modname, filename):
    """Load a sibling audit module by path."""
    spec = importlib.util.spec_from_file_location(modname, HERE / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Reuse audits 2, 3, 4, and 5: their system prompts and verdict parsers.
EDGES = _load_mod("audit_edges", "2_review_edges.py")
COUNTRY = _load_mod("audit_country", "3_review_country_links.py")
UMBRELLA = _load_mod("audit_umbrella", "4_review_umbrella_orgs.py")
NONCRIMINAL = _load_mod("audit_noncriminal", "5_review_non_criminal_orgs.py")


# ── Parsing the audit output files ────────────────────────────────────────

def load_structured(path):
    """{VARNAME: <literal dict/set>} for every top-level `NAME = {...}`."""
    out = {}
    if not Path(path).exists():
        return out
    tree = ast.parse(Path(path).read_text("utf-8"))
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, (ast.Dict, ast.Set))):
            try:
                out[node.targets[0].id] = ast.literal_eval(node.value)
            except Exception as e:
                print(f"WARNING: load_structured failed to eval "
                      f"{node.targets[0].id} in {path}: {e}")
    return out


_RE_OUTER = re.compile(r'^    "((?:[^"\\]|\\.)*)":\s*\{\s*$')
_RE_CLOSE = re.compile(r'^    \},\s*$')
_RE_INNER_EDGE = re.compile(r'^        "((?:[^"\\]|\\.)*)":\s*\{[^}]*\},?(.*)$')
_RE_INNER_SET = re.compile(r'^        "((?:[^"\\]|\\.)*)",(.*)$')
_RE_CONF = re.compile(r'\((high|medium|low)\)')


def _dec(raw):
    try:
        return json.loads('"' + raw + '"')
    except Exception:
        print(f"WARNING: _dec failed to JSON-decode {raw!r}, using raw string")
        return raw


def parse_meta(path):
    """{(outer, inner): {"conf","reason"}} from a rendered audit file's inline
    comments."""
    meta = {}
    if not Path(path).exists():
        return meta
    n_outer = 0
    n_extracted = 0
    outer = None
    for line in Path(path).read_text("utf-8").splitlines():
        mo = _RE_OUTER.match(line)
        if mo:
            outer = _dec(mo.group(1))
            n_outer += 1
            continue
        if _RE_CLOSE.match(line):
            outer = None
            continue
        if outer is None:
            continue
        m = _RE_INNER_EDGE.match(line) or _RE_INNER_SET.match(line)
        if m:
            inner = _dec(m.group(1))
            comment = (m.group(2) or "")
            comment = comment.split("#", 1)[1].strip() if "#" in comment else ""
            cm = _RE_CONF.search(comment)
            conf = cm.group(1) if cm else ""
            reason = comment[cm.end():].strip() if cm else comment
            meta[(outer, inner)] = {"conf": conf, "reason": reason}
            n_extracted += 1
    if n_outer > 0 and n_extracted == 0:
        print(f"WARNING: parse_meta found {n_outer} outer block(s) in {path} "
              f"but extracted 0 entries — the audit output format may have "
              f"changed. Re-review evidence will be missing.")
    return meta



# ── crimenet evidence ─────────────────────────────────────────────────────

def load_crimenet(path):
    """Returns (fold→node, edge_quotes, country_links, degree)."""
    fold_node = {}
    edge_quotes = defaultdict(list)
    country_links = {}
    degree = {}
    p = Path(path)
    if not p.exists():
        return fold_node, edge_quotes, country_links, degree
    data = json.loads(p.read_text("utf-8"))
    degree = judge.compute_degree(data.get("edges", []))
    for n in data.get("nodes", []):
        for name in judge.node_names(n):
            fold_node.setdefault(judge.fold(name), n)
        org = (n.get("standard_name") or "").strip()
        for link in n.get("country_links") or []:
            c = (link.get("country") or "").strip()
            if c:
                country_links[(judge.fold(org), judge.fold(c))] = (
                    link.get("evidence_quote") or "", link.get("context") or "")
    for e in data.get("edges", []):
        s, t = e.get("source", ""), e.get("target", "")
        if s and t:
            edge_quotes[frozenset((judge.fold(s), judge.fold(t)))].append(
                (e.get("relationship", ""), e.get("evidence_quote") or "",
                 e.get("source_urls") or []))
    return fold_node, edge_quotes, country_links, degree


# ── Re-judge prompt builders ──────────────────────────────────────────────

class Reviewer:
    def __init__(self, fold_node, edge_quotes, country_links, url_index,
                 content_cache, window, use_context):
        self.fold_node = fold_node
        self.edge_quotes = edge_quotes
        self.country_links = country_links
        self.url_index = url_index
        self.content_cache = content_cache
        self.window = window
        self.use_context = use_context

    def node(self, name):
        return self.fold_node.get(judge.fold(name))

    def desc(self, name):
        n = self.node(name)
        if not n:
            return ""
        d = (n.get("description") or "").strip()
        return (d[:300] + "…") if len(d) > 300 else d

    # --- blocklist (audit 0): keep block if DIFFERENT orgs ---
    def block_prompt(self, item):
        canon, wrong = item["outer"], item["inner"]
        a, b = self.node(canon), self.node(wrong)
        if (a is not None and b is not None and b is not a
                and a.get("standard_name") != b.get("standard_name")):
            return judge.build_pair_prompt(
                judge.describe_node(a), judge.describe_node(b),
                "share a name or name-fragment and were merged into one node")
        note = item["meta"].get("reason") or "(none)"
        return (
            "Two records were MERGED into one node because their names collided. "
            "Decide whether they are the same organization.\n\n"
            f"RECORD A:\n  name: {canon}\n"
            f"  description: {self.desc(canon) or '(no description on file)'}\n\n"
            f"RECORD B:\n  name: {wrong}\n  description: (no separate record on file "
            "— it was folded into A)\n\n"
            f"A prior automated reviewer flagged B as a DIFFERENT organization, "
            f"noting: {note}\n"
            "Independently decide: are RECORD A and RECORD B the same organization?"
        )

    # --- duplicates (audit 1): keep if SAME org ---
    def dup_prompt(self, item):
        canon, var = item["outer"], item["inner"]
        a, b = self.node(canon), self.node(var)
        if a and b:
            return judge.build_pair_prompt(
                judge.describe_node(a), judge.describe_node(b),
                "have similar or rearranged names")
        note = item["meta"].get("reason") or "(none)"
        return (
            "Two records have similar or rearranged names. Decide whether they "
            "are the same organization.\n\n"
            f"RECORD A:\n  name: {canon}\n  description: {self.desc(canon) or '(none)'}\n\n"
            f"RECORD B:\n  name: {var}\n  description: {self.desc(var) or '(none)'}\n\n"
            f"A prior automated reviewer's note: {note}\n"
            "Are RECORD A and RECORD B the same organization?"
        )

    def _excerpt(self, quote, srcs):
        if not (self.use_context and quote):
            return ""
        for u in srcs:
            path = self.url_index.get(u)
            if not path:
                continue
            if path not in self.content_cache:
                try:
                    self.content_cache[path] = path.read_text("utf-8")
                except Exception:
                    self.content_cache[path] = ""
            ex = EDGES.find_context(quote, self.content_cache[path], self.window)
            if ex:
                return ex
        return ""

    def _edge_evidence(self, outer, inner, rel, meta):
        quote = meta.get("quote") or ""
        srcs = list(meta.get("srcs") or [])
        if not quote or not srcs:
            for r, q, urls in self.edge_quotes.get(
                    frozenset((judge.fold(outer), judge.fold(inner))), []):
                if rel == "*" or r == rel:
                    quote = quote or q
                    srcs = srcs or list(urls)
                    if quote:
                        break
        return quote, srcs

    # --- edges (audit 2): keep block if NO real relationship ---
    def edge_prompt(self, item):
        outer, inner, rel = item["outer"], item["inner"], item["rel"]
        quote, srcs = self._edge_evidence(outer, inner, rel, item["meta"])
        excerpt = self._excerpt(quote, srcs)
        if excerpt:
            ctx = f'\nSource article excerpt (context around the quote):\n"""\n{excerpt}\n"""\n'
        else:
            ctx = "\n(No article excerpt available — judge from the quote.)\n"
        rel_label = "any type" if rel == "*" else rel
        return (
            f"Organization A: {outer}\n  what it is: {self.desc(outer) or '(no description on file)'}\n"
            f"Organization B: {inner}\n  what it is: {self.desc(inner) or '(no description on file)'}\n\n"
            f"Claimed relationship between {outer} and {inner} (type: {rel_label}).\n"
            f"  evidence quote: {quote or '(none)'}\n"
            f"{ctx}\n"
            f"Do {outer} and {inner} have a real org-to-org relationship of any kind, "
            f"based on this evidence?"
        )

    # --- country links (audit 3): keep block if link is UNSUPPORTED ---
    def country_prompt(self, item):
        org, country = item["outer"], item["inner"]
        n = self.node(org)
        home = (n.get("country") if n else "") or "(unknown)"
        quote = item["meta"].get("quote") or ""
        context = ""
        cl = self.country_links.get((judge.fold(org), judge.fold(country)))
        if cl:
            quote = quote or cl[0]
            context = cl[1]
        return (
            f"Organization: {org}\n  home country: {home}\n"
            f"  description: {self.desc(org) or '(none)'}\n\n"
            f"Claimed country link: {country}\n"
            f"  evidence quote: {quote or '(none)'}\n"
            f"  context: {context or '(none)'}\n\n"
            f"Does the evidence support that this organization is really linked to {country}?"
        )

    # --- umbrella orgs (audit 4): keep if IS umbrella ---
    def _mention_ctx(self, node):
        """Article passages where a description-less node is named (None if it
        has a description or context is off), so node re-judges see real text
        instead of just the name."""
        if not (self.use_context and node):
            return None
        if (node.get("description") or "").strip():
            return None
        return judge.node_mention_contexts(node, self.url_index, self.content_cache) or None

    def umbrella_prompt(self, item):
        name = item["name"]
        n = self.node(name)
        if n:
            # Reuse audit 4's own prompt builder (now with mention context) so
            # the re-judge asks the exact same question the original audit did.
            return UMBRELLA.build_prompt(n, self._mention_ctx(n))
        note = item.get("reason") or item["meta"].get("reason") or "(none)"
        return (
            f"Organization: {name}\n"
            f"  description: {self.desc(name) or '(no description on file)'}\n\n"
            f"A prior automated reviewer flagged this as an umbrella term / collective "
            f"category, noting: {note}\n"
            f"Is '{name}' a real single criminal organization, or is it an "
            f"umbrella term / collective category?"
        )

    # --- non-criminal entities (audit 5): keep if NON-CRIMINAL ---
    def noncriminal_prompt(self, item):
        name = item["name"]
        n = self.node(name)
        if n:
            # Reuse audit 5's own prompt builder (now with mention context) so
            # the re-judge asks the exact same question the original audit did.
            return NONCRIMINAL.build_prompt(n, self._mention_ctx(n))
        note = item.get("reason") or item["meta"].get("reason") or "(none)"
        return (
            f"Entry: {name}\n"
            f"  description: {self.desc(name) or '(no description on file — mention-only node; judge by the name)'}\n\n"
            f"A prior automated reviewer flagged this as a NON-CRIMINAL entity "
            f"(state / political / religious / civil body), noting: {note}\n"
            f"Is '{name}' a criminal or armed organization that belongs in a database "
            f"of organized crime, or a non-criminal entity that should be excluded?"
        )


# ── Audit configs ─────────────────────────────────────────────────────────

SOURCES = {
    "duplicates": {
        "file": "1_known_duplicates.py", "shape": "nameset",
        "vars": ["KNOWN_DUPLICATES", "KNOWN_DUPLICATES_LOW_CONFIDENCE"],
    },
    "blocklist": {
        "file": "0_node_blocklist.py", "shape": "nameset",
        "vars": ["BLOCKLIST"],
    },
    "edges": {
        "file": "2_edge_blocklist.py", "shape": "edge",
        "vars": ["EDGE_BLOCKLIST", "EDGE_BLOCKLIST_LOW_CONFIDENCE"],
    },
    "countries": {
        "file": "3_country_link_blocklist.py", "shape": "nameset",
        "vars": ["COUNTRY_LINK_BLOCKLIST", "COUNTRY_LINK_BLOCKLIST_LOW_CONFIDENCE"],
    },
    "umbrella": {
        "file": "4_umbrella_orgs.py", "shape": "set",
        "vars": ["TO_BE_EXCLUDED", "TO_BE_EXCLUDED_LOW_CONFIDENCE"],
    },
    "noncriminal": {
        "file": "5_non_criminal_orgs.py", "shape": "set",
        "vars": ["TO_BE_EXCLUDED", "TO_BE_EXCLUDED_LOW_CONFIDENCE"],
    },
}


def gather_items(kind, cfg, audit_dir, limit):
    """Flatten a suggestion file into per-decision items."""
    path = Path(audit_dir) / cfg["file"]
    structured = load_structured(path)
    meta = parse_meta(path)
    items = []

    if cfg["shape"] == "set":
        # TO_BE_EXCLUDED is a set of strings (audits 4 and 5)
        for vi, var in enumerate(cfg["vars"]):
            is_low = "LOW_CONFIDENCE" in var
            block = structured.get(var, set())
            if isinstance(block, dict):
                block = set(block.keys())
            for name in sorted(block):
                # Try to recover confidence/reason from inline comments
                # by parsing the file text
                item_meta = {"conf": "", "reason": ""}
                if isinstance(name, str):
                    items.append({"kind": kind, "name": str(name),
                                  "is_low": is_low, "meta": item_meta})
    elif cfg["shape"] == "edge":
        for vi, var in enumerate(cfg["vars"]):
            is_low = "LOW_CONFIDENCE" in var
            block = structured.get(var, {})
            for outer, inner_obj in block.items():
                if isinstance(inner_obj, dict):
                    for inner, rels in inner_obj.items():
                        rels_set = rels if isinstance(rels, set) else {rels}
                        for rel in sorted(rels_set):
                            items.append({"kind": kind, "outer": outer, "inner": inner,
                                         "rel": rel, "is_low": is_low,
                                         "meta": meta.get((str(outer), str(inner)), {})})
    else:
        # nameset shape (BLOCKLIST, KNOWN_DUPLICATES, COUNTRY_LINK_BLOCKLIST)
        for vi, var in enumerate(cfg["vars"]):
            is_low = "LOW_CONFIDENCE" in var
            block = structured.get(var, {})
            for outer, inner_obj in block.items():
                inners = inner_obj if isinstance(inner_obj, (set, list)) else (inner_obj.keys() if isinstance(inner_obj, dict) else [])
                for inner in sorted(inners):
                    items.append({"kind": kind, "outer": outer, "inner": str(inner),
                                 "rel": None, "is_low": is_low,
                                 "meta": meta.get((str(outer), str(inner)), {})})
    if limit:
        items = items[:limit]
    return items


def keep_predicate(kind, verdict, conf_ok):
    """Does this verdict CONFIRM the suggestion?"""
    if kind in ("blocklist",):
        same, conf, _ = judge.parse_verdict(verdict)
        return same is False and conf_ok(conf)
    if kind in ("duplicates",):
        same, conf, _ = judge.parse_verdict(verdict)
        return same is True and conf_ok(conf)
    if kind == "edges":
        sup, conf, _ = EDGES.parse_edge(verdict)
        return sup is False and conf_ok(conf)
    if kind == "countries":
        sup, conf, _ = COUNTRY.parse_supported(verdict)
        return sup is False and conf_ok(conf)
    if kind == "umbrella":
        is_umb, conf, _ = UMBRELLA.parse_verdict(verdict)
        return is_umb is True and conf_ok(conf)
    if kind == "noncriminal":
        is_non, conf, _ = NONCRIMINAL.parse_verdict(verdict)
        return is_non is True and conf_ok(conf)
    return False


def verdict_text(kind, verdict):
    """(confidence, reason) from a verdict dict."""
    if kind in ("blocklist", "duplicates"):
        _, conf, reason = judge.parse_verdict(verdict)
    elif kind == "edges":
        _, conf, reason = EDGES.parse_edge(verdict)
    elif kind == "countries":
        _, conf, reason = COUNTRY.parse_supported(verdict)
    elif kind == "umbrella":
        _, conf, reason = UMBRELLA.parse_verdict(verdict)
    elif kind == "noncriminal":
        _, conf, reason = NONCRIMINAL.parse_verdict(verdict)
    else:
        return "low", ""
    return conf, reason


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Re-review audit suggestions with DeepSeek and write a "
                    "verdict report (6_llm_verdicts.json). Does not modify "
                    "curated_corrections.py, but 7_apply_corrections.py reads the "
                    "report to veto the matching identity auto-suggestions. "
                    "Needs DEEPSEEK_API_KEY.")
    ap.add_argument("--audit-dir", default=judge.data_path(),
                    help="dir holding the audit output .py files")
    ap.add_argument("--crimenet", default=judge.root("data", "crimenet_raw.json"),
                    help="crimenet snapshot for evidence enrichment (default "
                         "crimenet_raw.json — it always exists post-pipeline, "
                         "whereas crimenet.json only exists after apply_corrections)")
    ap.add_argument("--txts", default=judge.root("data", "txts"),
                    help="dir of <page>/{url.txt,content.txt} for edge article context")
    ap.add_argument("--output", "-o", default=judge.data_path("6_llm_verdicts.json"))
    ap.add_argument("--only", default="duplicates,blocklist,edges,countries,umbrella,noncriminal",
                    help="comma list of audits to process (default all)")
    ap.add_argument("--min-confidence", choices=["high", "medium"], default="high",
                    help="lowest re-judge confidence to approve (default high)")
    ap.add_argument("--limit", type=int, default=0,
                    help="only re-judge the first N suggestions per audit (0=all)")
    ap.add_argument("--workers", "-w", type=int, default=judge.DEFAULT_WORKERS,
                    help="parallel DeepSeek API workers (default 50)")
    ap.add_argument("--no-context", action="store_true",
                    help="skip article excerpts for edge re-judging (cheaper)")
    ap.add_argument("--window", type=int, default=1500,
                    help="chars of article context on each side of the quote (default 1500)")
    args = ap.parse_args()

    wanted = {k.strip() for k in args.only.split(",")}
    conf_ok = (lambda c: c in ("high", "medium")) if args.min_confidence == "medium" else (lambda c: c == "high")

    # ── Load crimenet evidence ────────────────────────────────────────────
    fn, eq, cl, deg = load_crimenet(args.crimenet)
    print(f"Loaded crimenet: {len(fn)} fold→node, {len(eq)} edge-quote sets, "
          f"{len(cl)} country-link records")

    # Build url_index for edge article context
    url_index = {}
    txts_base = Path(args.txts)
    if txts_base.is_dir():
        for d in txts_base.iterdir():
            uf, cf = d / "url.txt", d / "content.txt"
            if uf.exists() and cf.exists():
                url = uf.read_text("utf-8").strip()
                if url:
                    url_index[url] = cf
    print(f"  article context index: {len(url_index)} article(s)")

    reviewer = Reviewer(fn, eq, cl, url_index, {}, args.window,
                        not args.no_context)

    # ── Gather items ──────────────────────────────────────────────────────
    all_items = {}
    for kind in sorted(wanted):
        if kind not in SOURCES:
            print(f"  unknown audit kind: {kind} — skipping")
            continue
        cfg = SOURCES[kind]
        items = gather_items(kind, cfg, args.audit_dir, args.limit)
        all_items[kind] = items
        print(f"  {kind}: {len(items)} suggestion(s) from {cfg['file']}")
        if not items:
            continue

    total = sum(len(v) for v in all_items.values())
    if total == 0:
        print("\nNo suggestions to re-judge.")
        Path(args.output).write_text(
            json.dumps({"verdicts": {}, "metadata": {"total": 0}}, indent=2))
        print(f"Saved → {args.output}")
        return

    # ── Re-judge ──────────────────────────────────────────────────────────
    report = {"metadata": {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "min_confidence": args.min_confidence,
        "cost": "",
    }, "verdicts": {}}

    for kind in sorted(wanted):
        items = all_items.get(kind, [])
        if not items:
            report["verdicts"][kind] = {"approved": [], "rejected": [], "errors": []}
            continue

        # Build a prompt function depending on kind
        if kind == "blocklist":
            prompt_fn = reviewer.block_prompt
            system = judge.SAME_ORG_SYSTEM
        elif kind == "duplicates":
            prompt_fn = reviewer.dup_prompt
            system = judge.SAME_ORG_SYSTEM
        elif kind == "edges":
            prompt_fn = reviewer.edge_prompt
            system = EDGES.EDGE_SYSTEM
        elif kind == "countries":
            prompt_fn = reviewer.country_prompt
            system = COUNTRY.COUNTRY_SYSTEM
        elif kind == "umbrella":
            prompt_fn = reviewer.umbrella_prompt
            system = UMBRELLA.UMBRELLA_SYSTEM
        elif kind == "noncriminal":
            prompt_fn = reviewer.noncriminal_prompt
            system = NONCRIMINAL.NONCRIMINAL_SYSTEM
        else:
            continue

        def build_prompt(item):
            return prompt_fn(item)

        def line_fmt(done, n, item, v):
            conf, reason = verdict_text(kind, v)
            if kind in ("umbrella", "noncriminal"):
                label = item.get("name", "")[:50]
            else:
                label = f"{item.get('outer','')[:22]} ~ {item.get('inner','')[:22]}"
            approved = keep_predicate(kind, v, conf_ok)
            flag = "APPROVE" if approved else ("REJECT" if v else "ERROR ")
            return (f"  [{done}/{n}] {flag:7s} ({conf:6s}) {label[:52]:52s} {reason[:40]}")

        print(f"\n  {kind}: re-judging {len(items)} suggestion(s) "
              f"({args.workers} workers)…")
        verdicts = judge.run_pass(items, build_prompt, args.workers, line_fmt,
                                  system=system)

        approved, rejected, errors = [], [], []
        for item, v in zip(items, verdicts):
            if v is None:
                errors.append(_item_summary(kind, item, "", "API/parse error"))
                continue
            conf, reason = verdict_text(kind, v)
            entry = _item_summary(kind, item, conf, reason)
            if keep_predicate(kind, v, conf_ok):
                approved.append(entry)
            else:
                rejected.append(entry)

        nc = len(items)
        print(f"  {kind}: {len(approved)} approved, {len(rejected)} rejected, "
              f"{len(errors)} error(s) of {nc}")
        report["verdicts"][kind] = {"approved": approved, "rejected": rejected,
                                     "errors": errors}

    report["metadata"]["cost"] = judge.usage_report()
    report["metadata"]["total_suggestions"] = total
    report["metadata"]["total_approved"] = sum(
        len(v["approved"]) for v in report["verdicts"].values())

    Path(args.output).write_text(
        json.dumps(report, ensure_ascii=False, indent=2), "utf-8")

    print(f"\nCost: {judge.usage_report()}")
    print(f"Report: {report['metadata']['total_approved']} approved / "
          f"{total} total suggestions")
    print(f"Saved → {args.output}")
    print("\nReview the approved suggestions and paste the good ones into "
          "audit/curated_corrections.py. Then re-run 7_apply_corrections.py to rebuild crimenet.json.")


def _item_summary(kind, item, conf, reason):
    """A portable summary of one suggestion + verdict."""
    if kind in ("umbrella", "noncriminal"):
        return {"name": item.get("name", ""), "confidence": conf, "reason": reason,
                "is_low": item.get("is_low", False)}
    elif kind == "edges":
        return {"outer": item.get("outer", ""), "inner": item.get("inner", ""),
                "rel": item.get("rel", ""), "confidence": conf, "reason": reason,
                "is_low": item.get("is_low", False)}
    else:
        return {"outer": item.get("outer", ""), "inner": item.get("inner", ""),
                "confidence": conf, "reason": reason,
                "is_low": item.get("is_low", False)}


if __name__ == "__main__":
    main()
