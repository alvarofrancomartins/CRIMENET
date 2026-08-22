"""
judge.py — shared infrastructure for the CRIMENET audit tools.

Every audit script that asks DeepSeek to verify a finding (wrong merges, missed
merges) uses the SAME machinery from here, so they behave and read alike:

  • paths      — root() / data_path(), resolved to the repo root.
  • DeepSeek    — config, a pooled Session, load_key(), llm_call(), and ONE
                  shared SAME_ORG_SYSTEM prompt + run_pass() parallel runner.
  • nodes       — fold()/norm() text keys, compute_degree(), describe_node(),
                  build_pair_prompt(), and source/section comment helpers.
  • output      — render_dict_file(): the single renderer that writes a
                  ready-to-paste {canonical: {variant, ...}} .py file
                  (KNOWN_DUPLICATES or BLOCKLIST), ordered however the caller
                  ordered its sections.

Needs the DEEPSEEK_API_KEY environment variable set for the LLM helpers.
"""

import os
import json
import time
import threading
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

# ── Paths ────────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent

def root(*parts):
    """A path under the repo root (parent of audit/)."""
    return str(_REPO_ROOT.joinpath(*parts))

def data_path(*parts):
    """Audit OUTPUT location: audit/audit_data/. Created on first use."""
    d = Path(__file__).resolve().parent / "audit_data"
    d.mkdir(exist_ok=True)
    return str(d.joinpath(*parts))


# ── DeepSeek config ─────────────────────────────────────────────────────────
API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
RETRIES = 3
MAX_RL_RETRIES = 6      # 429 backoff retries, separate from RETRIES
DEFAULT_WORKERS = 50
LLM_MAX_TOKENS = 512
_session = requests.Session()
_session.mount("https://", HTTPAdapter(pool_connections=100, pool_maxsize=100))

# One prompt for every "are these two records the same organization?" question,
# whether the collision was a shared name (wrong merge), a fuzzy spelling, or a
# word-order variant (missed merge). Each script supplies a one-line intro in
# the USER prompt to flag the specific flavor.
SAME_ORG_SYSTEM = """You are verifying entity resolution in a database of criminal organizations. You are given two records and must decide whether they refer to the SAME real-world organization or to TWO DIFFERENT organizations that merely share a name or name-fragment.

Return ONLY a JSON object:
{
  "same_org": true | false,
  "confidence": "high" | "medium" | "low",
  "reason": "one short sentence"
}

Rules:
- true  = the two records refer to the same real-world organization (an abbreviation, translation, alternate spelling, word-order variant, or added/dropped generic word like "clan"/"cartel" of it).
- false = they are different organizations that merely share words (different families, clans, chapters, or sets; different countries, eras, or types), or one record is not an organization at all (a place, a racket/method, a person, a generic word).
- Judge by what each record DESCRIBES, not just the name string. Different countries, types, eras, or unrelated descriptions mean DIFFERENT even when the names overlap.
- If the evidence is too thin to tell, set same_org=false with confidence "low" (safer to flag for human review than to assert a merge that may be wrong)."""


# Token accounting — every llm_call adds the API's reported usage here, so a
# script can print exactly what a run cost (and you can extrapolate from it).
_usage_lock = threading.Lock()
USAGE = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0}

def _record_usage(u):
    if not u:
        return
    with _usage_lock:
        USAGE["calls"] += 1
        USAGE["prompt_tokens"] += u.get("prompt_tokens", 0)
        USAGE["completion_tokens"] += u.get("completion_tokens", 0)

def usage_report(rate_in=0.27, rate_out=1.10):
    """A one-line token + rough-cost summary. Default rates are deepseek-chat
    list prices ($/1M tokens) at time of writing — check your own DeepSeek
    pricing, they change and cache hits are cheaper."""
    pt, ct = USAGE["prompt_tokens"], USAGE["completion_tokens"]
    cost = pt / 1e6 * rate_in + ct / 1e6 * rate_out
    return (f"{USAGE['calls']:,} API call(s) | {pt:,} prompt + {ct:,} completion "
            f"tokens | ~${cost:.2f} (at ${rate_in}/M in + ${rate_out}/M out)")


def load_key():
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError(
            "DeepSeek API key not found. Set it with:\n"
            '    export DEEPSEEK_API_KEY="sk-..."'
        )
    return key.strip()


def llm_call(api_key, prompt, system=SAME_ORG_SYSTEM):
    """One DeepSeek chat call returning the parsed JSON dict, or None on
    failure (caller treats None as 'unsure — send to review')."""
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL,
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "temperature": 0.0,
        "max_tokens": LLM_MAX_TOKENS,
    }
    for attempt in range(1, RETRIES + 1):
        try:
            # 429s retry on their own backoff budget so a burst of rate-limit
            # responses doesn't consume the content-retry attempts.
            r = None
            for rl in range(MAX_RL_RETRIES):
                r = _session.post(API_URL, headers=headers, json=payload,
                                  timeout=(10, 60))
                if r.status_code == 429:
                    time.sleep(min(60, 5 * (2 ** rl)))
                    continue
                break
            r.raise_for_status()
            resp = r.json()
            _record_usage(resp.get("usage"))
            return json.loads(resp["choices"][0]["message"]["content"].strip())
        except Exception:
            time.sleep(2 * attempt)
    return None


def parse_verdict(v):
    """Normalize an llm_call result → (same_org|None, confidence, reason).
    same_org is None when the call failed/!dict (an error to review)."""
    if not isinstance(v, dict):
        return None, "low", "API/parse error — check manually"
    same = bool(v.get("same_org", False))
    conf = str(v.get("confidence", "low")).lower()
    reason = v.get("reason", "")
    return same, conf, reason


def run_pass(items, build_prompt, workers, line_fmt, system=SAME_ORG_SYSTEM):
    """Judge every item in parallel and return a list of raw verdict dicts
    (or None) aligned with `items`.

    items       — list of opaque objects the caller understands.
    build_prompt — item → user-prompt string.
    line_fmt     — (done, total, item, verdict) → one progress line to print.
    """
    api_key = load_key()
    n = len(items)
    results = [None] * n
    done = 0

    def work(k):
        return llm_call(api_key, build_prompt(items[k]), system=system)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(work, k): k for k in range(n)}
        for fut in as_completed(futs):
            k = futs[fut]
            done += 1
            try:
                v = fut.result()
            except Exception:
                v = None
            results[k] = v
            print(line_fmt(done, n, items[k], v))
    return results


# ── Text keys ────────────────────────────────────────────────────────────────
def norm(s):
    """Lowercase, strip accents, drop punctuation → single-spaced ascii.
    Used for difflib fuzzy comparison and for blocking by whole words."""
    nfkd = unicodedata.normalize("NFKD", s or "")
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = "".join(c if (c.isalnum() or c.isspace()) else " "
                      for c in ascii_str.lower())
    return " ".join(cleaned.split())


def fold(s):
    """Lowercase, strip accents, keep only alphanumerics (no spaces). Used as
    an identity key for a single spelling."""
    nfkd = unicodedata.normalize("NFKD", s or "")
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return "".join(c for c in ascii_str.lower() if c.isalnum())


def node_names(node):
    """Every distinct name a node is known by (standard_name + aliases +
    original_text_names), order preserved."""
    names = []
    sn = (node.get("standard_name") or "").strip()
    if sn:
        names.append(sn)
    for a in node.get("aliases") or []:
        if isinstance(a, str) and a.strip():
            names.append(a.strip())
    for o in node.get("original_text_names") or []:
        if isinstance(o, str) and o.strip():
            names.append(o.strip())
    return names


def compute_degree(edges):
    """Connection degree per standard_name across the edge list."""
    degree = Counter()
    for e in edges:
        if e.get("source"):
            degree[e["source"]] += 1
        if e.get("target"):
            degree[e["target"]] += 1
    return degree


# ── LLM evidence (node-based) ────────────────────────────────────────────────
def describe_node(node):
    """Evidence for one node, straight from its crimenet.json record."""
    return {
        "name": (node.get("standard_name") or "").strip(),
        "description": (node.get("description") or "").strip(),
        "aliases": node.get("aliases") or [],
        "country": (node.get("country") or "").strip(),
        "time_period": (node.get("time_period") or "").strip(),
    }


def build_pair_prompt(a, b, intro, ctx_a=None, ctx_b=None):
    """Two-record user prompt from describe_node() dicts. `intro` names the
    flavor of collision (e.g. 'have similar or rearranged names'). `ctx_a`/`ctx_b`
    are optional article passages where a description-less node is named, so the
    judge sees real text instead of only the name."""
    al_a = ", ".join(a["aliases"][:12]) or "(none listed)"
    al_b = ", ".join(b["aliases"][:12]) or "(none listed)"

    def _ctx(ctxs):
        if not ctxs:
            return ""
        body = "\n    ".join("… " + " ".join(c.split())[:300] + " …" for c in ctxs)
        return f"  passages where it is named:\n    {body}\n"

    return (
        f"The two records below {intro}. Decide if they are the same organization.\n\n"
        f"RECORD A:\n"
        f"  name: {a['name']}\n"
        f"  country: {a['country'] or '(unknown)'}\n"
        f"  time period: {a['time_period'] or '(unknown)'}\n"
        f"  aliases: {al_a}\n"
        f"  description: {a['description'] or '(no description available)'}\n"
        f"{_ctx(ctx_a)}\n"
        f"RECORD B:\n"
        f"  name: {b['name']}\n"
        f"  country: {b['country'] or '(unknown)'}\n"
        f"  time period: {b['time_period'] or '(unknown)'}\n"
        f"  aliases: {al_b}\n"
        f"  description: {b['description'] or '(no description available)'}\n"
        f"{_ctx(ctx_b)}\n"
        f"Are RECORD A and RECORD B the same organization?"
    )


# ── Article context for mention-only nodes ──────────────────────────────────
# A node that appears only as a MENTION carries no description, so a node audit
# that judges it from the profile alone sees just the bare name and has to guess
# (this is how "Almighty Bishops", a street gang, got mislabeled a "religious
# body"). These pull the passage where the node is named in the article(s) that
# reference it, so the LLM judges from real text instead of the name.

def build_url_index(txts_dir):
    """{source url -> content.txt Path} across every data/txts/<page>/ dir."""
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


def find_name_context(names, content, window=350):
    """A passage of `content` around the first place any of `names` appears
    (longest/most-specific name first), or '' if none is found."""
    if not content or not names:
        return ""
    low = content.lower()
    for nm in sorted({n for n in names if n}, key=len, reverse=True):
        probe = " ".join(nm.split()).lower()
        if len(probe) < 3:
            continue
        i = low.find(probe)
        if i == -1:
            continue
        start = max(0, i - window)
        end = min(len(content), i + len(probe) + window)
        ex = content[start:end].strip()
        if start > 0:
            ex = "… " + ex
        if end < len(content):
            ex = ex + " …"
        return ex
    return ""


def node_mention_contexts(node, url_index, content_cache, window=350, max_ctx=3):
    """Up to `max_ctx` article passages where this node is named, taken from its
    own_sources + mentioned_in articles. For a description-less mention node this
    is the only real evidence about what the org actually is."""
    names = node_names(node)
    if not names:
        return []
    seen, urls = set(), []
    for s in (node.get("own_sources") or []) + (node.get("mentioned_in") or []):
        u = s.get("url") if isinstance(s, dict) else None
        if u and u not in seen:
            seen.add(u)
            urls.append(u)
    out = []
    for u in urls:
        path = url_index.get(u)
        if not path:
            continue
        if path not in content_cache:
            try:
                content_cache[path] = path.read_text("utf-8")
            except Exception:
                content_cache[path] = ""
        ex = find_name_context(names, content_cache[path], window)
        if ex:
            out.append(ex)
            if len(out) >= max_ctx:
                break
    return out


# ── Output rendering ─────────────────────────────────────────────────────────
def urls(source_list):
    """Pull url strings out of an own_sources / mentioned_in list (each item is
    {"url": ..., "title": ...}), de-duplicated, order kept."""
    out = []
    for s in source_list or []:
        u = (s.get("url") or "").strip() if isinstance(s, dict) else str(s).strip()
        if u and u not in out:
            out.append(u)
    return out


def source_comment_lines(node, indent, max_mentions):
    """Comment lines telling you where to verify a node: its OWN page(s) first
    (best — the org's own article), then the pages that MENTION it."""
    own = urls(node.get("own_sources"))
    mentioned = urls(node.get("mentioned_in"))
    pad = " " * indent
    lines = []
    if own:
        lines.append(f"{pad}#   own:       " + " | ".join(own))
    else:
        lines.append(f"{pad}#   own:       (unprofiled — no page of its own)")
    if mentioned:
        shown = mentioned[:max_mentions]
        extra = len(mentioned) - len(shown)
        tail = f"  (+{extra} more)" if extra > 0 else ""
        lines.append(f"{pad}#   mentioned: " + " | ".join(shown) + tail)
    else:
        lines.append(f"{pad}#   mentioned: (not mentioned anywhere)")
    return lines


def node_section_header(node, degree, max_mentions):
    """The '# ── "Name" (degree N, profiled) ──' block + its source lines, for
    one canonical node in a KNOWN_DUPLICATES file."""
    name = node["standard_name"]
    deg = degree.get(name, 0)
    prof = "profiled" if node.get("own_sources") else "unprofiled"
    head = [f'    # ── "{name}"  (degree {deg}, {prof}) ' + "─" * max(0, 40 - len(name))]
    head += source_comment_lines(node, 4, max_mentions)
    return head


def dict_block_lines(varname, sections):
    """Render ONE `VARNAME = { ... }` block as a list of lines.

    sections — ordered list of:
        {
          "key": <canonical string>,
          "header": [comment lines, already indented],   # optional
          "items": [ {"name": str,
                      "note": str (full '# ...' comment or ""),
                      "comments": [extra indented comment lines]} ],
        }
    A file can hold several blocks (e.g. a confident dict + a low-confidence
    dict of the same shape).
    """
    lines = [f"{varname} = {{"]
    for sec in sections:
        lines.append("")
        lines.extend(sec.get("header", []))
        lines.append(f'    {json.dumps(sec["key"], ensure_ascii=False)}: {{')
        for it in sec["items"]:
            note = f"  {it['note']}" if it.get("note") else ""
            lines.append(f'        {json.dumps(it["name"], ensure_ascii=False)},{note}')
            lines.extend(it.get("comments", []))
        lines.append("    },")
    lines.extend(["", "}", ""])
    return lines


def render_dict_file(out_path, varname, file_header, sections, review_lines=None):
    """Write a ready-to-paste {canonical: {variant, ...}} .py file (one block).

    file_header  — list of '# ...' lines for the top of the file (no VAR line).
    review_lines — optional list of '# ...' lines appended after the dict.
    """
    lines = list(file_header) + dict_block_lines(varname, sections)
    if review_lines:
        lines.extend(review_lines)
    Path(out_path).write_text("\n".join(lines), encoding="utf-8")
