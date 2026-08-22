"""
3_enrich_nodes.py

Per-folder enrichment, run AFTER step 2, independently of it. Reads each
folder's content.txt and profiles the article's SUBJECT organization.

    txts/<folder>/content.txt   ->   txts/<folder>/org_profile.json

TWO PASSES, both writing into the one org_profile.json:

  PASS 1 - PROFILE (one LLM call, lead paragraphs + infobox only)
    Identity and fixed facts: canonical_name, aliases, description,
    country (single primary origin), time_period, is_defunct,
    founded_year, dissolved_year. Also decides whether the article is
    about an org at all ({"canonical_name": null} if not).

  PASS 2 - COUNTRY FOOTPRINTS (chunked over the FULL body + infobox)
    Mirrors step 2's edge extraction: the article is split into
    paragraph-aware chunks, and for EACH chunk the model reports only the
    countries where that passage documents the ORG ITSELF having a real
    footprint, each backed by a verbatim evidence_quote. The org's HOME COUNTRY
    (from pass 1) is passed in so the model can skip origin-restatement, and
    the prompt excludes non-footprints: home/origin mentions, actions a
    country takes toward the org (designating, listing, arresting, allying,
    reporting on) with no stated presence, failed/proposed/attempted presence,
    statistical/background mentions, and a single member fleeing/arrested/dying
    somewhere. Results are unioned across chunks (strongest evidence wins).
    Every country claim is auditable; a country with no supporting quote or
    no stated org action is never emitted.

    country_links is a LIST OF RECORDS, not bare strings:
        {"country": "...", "evidence_quote": "...", "context": "..."}

    The origin country is NOT auto-added to country_links - it only appears
    if a passage actually supports the org operating there (and the
    home-country rule still filters out mere origin-restatement).

org_profile.json schema (org articles):
{
  "canonical_name":   "...",
  "aliases":          [...],
  "description":      "...",
  "country":          "primary origin, or Unknown",   (pass 1)
  "country_links": [{"country","evidence_quote","context"}, ...],  (pass 2)
  "time_period":      "...",
  "is_defunct":       true | false,
  "founded_year":     "YYYY" | null,
  "dissolved_year":   "YYYY" | null,
  "source_url":       "...",
  "source_file":      "..."
}
Non-org articles: {"canonical_name": null}.

Resumable: skips folders that already have org_profile.json. Delete a
folder's org_profile.json to reset it; --force redoes all.

Usage:
    python 3_enrich_nodes.py --dir ../data/txts
"""

import json
import re
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter

from lib.common import load_key, split_body_and_infobox, chunk_text, MAX_CHUNK_WORDS

API_URL = "https://api.deepseek.com/chat/completions"
MODEL = "deepseek-chat"
RETRIES = 3
DELAY = 0.2
DEFAULT_WORKERS = 50
MAX_TOKENS = 4096
# Pass-2 country extraction: a single passage can enumerate many countries
# (e.g. "chapters in 29 countries; Australia, Argentina, Belgium, ..."), whose
# JSON output overruns the base cap and truncates (finish_reason="length"),
# yielding cut-off, unparseable JSON. Escalate the cap on a truncation retry —
# mirroring step 2 — and sub-split the passage if it still won't fit.
COUNTRY_MAX_TOKENS = 2048
COUNTRY_MAX_TOKENS_RETRY = 8192
MAX_RL_RETRIES = 6   # 429 backoff retries, separate from RETRIES

LEAD_WORDS = 500   # send the first ~500 words (not paragraphs — a single-word
                    # section header like "History" burns a whole para slot)

session = requests.Session()
session.mount("https://", HTTPAdapter(pool_connections=100, pool_maxsize=100))


# -- helpers ---------------------------------------------------------------
# load_key(), split_body_and_infobox() and chunk_text() now live in
# lib/common.py, shared with step 2.

def lead_paragraphs(body, max_words=LEAD_WORDS):
    """First paragraphs of the body, up to max_words total.
    Uses word count, not paragraph count — avoids getting almost nothing
    when early paragraphs are section headers or single sentences."""
    paras = [p.strip() for p in re.split(r"\n{2,}", body) if p.strip()]
    result, wc = [], 0
    for p in paras:
        pw = len(p.split())
        if wc + pw > max_words and result:
            break
        result.append(p)
        wc += pw
    return "\n\n".join(result)


# -- shared country-name format rules --------------------------------------

COUNTRY_FORMAT = """COUNTRY NAME FORMAT:
- "United States", not "USA"/"US"/"America".
- "United Kingdom", not "UK"/"Britain". Northern Ireland rolls into "United Kingdom".
- "Hong Kong" is its own entry, distinct from China.
- "Czech Republic", not "Czechia". "Myanmar", not "Burma".
- Map defunct states (USSR, Yugoslavia, Czechoslovakia) to modern successors when unambiguous.
- Use the country's English short name. Do NOT expand a continent or region
  ("Europe", "the Balkans", "Latin America") into a list of countries - only
  name a country if the passage names it."""


# -- PASS 1: profile prompt ------------------------------------------------

SYSTEM_PROFILE = """You are an expert on global organized crime. You are given the opening text (first ~500 words) and INFOBOX of a Wikipedia article. Profile the criminal organization the article is about.

Almost every article is about an org - including broad or collective ones ("Russian mafia", "American Mafia", "People Nation"). Profile those normally.

If the article IS about a criminal organization, return a JSON object:

{
  "canonical_name": "The single most recognized international name. English where an established English name exists; otherwise the native name (e.g. 'Ndrangheta, Yamaguchi-gumi).",
  "aliases": ["EVERY other name used for THIS SPECIFIC org: full names, abbreviations, acronyms, native names, translations, historical names, nicknames. Include the article-title spelling if it differs. Exclude canonical_name itself. CRITICAL: do NOT include generic category words such as 'mafia', 'cartel'. An alias must be specific enough that it could only refer to this org. Empty list if none."],
  "description": "3 to 5 sentences, up to 100 words. What the org does, where it is based, when and by whom founded, distinguishing facts, current status. Factual, no editorializing.",
  "country": "country of ORIGIN only: where the org is FROM, the nationality or territory it emerged from. English short name. Do NOT use its current headquarters or main base when that differs from its origin (e.g. a Palestinian group headquartered in Damascus has country 'Palestine', not 'Syria'); a current HQ or base is captured separately as a footprint in pass 2. 'Unknown' if the lead and infobox do not establish an origin. This is the single home/origin country, NOT a list of where it operates.",
  "time_period": "human-readable activity span AS STATED in the text, e.g. '1988-2010', 'founded 1969', '1990s'. Use '-present' ONLY if the text indicates the org is still active (must agree with is_defunct=false); never assume it. 'Unknown' if unclear.",
  "is_defunct": "true, false, or \\"unknown\\" (see guidance below)",
  "founded_year": "YYYY as a string, or null if not stated",
  "dissolved_year": "YYYY as a string, or null if not stated / still active"
}

If - and ONLY if - the article is clearly NOT about a criminal group (a person/biography, a place, an event/clash/operation, an abstract concept, a book/film, a disambiguation or "List of ..." page, or a law-enforcement/government body), return exactly:
{"canonical_name": null}

is_defunct GUIDANCE (three values):
- true if disbanded, dissolved, dismantled, absorbed/merged and no longer independent, or described purely in the past tense.
- false if currently active or ongoing.
- "unknown" if the text gives no clear signal either way. Do NOT guess - when genuinely silent or ambiguous, return "unknown".

""" + COUNTRY_FORMAT + """

Return ONLY valid JSON. No prose, no markdown fences.

Example (org article):
{
  "canonical_name": "Sinaloa Cartel",
  "aliases": ["Cartel de Sinaloa", "CDS", "Pacific Cartel", "Guzman-Loera Organization"],
  "description": "Mexican drug-trafficking organization based in Sinaloa, founded in the late 1980s by Joaquin 'El Chapo' Guzman and others. One of the largest drug cartels in the world, trafficking cocaine, methamphetamine and fentanyl into the United States. Engaged in long-running conflicts with rival cartels.",
  "country": "Mexico",
  "time_period": "Late 1980s-present",
  "is_defunct": false,
  "founded_year": "1989",
  "dissolved_year": null
}

Example (non-org article):
{"canonical_name": null}"""


# -- PASS 2: country-links prompt ------------------------------------------

SYSTEM_COUNTRIES = """You are an expert on global organized crime. You are given a PASSAGE from a Wikipedia article about a specific criminal organization (named below), along with that org's HOME COUNTRY. Identify only the countries where THIS PASSAGE documents the organization ITSELF having a real footprint.

A footprint means the organization ITSELF is present or acting in/through the country. It MUST be one of these four situations:
- Based there: it has members, cells, chapters, a headquarters, bases, territory, offices, businesses or assets there; it established or expanded itself there.
- Acting there: it carries out a specific activity there - an attack, operation, killing, kidnapping, extortion, or local selling/trafficking on that country's soil.
- Supplying from there: it sources, buys, or imports goods / weapons / drugs / money FROM that country (a supplier link, even with no local base).
- Routing through there: it routes, ships, moves, or launders goods / money THROUGH that country (the country is a node on its trafficking route).
Past or present both count. If a country fits NONE of these, it is not a footprint - omit it.

Decide each candidate country IN THIS ORDER; emit it only if it survives all four:
1. QUOTE: find the verbatim span in this passage that names the country.
2. ACTION: state, in your own words, what the ORG ITSELF does in/through that country. It must match one of the four footprint situations above. If you cannot state such an action by the org itself - without hedging, negating, describing something done TO the org, or attributing it to a single individual - there is no footprint: omit it.
3. NOT-HOME: if the country is the org's HOME COUNTRY (given below), or the passage names it only as the org's origin / where it is "from" / "based in" as identity, omit it. Home is not a footprint.
4. EMIT: only now, output the record.

EXCLUDE - these are NOT footprints, however the country is named:
- The org's own home/origin country, or a passage merely restating where it is from.
- Actions a COUNTRY (its government, press, or courts) takes toward the org with NO stated org presence: designating, labeling, sanctioning, listing, banning, reporting on, or allying with / supporting it; arrests, captures, extraditions or prosecutions where the passage does not place the org's own operations in that country.
- An individual member fleeing to, hiding in, retiring in, living in, being arrested in, or dying in a country - that is one person, not the organization.
- Attempted, proposed, planned, intended, rumored, or FAILED presence - if it did not actually happen, omit it.
- Ideological, rhetorical, statistical, or background mentions (e.g. "one of the top targeted countries"); co-occurrence; inference from the org's name, ethnicity, or reputation; expanding a region/continent into countries.

For EACH surviving country, emit one record:
{
  "country": "English short name of the country",
  "context": "1 short sentence, your own words: what the ORG does in/through this country (this is step 2 above).",
  "evidence_quote": "Verbatim snippet from THIS passage supporting it. Copy character-for-character - same wording, punctuation, spelling, accents. No translation or paraphrase. Under 50 words; truncate longer spans at sentence boundaries with '...'. This is the audit trail."
}

If you cannot supply a verbatim quote AND state a real org action, do NOT emit the country. Omit rather than fabricate.

DECISION EXAMPLES (passage says -> verdict):
- "Boss X was arrested in / fled to / died in Brazil" -> OMIT (one individual; an action TO the org, not the org operating).
- "Brazil designated X a terrorist group" / "Brazilian media reported on X" / "Brazil was an ally of X" -> OMIT (a country/press/state acting toward the org).
- "X is one of the top countries targeted by X's attacks" -> OMIT (statistical/background ranking).
- "X sourced its cocaine from Brazil" -> EMIT (a real supply tie to the country).
- "an international trafficking route between Italy, Colombia and Brazil" -> EMIT (the org's network runs through it).
- "X has a chapter / cells / territory / a base in Brazil" -> EMIT (physical presence).
- "X carried out attacks in Brazil" -> EMIT (the org operated there).

""" + COUNTRY_FORMAT + """

Return ONLY valid JSON: {"country_links": [ ... ]}
If the passage documents no qualifying footprint: {"country_links": []}"""


def _post_with_rate_limit(headers, payload):
    """POST to DeepSeek, retrying 429s on their own backoff budget so a burst
    of rate-limit responses doesn't burn the content-retry attempts."""
    r = None
    for rl in range(MAX_RL_RETRIES):
        r = session.post(API_URL, headers=headers, json=payload, timeout=(10, 120))
        if r.status_code == 429:
            time.sleep(min(60, 5 * (2 ** rl)))
            continue
        return r
    return r  # give up on 429s; let the caller's raise_for_status surface it


def call_api(api_key, system, prompt, max_tokens=MAX_TOKENS, max_tokens_retry=None):
    """Call DeepSeek and return the parsed JSON dict, or None after retries.

    A length-truncated response (finish_reason="length") is cut off and thus
    invalid JSON, so re-parsing the same output would fail on every retry.
    Detect it and retry with an escalated token cap (max_tokens_retry) instead,
    mirroring step 2. Defaults to no escalation (retry at the base cap)."""
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    retry_cap = max_tokens_retry or max_tokens
    for attempt in range(1, RETRIES + 1):
        payload = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.0,
            "max_tokens": max_tokens if attempt == 1 else retry_cap,
        }
        try:
            r = _post_with_rate_limit(headers, payload)
            r.raise_for_status()
            choice = r.json()["choices"][0]
            if choice.get("finish_reason") == "length" and attempt < RETRIES:
                time.sleep(2)
                continue
            raw = choice["message"]["content"].strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            return json.loads(raw)
        except Exception:
            time.sleep(2 * attempt)
    return None


# -- coercion --------------------------------------------------------------

def _clean_str(x):
    return x.strip() if isinstance(x, str) and x.strip() else ""


def _clean_unknown(x):
    s = _clean_str(x)
    if not s or s.lower() in {"unknown", "none", "n/a", "null"}:
        return "Unknown"
    return s


def _clean_year(x):
    if x is None:
        return None
    s = str(x).strip()
    m = re.search(r"\b(1[0-9]{3}|20[0-9]{2})\b", s)
    return m.group(1) if m else None


def _coerce_defunct(x):
    """Three-state defunct: True, False, or 'unknown'. Accepts booleans or
    strings from the LLM; anything unrecognized becomes 'unknown'."""
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in {"true", "yes", "defunct"}:
        return True
    if s in {"false", "no", "active"}:
        return False
    return "unknown"


def _norm_country(s):
    return " ".join((s or "").strip().split()).lower()


def coerce_profile(rec, url, folder_name):
    """Validate pass-1 output into the profile skeleton (country_links
    filled by pass 2). Returns None if not a valid org profile."""
    if not isinstance(rec, dict):
        return None
    canonical = _clean_str(rec.get("canonical_name"))
    if not canonical:
        return None

    aliases = rec.get("aliases") or []
    if not isinstance(aliases, list):
        aliases = []
    clean_aliases, seen = [], set()
    seen.add(re.sub(r"[^a-z0-9]", "", canonical.lower()))
    for a in aliases:
        s = str(a).strip()
        if not s:
            continue
        key = re.sub(r"[^a-z0-9]", "", s.lower())
        if key and key not in seen:
            seen.add(key)
            clean_aliases.append(s)
    clean_aliases.sort(key=lambda x: x.lower())

    is_defunct = _coerce_defunct(rec.get("is_defunct"))
    dissolved = _clean_year(rec.get("dissolved_year"))
    if dissolved:
        is_defunct = True  # a dissolved year is hard evidence of defunct

    return {
        "canonical_name": canonical,
        "aliases": clean_aliases,
        "description": _clean_str(rec.get("description")),
        "country": _clean_unknown(rec.get("country")),
        "country_links": [],
        "time_period": _clean_unknown(rec.get("time_period")),
        "is_defunct": is_defunct,
        "founded_year": _clean_year(rec.get("founded_year")),
        "dissolved_year": dissolved,
        "source_url": url,
        "source_file": folder_name,
    }


def coerce_country_records(raw_list):
    """Keep only records with a country name AND a non-empty verbatim quote."""
    out = []
    if not isinstance(raw_list, list):
        return out
    for r in raw_list:
        if not isinstance(r, dict):
            continue
        country = _clean_str(r.get("country"))
        quote = _clean_str(r.get("evidence_quote"))
        if not country or country.lower() in {"unknown", "none", "n/a", "null"}:
            continue
        if not quote:
            continue
        out.append({
            "country": country,
            "evidence_quote": quote,
            "context": _clean_str(r.get("context")),
        })
    return out


def union_country_records(all_records):
    """Union per-chunk records by country, keeping the record with the most
    substantial evidence quote (longest); first seen wins ties."""
    by_country = {}
    for r in all_records:
        key = _norm_country(r["country"])
        if not key:
            continue
        cur = by_country.get(key)
        if cur is None or len(r.get("evidence_quote", "")) > len(cur.get("evidence_quote", "")):
            by_country[key] = r
    return sorted(by_country.values(), key=lambda r: r["country"].lower())


# -- per-folder processing -------------------------------------------------

def extract_countries(api_key, canonical, home_country, lead, body, infobox):
    """PASS 2: chunk the body (+ infobox as a final chunk) and collect
    auditable country records, unioned across chunks.

    Every chunk carries the LEAD as a fixed context header so the model
    knows what the organization IS (essential for obscure orgs whose name
    alone is uninformative), without sending the whole document — which
    would reintroduce cross-passage contamination. The HOME COUNTRY is
    passed in so the model can recognize (and skip) origin-restatement, the
    most common false footprint. The country judgment and its evidence_quote
    must still come from the passage itself.
    """
    chunks = chunk_text(body)
    if infobox:
        chunks.append("[INFOBOX]\n" + infobox)
    all_records = []
    for i, chunk in enumerate(chunks, 1):
        records, ok = _country_chunk(api_key, canonical, home_country, lead,
                                     chunk, i, len(chunks), MAX_CHUNK_WORDS)
        all_records.extend(records)
        if not ok:
            # A pass-2 chunk that fails even after cap escalation AND sub-
            # splitting drops its footprints. Don't hide it: surface it in the
            # run log (mirrors step 2) rather than silently yielding a profile
            # with missing country_links that looks complete.
            print(f"  [{canonical}] country chunk {i}/{len(chunks)} failed after retries")
    return union_country_records(all_records)


def _country_chunk(api_key, canonical, home_country, lead, chunk, idx, total, max_words):
    """Extract country footprints from ONE passage; returns (records, ok).

    If the call fails after retries — typically a country-dense passage whose
    JSON output truncates even at the escalated cap — halve the passage and
    recurse so each part fits. Mirrors step 2's extract_chunk. Normal passages
    succeed on the first call and never split, so this adds no cost for the
    overwhelming majority that already fit."""
    prompt = (
        f"ORGANIZATION: {canonical}\n"
        f"HOME COUNTRY (never emit this as a footprint): {home_country}\n\n"
        f"--- ABOUT THIS ORGANIZATION (context only, do NOT extract "
        f"countries from here) ---\n{lead}\n--- END CONTEXT ---\n\n"
        f"PASSAGE {idx}/{total} (extract countries ONLY from this "
        f"passage, with a verbatim quote from it):\n\n"
        f"--- PASSAGE ---\n{chunk}\n--- END ---"
    )
    rec = call_api(api_key, SYSTEM_COUNTRIES, prompt,
                   max_tokens=COUNTRY_MAX_TOKENS,
                   max_tokens_retry=COUNTRY_MAX_TOKENS_RETRY)
    time.sleep(DELAY)
    if isinstance(rec, dict):
        return coerce_country_records(rec.get("country_links")), True
    # Persistent failure (usually a country-dense passage truncating even at
    # the escalated cap). Sub-split if still worth halving.
    if max_words <= 400 or len(chunk.split()) <= 400:
        return [], False
    sub_max = max(400, max_words // 2)
    subs = chunk_text(chunk, max_words=sub_max)
    if len(subs) <= 1:
        return [], False  # couldn't divide further
    print(f"  [{canonical}] sub-splitting a truncating country chunk → "
          f"{len(subs)} part(s) (≤{sub_max} words)")
    records, ok_all = [], True
    for sub in subs:
        r2, ok = _country_chunk(api_key, canonical, home_country, lead,
                                sub, idx, total, sub_max)
        records += r2
        ok_all = ok_all and ok
    return records, ok_all


def process_folder(folder, api_key, idx, total):
    """Returns 'org', 'not_org', or 'fail'. Writes org_profile.json (a real
    profile, or {"canonical_name": null}); writes nothing on API failure so
    the folder is retried."""
    name = folder.name
    content_path = folder / "content.txt"
    url_path = folder / "url.txt"
    profile_path = folder / "org_profile.json"

    if not content_path.exists():
        print(f"[{idx}/{total}] {name} - no content.txt")
        return "fail"

    text = content_path.read_text("utf-8").strip()
    url = url_path.read_text("utf-8").strip() if url_path.exists() else ""

    def write_not_org():
        profile_path.write_text(
            json.dumps({"canonical_name": None}, ensure_ascii=False, indent=2),
            encoding="utf-8")

    if not text:
        write_not_org()
        print(f"[{idx}/{total}] {name} - empty, not an org")
        return "not_org"

    body, infobox = split_body_and_infobox(text)
    lead = lead_paragraphs(body)

    p1_prompt = (
        f"ARTICLE TITLE: {name.split('_', 1)[-1].replace('_', ' ')}\n"
        f"SOURCE: {url}\n\n"
        f"--- TEXT ---\n{lead}\n--- END TEXT ---\n\n"
        f"--- INFOBOX ---\n{infobox or '(none)'}\n--- END INFOBOX ---"
    )
    rec = call_api(api_key, SYSTEM_PROFILE, p1_prompt)
    time.sleep(DELAY)
    if rec is None:
        print(f"[{idx}/{total}] {name} x pass 1 API failed")
        return "fail"

    profile = coerce_profile(rec, url, name)
    if profile is None:
        write_not_org()
        print(f"[{idx}/{total}] {name} - not an org article")
        return "not_org"

    profile["country_links"] = extract_countries(
        api_key, profile["canonical_name"], profile["country"], lead, body, infobox)

    profile_path.write_text(
        json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    flag = " [DEFUNCT]" if profile["is_defunct"] is True else ""
    n_c = len(profile["country_links"])
    print(f"[{idx}/{total}] {name} ok {profile['canonical_name']} "
          f"({profile['country']}, {n_c} links){flag}")
    return "org"


# -- main ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", "-d", default="./txts")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Re-profile every folder, ignoring existing "
                             "org_profile.json files")
    parser.add_argument("--workers", "-w", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    api_key = load_key()
    root = Path(args.dir)
    all_folders = sorted(f for f in root.iterdir() if f.is_dir())
    total = len(all_folders)

    to_process, skip = [], 0
    for folder in all_folders:
        if not args.force and (folder / "org_profile.json").exists():
            skip += 1
            continue
        to_process.append(folder)

    print(f"Found {total} folders | {skip} skipped | "
          f"{len(to_process)} to process | {args.workers} workers")
    if not to_process:
        return

    folder_idx = {f: i + 1 for i, f in enumerate(all_folders)}
    org, not_org, fail = 0, 0, 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_folder, f, api_key, folder_idx[f], total): f
                   for f in to_process}
        for fut in as_completed(futures):
            try:
                status = fut.result()
                if status == "org":          org += 1
                elif status == "not_org":    not_org += 1
                elif status == "fail":       fail += 1
            except Exception as e:
                fail += 1
                print(f"  worker crashed: {e}")

    print("=" * 60)
    print(f"Done: {org} org profiles written, {not_org} non-org articles, "
          f"{fail} failed")
    if fail:
        print(f"! {fail} folder(s) failed. Re-run to retry.")


if __name__ == "__main__":
    main()