"""Shared pipeline helpers used by more than one step.

These were previously copy-pasted into 2_extract_network.py and
3_enrich_nodes.py and had begun to drift; centralizing them keeps the two
LLM steps chunking identically and loading the key the same way.

  * load_key()               — read DEEPSEEK_API_KEY from the environment.
  * split_body_and_infobox() — peel step 1's "--- INFOBOX ---" trailer off.
  * chunk_text()             — greedy, paragraph-aware chunking that also
                               sub-splits any single oversize block (e.g. a
                               long bulleted list, whose items step 1 joins
                               with single newlines) so no chunk ever blows
                               past max_words.
"""

import os
import re

MAX_CHUNK_WORDS = 2500


def load_key():
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError(
            "DeepSeek API key not found. Set it with:\n"
            '    export DEEPSEEK_API_KEY="sk-..."'
        )
    return key.strip()


def split_body_and_infobox(text):
    """Split content.txt into (body, infobox).

    Step 1 appends the infobox as a labeled trailer. We pull it out before
    chunking so (a) the infobox is never split across chunks and (b) every
    chunk's prompt can include it as one more source of relationships.
    """
    marker = "--- INFOBOX ---"
    idx = text.rfind(marker)
    if idx == -1:
        return text, ""
    return text[:idx].rstrip(), text[idx + len(marker):].strip()


def _split_oversize(block, max_words):
    """Split a single oversize block (>max_words) into <=max_words pieces.

    Prefers line boundaries first (step 1 emits list items separated by a
    single newline, so a long list is one block here), then sentence
    boundaries if the block is a single huge line, then a hard word cut as a
    last resort. This stops a long member/affiliate list from arriving at the
    LLM as one giant chunk that risks output truncation and inflates cost.
    """
    units = [ln.strip() for ln in block.split("\n") if ln.strip()]
    if len(units) <= 1:  # one long line → fall back to sentence units
        units = [s.strip() for s in re.split(r"(?<=[.!?])\s+", block) if s.strip()]

    pieces, buf, wc = [], [], 0
    for u in units:
        uw = len(u.split())
        if uw > max_words:
            # A single unit is still too big: flush, then hard-split by words.
            if buf:
                pieces.append("\n".join(buf)); buf, wc = [], 0
            words = u.split()
            for j in range(0, len(words), max_words):
                pieces.append(" ".join(words[j:j + max_words]))
            continue
        if wc + uw > max_words and buf:
            pieces.append("\n".join(buf)); buf, wc = [], 0
        buf.append(u); wc += uw
    if buf:
        pieces.append("\n".join(buf))
    return pieces


def chunk_text(text, max_words=MAX_CHUNK_WORDS):
    """Greedy paragraph-aware chunking. A paragraph longer than max_words is
    sub-split (see _split_oversize) instead of passing through whole."""
    chunks, buf, wc = [], [], 0
    for p in re.split(r"\n{2,}", text):
        p = p.strip()
        if not p:
            continue
        pw = len(p.split())
        if pw > max_words:
            if buf:
                chunks.append("\n\n".join(buf)); buf, wc = [], 0
            chunks.extend(_split_oversize(p, max_words))
            continue
        if wc + pw > max_words and buf:
            chunks.append("\n\n".join(buf)); buf, wc = [], 0
        buf.append(p); wc += pw
    if buf:
        chunks.append("\n\n".join(buf))
    return chunks
