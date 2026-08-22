#!/usr/bin/env python3
"""Pre-render the CRIMENET web app: static, deep-linkable, crawlable pages.

This is the whole front end — an A-Z browse hub and a sitemap — so search
engines can index it and every org/country has its own linkable URL (rendered
client-side from data/compact.json). Each page also carries an in-browser connection
finder (trace how two orgs link, directly or through one shared org) that routes
over evidence from the ``evidence/`` shards and per-pair relationship
summaries from the ``relationship_summaries/`` shards.

Output (all under ``--output``, default ``app/``, which is the deployed folder):

    app/data/compact.json          compact org metadata + country listings
    app/data/crimenet_adj.json     adjacency list for connection finder
    app/data/crimenet.json         full dataset (runtime load for maps/graphs)
    app/index.html                A-Z index of every page (the crawl hub for humans + bots)
    app/static_pages.css           shared stylesheet for all generated pages
    app/sitemap.xml                every generated URL, for search-engine submission

Pages cross-link densely (org -> neighbor orgs, org <-> country) so the whole
set forms one connected web with no dead ends — which is what makes it
discoverable.

Depends on the ``evidence/`` shards and ``relationship_summaries/`` shards being built first
(build_evidence_shards.py + build_relationship_summaries.py) — the connection finder reads them.

Usage:
    python build_static_pages.py --input ../data/crimenet.json --output ../app \
        --base-url https://www.alvarofrancomartins.com/crimenet
"""
from __future__ import annotations

import argparse
import html
import json
import re
import shutil
import unicodedata
from collections import defaultdict
from datetime import date

# Clean up <br> → ", " substitution artifacts from step 1's infobox extraction.
_FIX_ARTIFACTS = re.compile(r"\s+,\s+")
_FIX_ARTIFACTS_REPL = ", "
_FIX_COLON = re.compile(r"\s+:")
_FIX_COLON_REPL = ":"
_FIX_COLON_COMMA = re.compile(r":,\s*")
_FIX_COLON_COMMA_REPL = ": "
from pathlib import Path

# Note: country data is already validated/normalized upstream in step 4 (via
# pipeline/lib/countries.py), so crimenet.json contains only real, canonical
# countries. This generator just consumes it — no country filtering here.


# ── slugs ────────────────────────────────────────────────────────────────────

def slugify(name: str) -> str:
    """A URL-safe, accent-folded slug. Falls back to 'org' if nothing survives."""
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "org"


def build_slug_map(names: list[str]) -> dict[str, str]:
    """name -> unique slug. Deterministic: names are sorted, collisions get -2, -3…"""
    out: dict[str, str] = {}
    used: set[str] = set()
    for name in sorted(names):
        base = slugify(name)
        slug = base
        i = 2
        while slug in used:
            slug = f"{base}-{i}"
            i += 1
        used.add(slug)
        out[name] = slug
    return out


# ── small HTML helpers ───────────────────────────────────────────────────────

def esc(s) -> str:
    return html.escape(str(s if s is not None else ""), quote=True)


def truncate(s: str, n: int = 155) -> str:
    s = " ".join(str(s or "").split())
    return s if len(s) <= n else s[: n - 1].rstrip() + "…"


def title_from_url(u: str) -> str:
    m = re.search(r"[?&]title=([^&]+)", u)
    raw = m.group(1) if m else u.rstrip("/").split("/")[-1]
    try:
        raw = re.sub(r"&oldid=.*$", "", raw)
        from urllib.parse import unquote
        raw = unquote(raw)
    except Exception:
        pass
    return raw.replace("_", " ") or u


def page_head(title: str, description: str, canonical: str, css_href: str,
              wrap_class: str = "wrap") -> str:
    """Shared <head>: title, meta description, canonical, Open Graph, stylesheet."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{esc(title)}</title>
<link rel="icon" type="image/svg+xml" href="favicon.svg">
<meta name="description" content="{esc(description)}">
<link rel="canonical" href="{esc(canonical)}">
<meta name="robots" content="index, follow">
<meta property="og:type" content="article">
<meta property="og:title" content="{esc(title)}">
<meta property="og:description" content="{esc(description)}">
<meta property="og:url" content="{esc(canonical)}">
<meta name="twitter:card" content="summary_large_image">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Oswald:wght@500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="{esc(css_href)}">
</head>
<body>
<div class="{esc(wrap_class)}">
"""


PAGE_FOOT = """</div>
</body>
</html>
"""


# ── connection finder ────────────────────────────────────────────





# ── connection rendering ─────────────────────────────────────────────────────



# ── page builders ────────────────────────────────────────────────────────────

def status_label(is_defunct) -> tuple[str, str]:
    if is_defunct is True:
        return ("Defunct", "defunct")
    if is_defunct is False:
        return ("Active", "active")
    return ("Status unknown", "unknown")







def build_browse_page(org_names: list[str], country_names: list[str], degree: dict[str, int],
                      country_total: dict[str, int], by_name: dict[str, dict],
                      org_slug: dict[str, str], country_slug: dict[str, str],
                      base_url: str, n_articles: int) -> str:
    canonical = f"{base_url}/index.html"
    title = "Browse all organizations & countries | CRIMENET"
    meta_desc = truncate(
        f"Browse {len(org_names)} criminal organizations and {len(country_names)} countries in "
        "CRIMENET, a Knowledge Graph of global organized crime sourced from Wikipedia.")

    out = [page_head(title, meta_desc, canonical, "css/main.css",
                     wrap_class="wrap wide browse-wrap")]

    out.append(
        '<div class="browse-head">'
        '<div class="head-col-a">'
        '<h1>CRIMENET</h1>'
        '<nav class="head-nav" aria-label="Primary">'
        '<a class="nav-btn" href="about.html">About'
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/>'
        '</svg></a>'
        '<a class="nav-btn" href="browse.html">Browse the network'
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/>'
        '</svg></a>'
        '<a class="nav-btn nav-ai" href="ask.html">CRIMENET AI'
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/>'
        '</svg></a>'
        '</nav>'
        '</div>'
        '</div>')

    out.append('<div id="tab-browse">')
    out.append('<div class="browse-cols">')

    # ── Left panel: toggleable Organizations / Countries.
    # Rows are clickable <span> elements (not <a href>); clicking renders
    # the org or country detail into the right panel.
    # Country name list is populated server-side so the initial page is
    # ready before compact.json loads.
    out.append('<section class="browse-panel panel-browse">')
    out.append('<header class="panel-head">'
               '<div class="panel-head-row">'
               f'<h2 id="browse-panel-title">Organizations</h2>'
               f'<span class="panel-count" id="browse-panel-count">{len(org_names):,}</span></div>'
               '<p class="panel-sub" id="browse-panel-sub">Click any name for full profile &amp; sourced connections</p>'
               '<div class="browse-toggle">'
               '<button class="browse-toggle-btn active" data-view="orgs">Organizations</button>'
               '<button class="browse-toggle-btn" data-view="countries">Countries</button>'
               '</div>'
               '<input type="search" id="org-search" class="panel-search" '
               'placeholder="Search organizations…" autocomplete="off" spellcheck="false" '
               'aria-label="Search organizations">'
               '<input type="search" id="country-search" class="panel-search" '
               'placeholder="Search countries…" autocomplete="off" spellcheck="false" '
               'aria-label="Search countries" style="display:none">'
               '</header>')
    # Org rows: clickable spans with data-name attribute
    org_rows_html = "".join(
        f'<span class="bo" data-name="{esc(n)}">'
        f'<span class="bo-name">{esc(n)}</span>'
        f'<span class="bo-meta">'
        f'<span class="bo-dot status-{status_label(by_name.get(n, {}).get("is_defunct"))[1]}" '
        f'title="{esc(status_label(by_name.get(n, {}).get("is_defunct"))[0])}"></span>'
        f'<span class="bo-deg"><b>{degree.get(n, 0):,}</b> linkages</span>'
        f'</span></span>'
        for n in sorted(org_names, key=lambda n: (-degree.get(n, 0), n.lower())))
    cl = "".join(
        f'<li class="cn" data-country="{esc(c)}">'
        f'<span class="cn-name">{esc(c)}</span>'
        f'<span class="deg">{country_total.get(c, 0)}</span></li>'
        for c in sorted(country_names, key=lambda c: (-country_total.get(c, 0), c.lower())))
    out.append(f'<div class="panel-scroll">'
               f'<div class="org-list" id="org-list">{org_rows_html}</div>'
               f'<ul class="country-list" id="country-list" style="display:none">{cl}</ul>'
               f'</div>')
    out.append('<div class="panel-foot">'
               '<button type="button" class="load-more" id="load-more">'
               'Load more</button>'
               '<button type="button" class="load-more" id="load-more-c" style="display:none">'
               'Load more</button>'
               '</div>')
    out.append('</section>')

    # ── Right panel: empty initial state ──
    out.append('<section class="browse-panel panel-detail" id="panel-detail">'
               '<header class="panel-head">'
               '<div class="panel-head-row">'
               '<button type="button" class="detail-back" id="detail-back" aria-label="Back to list">'
               '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="15 18 9 12 15 6"/></svg>'
               '<span>Back</span></button>'
               '<h2 id="detail-panel-title">Details</h2>'
               '</div>'
               '<p class="panel-sub" id="detail-panel-sub">'
               'Select an organization or country to see its details</p>'
               '</header>'
               '<div class="panel-scroll" id="detail-scroll">'
               '<div class="rp-empty" id="detail-empty">'
               'Select an organization or country from the left panel.</div>'
               '<div id="detail-content" style="display:none"></div>'
               '</div>'
               '</section>')

    out.append('</div>')  # .browse-cols
    out.append('</div>')  # #tab-browse

    # ── Inline scripts ────────────────────────────────────────────────────

    # Script 1: Toggle, pagination, click handlers, detail renderer
    out.append(f"""<script>
(function(){{
  var orgCount = {len(org_names)};
  var countryCount = {len(country_names)};

  // ── Globals ───────────────────────────────────────────────────
  var compact = null;  // loaded compact.json

  // ── Helpers ───────────────────────────────────────────────────
  function esc(s){{return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}}
  function titleFromUrl(url){{if(!url)return'Wikipedia';try{{var u=new URL(url),m=u.searchParams.get('title');if(m)return decodeURIComponent(m.replace(/_/g,' '));var p=u.pathname.split('/wiki/')[1];if(p)return decodeURIComponent(p.split('?')[0].split('#')[0].replace(/_/g,' '));}}catch(e){{}}return'Wikipedia';}}

  function statusLabel(defunct){{
    if(defunct===true)return{{text:'Defunct',cls:'defunct'}};
    if(defunct===false)return{{text:'Active',cls:'active'}};
    return {{text:'Status unknown',cls:'unknown'}};
  }}

  function plural(n,w){{return n+' '+w+(n===1?'':'s');}}

  // ── Load compact.json ─────────────────────────────────────────
  function loadCompact(){{
    if(compact)return Promise.resolve(compact);
    return fetch('data/compact.json').then(function(r){{if(!r.ok)throw new Error('compact.json');return r.json();}}).then(function(d){{compact=d;return d;}});
  }}

  // Mobile master-detail: swap the list for the detail panel and scroll it into view.
  function openDetail(){{
    var w=document.querySelector('.browse-wrap');
    if(w)w.classList.add('show-detail');
    if(window.matchMedia('(max-width:820px)').matches){{
      var d=document.getElementById('panel-detail');
      if(d)setTimeout(function(){{d.scrollIntoView({{block:'start'}});}},30);
    }}
  }}

  // ── Render org detail ─────────────────────────────────────────
  function renderOrg(name){{
    var o=compact.orgs[name];
    if(!o)return;
    var scroll=document.getElementById('detail-scroll');
    var empty=document.getElementById('detail-empty');
    var content=document.getElementById('detail-content');
    var title=document.getElementById('detail-panel-title');
    var sub=document.getElementById('detail-panel-sub');

    title.textContent=name;
    sub.textContent='';

    // Highlight selected org row
    document.querySelectorAll('.bo.selected').forEach(function(r){{r.classList.remove('selected');}});
    var row=document.querySelector('.bo[data-name="'+CSS.escape(name)+'"]');
    if(row)row.classList.add('selected');

    var html='<div style="padding:18px 22px">';

    // ── Aliases ──
    var aliases=(o.aliases||[]).filter(function(a){{return a!==name;}});
    if(aliases.length){{
      html+='<div class="org-title-line"><h1>'+esc(name)+'</h1><span class="org-aka">Also known as: '+esc(aliases.join(', '))+'</span></div>';
    }}else{{
      html+='<h1>'+esc(name)+'</h1>';
    }}

    // ── org-meta ──
    var meta=[];
    if(o.country)meta.push('<span>Based in '+esc(o.country)+'</span>');
    var st=statusLabel(o.is_defunct);
    meta.push('<span class="org-status-'+st.cls+'"><span class="org-dot"></span>'+esc(st.text)+'</span>');
    if(o.time_period)meta.push('<span>'+esc(o.time_period)+'</span>');
    else if(o.founded_year)meta.push('<span>'+esc(o.founded_year)+(o.dissolved_year?'–'+esc(o.dissolved_year):'–present')+'</span>');
    var srcs=(o.own_sources||[]).slice();
    var srcLabel='Sources';
    if(!srcs.length){{srcs=(o.mentioned_in||[]).slice(0,5);srcLabel='Mentioned in';}}
    if(srcs.length){{
      var pills=srcs.map(function(s){{return'<a class="src-pill" href="'+esc(s.url)+'" target="_blank" rel="noopener nofollow">'+esc(s.title||titleFromUrl(s.url))+'</a>';}}).join('');
      meta.push('<span class="org-meta-srcs"><span class="org-meta-srcs-lbl">'+srcLabel+'</span>'+pills+'</span>');
    }}
    var orgMeta='<div class="org-meta">'+meta.join('')+'</div>';

    if(o.description&&o.profiled){{
      // ── desc-card ──
      var fpCount=(o.country_links||[]).length;
      var countryWord='country';
      if(fpCount===1)countryWord='country';else countryWord='countries';
      var clauses=[];
      if(fpCount)clauses.push('footprints in '+fpCount+' '+countryWord);
      var statsHtml='';
      if(clauses.length){{
        statsHtml='<p class="desc-stats">CRIMENET has extracted <b class="c-all">'+o.degree.toLocaleString()+' linkages</b> for this organization, including '+clauses.join(', ')+'.</p>';
      }}else{{
        statsHtml='<p class="desc-stats">CRIMENET has extracted <b class="c-all">'+o.degree.toLocaleString()+' linkages</b> for this organization.</p>';
      }}
      html+='<div class="desc-card">'+orgMeta+'<p class="desc">'+esc(o.description)+'</p>'+statsHtml+'</div>';
    }}else if(!o.profiled){{
      html+=orgMeta+'<p class="note">This organization is referenced in other articles but does not have its own dedicated profile.</p>';
    }}

    // ── Footprints abroad ──
    var fps=(o.country_links||[]).filter(function(fp){{
      var c=((fp.country||'')+'').trim();
      return c && c!==o.country;
    }});
    if(fps.length){{
      var fpRows=[];
      fps.forEach(function(fp,i){{
        var c=((fp.country||'')+'').trim();
        var ctx=(fp.context||'').trim();
        var quote=(fp.evidence_quote||'').trim();
        var ctxHtml=ctx?'<div class="fp-context">'+esc(ctx)+'</div>':'';
        var qid='ofpq-'+i;
        // Per-footprint source — each quote names its own source article
        var fpSrc='';
        if(quote){{
          var su=fp.source_url||'';
          var st=fp.source_title||(su?titleFromUrl(su):'');
          if(su) fpSrc='<div class="ev-srcs"><span class="ev-srcs-label">Source:</span> <a href="'+esc(su)+'" target="_blank" rel="noopener nofollow">'+esc(st)+'</a></div>';
        }}
        var qPill=quote?'<button type="button" class="ev-toggle q" data-target="'+qid+'"><span>Quote</span><span class="chev">▾</span></button>':'';
        var qReveal=quote?'<div class="fp-reveal" id="'+qid+'" hidden><blockquote class="ev-quote">'+esc(quote)+'</blockquote>'+fpSrc+'</div>':'';
        var pillRow=qPill?'<div class="ev-toggles">'+qPill+'</div>':'';
        var body=ctx||quote?ctxHtml+pillRow+qReveal:'<div class="ev-none">No evidence text was recorded for this link.</div>';
        fpRows.push('<div class="fp-row"><div class="fp-rank">'+(i+1)+'</div><div class="fp-main"><div class="fp-name">'+esc(c)+'</div>'+body+'</div></div>');
      }});
      html+='<div style="margin-top:24px"><section class="cc-panel"><h2 class="section-h">Footprint abroad <span class="count">'+fps.length+'</span></h2><div class="fp-list" id="org-fp-list">'+fpRows.join('')+'</div></section></div>';
    }}

    html+='</div>';
    content.innerHTML=html;
    empty.style.display='none';
    content.style.display='';

    // Wire footprint pagination + quote toggles
    wireFpPaginate('org-fp-list','.fp-row',10);
    wireFpQuotes('org-fp-list');

    // Scroll detail to top
    scroll.scrollTop=0;
    openDetail();
  }}

  // ── Render country detail ─────────────────────────────────────
  function renderCountry(name){{
    var c=compact.countries[name];
    if(!c)return;
    var scroll=document.getElementById('detail-scroll');
    var empty=document.getElementById('detail-empty');
    var content=document.getElementById('detail-content');
    var title=document.getElementById('detail-panel-title');
    var sub=document.getElementById('detail-panel-sub');

    title.textContent=name;
    sub.textContent='';

    // Highlight selected country row
    document.querySelectorAll('.cn.selected').forEach(function(r){{r.classList.remove('selected');}});
    var row=document.querySelector('.cn[data-country="'+CSS.escape(name)+'"]');
    if(row)row.classList.add('selected');

    var html='<div style="padding:18px 22px">';
    html+='<h1>Organized crime in '+esc(name)+'</h1>';

    html+='<div class="stats">';
    html+='<div class="stat"><b>'+c.based_here.length.toLocaleString()+'</b><span>based here</span></div>';
    html+='<div class="stat"><b>'+c.footprints_here.length.toLocaleString()+'</b><span>foreign footprints</span></div>';
    html+='</div>';

    // Based here
    var basedRows='';
    if(c.based_here.length){{
      c.based_here.forEach(function(nm){{
        var o=compact.orgs[nm]||{{}};
        var st=statusLabel(o.is_defunct);
        basedRows+='<span class="bo" data-name="'+esc(nm)+'" style="cursor:pointer"><span class="bo-name">'+esc(nm)+'</span><span class="bo-meta"><span class="bo-dot status-'+st.cls+'" title="'+esc(st.text)+'"></span><span class="bo-deg"><b>'+o.degree.toLocaleString()+'</b> linkages</span></span></span>';
      }});
    }}

    // Foreign footprints
    var fpRows='';
    if(c.footprints_here.length){{
      c.footprints_here.forEach(function(fp,i){{
        var nm=fp.org;
        var o=compact.orgs[nm]||{{}};
        var ctx=(fp.context||'').trim();
        var quote=(fp.quote||'').trim();
        var ctxHtml=ctx?'<div class="fp-context">'+esc(ctx)+'</div>':'';
        var originHtml='';
        if(fp.origin)originHtml='<div class="fp-origin">based in '+esc(fp.origin)+'</div>';
        var qid='fpq-'+i;
        // Per-footprint source — each quote names its own source article
        var fpSrc='';
        if(quote){{
          var su=fp.source_url||'';
          var st=fp.source_title||(su?titleFromUrl(su):'');
          if(su) fpSrc='<div class="ev-srcs"><span class="ev-srcs-label">Source:</span> <a href="'+esc(su)+'" target="_blank" rel="noopener nofollow">'+esc(st)+'</a></div>';
        }}
        var qPill=quote?'<button type="button" class="ev-toggle q" data-target="'+qid+'"><span>Quote</span><span class="chev">▾</span></button>':'';
        var qReveal=quote?'<div class="fp-reveal" id="'+qid+'" hidden><blockquote class="ev-quote">'+esc(quote)+'</blockquote>'+fpSrc+'</div>':'';
        var pillRow=qPill?'<div class="ev-toggles">'+qPill+'</div>':'';
        var body=ctx||quote?ctxHtml+pillRow+qReveal:'<div class="ev-none">No evidence text was recorded for this link.</div>';
        fpRows+='<div class="fp-row"><div class="fp-rank">'+(i+1)+'</div><div class="fp-main"><div class="fp-name">'+esc(nm)+'</div>'+originHtml+body+'</div></div>';
      }});
    }}

    var both=basedRows&&fpRows;
    if(both){{
      html+='<div class="country-cols">';
      html+='<section class="cc-panel"><h2 class="section-h">Based in '+esc(name)+' <span class="count">'+c.based_here.length+'</span></h2><div class="org-list" id="country-based-list">'+basedRows+'</div></section>';
      html+='<section class="cc-panel"><h2 class="section-h">Foreign groups with a footprint in '+esc(name)+' <span class="count">'+c.footprints_here.length+'</span></h2><div class="fp-list" id="country-fp-list">'+fpRows+'</div></section>';
      html+='</div>';
    }}else if(basedRows){{
      html+='<section class="cc-panel"><h2 class="section-h">Based in '+esc(name)+' <span class="count">'+c.based_here.length+'</span></h2><div class="org-list" id="country-based-list">'+basedRows+'</div></section>';
    }}else if(fpRows){{
      html+='<section class="cc-panel"><h2 class="section-h">Foreign groups with a footprint in '+esc(name)+' <span class="count">'+c.footprints_here.length+'</span></h2><div class="fp-list" id="country-fp-list">'+fpRows+'</div></section>';
    }}

    html+='</div>';
    content.innerHTML=html;
    empty.style.display='none';
    content.style.display='';

    // Wire based-here org list pagination + footprint pagination + quote toggles
    if(basedRows) wireFpPaginate('country-based-list','.bo',50);
    if(fpRows) wireFpPaginate('country-fp-list','.fp-row',15);
    wireFpQuotes('country-fp-list');

    // Wire clickable org rows inside country detail
    content.querySelectorAll('.bo[data-name]').forEach(function(el){{
      el.addEventListener('click',function(){{
        document.querySelector('.browse-toggle-btn[data-view="orgs"]').click();
        renderOrg(this.dataset.name);
      }});
    }});

    scroll.scrollTop=0;
    openDetail();
  }}

  // ── Footprint helpers ─────────────────────────────────────────
  function wireFpPaginate(listId,rowSel,batch){{
    var list=document.getElementById(listId);
    if(!list)return;
    var rows=[].slice.call(list.querySelectorAll(rowSel));
    if(rows.length<=batch)return;
    // Show first batch, add Load more button
    for(var i=batch;i<rows.length;i++)rows[i].hidden=true;
    var btn=document.createElement('button');
    btn.type='button';
    btn.className='load-more';
    btn.style.marginTop='12px';
    btn.textContent='Load more ('+(rows.length-batch)+' remaining)';
    btn.addEventListener('click',function(){{
      var visible=rows.filter(function(r){{return !r.hidden;}}).length;
      var next=Math.min(visible+batch,rows.length);
      for(var j=visible;j<next;j++)rows[j].hidden=false;
      if(next>=rows.length)btn.style.display='none';
      else btn.textContent='Load more ('+(rows.length-next)+' remaining)';
    }});
    if(list.parentNode)list.parentNode.appendChild(btn);
  }}

  function wireFpQuotes(listId){{
    var list=document.getElementById(listId);
    if(!list)return;
    list.addEventListener('click',function(ev){{
      var b=ev.target.closest('.ev-toggle');if(!b)return;
      var t=document.getElementById(b.dataset.target);if(!t)return;
      var opening=t.hasAttribute('hidden');
      if(opening)t.removeAttribute('hidden');else t.setAttribute('hidden','');
      b.classList.toggle('open',opening);
    }});
  }}

  // ── Left panel toggle ─────────────────────────────────────────
  document.querySelectorAll('.panel-browse .browse-toggle-btn').forEach(function(btn){{
    btn.addEventListener('click',function(){{
      var view=this.dataset.view;
      document.querySelectorAll('.panel-browse .browse-toggle-btn').forEach(function(b){{b.classList.remove('active');}});
      this.classList.add('active');
      var showOrgs=view==='orgs';
      document.getElementById('org-list').style.display=showOrgs?'':'none';
      document.getElementById('org-search').style.display=showOrgs?'':'none';
      document.getElementById('load-more').style.display=showOrgs?'':'none';
      document.getElementById('country-list').style.display=showOrgs?'none':'';
      document.getElementById('country-search').style.display=showOrgs?'none':'';
      document.getElementById('load-more-c').style.display=showOrgs?'none':'';
      document.getElementById('browse-panel-title').textContent=showOrgs?'Organizations':'Countries';
      document.getElementById('browse-panel-count').textContent=showOrgs?orgCount.toLocaleString():countryCount.toLocaleString();
      document.getElementById('browse-panel-sub').textContent=showOrgs?'Click any name for full profile & sourced connections':'Click to see local groups & foreign footprints';
    }});
  }});

  // ── Click handlers for org/country rows ──────────────────────
  document.getElementById('org-list').addEventListener('click',function(ev){{
    var row=ev.target.closest('.bo[data-name]');
    if(!row)return;
    loadCompact().then(function(){{renderOrg(row.dataset.name);}});
  }});
  document.getElementById('country-list').addEventListener('click',function(ev){{
    var row=ev.target.closest('.cn[data-country]');
    if(!row)return;
    loadCompact().then(function(){{renderCountry(row.dataset.country);}});
  }});

  // ── Back to list (mobile) ────────────────────────────────────
  document.getElementById('detail-back').addEventListener('click',function(){{
    var w=document.querySelector('.browse-wrap');
    if(w)w.classList.remove('show-detail');
  }});

  // ── Deep link (?org=<name>) ──────────────────────────────────
  (function(){{
    var name=null;
    try{{name=new URLSearchParams(location.search).get('org');}}catch(e){{}}
    if(name){{
      var orgBtn=document.querySelector('.panel-browse .browse-toggle-btn[data-view="orgs"]');
      if(orgBtn)orgBtn.click();
      loadCompact().then(function(){{renderOrg(name);}});
    }}
  }})();

  // ── Pagination ────────────────────────────────────────────────
  function paginate(listId,rowSel,btnId,batch,searchId){{
    var list=document.getElementById(listId),btn=document.getElementById(btnId);
    if(!list||!btn)return;
    var rows=[].slice.call(list.querySelectorAll(rowSel)),shown=0,q='';
    function matches(){{return q?rows.filter(function(r){{return r.textContent.toLowerCase().indexOf(q)!==-1;}}):rows;}}
    function render(){{
      var m=matches();
      for(var i=0;i<rows.length;i++)rows[i].hidden=true;
      for(var j=0;j<m.length&&j<shown;j++)m[j].hidden=false;
      if(shown>=m.length){{btn.style.display='none';}}
      else{{btn.style.display='';btn.textContent='Load more ('+(m.length-shown)+' remaining)';}}
    }}
    shown=Math.min(batch,rows.length);render();
    btn.addEventListener('click',function(){{shown=Math.min(shown+batch,matches().length);render();}});
    if(searchId){{
      var box=document.getElementById(searchId);
      if(box)box.addEventListener('input',function(){{q=box.value.trim().toLowerCase();shown=batch;render();}});
    }}
  }}
  paginate('org-list','.bo','load-more',__ORG_BATCH__,'org-search');
  paginate('country-list','.cn','load-more-c',__CTRY_BATCH__,'country-search');
  document.getElementById('load-more-c').style.display='none';
}})();
</script>""".replace("__ORG_BATCH__", "50").replace("__CTRY_BATCH__", "40"))

    out.append(PAGE_FOOT)
    return "".join(out)


def build_sitemap(urls: list[str], lastmod: str) -> str:
    body = "".join(
        f"<url><loc>{esc(u)}</loc><lastmod>{lastmod}</lastmod></url>" for u in urls)
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            f"{body}</urlset>")


# ── CSS ──────────────────────────────────────────────────────────────────────

CSS = """/* Shared styles for the CRIMENET static pages.
   Relationship accent is carried in --rc, set per .rel-<rel> section so headers,
   card borders, and quote rules all share one color. Default is amber. */
:root{
  --bg:#07080a; --s1:#0d1117; --s2:#111820; --s3:#161e2c;
  --b1:#1a2535; --b2:#243347; --b3:#2e4060;
  --amber:#e8a020; --amber2:#b87818;
  --green:#2db87d; --red:#d63b3b;
  --fam-coop:#5eead4; --fam-hostile:#fb7185; --fam-loose:#818cf8;
  --text-bright:#ffffff; --text-main:#cbd5e1; --text-muted:#64748b; --text2:#56708a; --text3:#3a5060;
  --body:'Inter',-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
  --disp:'Oswald',var(--body);
  --rc:var(--amber);
  /* document/source glyph for source buttons */
  --wiki-ico:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z'/%3E%3Cpolyline points='14 2 14 8 20 8'/%3E%3Cline x1='8' y1='13' x2='16' y2='13'/%3E%3Cline x1='8' y1='17' x2='16' y2='17'/%3E%3C/svg%3E");
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
/* the `hidden` attribute must win over component display rules (e.g. .bo{display:flex})
   — the org/country lists toggle row visibility via [hidden] for search + Load more. */
[hidden]{display:none !important}
body{font-family:var(--body);color:var(--text-main);line-height:1.6;
  background:var(--bg);
  background-image:radial-gradient(1100px 700px at 78% -12%,rgba(232,160,32,.06),transparent),
    radial-gradient(900px 700px at -12% 112%,rgba(94,234,212,.045),transparent);}
a{color:var(--amber);text-decoration:none}
a:hover{text-decoration:underline}
.wrap{max-width:900px;margin:0 auto;padding:30px 22px 90px}
.wrap.wide{max-width:100%;padding:30px 16px 30px 22px;margin-left:0;margin-right:auto}
.crumb{font-size:16px;color:var(--text-muted);margin-bottom:22px;letter-spacing:.3px}
.crumb a{color:var(--text2)}
.crumb span{color:var(--text-muted)}
h1{font-family:var(--disp);font-weight:600;font-size:48px;letter-spacing:.5px;
  color:var(--text-bright);line-height:1.08;margin-bottom:14px}
.facts{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px}
.fact{font-size:12.5px;color:var(--text-main);background:var(--s2);
  border:1px solid var(--b1);border-radius:5px;padding:5px 11px;letter-spacing:.2px}
.fact a{color:var(--amber)}
/* status badge: dot + tinted text/border + dark fill, no glow */
.fact-status{display:inline-flex;align-items:center;gap:7px;background:rgba(0,0,0,.25)}
.fact-status .st-dot{width:7px;height:7px;border-radius:50%;background:currentColor}
.status-active{color:var(--green)}
.status-defunct{color:var(--red)}
.facts{align-items:center}
.fact-srcs{display:inline-flex;align-items:center;flex-wrap:wrap;gap:6px}
.fact-srcs-lbl{font-size:11px;font-weight:700;letter-spacing:.6px;text-transform:uppercase;
  color:var(--text-muted);margin-right:1px}
.aliases{font-size:14px;color:var(--text-main);margin-bottom:18px}
.aliases span{color:var(--text-muted);font-weight:700;text-transform:uppercase;
  font-size:11px;letter-spacing:.6px;margin-right:4px}
.cta{margin:20px 0 6px}
.btn{display:inline-flex;align-items:center;gap:8px;font-weight:700;font-size:13px;
  letter-spacing:1px;text-transform:uppercase;padding:11px 18px;border-radius:6px;
  color:var(--amber);border:1px solid var(--amber2);
  background:linear-gradient(180deg,rgba(232,160,32,.16),rgba(232,160,32,.05))}
.btn:hover{text-decoration:none;border-color:var(--amber);background:rgba(232,160,32,.15)}
.desc{font-size:19px;color:var(--text-main);margin:18px 0 6px;line-height:1.65}
.note{font-size:15px;color:var(--text-muted);font-style:italic;margin:16px 0 6px;
  border-left:2px solid var(--b2);padding-left:14px;max-width:78ch}
.stats{display:flex;flex-wrap:wrap;gap:32px;margin:28px 0 18px}
.stat{display:flex;flex-direction:column}
.stat b{font-family:var(--disp);font-size:42px;font-weight:600;line-height:1.05;color:var(--text-bright)}
.stat span{font-size:15px;color:var(--text2);text-transform:uppercase;letter-spacing:.7px;margin-top:2px}
.stat.all b{color:var(--amber)}
.stat.coop b{color:var(--green)}
.stat.conf b{color:var(--red)}
.stat.other b{color:#8090a0}
/* org name + aliases on one line, pipe-separated */
.org-title-line{display:flex;align-items:center;gap:0;margin-bottom:12px}
.org-title-line h1{margin-bottom:0}
.org-aka{font-size:19px;color:var(--text-muted);white-space:nowrap}
.org-aka::before{content:"|";margin:0 16px;color:var(--b2);font-weight:300}
/* org-meta — also used as card header inside desc-card */
.org-meta{display:flex;flex-wrap:wrap;align-items:center;gap:18px;
  font-size:18px;color:var(--text-main);margin-bottom:0}
.org-meta>span{display:inline-flex;align-items:center;white-space:nowrap;gap:0.4em}
.org-meta>span+span::before{content:"|";margin-right:18px;color:var(--b2);font-weight:300}
.org-meta a{color:var(--amber)}
.org-meta-srcs{display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap}
.org-meta-srcs-lbl{font-weight:600;font-size:12px;text-transform:uppercase;
  letter-spacing:.5px;color:var(--text2)}
/* status dot */
.org-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;margin-right:8px}
.org-status-active .org-dot{background:var(--green)}
.org-status-defunct .org-dot{background:var(--red)}
.org-status-active{color:var(--text-main)}
.org-status-defunct{color:var(--text-main);opacity:.65}
.org-meta .src-pill{font-size:13px;padding:2px 8px}
/* description card — org-meta is its header */
.desc-card{background:var(--s1);border:1px solid var(--b1);border-radius:10px;
  padding:16px 24px 20px;margin:0 0 10px}
.desc-card .org-meta{padding-bottom:12px;border-bottom:1px solid var(--b1);margin-bottom:14px}
.desc-card .desc{margin:0}
.desc-stats{font-size:19px;color:var(--text-main);margin:16px 0 0;line-height:1.6}
.c-all{color:var(--amber);font-weight:700}
.c-coop{color:var(--green);font-weight:700}
.c-conf{color:var(--red);font-weight:700}
.c-other{color:#8090a0;font-weight:700}
@media(max-width:820px){
  .org-meta{flex-direction:column;align-items:stretch;font-size:16px;gap:10px}
  .org-meta>span{white-space:normal}
  .org-meta>span+span::before{content:none;margin-right:0}
  .org-title-line{flex-direction:column;align-items:flex-start;gap:6px}
  .org-aka{white-space:normal;font-size:16px}
  .org-aka::before{content:none;margin:0}
}
.section-h{font-family:var(--disp);font-size:22px;font-weight:600;letter-spacing:1.4px;
  text-transform:uppercase;color:var(--amber);margin:42px 0 16px;
  padding-bottom:10px;border-bottom:1px solid var(--b1)}
.count{font-size:14px;color:var(--text-muted);font-weight:600}
/* relationship groups */
.rel{margin:22px 0}
.rel-cooperation{--rc:var(--fam-coop)}
.rel-conflict{--rc:var(--fam-hostile)}
.rel-other{--rc:var(--fam-loose)}
.rel-head{display:flex;align-items:center;gap:12px;margin-bottom:4px}
.rel-tag{font-family:var(--disp);font-size:15px;font-weight:600;letter-spacing:1.4px;
  text-transform:uppercase;color:var(--rc);border:1px solid var(--rc);
  border-radius:5px;padding:3px 12px}
.rel-count{font-size:13px;color:var(--text-muted);font-weight:600}
.rel-blurb{font-size:13px;color:var(--text-muted);margin:0 0 14px}
/* connection cards */
.conn-list{list-style:none}
.conn{background:var(--s1);border:1px solid var(--b1);border-left:3px solid var(--rc);
  border-radius:7px;padding:14px 16px;margin-bottom:10px;transition:background .12s,border-color .12s}
.conn:hover{background:var(--s2);border-color:var(--b2);border-left-color:var(--rc)}
.conn-name{font-size:19px;font-weight:700;color:var(--text-bright)}
.conn-name a{color:var(--text-bright)} .conn-name a:hover{color:var(--amber);text-decoration:none}
.conn-badge{font-size:10.5px;color:var(--text-muted);font-weight:700;margin-left:10px;
  text-transform:uppercase;letter-spacing:.6px}
.conn-desc{font-size:14px;color:var(--text-muted);margin-top:4px}
.stmt{margin-top:11px;padding-top:11px;border-top:1px dashed var(--b1)}
.stmt:first-of-type{margin-top:8px;padding-top:0;border-top:none}
.stmt-desc{font-size:15px;color:var(--text-main);line-height:1.6}
.conn blockquote{font-size:14.5px;color:var(--text-main);font-style:italic;margin:9px 0 0;
  padding:9px 14px;border-left:2px solid var(--rc);background:rgba(255,255,255,.025);
  border-radius:0 5px 5px 0}
.srcs{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-top:9px}
.srcs-lbl{font-size:10.5px;font-weight:700;letter-spacing:.7px;text-transform:uppercase;color:var(--text2)}
/* Source button: clearly clickable, clearly leaves to the source (Wikipedia).
   Amber-accented so it reads as an action, with a trailing external-link glyph. */
.src-pill{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;
  letter-spacing:.2px;color:var(--amber);background:rgba(232,160,32,.10);
  border:1px solid var(--amber2);border-radius:6px;padding:5px 11px;line-height:1.2;
  transition:background .12s,border-color .12s,color .12s}
.src-pill::before{content:"";flex-shrink:0;width:13px;height:13px;background-color:currentColor;
  -webkit-mask:var(--wiki-ico) center/contain no-repeat;mask:var(--wiki-ico) center/contain no-repeat}
.src-pill::after{content:"\\2197";font-size:12px;opacity:.85;font-weight:700}
.src-pill:hover{background:rgba(232,160,32,.2);border-color:var(--amber);color:#ffd27a;
  text-decoration:none}
.srcs-list{list-style:none;font-size:14px}
.srcs-list li{margin:7px 0}
/* country page — two side-by-side panels (based here · foreign footprints), each
   independently paginated with its own Load more. Single panel spans full width. */
.country-cols{display:grid;grid-template-columns:1fr 1fr;gap:34px;align-items:start;margin-top:8px}
/* org page: relationships panel dominates (~70%), footprints take the rest */
.country-cols.org-cols{grid-template-columns:minmax(0,2.3fr) minmax(0,1fr)}
.cc-panel{min-width:0}
.cc-panel .section-h{margin-top:0}
.cc-panel .load-more{margin-top:16px}
/* country page & org page — fill viewport, each column scrolls independently
   (same flex/grid/overflow pattern as the browse page dashboard) */
.country-page,.org-page{height:100vh;display:flex;flex-direction:column;padding-bottom:0}
.country-page .crumb,.org-page .org-top{flex-shrink:0}
.country-page h1,.org-page h1{flex-shrink:0;margin-bottom:12px}
.country-page .stats,.org-page .stats{flex-shrink:0}
.country-page .country-cols,.org-page .country-cols{flex:1;min-height:0;align-items:stretch;margin-bottom:0;gap:20px}
.country-page .cc-panel,.org-page .cc-panel{display:flex;flex-direction:column;min-height:0;overflow:hidden;
  background:var(--s1);border:1px solid var(--b1);border-radius:12px}
.country-page .cc-panel .section-h,.org-page .cc-panel .section-h{flex-shrink:0;padding:18px 22px 14px;margin:0;
  border-bottom:1px solid var(--b1);font-size:26px}
.country-page .cc-panel .panel-scroll,.org-page .cc-panel .panel-scroll{flex:1;min-height:0;overflow-y:auto;
  overscroll-behavior:contain;scrollbar-gutter:stable}
.country-page .cc-panel .load-more,.org-page .cc-panel .load-more{flex-shrink:0;margin:0;border-top:1px solid var(--b1);
  border-radius:0}
@media(max-width:820px){
  .country-cols{grid-template-columns:1fr;gap:8px}
  .country-page,.org-page{height:auto;min-height:auto;padding-bottom:30px}
  .country-page .country-cols,.org-page .country-cols{flex:none;height:auto}
  .country-page .cc-panel,.org-page .cc-panel{height:auto}
  .country-page .cc-panel .panel-scroll,.org-page .cc-panel .panel-scroll{max-height:360px}
}
/* "Foreign groups with a footprint in X" evidence rows (mirrors the footprints map's
   reading panel: rank · name · origin · context · Quote · Sources) */
.fp-list{display:flex;flex-direction:column}
.fp-row{display:grid;grid-template-columns:34px 1fr;gap:16px;align-items:start;
  padding:18px 4px;border-bottom:1px solid var(--b1)}
.fp-row:last-child{border-bottom:none}
.fp-rank{color:var(--text-muted);font-size:15px;text-align:right;padding-top:2px;font-variant-numeric:tabular-nums}
.fp-main{min-width:0}
.fp-name{font-size:18px;font-weight:700;color:var(--text-bright);margin-bottom:3px}
.fp-name a{color:var(--text-bright)} .fp-name a:hover{color:var(--amber);text-decoration:none}
.fp-origin{font-size:13.5px;color:var(--text-muted);margin-bottom:11px}
.fp-context{font-size:15.5px;color:var(--text-main);line-height:1.65;margin-bottom:11px}
.ev-none{font-size:14px;color:var(--text-muted);font-style:italic}
.ev-toggles{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
.ev-toggle{font-family:var(--body);font-size:13.5px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;
  color:var(--text2);background:var(--s2);border:1px solid var(--b1);border-radius:999px;
  padding:6px 14px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;transition:all .12s}
.ev-toggle:hover{color:var(--amber);border-color:var(--amber)}
.ev-toggle .chev{font-size:10px;opacity:.7;transition:transform .15s}
.ev-toggle.open .chev{transform:rotate(180deg)}
.ev-toggle.q.open{color:var(--amber);border-color:var(--amber);background:rgba(232,160,32,.08)}
.ev-srcs{margin-left:auto;font-size:15px;color:var(--text-muted);line-height:1.55}
.ev-srcs-label{font-weight:700;color:var(--text2);text-transform:uppercase;font-size:12px;letter-spacing:.5px}
.ev-srcs a{color:var(--amber)}
/* Footprint/country reading-panel reveal — its own class so the finder's
   `.ev-reveal{display:none}` (further down) can't clobber it. Hidden via the
   `hidden` attribute (see the global [hidden] rule); visible when removed. */
.fp-reveal{margin-top:11px}
.ev-quote{font-size:17px;color:var(--text-main);line-height:1.65;font-style:italic;
  padding:11px 15px;margin:0;border-left:2px solid var(--amber2);background:rgba(255,255,255,.025);border-radius:0 6px 6px 0}
.ev-quote::before{content:"\\201C"} .ev-quote::after{content:"\\201D"}
/* browse index */
.index-list{list-style:none;columns:3;column-gap:26px;margin:16px 0 6px}
.index-list li{break-inside:avoid;font-size:14px;margin-bottom:6px}
.index-list.ranked{columns:2}
.index-list .deg{color:var(--text-muted);font-size:12px;margin-left:6px;font-variant-numeric:tabular-nums}
/* ── browse: header (title+desc spans cols 1-2, cards in col 3, aligned with panel-finder) ─── */
/* ── browse tabs ───────────────────────────────────────────────────────── */
/* ── browse head ────────────────────────────────────────────────────────── */
.browse-head{display:grid;grid-template-columns:1fr;gap:20px;align-items:center;margin-bottom:16px}
.head-col-a{display:flex;flex-direction:column;align-items:flex-start;gap:12px;min-width:0}
.head-col-a h1{margin:0;white-space:nowrap;text-align:center}
/* ── header nav: three quiet chips ───────────────────────────────────────── */
.head-nav{display:flex;justify-content:center;align-items:center;gap:10px;flex-wrap:wrap}
.nav-btn{display:inline-flex;align-items:center;gap:6px;height:44px;padding:0 15px;
  font-weight:700;font-size:12.5px;letter-spacing:.8px;text-transform:uppercase;
  color:var(--text-main);background:var(--s1);border:1px solid var(--b2);border-radius:7px;
  white-space:nowrap;transition:color .12s,border-color .12s,background .12s}
.nav-btn:hover{color:var(--amber);border-color:var(--amber2);background:var(--s2);text-decoration:none}
.nav-btn svg{flex-shrink:0;color:var(--text-muted);transition:color .12s,transform .12s}
.nav-btn:hover svg{color:var(--amber);transform:translate(1px,-1px)}
.browse-cards{display:flex;flex-direction:row;gap:12px;grid-column:2}
/* ── browse: compact entry cards (footprints map + 3D graph) ── */
.explore-card{display:flex;align-items:center;gap:12px;padding:14px 20px;background:var(--s1);
  border:1px solid var(--b1);border-radius:10px;text-decoration:none;flex:1;min-width:0;
  transition:border-color .12s,background .12s,transform .12s}
.explore-card:hover{border-color:var(--amber2);background:var(--s2);text-decoration:none;transform:translateY(-2px)}
.explore-ic{flex-shrink:0;width:40px;height:40px;display:flex;align-items:center;justify-content:center;
  border-radius:8px;background:rgba(232,160,32,.10);border:1px solid var(--amber2);color:var(--amber)}
.explore-ic svg{width:20px;height:20px}
.explore-tx{display:flex;flex-direction:column;gap:2px;min-width:0;flex:1}
.explore-tx b{color:var(--text-bright);font-size:16px;font-weight:700}
.explore-tx span{color:var(--text-muted);font-size:15px;line-height:1.4}
.explore-arrow{flex-shrink:0;color:var(--text-muted);width:18px;height:18px;transition:transform .12s,color .12s}
.explore-arrow svg{width:100%;height:100%}
.explore-card:hover .explore-arrow{color:var(--amber);transform:translate(2px,-2px)}
/* ── browse page: flex wrapper so columns fill the viewport exactly ─── */
.browse-wrap{display:flex;flex-direction:column;height:100vh;overflow:hidden;padding-bottom:0}
#tab-browse{display:flex;flex-direction:column;flex:1;min-height:0;overflow:hidden}
/* ── browse: two-panel dashboard ────────────────────────────────────────
   Two bordered panels fill remaining viewport height; each has a sticky
   header, an internally-scrolling body, and a sticky footer. The left
   panel toggles between Organizations and Countries; the right hosts
   explore cards. */
.browse-cols{display:grid;grid-template-columns:1fr 2.2fr;gap:20px;
  align-items:stretch;flex:1;min-height:0}
.browse-panel{display:flex;flex-direction:column;min-height:0;
  background:var(--s1);border:1px solid var(--b1);border-radius:12px;overflow:hidden}
.panel-finder{overflow:hidden}
.panel-head{flex-shrink:0;padding:18px 22px 14px;border-bottom:1px solid var(--b1);
  background:linear-gradient(180deg,var(--s2),var(--s1))}
.panel-head-row{display:flex;align-items:baseline;gap:12px}
.panel-head h2{font-family:var(--disp);font-size:26px;font-weight:600;letter-spacing:1.2px;
  text-transform:uppercase;color:var(--amber)}
.panel-count{font-size:16px;color:var(--text-muted);font-weight:600;font-variant-numeric:tabular-nums}
.panel-sub{font-size:18px;color:var(--text-main);margin-top:4px}
.detail-back{display:none;align-items:center;gap:6px;flex-shrink:0;
  font-family:var(--body);font-size:12.5px;font-weight:700;letter-spacing:.6px;
  text-transform:uppercase;color:var(--amber);background:none;border:1px solid var(--b2);
  border-radius:6px;padding:7px 12px;cursor:pointer;line-height:1}
.detail-back:hover{color:var(--text-bright);border-color:var(--amber2)}
.detail-back svg{width:14px;height:14px}
.panel-search{width:100%;margin-top:12px;padding:10px 14px;font-family:var(--body);font-size:16px;
  color:var(--text-bright);background:var(--bg);border:1px solid var(--b2);border-radius:7px;outline:none;
  box-sizing:border-box;transition:border-color .12s}
.panel-search:focus{border-color:var(--amber2)}
.panel-search::placeholder{color:var(--text-muted)}
/* organizations / countries toggle */
.browse-toggle{display:flex;gap:2px;margin-top:10px;
  background:var(--bg);border:1px solid var(--b2);border-radius:7px}
.browse-toggle-btn{flex:1;padding:8px 16px;font-family:var(--body);font-size:0.9rem;
  font-weight:500;color:var(--text-muted);background:transparent;border:none;cursor:pointer;
  transition:background .12s,color .12s}
.browse-toggle-btn:not(:last-child){border-right:1px solid var(--b2)}
.browse-toggle-btn.active{background:var(--amber);color:var(--bg);font-weight:600}
.browse-toggle-btn:hover:not(.active){color:var(--text-bright)}
.panel-scroll{flex:1;min-height:0;overflow-y:auto;overscroll-behavior:contain;scrollbar-gutter:stable}
.panel-foot{flex-shrink:0;padding:12px 16px;border-top:1px solid var(--b1);background:var(--s1)}
/* custom scrollbars — wide enough to grab easily */
.panel-scroll::-webkit-scrollbar{width:16px}
.panel-scroll::-webkit-scrollbar-track{background:var(--bg)}
.panel-scroll::-webkit-scrollbar-thumb{background:var(--b3);border-radius:8px;
  border:4px solid var(--s1)}
.panel-scroll::-webkit-scrollbar-thumb:hover{background:var(--amber2)}
.panel-scroll{scrollbar-width:auto;scrollbar-color:var(--b3) var(--bg)}

/* org rows: flat, divided link rows — each links to the org's dedicated page */
.org-list{display:flex;flex-direction:column}
.bo{display:flex;align-items:center;gap:14px;padding:13px 22px;
  border-bottom:1px solid var(--b1);color:inherit;transition:background .12s}
.bo:last-child{border-bottom:none}
.bo:hover{background:var(--s2);text-decoration:none}
.bo.no-link:hover{background:transparent}
.bo.no-link .bo-name{color:var(--text-main)}
.bo-name{flex:1;min-width:0;font-size:18px;font-weight:600;color:var(--text-bright);
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bo:hover .bo-name{color:var(--amber)}
.bo-meta{display:flex;align-items:center;gap:12px;flex-shrink:0}
.bo-dot{width:8px;height:8px;border-radius:50%;background:var(--text-muted)}
.bo-dot.status-active{background:var(--green)}
.bo-dot.status-defunct{background:var(--red)}
.bo-deg{font-size:15px;color:var(--text-muted);white-space:nowrap;
  font-variant-numeric:tabular-nums;min-width:92px;text-align:right}
.bo-deg b{color:var(--text-main);font-weight:700}

/* countries rail */
.country-list{list-style:none}
.country-list li,
.country-list .cn{border-bottom:1px solid var(--b1)}
.country-list li:last-child,
.country-list .cn:last-child{border-bottom:none}
.country-list a,
.country-list .cn{display:flex;align-items:center;justify-content:space-between;gap:12px;
  padding:12px 22px;color:var(--text-main);transition:background .12s;cursor:pointer}
.country-list a:hover,
.country-list .cn:hover{background:var(--s2);text-decoration:none}
.country-list a:hover .cn-name,
.country-list .cn:hover .cn-name{color:var(--amber)}
.country-list .cn-name{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
  font-size:18px;font-weight:600;color:var(--text-bright)}
.country-list .deg{flex-shrink:0;color:var(--text-main);font-size:15px;
  font-variant-numeric:tabular-nums}

.load-more{display:block;width:100%;padding:11px;font-family:var(--body);
  font-size:12.5px;font-weight:700;letter-spacing:1px;text-transform:uppercase;cursor:pointer;
  color:var(--amber);background:rgba(232,160,32,.06);border:1px solid var(--amber2);border-radius:7px;
  transition:background .12s,border-color .12s}
.load-more:hover{background:rgba(232,160,32,.13);border-color:var(--amber)}

@media(max-width:820px){
  .browse-wrap{height:auto;min-height:auto;padding-bottom:30px;overflow:visible}
  .browse-head{grid-template-columns:1fr;gap:14px;align-items:stretch}
  .head-col-a{align-items:center;gap:16px}
  .head-col-a h1{white-space:normal}
  .head-nav{width:100%;flex-wrap:wrap;gap:10px}
  .nav-btn{flex:1 1 auto;justify-content:center}
  .nav-ai{order:3;width:100%}
  .browse-cards{flex-direction:row;flex-wrap:wrap;width:100%}
  .browse-cols{grid-template-columns:1fr;flex:none;height:auto;gap:20px}
  .browse-panel{height:auto}
  .panel-browse .panel-scroll,.panel-detail .panel-scroll{flex:none;overflow:visible}
  .browse-panel.panel-detail{display:none}
  .browse-wrap.show-detail .panel-browse{display:none}
  .browse-wrap.show-detail .panel-detail{display:flex}
  .detail-back{display:inline-flex}
  .panel-detail .panel-head-row{align-items:center;gap:10px}
  .panel-orgs .panel-scroll{max-height:none}
  .panel-countries .panel-scroll{max-height:360px}
  .panel-finder .panel-scroll{max-height:none}
}

/* ── connection finder (third column, inside a browse-panel) ────────────── */
.finder{padding:0}
.finder-head{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin-bottom:14px}
.finder-title{font-family:var(--disp);font-size:18px;font-weight:600;letter-spacing:1px;
  text-transform:uppercase;color:var(--amber)}
.finder-hint{font-size:12.5px;color:var(--text-muted)}
.finder-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:12px}
.pf-field{position:relative;flex:1;min-width:200px}
.pf-input{width:100%;margin-top:0;padding:10px 14px;font-family:var(--body);font-size:16px;
  color:var(--text-bright);background:var(--bg);border:1px solid var(--b2);border-radius:7px;
  outline:none;box-sizing:border-box;transition:border-color .12s}
.pf-input:focus{border-color:var(--amber2)}
.pf-input::placeholder{color:var(--text-muted)}
.pf-locked{display:flex;align-items:center;gap:9px;flex:1;min-width:200px;padding:10px 14px;
  background:rgba(232,160,32,.08);border:1px solid var(--amber2);border-radius:7px}
.pf-lock-lbl{font-size:10.5px;font-weight:700;letter-spacing:.7px;text-transform:uppercase;color:var(--text-muted)}
.pf-lock-name{font-size:18px;font-weight:700;color:var(--amber)}
.pf-arrow{color:var(--text-muted);font-size:18px;flex-shrink:0}
.pf-arrow::after{content:"—"}
.pf-go{flex-shrink:0;padding:10px 22px;font-family:var(--body);font-size:12.5px;font-weight:700;
  letter-spacing:1px;text-transform:uppercase;cursor:pointer;color:#120a06;background:var(--amber);
  border:1px solid var(--amber);border-radius:7px;transition:filter .12s}
.pf-go:hover{filter:brightness(1.08)}
.pf-reset{flex-shrink:0;padding:11px 16px;font-family:var(--body);font-size:12.5px;font-weight:700;
  letter-spacing:1px;text-transform:uppercase;cursor:pointer;color:var(--text-muted);
  background:transparent;border:1px solid var(--b2);border-radius:7px}
.pf-reset:hover{color:var(--text-bright);border-color:var(--b3)}
/* autocomplete dropdown */
.pf-dd{position:absolute;top:calc(100% + 4px);left:0;right:0;z-index:50;display:none;
  background:var(--s3);border:1px solid var(--b2);border-radius:7px;max-height:280px;
  overflow-y:auto;box-shadow:0 14px 36px rgba(0,0,0,.6)}
.pf-dd.open{display:block}
.pf-opt{display:flex;align-items:center;justify-content:space-between;gap:10px;
  padding:10px 14px;font-size:17px;color:var(--text-main);cursor:pointer;border-bottom:1px solid var(--b1)}
.pf-opt:last-child{border-bottom:none}
.pf-opt:hover{background:var(--s2);color:var(--amber)}
.pf-opt-d{font-size:11.5px;color:var(--text-muted);font-variant-numeric:tabular-nums}
.finder-results{display:none;margin-top:18px;padding:0 18px 22px}
.finder-results.open{display:block}
.pf-head-line{font-size:18px;color:var(--text-main);margin-bottom:4px;padding-left:2px}
.pf-head-line b{color:var(--text-bright)}
.pf-msg{font-size:14px;color:var(--text-muted);padding:10px 2px}
@keyframes spin{100%{transform:rotate(360deg)}}

/* ── results: Direct linkages ── */
.rp-section-title{font-family:var(--disp);font-size:19px;letter-spacing:1.8px;color:var(--amber2);
  text-transform:uppercase;margin:26px 4px 14px;padding-bottom:10px;border-bottom:1px solid var(--b1);
  display:flex;align-items:baseline;gap:6px;
  cursor:pointer;user-select:none}
.rp-section-count{color:var(--text-muted);font-weight:400}
.rp-section-title::after{content:"▾";font-size:16px;margin-left:4px;transition:transform .15s;flex-shrink:0}
.rp-section-title.collapsed{border-bottom-color:transparent;margin-bottom:10px}
.rp-section-title.collapsed::after{content:"▸"}
.rp-section-body.collapsed{display:none}
/* Relationship Summary paragraph */
.rs-summary{font-size:18px;color:var(--text-main);line-height:1.75;padding:0 4px;margin-bottom:4px}
.rs-summary p{margin:0}
.rp-empty{padding:32px 22px;text-align:center;color:var(--text-muted);font-size:20px;font-style:italic;
  border:1px dashed var(--b1);border-radius:6px;margin-top:18px;line-height:1.6}
.pf-summary{font-size:15px;color:var(--text-muted);margin:0 4px 14px;line-height:1.5}
.pf-summary b{color:var(--amber);font-weight:700}
.pf-summary .nolink{color:var(--fam-hostile);font-weight:700}
.pf-loading{display:flex;align-items:center;gap:11px;padding:18px 4px;color:var(--text-main);font-size:14px}
.rp-interactions{margin-bottom:4px}
/* evidence pills (Source / Time / Quote) */
.ev-pills{display:inline-flex;gap:6px;flex-shrink:0;flex-wrap:wrap;align-items:center}
.ev-pill{display:inline-flex;align-items:center;gap:5px;font-size:12px;font-weight:700;letter-spacing:.6px;
  text-transform:uppercase;padding:4px 9px;border-radius:4px;border:1px solid var(--b1);color:var(--text2);
  background:var(--s2);cursor:pointer;text-decoration:none;font-family:var(--body);transition:all .12s;user-select:none}
.ev-pill:hover{border-color:var(--amber);color:var(--amber);background:rgba(232,160,32,0.06)}
.ev-pill.src,.ev-pill.time,.ev-pill.quote{color:var(--text2)}
.ev-pill.open{background:rgba(232,160,32,0.10);border-color:var(--amber);color:var(--amber)}
.ev-reveal{display:none;margin-top:10px;width:100%;border-radius:5px;overflow:hidden;border:1px solid var(--b1);background:rgba(0,0,0,0.30)}
.ev-reveal.open{display:block}
.ev-reveal-block{padding:10px 13px;font-size:14px;line-height:1.55}
.ev-reveal-block + .ev-reveal-block{border-top:1px solid var(--b1)}
.ev-reveal-block .erl{display:block;font-size:10px;font-weight:800;letter-spacing:1.3px;text-transform:uppercase;margin-bottom:5px}
.ev-reveal-block.time .erl,.ev-reveal-block.quote .erl{color:var(--text-muted)}
.ev-reveal-block.quote .qt{color:var(--text-main);font-style:italic;border-left:2px solid var(--b2);padding-left:11px;display:block}
.ev-reveal-block .muted{color:var(--text-muted);font-style:italic}
@media(max-width:640px){h1{font-size:34px}.head-col-a h1{font-size:40px}.index-list{columns:2}.index-list.ranked{columns:1}
  .finder-row{flex-direction:column;align-items:stretch}.pf-arrow{display:none}
  .panel-browse .panel-head,.panel-detail .panel-head{padding:14px 16px 12px}
  .panel-browse .panel-head h2,.panel-detail .panel-head h2{font-size:22px}
  .panel-browse .panel-sub,.panel-detail .panel-sub{font-size:16px}
  .bo{padding:12px 16px;gap:12px}
  .bo-name{font-size:17px}
  .bo-deg{min-width:0}
  .country-list .cn{padding:12px 16px}
  .desc-card .desc,.desc-stats{font-size:17px}
  .org-title-line{flex-wrap:wrap;row-gap:4px}
  .org-aka{white-space:normal;font-size:16px}
  .org-aka::before{margin:0 10px}}


/* ── AI evidence rendering extensions ── */
.rs-heading{font-family:var(--disp);font-size:22px;letter-spacing:1.5px;color:var(--amber);
  text-transform:uppercase;margin:0 0 8px}
.rs-ai-note{font-size:14px;color:var(--text-muted);margin:0 0 18px;font-style:italic}
.rp-linkage{border:1px solid var(--b1);border-radius:6px;background:var(--s2);padding:16px 18px}
.rp-linkage + .rp-linkage{margin-top:12px}
.rp-linkage-desc{font-size:17px;color:var(--text-main);line-height:1.7;margin-bottom:14px}
.ev-meta{display:flex;gap:18px;flex-wrap:wrap;align-items:center;margin-top:6px}
.ev-meta .ev-srcs{font-size:15px;margin-left:0}
.rp-linkage .fp-reveal{margin-top:14px}
.rp-linkage .ev-time{font-size:15.5px;color:var(--text-muted);font-style:italic}

/* ── Communities tab ────────────────────────────────────────────────────── */
.cc-list{display:flex;flex-direction:column;padding:12px 18px 22px}

.cc-card{border:1px solid var(--b1);border-radius:8px;background:var(--s2);overflow:hidden;
  transition:border-color .12s}
.cc-card + .cc-card{margin-top:12px}
.cc-card:has(.cc-card-head.open){border-color:var(--amber2)}

.cc-card-head{display:flex;align-items:baseline;gap:12px;padding:14px 18px;cursor:pointer;user-select:none;
  transition:background .12s}
.cc-card-head:hover{background:var(--s3)}
.cc-card-head::after{content:"▸";font-size:14px;color:var(--text-muted);margin-left:auto;flex-shrink:0;
  transition:transform .15s}
.cc-card-head.open::after{content:"▾"}

.cc-title{font-family:var(--disp);font-size:20px;font-weight:600;letter-spacing:1px;
  color:var(--amber);min-width:0}
.cc-size{flex-shrink:0;font-size:14px;color:var(--text-muted);font-weight:600;
  font-variant-numeric:tabular-nums;white-space:nowrap}

.cc-card-body{padding:0 18px 16px}
.cc-card-body.collapsed{display:none}

.cc-summary{font-size:16px;color:var(--text-main);line-height:1.75;margin:0 0 16px}

.cc-orgs{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.cc-orgs-dots{font-size:14px;color:var(--text-muted)}
.cc-org{display:inline-block;font-size:13.5px;font-weight:500;padding:4px 10px;
  color:var(--text-main);background:var(--bg);border:1px solid var(--b1);border-radius:5px;
  transition:color .1s,border-color .1s,background .1s}
.cc-org:hover,.cc-org:focus{color:var(--amber);border-color:var(--amber2);text-decoration:none;
  background:var(--s3)}

.cc-more{display:block;width:100%;margin-top:10px;padding:9px;font-family:var(--body);
  font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;cursor:pointer;
  color:var(--amber);background:rgba(232,160,32,.06);border:1px solid var(--amber2);border-radius:7px;
  transition:background .12s,border-color .12s}
.cc-more:hover{background:rgba(232,160,32,.13);border-color:var(--amber)}

/* ── Bridges tab ────────────────────────────────────────────────── */
.br-list{display:flex;flex-direction:column;padding:12px 18px 22px}
.br-comm-tag{display:inline-block;font-size:13.5px;font-weight:500;padding:4px 10px;
  color:var(--amber);border:1px solid var(--amber2);border-radius:7px}
.br-comm-tag:hover{background:rgba(232,160,32,.06)}

@media(max-width:820px){
  .cc-list{padding:16px 12px 22px}
  .cc-card-head{padding:12px 14px}
  .cc-card-body{padding:0 14px 14px}
  .cc-title{font-size:18px}
}

/* ── Triadic Signals tab ─────────────────────────────── */
.sig-list{padding:12px 18px 22px}
.sig-filters{display:flex;align-items:center;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.sig-filters-label{font-size:14px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.5px;font-weight:600;margin-right:2px}
.sig-pill{font-family:var(--body);font-size:14px;font-weight:600;padding:6px 14px;border:1px solid var(--b2);border-radius:6px;background:transparent;color:var(--text-muted);cursor:pointer;letter-spacing:.3px;transition:all .15s}
.sig-pill:hover{color:var(--text-main);border-color:var(--b1)}
.sig-pill.active{color:var(--amber);border-color:var(--amber2);background:rgba(232,160,32,.08)}
.sig-pill-count{font-weight:400;font-size:13px;margin-left:2px;opacity:.7}

.sig-table-wrap{overflow-x:auto;border:1px solid var(--b1);border-radius:9px;background:var(--s2)}
.sig-table{width:100%;border-collapse:collapse;font-size:15px;min-width:800px}
.sig-table thead{background:var(--s2);position:sticky;top:0;z-index:1}
.sig-table th{padding:10px 12px;text-align:left;font-weight:600;font-size:13px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--b2);white-space:nowrap;cursor:pointer;user-select:none}
.sig-table th:hover{color:var(--amber)}
.sig-table th.sort-asc::after{content:" ▲";font-size:10px;color:var(--amber)}
.sig-table th.sort-desc::after{content:" ▼";font-size:10px;color:var(--amber)}
.sig-table td{padding:9px 12px;border-bottom:1px solid var(--b1);vertical-align:top;white-space:nowrap}
.sig-table tbody tr:hover{background:rgba(232,160,32,.03)}
.sig-table tbody tr:nth-child(even){background:rgba(255,255,255,.01)}
.sig-table tbody tr:nth-child(even):hover{background:rgba(232,160,32,.04)}

.sig-org-name{color:var(--text-bright);font-weight:700;font-size:16px}
.sig-val-count{font-weight:600;color:var(--text-bright);text-align:center}
.sig-val-score{font-weight:600;color:var(--text-bright);font-size:16px;text-align:center}
.sig-val-zero{color:var(--text-muted);opacity:.4;text-align:center}

.sig-tag{display:inline-block;font-size:13px;font-weight:600;padding:2px 8px;border-radius:4px;line-height:1.4;white-space:nowrap}
.sig-tag.cp{color:#5eead4;border:1px solid rgba(94,234,212,.25);background:rgba(94,234,212,.06)}
.sig-tag.sa{color:#fb7185;border:1px solid rgba(251,113,133,.25);background:rgba(251,113,133,.06)}
.sig-tag.both{color:var(--amber);border:1px solid rgba(232,160,32,.25);background:rgba(232,160,32,.06)}

.sig-items{max-width:340px;white-space:normal;font-size:14px;color:var(--text-main);line-height:1.55}
.sig-item-name{color:var(--text-main);font-weight:500}
.sig-item-name:hover{color:var(--amber);cursor:pointer}
.sig-item-more{color:var(--amber);cursor:pointer;font-size:13px;text-decoration:underline;text-underline-offset:2px}
.sig-item-more:hover{color:#f0c040}
.sig-item-rest{display:none}
.sig-items.expanded .sig-item-rest{display:inline}
.sig-items.expanded .sig-item-more{display:none}
.sig-load-more{display:block;width:100%;padding:9px;font-family:var(--body);
  font-size:13px;font-weight:600;color:var(--amber);background:transparent;
  border:1px solid var(--amber2);border-radius:6px;cursor:pointer;
  letter-spacing:.3px;transition:all .15s;margin-bottom:14px}
.sig-load-more:hover{background:rgba(232,160,32,.08);border-color:var(--amber)}

@media(max-width:900px){
  .sig-table{font-size:14px;min-width:700px}
  .sig-table td,.sig-table th{padding:7px 8px}
}

"""


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default="data/crimenet.json", type=Path)
    ap.add_argument("--output", default="app", type=Path)
    ap.add_argument("--base-url", default="https://www.alvarofrancomartins.com/crimenet",
                    help="Absolute base URL the app is served from (for canonical/OG/sitemap).")
    args = ap.parse_args()

    base_url = args.base_url.rstrip("/")

    data = json.loads(args.input.read_text(encoding="utf-8"))
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    # Count unique Wikipedia articles used as sources
    article_urls: set[str] = set()
    for n in nodes:
        for s in (n.get("own_sources") or []) + (n.get("mentioned_in") or []):
            if s.get("url"):
                article_urls.add(s["url"])
    n_articles = len(article_urls)

    by_name = {n["standard_name"]: n for n in nodes if n.get("standard_name")}

    # Degree
    degree: dict[str, int] = defaultdict(int)
    for e in edges:
        s, t = e.get("source"), e.get("target")
        if s:
            degree[s] += 1
        if t:
            degree[t] += 1

    # Every node with a standard_name gets a page.
    org_names = [n["standard_name"] for n in nodes
                 if n.get("standard_name")]

    # Countries: any origin country + any footprint destination. (Already
    # validated/normalized upstream in step 4, so these are clean.)
    based_by_country: dict[str, list[dict]] = defaultdict(list)
    footprints_by_country: dict[str, list[dict]] = defaultdict(list)
    for n in nodes:
        c = n.get("country")
        if c and c != "Unknown":
            based_by_country[c].append(n)
        origin = n.get("country")
        for fp in (n.get("country_links") or []):
            dest = ((fp.get("country") if isinstance(fp, dict) else fp) or "").strip()
            if not dest:
                continue
            # Footprints ABROAD only — a link back to the org's own home country
            # isn't a foreign footprint (mirrors footprints.html). Without this,
            # e.g. US-based orgs show up under "Foreign groups with a footprint in the US".
            if origin and origin != "Unknown" and dest == origin:
                continue
            # Carry the org's full footprint evidence (context + quote + its own
            # sources + origin), so the country page can render it like the
            # footprints map's reading panel.
            footprints_by_country[dest].append({
                "org": n["standard_name"],
                "origin": origin if origin not in (None, "Unknown") else None,
                "context": _FIX_COLON_COMMA.sub(_FIX_COLON_COMMA_REPL, _FIX_COLON.sub(_FIX_COLON_REPL, _FIX_ARTIFACTS.sub(_FIX_ARTIFACTS_REPL, (fp.get("context") if isinstance(fp, dict) else "") or ""))),
                "quote": _FIX_COLON_COMMA.sub(_FIX_COLON_COMMA_REPL, _FIX_COLON.sub(_FIX_COLON_REPL, _FIX_ARTIFACTS.sub(_FIX_ARTIFACTS_REPL, (fp.get("evidence_quote") if isinstance(fp, dict) else "") or ""))),
                "sources": [s for s in (n.get("own_sources") or []) if s.get("url")][:3],
            })
    country_names = sorted(set(based_by_country) | set(footprints_by_country))
    # Total cross-border activity per country: orgs based here + foreign
    # footprints here. Used to order the browse index.
    country_total = {c: len(based_by_country.get(c, [])) + len(footprints_by_country.get(c, []))
                     for c in country_names}

    org_slug = build_slug_map(org_names)
    country_slug = build_slug_map(country_names)

    # Write shared CSS
    (args.output / "css" / "main.css").write_text(CSS, encoding="utf-8")

    # Browse index
    (args.output / "index.html").write_text(
        build_browse_page(org_names, country_names, degree, country_total, by_name,
                          org_slug, country_slug, base_url, n_articles),
        encoding="utf-8")

    # Visualization data: the in-app maps/graphs (footprints.html,
    # knowledge_graph*.html) load the full dataset at runtime, so ship a copy
    # inside the deploy bundle. They fetch "crimenet.json" from the data/ subdir.
    (args.output / "data").mkdir(parents=True, exist_ok=True)
    bundled = args.output / "data" / "crimenet.json"
    if not (bundled.exists() and args.input.resolve() == bundled.resolve()):
        shutil.copyfile(args.input, bundled)

    # Sitemap
    urls = [f"{base_url}/index.html"]
    urls += [f"{base_url}/{v}" for v in ("browse.html", "ask.html", "about.html", "footprints.html", "knowledge_graph.html", "triadic_signals.html")]
    (args.output / "sitemap.xml").write_text(
        build_sitemap(urls, date.today().isoformat()), encoding="utf-8")

    # Remove stale org/country pages (no longer generated)
    for d in (args.output / "org", args.output / "country"):
        for old in d.glob("*.html") if d.exists() else []:
            old.unlink()

    print(f"wrote index.html, static_pages.css, sitemap.xml ({len(urls)} urls) → {args.output}")
    print(f"copied crimenet.json ({args.input.stat().st_size // 1024:,} KB) → {bundled}")


if __name__ == "__main__":
    main()
