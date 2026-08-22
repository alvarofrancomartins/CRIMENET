// CRIMENET browse page: Trace a Connection, Communities, Bridges, Triadic Signals
(function(){
  var U = window.CrimenetUtils;

  // ── Trace a Connection / Communities / Bridges / Signals toggle ──────────────
  var VIEWS=['finder','communities','bridges','signals'];
  var subtitles={
    finder:'Find how two organizations link: directly or through a shared group',
    communities:'Explore community clusters detected from cooperation ties in the organized crime network',
    bridges:'Organizations and edges that bridge community boundaries in the cooperation network',
    signals:'When two organizations A and B have no direct edge between them but share several of the same cooperation partners or the same adversaries, these common neighbors suggest a real-world relationship that may be missing from Wikipedia. This tab lists every such pair. Each common neighbor C₁ contributes min(statements A↔C₁, statements B↔C₁) and the Total Score is the sum across all shared neighbors, so relationships backed by more evidence weigh proportionally. 2,561 unique pairs: 1,609 with both signals, 486 cooperation-partner-only, 466 adversary-only.'
  };

  function switchToPanel(view){
    document.querySelectorAll('.panel-finder .browse-toggle-btn').forEach(function(b){b.classList.remove('active')});
    var btn = document.querySelector('.panel-finder .browse-toggle-btn[data-view="'+view+'"]');
    if(btn) btn.classList.add('active');
    VIEWS.forEach(function(v){
      var el = document.getElementById(v+'-view');
      if(el) el.style.display = v===view ? '' : 'none';
    });
    document.getElementById('finder').style.display = view==='finder' ? '' : 'none';
    document.getElementById('finder-panel-title').textContent = view==='finder' ? 'Trace a Connection' : view==='communities' ? 'Communities' : view==='bridges' ? 'Bridges' : 'Triadic Signals';
    document.getElementById('finder-panel-sub').textContent = subtitles[view]||'';
  }

  document.querySelectorAll('.panel-finder .browse-toggle-btn').forEach(function(btn){
    btn.addEventListener('click',function(){
      switchToPanel(this.dataset.view);
    });
  });

  // Show the default (finder) view on load — the *-view containers start
  // display:none in the HTML, so without this the Trace results render into a
  // hidden #finder-view and nothing appears until another tab is clicked.
  switchToPanel('finder');
})();

// ── Browser connection finder ──────────────────────────────────────────────────
// Fetches evidence from evidence/ shards and per-pair relationship summaries, and
// renders Relationship Summary + Direct linkages with Source/Time/Quote pills.
(function(){
  var U = window.CrimenetUtils;
  var F=document.getElementById('finder'); if(!F)return;
  var BASE=F.dataset.base||'', LOCKED=F.dataset.locked||'';
  var adjNeighbors={}, adjReady=false;
  var orgADJ={}, names=[];
  var elA=document.getElementById('pf-a'), elB=document.getElementById('pf-b'),
      go=document.getElementById('pf-go'), reset=document.getElementById('pf-reset'),
      out=document.getElementById('pf-results');

  // Shared evidence / summary loaders
  var loadEvidence = U.evidenceLoader();
  var loadRelationshipSummary = U.summaryLoader();

  // ── load adjacency list ──
  function loadAdj(){
    if(adjReady)return Promise.resolve();
    return fetch(BASE+'data/crimenet_adj.json').then(function(r){if(!r.ok)throw new Error('adj');return r.json();}).then(function(d){
      for(var k in d){if(!d.hasOwnProperty(k))continue;
        adjNeighbors[k]=d[k].map(function(e){return e.t;});
      }
      adjReady=true;
      names=Object.keys(adjNeighbors).sort(function(a,b){var da=adjNeighbors[b].length-adjNeighbors[a].length;return da!==0?da:a.toLowerCase().localeCompare(b.toLowerCase());});
    }).catch(function(err){console.error('Failed to load adjacency:', err);});
  }

  function fetchConnections(orgName){
    if(orgADJ[orgName])return Promise.resolve();
    return loadEvidence(orgName).then(function(entry){
      var edges=(entry&&entry.edges)?entry.edges:[];
      orgADJ[orgName]=[];
      edges.forEach(function(e){var other=(e.source===orgName)?e.target:e.source;orgADJ[orgName].push(Object.assign({},e,{_other:other}));});
    });
  }

  // ── autocomplete dropdown ──
  function wireInput(el,dd){
    if(!el)return;
    var sel=-1, matches=[];

    function render(){
      dd.innerHTML='';
      matches.forEach(function(m,i){
        var opt=document.createElement('div');
        opt.className='pf-opt'+(i===sel?' selected':'');
        opt.innerHTML=U.esc(m)+'<span class="pf-opt-d">'+adjNeighbors[m].length+'</span>';
        opt.addEventListener('mousedown',function(e){e.preventDefault();el.value=m;dd.classList.remove('open');});
        dd.appendChild(opt);
      });
    }

    el.addEventListener('input',function(){
      var q=U.fold(this.value);
      if(q.length<1){dd.classList.remove('open');return;}
      matches=names.filter(function(n){return U.fold(n).indexOf(q)!==-1;}).slice(0,20);
      sel=-1;
      render();
      dd.classList.add('open');
    });

    el.addEventListener('keydown',function(e){
      if(!dd.classList.contains('open'))return;
      if(e.key==='ArrowDown'){e.preventDefault();sel=(sel+1)%matches.length;render();}
      else if(e.key==='ArrowUp'){e.preventDefault();sel=(sel-1+matches.length)%matches.length;render();}
      else if(e.key==='Enter'){e.preventDefault();
        if(sel>=0&&sel<matches.length){el.value=matches[sel];dd.classList.remove('open');}
      }
      else if(e.key==='Escape'){dd.classList.remove('open');}
    });

    el.addEventListener('blur',function(){setTimeout(function(){dd.classList.remove('open');},150);});
  }

  function wireB(el,dd){
    if(!el)return;
    var sel=-1, matches=[];

    function edgeCount(a,b){
      var cnt=0;
      var fromA=orgADJ[a]||[], fromB=orgADJ[b]||[];
      fromA.forEach(function(e){if(e._other===b)cnt++;});
      fromB.forEach(function(e){if(e._other===a)cnt++;});
      return cnt;
    }

    function render(){
      dd.innerHTML='';
      var a=(elA.value||'').trim();
      matches.forEach(function(m,i){
        var opt=document.createElement('div');
        opt.className='pf-opt'+(i===sel?' selected':'');
        var cnt=a?edgeCount(a,m):adjNeighbors[m]?adjNeighbors[m].length:0;
        opt.innerHTML=U.esc(m)+'<span class="pf-opt-d">'+cnt+'</span>';
        opt.addEventListener('mousedown',function(e){e.preventDefault();el.value=m;dd.classList.remove('open');});
        dd.appendChild(opt);
      });
    }

    function cands(){
      var a=(elA.value||'').trim();
      if(!a||!adjNeighbors[a]||adjNeighbors[a].length===0)return names.slice();
      var nbrs=adjNeighbors[a].slice();
      return nbrs.sort(function(x,y){
        var dx=edgeCount(a,y)-edgeCount(a,x);
        return dx!==0?dx:x.toLowerCase().localeCompare(y.toLowerCase());
      });
    }

    function filterAndShow(q){
      var a=(elA.value||'').trim();
      if(a&&adjNeighbors[a]&&!orgADJ[a]){fetchConnections(a).then(function(){_filterAndShow(q);});return;}
      _filterAndShow(q);
    }
    function _filterAndShow(q){
      var pool=cands();
      matches=pool.filter(function(n){return U.fold(n).indexOf(q||'')!==-1;}).slice(0,20);
      sel=-1;
      render();
      dd.classList.add('open');
    }

    el.addEventListener('input',function(){
      var q=U.fold(this.value);
      if(q.length<1){dd.classList.remove('open');return;}
      filterAndShow(q);
    });

    el.addEventListener('focus',function(){
      if(dd.classList.contains('open'))return;
      filterAndShow(U.fold(this.value));
    });

    el.addEventListener('keydown',function(e){
      if(!dd.classList.contains('open'))return;
      if(e.key==='ArrowDown'){e.preventDefault();sel=(sel+1)%matches.length;render();}
      else if(e.key==='ArrowUp'){e.preventDefault();sel=(sel-1+matches.length)%matches.length;render();}
      else if(e.key==='Enter'){e.preventDefault();
        if(sel>=0&&sel<matches.length){el.value=matches[sel];dd.classList.remove('open');}
      }
      else if(e.key==='Escape'){dd.classList.remove('open');}
    });

    el.addEventListener('blur',function(){setTimeout(function(){dd.classList.remove('open');},150);});

    // Rebuild candidate pool when A changes
    elA.addEventListener('input',function(){
      var q=U.fold(el.value);
      if(!q){dd.classList.remove('open');return;}
      filterAndShow(q);
    });
  }

  function renderRelationshipGroup(edges,label){
    var fam=U.edgeFamily(edges[0]?edges[0].relationship:'other');
    var color=U.FAMILY_COLOR[fam]||U.FAMILY_COLOR.loose;
    var html='<div class="rp-section" style="--rc:'+color+'">';
    html+='<div class="rp-section-title collapsed" onclick="var b=this.nextElementSibling;this.classList.toggle(\'collapsed\');b.classList.toggle(\'collapsed\');">'+U.esc(label)+'<span class="rp-section-count">'+edges.length+'</span></div>';
    html+='<div class="rp-section-body collapsed">';
    edges.forEach(function(e,i){
      var descs=e.descriptions||[], srcs=e.source_urls||[], times=e.time_periods||[];
      times=times.filter(function(t){return t&&t.toLowerCase()!=='unknown';});
      var q=e.evidence_quote;
      var quotes=Array.isArray(q)?q:[q];
      quotes=quotes.filter(function(s){return s&&String(s).trim();});
      var descHtml=descs.length?descs.map(function(d){return U.esc(d);}).join('<br>'):'<em style="color:var(--text-muted);">No description.</em>';
      var srcLine='';
      srcs.forEach(function(u){srcLine+='<div class="ev-srcs"><span class="ev-srcs-label">Source:</span> <a href="'+U.esc(u)+'" target="_blank" rel="noopener nofollow">'+U.esc(U.titleFromUrl(u))+'</a></div>';});
      var tids=[],tPills='';
      var tid='pft-'+Math.random().toString(36).slice(2,10);
      tids.push(tid);
      tPills+='<button type="button" class="ev-toggle t" data-target="'+tid+'"><span>Time</span><span class="chev">▾</span></button>';
      var timeVal=times.length?times.map(U.esc).join(' · '):'Unknown';
      var qids=[],qPills='';
      quotes.forEach(function(q,j){var qid='pfq-'+Math.random().toString(36).slice(2,10);qids.push(qid);qPills+='<button type="button" class="ev-toggle q" data-target="'+qid+'"><span>Quote</span><span class="chev">▾</span></button>';});
      var metaRow=srcLine||tPills||qPills?'<div class="ev-meta">'+srcLine+tPills+qPills+'</div>':'';
      html+='<div class="rp-linkage">';
      html+='<div class="rp-linkage-desc">'+descHtml+'</div>';
      html+=metaRow;
      html+='<div class="fp-reveal" id="'+tids[0]+'" hidden><span class="ev-time">'+timeVal+'</span></div>';
      quotes.forEach(function(q,j){html+='<div class="fp-reveal" id="'+qids[j]+'" hidden><blockquote class="ev-quote">'+U.esc(q)+'</blockquote></div>';});
      html+='</div>';
    });
    html+='</div></div>';
    return html;
  }

  function renderResults(a,b,directSummary){
    var directEdges=orgADJ[a]?orgADJ[a].filter(function(e){return e._other===b;}):[];
    var html='';
    if(directEdges.length===0){
      html+='<div class="pf-msg">No direct connection found.</div>';
    }else{
      if(directEdges.length>0){
        html+='<div class="rs-heading">Relationship Summary</div>';
        html+='<div class="rs-ai-note">AI-generated summary based on the connection details below</div>';
        if(directSummary&&directSummary.summary)html+='<div class="rs-summary">'+U.esc(directSummary.summary)+'</div>';
        var byRel={};
        directEdges.forEach(function(e){var r=e.relationship||'other';(byRel[r]=byRel[r]||[]).push(e);});
        var order=['cooperation','conflict','other'];
        order.forEach(function(r){if(byRel[r])html+=renderRelationshipGroup(byRel[r],U.edgeLabel(r));});
      }
    }
    out.innerHTML=html;
    out.classList.add('open');
    return Promise.resolve();
  }

  function trace(){
    var a=(LOCKED||elA.value).trim(),b=elB.value.trim();
    if(!a||!b){out.innerHTML='<div class="pf-msg">Select two organizations.</div>';out.classList.add('open');return;}
    if(a===b){out.innerHTML='<div class="pf-msg">Same organization.</div>';out.classList.add('open');return;}
    out.innerHTML='<div style="padding:18px 4px">'+U.SPINNER_SVG+'</div>';
    out.classList.add('open');
    loadAdj().then(function(){
      return Promise.all([fetchConnections(a),fetchConnections(b)]);
    }).then(function(){
      return Promise.all([loadEvidence(a),loadEvidence(b)]).then(function(){
        return loadRelationshipSummary(a,b);
      }).then(function(summary){
        renderResults(a,b,summary);
      });
    }).catch(function(err){out.innerHTML='<div class="pf-msg">Error: '+U.esc(err.message)+'</div>';out.classList.add('open');});
  }

  function clearResults(){
    if(!LOCKED)elA.value='';elB.value='';
    out.innerHTML='';out.classList.remove('open');
    reset.setAttribute('hidden','');
  }

  go.addEventListener('click',trace);
  reset.addEventListener('click',clearResults);

  [elA,elB].forEach(function(el){
    if(!el)return;
    el.addEventListener('keydown',function(e){
      if(e.key==='Enter'){e.preventDefault();trace();}
    });
  });

  // Global toggle handler for evidence reveals — matches index.html behavior
  window.pfToggleEv=function(btn){
    var t=document.getElementById(btn.dataset.target);if(!t)return;
    var opening=t.hasAttribute('hidden');
    if(opening)t.removeAttribute('hidden');else t.setAttribute('hidden','');
    btn.classList.toggle('open',opening);
  };

  document.addEventListener('click',function(ev){
    var b=ev.target.closest('.ev-toggle');if(!b)return;
    pfToggleEv(b);
  });

  wireInput(elA,document.getElementById('pf-dd-a'));
  wireB(elB,document.getElementById('pf-dd-b'));

  loadAdj();
})();

// Forward wheel events from panel surface (e.g. header) to the scroll container
document.querySelectorAll('.browse-panel').forEach(function(p){p.addEventListener('wheel',function(e){var s=p.querySelector('.panel-scroll');if(!s||e.target===s||s.contains(e.target))return;if(s.scrollHeight<=s.clientHeight)return;e.preventDefault();s.scrollTop+=e.deltaY;},{passive:false});});

// ── Communities tab ────────────────────────────────────────────────────────────
(function(){
  var U = window.CrimenetUtils;
  var communities=null, loading=false;

  function loadCommunities(){
    if(communities)return Promise.resolve(communities);
    if(loading)return new Promise(function(resolve){
      var check=setInterval(function(){if(communities){clearInterval(check);resolve(communities);}},50);
    });
    loading=true;
    return fetch('data/communities.json').then(function(r){if(!r.ok)throw new Error('communities.json');return r.json();}).then(function(d){
      communities=d.communities||[];loading=false;return communities;
    }).catch(function(err){loading=false;throw err;});
  }

  function renderOrgs(cid, orgs, start){
    start=start||0;
    var batch=10;
    var listEl=document.getElementById('cc-orgs-'+cid);
    if(!listEl)return;
    var slice=orgs.slice(start,start+batch);
    var html='';
    slice.forEach(function(name){
      html+='<a class="cc-org" href="'+U.orgUrl(name)+'">'+U.esc(name)+'</a>';
    });
    listEl.insertAdjacentHTML('beforeend',html);
    if(start+batch>=orgs.length){
      var more=listEl.parentNode.querySelector('.cc-more');
      if(more)more.style.display='none';
    }else{
      var btn=listEl.parentNode.querySelector('.cc-more');
      if(btn)btn.dataset.next=start+batch;
      else{
        btn=document.createElement('button');
        btn.className='cc-more';
        btn.textContent='Show more organizations';
        btn.dataset.next=start+batch;
        btn.dataset.cid=cid;
        var container=listEl.parentNode;
        container.appendChild(btn);
        btn.addEventListener('click',function(e){
          e.preventDefault();
          var next=parseInt(this.dataset.next,10);
          renderOrgs(parseInt(this.dataset.cid,10), orgs, next);
          this.dataset.next=Math.min(next+10,orgs.length);
          if(this.dataset.next>=orgs.length)this.style.display='none';
        });
      }
    }
  }

  function renderCommunities(list){
    var el=document.getElementById('cc-list');
    if(!el)return;

    var html='';
    list.forEach(function(c){
      var shortSummary=c.b||c.m||'';
      var fullSummary=c.m||'';
      html+='<article class="cc-card">';
      html+='<header class="cc-card-head" data-cc="'+c.i+'">';
      html+='<span class="cc-title">'+U.esc(c.t)+'</span>';
      html+='<span class="cc-size">'+c.s+' orgs</span>';
      html+='</header>';
      html+='<div class="cc-card-body collapsed">';
      html+='<p class="cc-short-summary">'+U.esc(shortSummary)+'</p>';
      html+='<p class="cc-summary">'+U.esc(fullSummary)+'</p>';
      html+='<div class="cc-orgs" id="cc-orgs-'+c.i+'"></div>';
      html+='</div>';
      html+='</article>';
    });
    el.innerHTML=html;

    el.querySelectorAll('.cc-card-head').forEach(function(head){
      head.addEventListener('click',function(){
        var card=this.parentNode;
        var body=card.querySelector('.cc-card-body');
        var opening=body.classList.contains('collapsed');
        if(opening){
          body.classList.remove('collapsed');
          this.classList.add('open');
          var cid=parseInt(this.dataset.cc,10);
          var community=communities.find(function(c){return c.i===cid;});
          if(community){
            var fullOrgList=body.querySelector('.cc-orgs');
            if(fullOrgList&&!fullOrgList.children.length){
              renderOrgs(cid,community.o,0);
            }
          }
        }else{
          body.classList.add('collapsed');
          this.classList.remove('open');
        }
      });
    });
  }

  function initCommunities(){
    var el=document.getElementById('cc-list');
    if(!el)return;
    el.innerHTML='<div style="padding:18px 4px;display:flex;align-items:center;gap:11px;color:var(--text-main);font-size:14px">'+U.SPINNER_SVG+' Loading communities…</div>';
    loadCommunities().then(function(list){
      renderCommunities(list);
    }).catch(function(err){
      el.innerHTML='<div class="rp-empty">Failed to load communities.</div>';
    });
  }

  var commBtn=document.querySelector('.browse-toggle-btn[data-view="communities"]');
  if(commBtn){
    commBtn.addEventListener('click',function(){
      if(!communities)initCommunities();
    });
  }

  if(document.getElementById('communities-view').style.display===''){
    initCommunities();
  }
})();

// ── Bridges tab ────────────────────────────────────────────────────────────────
(function(){
  var U = window.CrimenetUtils;
  var bridges=null, loading=false, bridgeNodes=null;

  function loadBridges(){
    if(bridges)return Promise.resolve(bridges);
    if(loading)return new Promise(function(resolve){
      var check=setInterval(function(){if(bridges){clearInterval(check);resolve(bridges);}},50);
    });
    loading=true;
    return fetch('data/bridges.json').then(function(r){if(!r.ok)throw new Error('bridges.json');return r.json();}).then(function(d){
      bridges=d;bridgeNodes=d.bridge_nodes||[];loading=false;return bridges;
    }).catch(function(err){loading=false;throw err;});
  }

  function renderTags(cid, tags, start){
    start=start||0;
    var batch=8;
    var listEl=document.getElementById('br-tags-'+cid);
    if(!listEl)return;
    var slice=tags.slice(start,start+batch);
    var html='';
    slice.forEach(function(label){
      html+='<span class="br-comm-tag">'+U.esc(label)+'</span>';
    });
    listEl.insertAdjacentHTML('beforeend',html);
    if(start+batch>=tags.length){
      var more=listEl.parentNode.querySelector('.cc-more');
      if(more)more.style.display='none';
    }else{
      var btn=listEl.parentNode.querySelector('.cc-more');
      if(btn)btn.dataset.next=start+batch;
      else{
        btn=document.createElement('button');
        btn.className='cc-more';
        btn.textContent='Show more communities';
        btn.dataset.next=start+batch;
        btn.dataset.cid=cid;
        var container=listEl.parentNode;
        container.appendChild(btn);
        btn.addEventListener('click',function(e){
          e.preventDefault();
          var next=parseInt(this.dataset.next,10);
          renderTags(parseInt(this.dataset.cid,10), tags, next);
          this.dataset.next=Math.min(next+8,tags.length);
          if(this.dataset.next>=tags.length)this.style.display='none';
        });
      }
    }
  }

  function renderBridgeNodes(nodes){
    var el=document.getElementById('br-list');
    if(!el)return;

    var html='';
    nodes.forEach(function(n){
      html+='<article class="cc-card">';
      html+='<header class="cc-card-head" data-br="'+n.r+'">';
      html+='<span class="cc-title">'+U.esc(n.n)+'</span>';
      html+='<span class="cc-size">'+n.x+' cross-edges / '+n.b+' communities</span>';
      html+='</header>';
      html+='<div class="cc-card-body collapsed">';
      html+='<p class="cc-summary">'+n.x+' cross-community cooperation ties reaching '+n.b+' distinct communities. Based in '+U.esc(n.c||'unknown country')+'.</p>';
      html+='<div class="cc-orgs" id="br-tags-'+n.r+'"></div>';
      html+='</div>';
      html+='</article>';
    });
    el.innerHTML=html;

    el.querySelectorAll('.cc-card-head').forEach(function(head){
      head.addEventListener('click',function(){
        var card=this.parentNode;
        var body=card.querySelector('.cc-card-body');
        var opening=body.classList.contains('collapsed');
        if(opening){
          body.classList.remove('collapsed');
          this.classList.add('open');
          var rid=parseInt(this.dataset.br,10);
          var node=bridgeNodes.find(function(n){return n.r===rid;});
          if(node){
            var tagList=body.querySelector('#br-tags-'+rid);
            if(tagList&&!tagList.children.length){
              renderTags(rid, node.d, 0);
            }
          }
        }else{
          body.classList.add('collapsed');
          this.classList.remove('open');
        }
      });
    });
  }

  function initBridges(){
    var el=document.getElementById('br-list');
    if(!el)return;
    el.innerHTML='<div style="padding:18px 4px;display:flex;align-items:center;gap:11px;color:var(--text-main);font-size:14px">'+U.SPINNER_SVG+' Loading bridges…</div>';
    loadBridges().then(function(data){
      renderBridgeNodes(bridgeNodes);
    }).catch(function(err){
      el.innerHTML='<div class="rp-empty">Failed to load bridges data.</div>';
    });
  }

  var brBtn=document.querySelector('.browse-toggle-btn[data-view="bridges"]');
  if(brBtn){
    brBtn.addEventListener('click',function(){
      if(!bridges)initBridges();
    });
  }

  if(document.getElementById('bridges-view').style.display===''){
    initBridges();
  }
})();

// ── Triadic Signals tab ────────────────────────────────────────────────────────
(function(){
  var U = window.CrimenetUtils;
  var sigData=null, loading=false;

  function loadSigData(){
    if(sigData)return Promise.resolve(sigData);
    if(loading)return new Promise(function(resolve){
      var check=setInterval(function(){if(sigData){clearInterval(check);resolve(sigData);}},50);
    });
    loading=true;
    return fetch('data/triadic_signals.json').then(function(r){if(!r.ok)throw new Error('triadic_signals.json');return r.json();}).then(function(d){
      sigData=d;loading=false;return sigData;
    }).catch(function(err){loading=false;throw err;});
  }

  var currentSignal='all';
  var currentSort={col:'total_weighted',dir:'desc'};
  var sigPageSize=100, sigVisible=100;

  var SIG_COLUMNS=[
    {key:'rank',label:'#',sortable:true},
    {key:'signal',label:'Signal',sortable:true},
    {key:'a',label:'Org A',sortable:true},
    {key:'b',label:'Org B',sortable:true},
    {key:'cp_count',label:'CP Count',sortable:true},
    {key:'adv_count',label:'CA Count',sortable:true},
    {key:'total_weighted',label:'Total Score',sortable:true},
    {key:'cp_items',label:'Common Partners',sortable:false},
    {key:'adv_items',label:'Common Adversaries',sortable:false}
  ];

  function sigValue(r,key){
    var v=r[key];
    if(key==='signal')return CrimenetTriadic.signalTag(r);
    if(key==='cp_items'||key==='adv_items')return'<div class="sig-items">'+CrimenetTriadic.formatItems(v)+'</div>';
    if(key==='a'||key==='b')return'<span class="sig-org-name">'+U.esc(v)+'</span>';
    return String(v!=null?v:'');
  }

  function sigCellClass(key,v){
    if(key==='cp_count'||key==='adv_count'){
      return v>0?'sig-val-count':'sig-val-zero';
    }
    if(key==='total_weighted')return'sig-val-score';
    return'';
  }

  function getSigRows(){
    return CrimenetTriadic.filterAndSort(sigData,currentSignal,currentSort.col,currentSort.dir);
  }

  function renderSigTable(){
    var rows=getSigRows();
    var html='<div class="sig-filters">';
    html+='<span class="sig-filters-label">Signal:</span>';
    var pills=[{key:'all',label:'All',count:sigData.length},{key:'both',label:'Both',count:1609},{key:'cp',label:'Only Common Partners',count:486},{key:'sa',label:'Only Common Adversaries',count:466}];
    pills.forEach(function(p){
      html+='<button class="sig-pill'+(p.key===currentSignal?' active':'')+'" data-sig="'+p.key+'">'+p.label+' <span class="sig-pill-count">'+p.count+'</span></button>';
    });
    html+='</div>';

    html+='<div class="sig-table-wrap"><table class="sig-table" id="sig-table"><thead><tr>';
    SIG_COLUMNS.forEach(function(c){
      var cls=c.key===currentSort.col?(currentSort.dir==='asc'?'sort-asc':'sort-desc'):'';
      html+='<th data-col="'+c.key+'" class="'+cls+'">'+c.label+'</th>';
    });
    html+='</tr></thead><tbody>';

    for(var i=0;i<rows.length&&i<sigVisible;i++){
      var r=rows[i];
      html+='<tr>';
      for(var j=0;j<SIG_COLUMNS.length;j++){
        var c=SIG_COLUMNS[j];
        var v=r[c.key];
        html+='<td class="'+sigCellClass(c.key,v)+'">'+sigValue(r,c.key)+'</td>';
      }
      html+='</tr>';
    }
    html+='</tbody></table></div>';
    if(sigVisible<rows.length){
      html+='<button type="button" class="sig-load-more" id="sig-load-more">Load more ('+(rows.length-sigVisible)+' remaining)</button>';
    }
    return html;
  }

  function renderSig(){
    var el=document.getElementById('sig-list');
    if(!el)return;
    el.innerHTML=renderSigTable();

    el.querySelectorAll('.sig-pill').forEach(function(btn){
      btn.addEventListener('click',function(){
        el.querySelectorAll('.sig-pill').forEach(function(b){b.classList.remove('active');});
        btn.classList.add('active');
        currentSignal=btn.getAttribute('data-sig');
        currentSort={col:'total_weighted',dir:'desc'};
        sigVisible=sigPageSize;
        renderSig();
      });
    });

    var thead=el.querySelector('#sig-table thead');
    thead.addEventListener('click',function(e){
      var th=e.target.closest('th[data-col]');
      if(!th)return;
      var col=th.getAttribute('data-col');
      var def=null;
      for(var i=0;i<SIG_COLUMNS.length;i++){if(SIG_COLUMNS[i].key===col){def=SIG_COLUMNS[i];break;}}
      if(!def||!def.sortable)return;
      if(currentSort.col===col){currentSort.dir=currentSort.dir==='asc'?'desc':'asc';}
      else{currentSort.col=col;currentSort.dir='desc';}
      renderSig();
    });

    var lm=el.querySelector('#sig-load-more');
    if(lm){
      lm.addEventListener('click',function(){
        sigVisible+=sigPageSize;
        renderSig();
      });
    }
  }

  function initSignals(){
    var el=document.getElementById('sig-list');
    if(!el)return;
    el.innerHTML='<div style="padding:18px 4px;display:flex;align-items:center;gap:11px;color:var(--text-main);font-size:14px">'+U.SPINNER_SVG+' Loading triadic signals…</div>';
    loadSigData().then(function(){
      renderSig();
    }).catch(function(err){
      el.innerHTML='<div class="rp-empty">Failed to load triadic signals data.</div>';
    });
  }

  var sigBtn=document.querySelector('.browse-toggle-btn[data-view="signals"]');
  if(sigBtn){
    sigBtn.addEventListener('click',function(){
      if(!sigData)initSignals();
    });
  }

  if(document.getElementById('signals-view').style.display===''){
    initSignals();
  }
})();
