// ── Ask CRIMENET AI tab ──────────────────────────────────────────────────
(function(){
  var U = window.CrimenetUtils;
  var esc = U.esc, fold = U.fold, titleFromUrl = U.titleFromUrl;
  var initialized = false;
  var MAX_ITERATIONS = 8;

  // Strip any HTML tags that leaked from the LLM (it's told not to, but safety net)
  // First removes entire <details> blocks (tags + content), then any
  // stray tags that survived. This prevents the LLM's old-format evidence
  // text from leaking into the response alongside the app-generated section.
  function stripHtmlTags(t){
    // Kill full <details>...</details> blocks including their inner content
    t=(t||'').replace(/<details[^>]*>[\s\S]*?<\/details>/gi,'');
    // Kill full <summary>...</summary> blocks
    t=t.replace(/<summary[^>]*>[\s\S]*?<\/summary>/gi,'');
    // Kill any remaining bare tags
    t=t.replace(/<\/?details[^>]*>/gi,'').replace(/<\/?summary[^>]*>/gi,'')
         .replace(/<\/?div[^>]*>/gi,'').replace(/<\/?p[^>]*>/gi,'')
         .replace(/<\/?span[^>]*>/gi,'').replace(/<\/?br\s*\/?>/gi,'');
    return t;
  }

  function renderMarkdown(t){
    // Block-level: split into paragraphs/lines, process heading / hr / table / list
    // then join, then process inline formatting.
    var lines=(t||'').split('\n');
    var out=[], inTable=false, inList=false, tableHtml='', listHtml='';

    function flushTable(){
      if(!inTable)return;
      var rows=tableHtml.trim().split('\n');
      if(rows.length<2){tableHtml='';inTable=false;return;}
      // First row = header, second = separator, rest = body
      var hdr=rows[0], sep=rows[1], bodyRows=rows.slice(2);
      // If second row isn't a separator, treat whole thing as non-table
      if(!/^\|?[\s\-:|]+\|?$/.test(sep)){out.push('<p>'+esc(rows.join('<br>'))+'</p>');tableHtml='';inTable=false;return;}
      // Build table
      var html='<table class="ask-table"><thead><tr>';
      var hdrCells=hdr.split('|').filter(function(c){return c.trim();});
      for(var i=0;i<hdrCells.length;i++){html+='<th>'+renderInline(hdrCells[i].trim())+'</th>';}
      html+='</tr></thead><tbody>';
      for(var j=0;j<bodyRows.length;j++){
        var cells=bodyRows[j].split('|').filter(function(c){return c.trim();});
        if(cells.length===0)continue;
        html+='<tr>';
        for(var k=0;k<cells.length;k++){html+='<td>'+renderInline(cells[k].trim())+'</td>';}
        html+='</tr>';
      }
      html+='</tbody></table>';
      out.push(html);
      tableHtml='';inTable=false;
    }

    function flushList(){
      if(!inList)return;
      out.push('<ul>'+listHtml+'</ul>');
      listHtml='';inList=false;
    }

    for(var i=0;i<lines.length;i++){
      var raw=lines[i], trimmed=raw.trim();

      // Empty line → flush blocks
      if(!trimmed){flushTable();flushList();out.push('');continue;}

      // Heading
      var hdrMatch=trimmed.match(/^(#{1,4})\s+(.+)$/);
      if(hdrMatch&&!inTable&&!inList){
        flushTable();flushList();
        var lvl=hdrMatch[1].length;
        out.push('<h'+lvl+' class="ask-h">'+renderInline(hdrMatch[2])+'</h'+lvl+'>');
        continue;
      }

      // Horizontal rule
      if(/^\-{3,}$/.test(trimmed)||/^\*{3,}$/.test(trimmed)){
        flushTable();flushList();
        out.push('<hr class="ask-hr">');
        continue;
      }

      // Table row detection
      if(trimmed.indexOf('|')>=0){
        if(!inTable){flushList();inTable=true;}
        tableHtml+=raw+'\n';
        continue;
      }else{flushTable();}

      // List item
      if(/^[\-\*]\s+/.test(trimmed)){
        if(!inList){flushTable();inList=true;}
        var itemText=trimmed.replace(/^[\-\*]\s+/,'');
        listHtml+='<li>'+renderInline(itemText)+'</li>';
        continue;
      }else{flushList();}

      // Regular paragraph line
      out.push('<p>'+renderInline(trimmed)+'</p>');
    }
    flushTable();flushList();

    return out.join('\n');
  }

  function renderInline(s){
    s=esc(s);
    // Links [text](url)
    s=s.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g,'<a href="$2" target="_blank" rel="noopener nofollow">$1</a>');
    // Bold **text**
    s=s.replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>');
    // Italic *text* (but not inside words with **)
    s=s.replace(/(^|[^*])\*([^*]+)\*($|[^*])/g,'$1<em>$2</em>$3');
    // Backtick code
    s=s.replace(/`([^`]+)`/g,'<code>$1</code>');
    return s;
  }

  function simpleMarkdown(t){
    return renderMarkdown(t);
  }

  // ── Data loading ──────────────────────────────────────────────────────
  var compactData=null, adjData=null, communitiesData=null;
  var adjNeighbors={};
  var triadicData=null, bridgesData=null, centralityData=null;

  var loadEvidence = U.evidenceLoader();
  var loadRelationshipSummary = U.summaryLoader();
  var dataReady;

  function loadCompact(){
    if(compactData)return Promise.resolve(compactData);
    return fetch('data/compact.json').then(function(r){
      if(!r.ok)throw new Error('compact');
      return r.json();
    }).then(function(d){compactData=d;return d;}).catch(function(err){console.error('Failed to load compact:', err);throw err;});
  }

  function loadAdj(){
    if(adjData)return Promise.resolve(adjData);
    return fetch('data/crimenet_adj.json').then(function(r){
      if(!r.ok)throw new Error('adj');
      return r.json();
    }).then(function(d){
      adjData=d;
      for(var k in d){if(d.hasOwnProperty(k))adjNeighbors[k]=d[k].map(function(e){return e.t;});}
      return d;
    }).catch(function(err){console.error('Failed to load adjacency:', err);throw err;});
  }

  function loadCommunitiesData(){
    if(communitiesData)return Promise.resolve(communitiesData);
    return fetch('data/communities.json').then(function(r){
      if(!r.ok)throw new Error('communities');
      return r.json();
    }).then(function(d){communitiesData=d.communities||[];return communitiesData;}).catch(function(err){console.error('Failed to load communities:', err);throw err;});
  }

  function loadTriadicSignals(){
    if(triadicData)return Promise.resolve(triadicData);
    return fetch('data/triadic_signals.json').then(function(r){
      if(!r.ok)throw new Error('triadic');
      return r.json();
    }).then(function(d){triadicData=d;return d;}).catch(function(err){console.error('Failed to load triadic signals:', err);throw err;});
  }

  function loadBridgesData(){
    if(bridgesData)return Promise.resolve(bridgesData);
    return fetch('data/bridges.json').then(function(r){
      if(!r.ok)throw new Error('bridges');
      return r.json();
    }).then(function(d){bridgesData=d.bridge_nodes||[];return bridgesData;}).catch(function(err){console.error('Failed to load bridges:', err);throw err;});
  }

  function loadCentralityData(){
    if(centralityData)return Promise.resolve(centralityData);
    return fetch('data/centrality.json').then(function(r){
      if(!r.ok)throw new Error('centrality');
      return r.json();
    }).then(function(d){centralityData=d.orgs||[];return centralityData;}).catch(function(err){console.error('Failed to load centrality:', err);throw err;});
  }

  // ── Continent mapping for footprint summaries ────────────────────────
  var CONTINENT_MAP={
    'Albania':'Europe','Andorra':'Europe','Austria':'Europe','Belarus':'Europe',
    'Belgium':'Europe','Bosnia and Herzegovina':'Europe','Bulgaria':'Europe',
    'Croatia':'Europe','Cyprus':'Europe','Czech Republic':'Europe','Denmark':'Europe',
    'Estonia':'Europe','Finland':'Europe','France':'Europe','Germany':'Europe',
    'Greece':'Europe','Hungary':'Europe','Iceland':'Europe','Ireland':'Europe',
    'Italy':'Europe','Kosovo':'Europe','Latvia':'Europe','Liechtenstein':'Europe',
    'Lithuania':'Europe','Luxembourg':'Europe','Malta':'Europe','Moldova':'Europe',
    'Monaco':'Europe','Montenegro':'Europe','Netherlands':'Europe',
    'North Macedonia':'Europe','Norway':'Europe','Poland':'Europe','Portugal':'Europe',
    'Romania':'Europe','Russia':'Europe','San Marino':'Europe','Serbia':'Europe',
    'Slovakia':'Europe','Slovenia':'Europe','Spain':'Europe','Sweden':'Europe',
    'Switzerland':'Europe','Ukraine':'Europe','United Kingdom':'Europe',
    'Vatican City':'Europe',
    'Canada':'North America','Mexico':'North America','United States':'North America',
    'Belize':'North America','Costa Rica':'North America','Cuba':'North America',
    'Dominican Republic':'North America','El Salvador':'North America',
    'Guatemala':'North America','Haiti':'North America','Honduras':'North America',
    'Jamaica':'North America','Nicaragua':'North America','Panama':'North America',
    'Puerto Rico':'North America','Trinidad and Tobago':'North America',
    'Argentina':'South America','Bolivia':'South America','Brazil':'South America',
    'Chile':'South America','Colombia':'South America','Ecuador':'South America',
    'Guyana':'South America','Paraguay':'South America','Peru':'South America',
    'Suriname':'South America','Uruguay':'South America','Venezuela':'South America',
    'Afghanistan':'Asia','Armenia':'Asia','Azerbaijan':'Asia','Bahrain':'Asia',
    'Bangladesh':'Asia','Bhutan':'Asia','Brunei':'Asia','Cambodia':'Asia',
    'China':'Asia','Georgia':'Asia','India':'Asia','Indonesia':'Asia','Iran':'Asia',
    'Iraq':'Asia','Israel':'Asia','Japan':'Asia','Jordan':'Asia','Kazakhstan':'Asia',
    'Kuwait':'Asia','Kyrgyzstan':'Asia','Laos':'Asia','Lebanon':'Asia','Malaysia':'Asia',
    'Mongolia':'Asia','Myanmar':'Asia','Nepal':'Asia','North Korea':'Asia',
    'Oman':'Asia','Pakistan':'Asia','Palestine':'Asia','Philippines':'Asia',
    'Qatar':'Asia','Saudi Arabia':'Asia','Singapore':'Asia','South Korea':'Asia',
    'Sri Lanka':'Asia','Syria':'Asia','Taiwan':'Asia','Tajikistan':'Asia',
    'Thailand':'Asia','Timor-Leste':'Asia','Turkey':'Asia','Turkmenistan':'Asia',
    'United Arab Emirates':'Asia','Uzbekistan':'Asia','Vietnam':'Asia','Yemen':'Asia',
    'Algeria':'Africa','Angola':'Africa','Benin':'Africa','Botswana':'Africa',
    'Burkina Faso':'Africa','Burundi':'Africa','Cameroon':'Africa','Cape Verde':'Africa',
    'Central African Republic':'Africa','Chad':'Africa','Comoros':'Africa',
    'Congo':'Africa',"Côte d'Ivoire":'Africa','Democratic Republic of the Congo':'Africa',
    'Djibouti':'Africa','Egypt':'Africa','Equatorial Guinea':'Africa',
    'Eritrea':'Africa','Eswatini':'Africa','Ethiopia':'Africa','Gabon':'Africa',
    'Gambia':'Africa','Ghana':'Africa','Guinea':'Africa','Guinea-Bissau':'Africa',
    'Kenya':'Africa','Lesotho':'Africa','Liberia':'Africa','Libya':'Africa',
    'Madagascar':'Africa','Malawi':'Africa','Mali':'Africa','Mauritania':'Africa',
    'Mauritius':'Africa','Morocco':'Africa','Mozambique':'Africa','Namibia':'Africa',
    'Niger':'Africa','Nigeria':'Africa','Rwanda':'Africa','Senegal':'Africa',
    'Seychelles':'Africa','Sierra Leone':'Africa','Somalia':'Africa',
    'South Africa':'Africa','South Sudan':'Africa','Sudan':'Africa','Tanzania':'Africa',
    'Togo':'Africa','Tunisia':'Africa','Uganda':'Africa','Zambia':'Africa',
    'Zimbabwe':'Africa',
    'Australia':'Oceania','Fiji':'Oceania','New Zealand':'Oceania',
    'Papua New Guinea':'Oceania','Samoa':'Oceania','Tonga':'Oceania','Vanuatu':'Oceania'
  };

  function continentFor(country){
    var c=CONTINENT_MAP[country];
    return c||'Other';
  }

  function buildFootprintSummary(countryLinks, homeCountry){
    // Group country_links by continent, excluding home country.
    // Returns {total, by_continent: {continent: {count, countries: [...]}}}
    var seen={};
    var byCont={};
    for(var i=0;i<countryLinks.length;i++){
      var c=countryLinks[i].country;
      if(c===homeCountry)continue; // home country is not a footprint
      if(seen[c])continue;
      seen[c]=true;
      var cont=continentFor(c);
      if(!byCont[cont])byCont[cont]={count:0,countries:[]};
      byCont[cont].count++;
      byCont[cont].countries.push(c);
    }
    var total=0;
    var contKeys=Object.keys(byCont).sort();
    for(var j=0;j<contKeys.length;j++){
      var entry=byCont[contKeys[j]];
      entry.countries.sort();
      total+=entry.count;
    }
    // Order by count descending
    contKeys.sort(function(a,b){return byCont[b].count-byCont[a].count;});
    var ordered={};
    for(var k=0;k<contKeys.length;k++){
      ordered[contKeys[k]]=byCont[contKeys[k]];
    }
    return {total:total,by_continent:ordered};
  }

  function bestMatch(query){
    if(!compactData)return null;
    var q=fold(query);
    var orgs=compactData.orgs||{};
    for(var name in orgs){
      if(!orgs.hasOwnProperty(name))continue;
      if(fold(name)===q)return name;
    }
    for(var name in orgs){
      if(!orgs.hasOwnProperty(name))continue;
      if(fold(name).indexOf(q)===0)return name;
    }
    var best='';
    for(var name in orgs){
      if(!orgs.hasOwnProperty(name))continue;
      if(fold(name).indexOf(q)>=0){if(!best||name.length<best.length)best=name;}
    }
    if(!best){
      for(var name in orgs){
        var o=orgs[name];if(!o||!o.aliases)continue;
        for(var i=0;i<o.aliases.length;i++){
          if(fold(o.aliases[i])===q||fold(o.aliases[i]).indexOf(q)>=0){
            if(!best||name.length<best.length){best=name;break;}
          }
        }
        if(best)break;
      }
    }
    return best||null;
  }

  // ── Tool schemas ──────────────────────────────────────────────────────
  var TOOLS=[
    {type:'function',function:{name:'get_organization',
      description:'Look up a SPECIFIC organization by its name or alias. Returns: standard_name, aliases, description, country, time_period, is_defunct, founded_year, dissolved_year, degree, profiled flag, network_rank (degree/betweenness/pagerank each with value and rank out of ~3,500 connected orgs — gives immediate comparative context), country_links (footprints with country, context, quote, source_url, source_title), footprint_summary (total count plus breakdown by continent with country names — always use its exact counts, never count from country_links yourself), and sources (own_sources or mentioned_in fallback). Use this when the user asks about a named organization (“Tell me about the Sinaloa Cartel”, “Who are CJNG?”). For a complete profile, also call get_connections and get_community. Returns up to 15 results ranked by match quality and network degree.',
      parameters:{type:'object',properties:{query:{type:'string',description:'Organization name to look up'},is_defunct:{type:'boolean',description:'Optional filter: true returns only defunct orgs, false returns only active orgs. Omit to return both.'}},required:['query']}}},
    {type:'function',function:{name:'find_by_type',
      description:'Find organizations by CATEGORY, TYPE, or KEYWORD — not a specific name. Searches across names, aliases, and descriptions. Use this when the user asks about a kind of group ("Russian mafia", "motorcycle clubs", "political crime groups", "paramilitaries", "women-led cartels") or any descriptive trait. Also use as a fallback after get_organization returns empty for a category query. Returns up to 25 results.',
      parameters:{type:'object',properties:{keyword:{type:'string',description:'Category, type, or keyword to find (e.g. "russian", "motorcycle", "political")'},is_defunct:{type:'boolean',description:'Optional filter: true returns only defunct orgs, false returns only active orgs. Omit to return both.'}},required:['keyword']}}},
    {type:'function',function:{name:'get_connections',
      description:'Get all connections for an organization, optionally filtered by relationship type (cooperation/conflict/other), or get connections between two specific organizations. Every connection includes relationship types, evidence quote, time period, and source URLs. Returns type_counts (object with cooperation/conflict/other keys) and connections_total — always quote these exact numbers, never estimate from your own count.',
      parameters:{type:'object',properties:{
        organization:{type:'string',description:'Organization name'},
        relationship_type:{type:'string',enum:['cooperation','conflict','other'],description:'Optional filter by relationship type'},
        target_organization:{type:'string',description:'Optional second organization to find connections between these two specifically'}
      },required:['organization']}}},
    {type:'function',function:{name:'get_relationship_summary',
      description:'Get the pre-written LLM relationship summary between two specific organizations. Provides a rich narrative paragraph synthesizing all documented interactions between them. Use this when the user asks how two specific orgs relate to each other.',
      parameters:{type:'object',properties:{
        organization_a:{type:'string',description:'First organization name'},
        organization_b:{type:'string',description:'Second organization name'}
      },required:['organization_a','organization_b']}}},
    {type:'function',function:{name:'find_by_country',
      description:'Find organizations associated with a country. Returns orgs based there (headquartered) and orgs with operational footprints there (active but not headquartered).',
      parameters:{type:'object',properties:{country:{type:'string',description:'Country name'}},required:['country']}}},
    {type:'function',function:{name:'find_by_countries',
      description:'Find organizations that have a documented footprint in ALL of the listed countries. Returns only orgs active in every country specified. Use this to answer questions like "which organizations operate in both X and Y?" or "which orgs have a presence across multiple countries?" Pass at least 2 countries.',
      parameters:{type:'object',properties:{countries:{type:'array',items:{type:'string'},description:'List of country names. Returns orgs with a documented footprint in ALL of them.'}},required:['countries']}}},
    {type:'function',function:{name:'find_paths',
      description:'Find shortest paths between two organizations through the network. Shows how two orgs are connected through intermediate organizations.',
      parameters:{type:'object',properties:{
        from_organization:{type:'string',description:'Starting organization'},
        to_organization:{type:'string',description:'Target organization'},
        max_hops:{type:'integer',description:'Maximum path length (2-5)',default:3}
      },required:['from_organization','to_organization']}}},
    {type:'function',function:{name:'find_cooperation_routes',
      description:'Find cooperation routes between two organizations. First checks if there is a direct cooperation edge. If none exists, finds up to 3 shortest cooperation-only paths (where every hop is a cooperation relationship). Use this when the user asks whether two orgs cooperate, are allied, or work together. Only follows cooperation edges, never conflict or other.',
      parameters:{type:'object',properties:{
        organization_a:{type:'string',description:'First organization name'},
        organization_b:{type:'string',description:'Second organization name'}
      },required:['organization_a','organization_b']}}},
    {type:'function',function:{name:'get_network_neighborhood',
      description:'Get the local network around an organization. Returns direct connections each with a types array (cooperation/conflict/other), plus type_counts. Top second-degree connections include hop-by-hop relationship types: each via-org shows types (1st hop) and target_relationship (2nd hop). Use with relationship_type: "cooperation" to scope both hops to allies-of-allies.',
      parameters:{type:'object',properties:{
        organization:{type:'string',description:'Organization name'},
        relationship_type:{type:'string',enum:['cooperation','conflict','other'],description:'Optional filter to only follow edges of this type at the first hop'}
      },required:['organization']}}},
    {type:'function',function:{name:'get_community',
      description:'Get a community or list all communities. Three modes: (1) no arguments — list all communities, each with id, title, short_summary (one sentence for scanning), size, and top_hubs (top 5). Member lists are NOT included in list mode to keep the response compact. Full summaries are also NOT included in list mode. (2) community_id — get a single community by its numeric id, with the full summary, title, short_summary, size, top_hubs, and full member list. (3) organization — find which community a specific org belongs to, with the full summary and full member list. Always use mode (1) first to scan communities with short_summary; then use mode (2) with community_id to read the full summary and members of the one(s) you focus on. Pass the keyword parameter to return only communities whose member names, title, or summary contain the keyword. Use keyword to search for a specific org name or term inside communities — never scan member lists yourself, let the tool filter for you.',
      parameters:{type:'object',properties:{organization:{type:'string',description:'Organization name to find its community. Mutually exclusive with community_id and keyword.'},community_id:{type:'integer',description:'Numeric community id to fetch a single community with its full summary. Use after scanning the list to drill into a specific community.'},keyword:{type:'string',description:'Filter communities: return only those whose member names, title, or summary contain this keyword (case-insensitive). In list-all mode only (ignored with community_id or organization).'}},required:[]}}},
    {type:'function',function:{name:'get_triadic_signals',
      description:'Get triadic signals for an organization: pairs where this org and another org share multiple common cooperation partners or common adversaries but have no direct edge between them. These are candidate undocumented relationships. High-scoring pairs are more likely to have a real-world connection not yet documented in Wikipedia.',
      parameters:{type:'object',properties:{
        organization:{type:'string',description:'Organization name'},
        signal_type:{type:'string',enum:['cooperation','adversaries','both'],description:'Filter by signal type: cooperation (shared partners), adversaries (shared enemies), or both'},
        min_score:{type:'integer',description:'Minimum total score filter (default 5)'}
      },required:['organization']}}},
    {type:'function',function:{name:'get_bridges',
      description:'Get bridge organizations that span multiple communities. These are highly-connected orgs with ties across different criminal ecosystems. Use this when asked about which organizations connect different criminal networks or worlds.',
      parameters:{type:'object',properties:{
        min_communities:{type:'integer',description:'Minimum number of communities spanned (default 3)'}
      },required:[]}}},
    {type:'function',function:{name:'get_centrality',
      description:'Get network centrality rankings and metrics for organizations. Supports three metrics: degree (number of direct connections, raw popularity), betweenness (how often an org sits on shortest paths, bridging power), and PageRank (importance weighted by neighbor quality). Use for questions about "most important", "most connected", "most powerful", "most influential", "most central", or "most popular" organizations. Can return global top-N or metrics for one specific org. IMPORTANT: only connected orgs have a centrality score, so this ranks fewer orgs than the full database. The response returns BOTH connected_orgs_ranked and total_orgs_in_database — quote total_orgs_in_database (not the ranked count) whenever you state how many organizations CRIMENET holds, and never present the ranked count as the database size. Also supports filtering by relationship composition: use min_conflicts to find orgs with at least N conflict edges, and max_cooperation to cap cooperation edges (set to 0 to find orgs that have only conflicts and no cooperation).',
      parameters:{type:'object',properties:{
        metric:{type:'string',enum:['degree','betweenness','pagerank'],description:'Which centrality metric to use. degree = raw connection count. betweenness = bridging importance (gatekeeper role). pagerank = weighted importance (quality over quantity of connections).'},
        organization:{type:'string',description:'Optional: get metrics for a specific organization instead of the global ranking.'},
        limit:{type:'integer',description:'Number of top orgs to return (default 20, max 50). Ignored when organization is set.'},
        country:{type:'string',description:'Optional: filter the ranking to orgs from a specific country.'},
        min_conflicts:{type:'integer',description:'Optional: only return orgs with at least this many conflict edges.'},
        max_cooperation:{type:'integer',description:'Optional: only return orgs with at most this many cooperation edges. Set to 0 to exclude orgs that have any cooperation.'}
      },required:['metric']}}}
  ];

  // ── Tool implementations ──────────────────────────────────────────────

  function tool_get_organization(args){
    var query=args.query||'', isDefunctFilter=args.hasOwnProperty('is_defunct')?args.is_defunct:null;
    if(!compactData)return JSON.stringify({error:'Data not loaded'});
    var q=fold(query);
    var orgs=compactData.orgs||{}, matches=[], cap=isDefunctFilter!==null?100:15;
    for(var name in orgs){
      if(!orgs.hasOwnProperty(name))continue;
      var o=orgs[name], match=false;
      if(fold(name).indexOf(q)>=0){match=true;}
      else if(o.aliases){for(var i=0;i<o.aliases.length;i++){if(fold(o.aliases[i]).indexOf(q)>=0){match=true;break;}}}
      if(!match)continue;
      // Apply is_defunct filter
      if(isDefunctFilter===true&&o.is_defunct!==true)continue;
      if(isDefunctFilter===false&&o.is_defunct!==false)continue;
      var entry={standard_name:name,aliases:o.aliases||[],description:o.description||null,country:o.country||null,time_period:o.time_period||null,is_defunct:o.is_defunct||'unknown',founded_year:o.founded_year||null,dissolved_year:o.dissolved_year||null,degree:o.degree||0,profiled:o.profiled===true};
      // Attach centrality rank data if available — gives the LLM comparative context
      // without requiring a separate get_centrality call.
      if(centralityData){
        for(var ci=0;ci<centralityData.length;ci++){
          if(centralityData[ci].n===name){
            entry.network_rank={
              degree:{value:centralityData[ci].d,rank:centralityData[ci].dr},
              betweenness:{value:centralityData[ci].b,rank:centralityData[ci].br},
              pagerank:{value:centralityData[ci].p,rank:centralityData[ci].pr},
              out_of:centralityData.length
            };
            break;
          }
        }
      }
      if(o.country_links&&o.country_links.length){
        entry.country_links=o.country_links.map(function(cl){return {country:cl.country,context:cl.context,quote:cl.evidence_quote,source_url:cl.source_url||null,source_title:cl.source_title||null};});
        entry.footprint_summary=buildFootprintSummary(o.country_links, o.country);
      }
      if(o.own_sources&&o.own_sources.length)entry.sources=o.own_sources.slice(0,5).map(function(s){return {title:s.title,url:s.url};});
      if(!entry.sources&&o.mentioned_in&&o.mentioned_in.length)entry.sources=o.mentioned_in.slice(0,5).map(function(s){return {title:s.title,url:s.url};});
      matches.push(entry);
      if(matches.length>=cap)break;
    }
    matches.sort(function(a,b){return(b.description?1:0)-(a.description?1:0)||b.degree-a.degree;});
    if(isDefunctFilter!==null){matches.sort(function(a,b){return(b.is_defunct===true?1:0)-(a.is_defunct===true?1:0)||(b.description?1:0)-(a.description?1:0)||b.degree-a.degree;});}
    if(matches.length===0){var hint='No organizations found matching "'+query+'"';if(isDefunctFilter===true)hint+=' that are defunct.';else if(isDefunctFilter===false)hint+=' that are still active.';else hint+='. Try a different spelling or a shorter query.';return JSON.stringify({matches:[],hint:hint});}
    return JSON.stringify({matches:matches,total_found:matches.length});
  }

  function tool_find_by_type(args){
    var keyword=args.keyword||'', isDefunctFilter=args.hasOwnProperty('is_defunct')?args.is_defunct:null;
    if(!compactData)return JSON.stringify({error:'Data not loaded'});
    var q=fold(keyword);
    var orgs=compactData.orgs||{}, matches=[], cap=isDefunctFilter!==null?100:25;
    for(var name in orgs){
      if(!orgs.hasOwnProperty(name))continue;
      var o=orgs[name];
      // Search name, aliases, and description
      var haystack=fold(name);
      if(o.description)haystack+=' '+fold(o.description);
      if(o.aliases)for(var i=0;i<o.aliases.length;i++)haystack+=' '+fold(o.aliases[i]);
      if(haystack.indexOf(q)<0)continue;
      if(isDefunctFilter===true&&o.is_defunct!==true)continue;
      if(isDefunctFilter===false&&o.is_defunct!==false)continue;
      var entry={standard_name:name,aliases:o.aliases||[],description:o.description||null,country:o.country||null,time_period:o.time_period||null,is_defunct:o.is_defunct||'unknown',founded_year:o.founded_year||null,dissolved_year:o.dissolved_year||null,degree:o.degree||0};
      if(o.country_links&&o.country_links.length)entry.country_links=o.country_links.map(function(cl){return {country:cl.country,context:cl.context,quote:cl.evidence_quote,source_url:cl.source_url||null,source_title:cl.source_title||null};});
      matches.push(entry);
      if(matches.length>=cap)break;
    }
    matches.sort(function(a,b){return(b.description?1:0)-(a.description?1:0)||b.degree-a.degree;});
    if(matches.length===0){var hint='No organizations found matching "'+keyword+'"';if(isDefunctFilter===true)hint+=' that are defunct.';else if(isDefunctFilter===false)hint+=' that are still active.';else hint+='. Try a different keyword.';return JSON.stringify({matches:[],hint:hint});}
    return JSON.stringify({matches:matches,total_found:matches.length});
  }

  function tool_get_connections(args){
    var org=args.organization||'', relType=args.relationship_type||null, targetOrg=args.target_organization||null;
    return Promise.all([loadAdj(),loadCompact()]).then(function(){
      var match=bestMatch(org);
      if(!match)return JSON.stringify({error:'Organization not found: '+org});
      var edges=adjData[match]||[];
      if(relType)edges=edges.filter(function(e){return e.r.indexOf(relType)>=0;});
      if(targetOrg){
        var tMatch=bestMatch(targetOrg);
        if(!tMatch)return JSON.stringify({error:'Target organization not found: '+targetOrg});
        edges=edges.filter(function(e){return e.t===tMatch;});
        // When targeting a specific pair, return ALL evidence edges from both orgs' shards
        return Promise.all([loadEvidence(match),loadEvidence(tMatch)]).then(function(results){
          var e1=results[0], e2=results[1];
          var allEdges=[];
          var seen=new Set();
          function addEdges(entry){
            if(!entry||!entry.edges)return;
            for(var i=0;i<entry.edges.length;i++){
              var ee=entry.edges[i];
              if((ee.source===match&&ee.target===tMatch)||(ee.source===tMatch&&ee.target===match)){
                var key=JSON.stringify([ee.source,ee.target,ee.relationship,ee.evidence_quote]);
                if(seen.has(key))continue;seen.add(key);
                if(relType&&ee.relationship.indexOf(relType)<0)continue;
                allEdges.push({
                  source:ee.source,target:ee.target,relationship:ee.relationship,
                  description:ee.descriptions?ee.descriptions[0]:'',
                  time_period:ee.time_periods?ee.time_periods.join(', '):'',
                  evidence_quote:ee.evidence_quote||'',
                  source_urls:ee.source_urls||[]
                });
              }
            }
          }
          addEdges(e1);addEdges(e2);
          // Count by relationship type
          var counts={cooperation:0,conflict:0,other:0};
          for(var j=0;j<allEdges.length;j++){
            var r=allEdges[j].relationship;
            if(counts.hasOwnProperty(r))counts[r]++;else counts[r]=1;
          }
          // Collect unique source URLs across ALL edges
          var sourceUrls={};
          for(var k=0;k<allEdges.length;k++){
            var urls=allEdges[k].source_urls||[];
            for(var u=0;u<urls.length;u++)sourceUrls[urls[u]]=true;
          }
          return JSON.stringify({
            organization_a:match,organization_b:tMatch,
            total_edges:allEdges.length,type_counts:counts,
            source_urls:Object.keys(sourceUrls),
            edges:allEdges
          });
        });
      }
      // Count by type from the FULL adjacency before capping to 25
      var total=edges.length;
      var typeCounts={cooperation:0,conflict:0,other:0};
      for(var j=0;j<edges.length;j++){
        var rs=edges[j].r;
        for(var k=0;k<rs.length;k++){
          var r=rs[k];
          if(typeCounts.hasOwnProperty(r))typeCounts[r]++;else typeCounts[r]=1;
        }
      }
      if(edges.length>25)edges=edges.slice(0,25);
      return loadEvidence(match).then(function(entry){
        var evidenceEdges=(entry&&entry.edges)?entry.edges:[];
        var results=edges.map(function(n){
          var found=null;
          for(var i=0;i<evidenceEdges.length;i++){
            var ee=evidenceEdges[i];
            if((ee.source===match&&ee.target===n.t)||(ee.source===n.t&&ee.target===match)){found=ee;break;}
          }
          if(found)return {source:found.source,target:found.target,relationship:found.relationship,description:found.descriptions?found.descriptions[0]:'',time_period:found.time_periods?found.time_periods.join(', '):'',evidence_quote:found.evidence_quote||'',source_urls:found.source_urls||[]};
          return {source:match,target:n.t,relationship:n.r.join(', '),description:'',time_period:'',evidence_quote:'',source_urls:[]};
        });
        return JSON.stringify({organization:match,connections_shown:results.length,connections_total:total,type_counts:typeCounts,connections:results});
      });
    });
  }

  function tool_get_relationship_summary(args){
    var orgA=args.organization_a||'', orgB=args.organization_b||'';
    return Promise.all([loadCompact(),loadAdj()]).then(function(){
      var a=bestMatch(orgA), b=bestMatch(orgB);
      if(!a)return JSON.stringify({error:'Organization not found: '+orgA});
      if(!b)return JSON.stringify({error:'Organization not found: '+orgB});
      if(a===b)return JSON.stringify({error:'Same organization: '+a+'. Use get_organization for details.'});
      return loadRelationshipSummary(a,b).then(function(summary){
        // Also find direct edges between them
        var aEdges=adjData[a]||[], directEdges=[];
        for(var i=0;i<aEdges.length;i++){if(aEdges[i].t===b)directEdges.push(aEdges[i].r.join(', '));}
        var result={organization_a:a,organization_b:b};
        if(summary)result.summary=summary.summary;
        result.has_direct_edge=directEdges.length>0;
        if(directEdges.length>0)result.direct_relationship_types=directEdges;
        if(!summary&&directEdges.length===0)result.message='No documented relationship found between '+a+' and '+b+'.';
        return JSON.stringify(result);
      });
    });
  }

  function tool_find_by_country(args){
    var country=args.country||'';
    if(!compactData)return JSON.stringify({error:'Data not loaded'});
    var countries=compactData.countries||{};
    var q=fold(country), match=null;
    for(var name in countries){if(!countries.hasOwnProperty(name))continue;if(fold(name)===q){match=name;break;}}
    if(!match){for(var name in countries){if(!countries.hasOwnProperty(name))continue;if(fold(name).indexOf(q)>=0){match=name;break;}}}
    if(!match)return JSON.stringify({error:'Country not found: '+country+'. Try another spelling.'});
    var c=countries[match];
    return JSON.stringify({
      country:match,total_connections:c.total||0,
      based_here:(c.based_here||[]).slice(0,30),
      footprints_here:(c.footprints_here||[]).slice(0,30).map(function(f){return {org:f.org,origin:f.origin,context:f.context,quote:f.quote};})
	    });
  }

  function tool_find_by_countries(args){
    var reqCountries=args.countries||[];
    if(!compactData)return JSON.stringify({error:'Data not loaded'});
    var countries=compactData.countries||{};
    // Resolve each country name
    var resolved=[], unresolved=[];
    for(var i=0;i<reqCountries.length;i++){
      var raw=reqCountries[i];
      var q=fold(raw), found=null;
      for(var name in countries){if(!countries.hasOwnProperty(name))continue;if(fold(name)===q){found=name;break;}}
      if(!found){for(var name in countries){if(!countries.hasOwnProperty(name))continue;if(fold(name).indexOf(q)>=0){found=name;break;}}}
      if(found)resolved.push(found);else unresolved.push(raw);
    }
    if(unresolved.length)return JSON.stringify({error:'Countries not found: '+unresolved.join(', ')+'. Try another spelling.'});
    if(resolved.length<2)return JSON.stringify({error:'Pass at least 2 countries. Use find_by_country for single-country lookups.'});

    // Build index: org name -> set of countries it has footprints in
    var orgs=compactData.orgs||{};
    var orgFootprints={};
    for(var name in orgs){
      if(!orgs.hasOwnProperty(name))continue;
      var cls=orgs[name].country_links;
      if(!cls||!cls.length)continue;
      var fpSet={};
      for(var j=0;j<cls.length;j++)fpSet[cls[j].country]=true;
      // Also count the home country as a footprint
      if(orgs[name].country)fpSet[orgs[name].country]=true;
      orgFootprints[name]=Object.keys(fpSet);
    }

    // Find orgs with footprints in ALL resolved countries
    var matches=[];
    for(var name in orgFootprints){
      var fps=orgFootprints[name];
      var allMatch=true;
      for(var k=0;k<resolved.length;k++){
        if(fps.indexOf(resolved[k])<0){allMatch=false;break;}
      }
      if(!allMatch)continue;
      var o=orgs[name];
      var entry={standard_name:name,country:o.country||null,degree:o.degree||0,total_footprints:fps.length};
      if(o.description)entry.description=o.description;
      if(o.is_defunct)entry.is_defunct=o.is_defunct;
      matches.push(entry);
    }
    matches.sort(function(a,b){return b.total_footprints-a.total_footprints||b.degree-a.degree;});
    return JSON.stringify({countries:resolved,organizations_found:matches.length,organizations:matches.slice(0,50)});
  }

  function tool_find_paths(args){
    var from=args.from_organization||'', to=args.to_organization||'', maxHops=args.max_hops||3;
    if(maxHops<2)maxHops=2;if(maxHops>5)maxHops=5;
    return Promise.all([loadAdj(),loadCompact()]).then(function(){
      var fromMatch=bestMatch(from), toMatch=bestMatch(to);
      if(!fromMatch)return JSON.stringify({error:'Organization not found: '+from});
      if(!toMatch)return JSON.stringify({error:'Organization not found: '+to});
      if(fromMatch===toMatch)return JSON.stringify({paths:[],message:'Same organization.'});
      var visited={}, queue=[{node:fromMatch,path:[]}], foundPaths=[];
      visited[fromMatch]=true;
      while(queue.length>0&&foundPaths.length<3){
        var cur=queue.shift();
        if(cur.path.length>=maxHops)continue;
        var nbrs=adjNeighbors[cur.node]||[];
        for(var i=0;i<nbrs.length;i++){
          var n=nbrs[i];
          if(n===toMatch){foundPaths.push(cur.path.concat([{from:cur.node,to:n}]));if(foundPaths.length>=3)break;}
          if(!visited[n]){visited[n]=true;queue.push({node:n,path:cur.path.concat([{from:cur.node,to:n}])});}
        }
      }
      if(foundPaths.length===0)return JSON.stringify({paths:[],message:'No path found within '+maxHops+' hops between '+fromMatch+' and '+toMatch+'.'});
      // Collect all orgs involved to load their evidence shards
      var allOrgs={};allOrgs[fromMatch]=true;allOrgs[toMatch]=true;
      foundPaths.forEach(function(p){p.forEach(function(s){allOrgs[s.from]=true;allOrgs[s.to]=true;});});
      var orgNames=Object.keys(allOrgs);
      return Promise.all(orgNames.map(function(o){return loadEvidence(o).then(function(e){return e||{};});})).then(function(evidences){
        var evByOrg={};
        for(var ei=0;ei<orgNames.length;ei++){evByOrg[orgNames[ei]]=evidences[ei];}
        var allSourceUrls={};
        var enriched=foundPaths.map(function(path){
          return path.map(function(step){
            var aedges=adjData[step.from]||[], aedge=aedges.find(function(e){return e.t===step.to;});
            var evidence=null;
            var ee=evByOrg[step.from];
            if(ee&&ee.edges){
              for(var i=0;i<ee.edges.length;i++){
                var ex=ee.edges[i];
                if((ex.source===step.from&&ex.target===step.to)||(ex.source===step.to&&ex.target===step.from)){
                  evidence={description:ex.descriptions?ex.descriptions[0]:'',time_period:ex.time_periods?ex.time_periods.join(', '):'',evidence_quote:ex.evidence_quote||'',source_urls:ex.source_urls||[]};
                  if(ex.source_urls)ex.source_urls.forEach(function(u){allSourceUrls[u]=true;});
                  break;
                }
              }
            }
            return {from:step.from,to:step.to,relationships:aedge?aedge.r:['unknown'],evidence:evidence};
          });
        });
        return JSON.stringify({from:fromMatch,to:toMatch,paths:enriched,source_urls:Object.keys(allSourceUrls)});
      });
    });
  }

  function tool_find_cooperation_routes(args){
    var orgA=args.organization_a||'', orgB=args.organization_b||'';
    return Promise.all([loadAdj(),loadCompact()]).then(function(){
      var a=bestMatch(orgA), b=bestMatch(orgB);
      if(!a)return JSON.stringify({error:'Organization not found: '+orgA});
      if(!b)return JSON.stringify({error:'Organization not found: '+orgB});
      if(a===b)return JSON.stringify({error:'Same organization: '+a});

      // Check direct cooperation
      var aEdges=adjData[a]||[], directCoop=false;
      for(var i=0;i<aEdges.length;i++){
        if(aEdges[i].t===b&&aEdges[i].r.indexOf('cooperation')>=0){directCoop=true;break;}
      }
      if(directCoop){
        return loadEvidence(a).then(function(entry){
          var evEdges=(entry&&entry.edges)?entry.edges:[];
          var found=null;
          for(var i=0;i<evEdges.length;i++){
            var ee=evEdges[i];
            if((ee.source===a&&ee.target===b)||(ee.source===b&&ee.target===a)){found=ee;break;}
          }
          var result={organization_a:a,organization_b:b,direct_cooperation:true};
          if(found){result.evidence_quote=found.evidence_quote;result.description=found.descriptions?found.descriptions[0]:'';result.time_period=found.time_periods?found.time_periods.join(', '):'';result.source_urls=found.source_urls||[];}
          return JSON.stringify(result);
        });
      }

      // No direct cooperation — BFS cooperation-only paths (max 5 hops)
      var visited={}, queue=[{node:a,path:[a]}], coopPaths=[];
      visited[a]=true;
      while(queue.length>0&&coopPaths.length<3){
        var cur=queue.shift();
        if(cur.path.length>=5)continue; // max 5 hops = up to 4 intermediates + target
        var nbrs=adjData[cur.node]||[];
        for(var i=0;i<nbrs.length;i++){
          var e=nbrs[i];
          if(e.r.indexOf('cooperation')<0)continue; // only follow cooperation
          var n=e.t;
          if(n===b){
            coopPaths.push(cur.path.concat([n]));
            if(coopPaths.length>=3)break;
          }
          if(!visited[n]){visited[n]=true;queue.push({node:n,path:cur.path.concat([n])});}
        }
      }

      if(coopPaths.length===0){
        return JSON.stringify({organization_a:a,organization_b:b,direct_cooperation:false,cooperation_routes:[],message:'No cooperation route found between '+a+' and '+b+'. They are not directly allied and no chain of cooperation links connects them.'});
      }

      // Collect all orgs on cooperation paths, load evidence for source URLs
      var coopOrgs={};
      coopPaths.forEach(function(p){p.forEach(function(o){coopOrgs[o]=true;});});
      var coopOrgNames=Object.keys(coopOrgs);
      return Promise.all(coopOrgNames.map(function(o){return loadEvidence(o).then(function(e){return e||{};});})).then(function(coopEv){
        var coopEvByOrg={};
        for(var ei=0;ei<coopOrgNames.length;ei++){coopEvByOrg[coopOrgNames[ei]]=coopEv[ei];}
        var allSrc={};
        var routes=coopPaths.map(function(path){
          var steps=[];
          for(var s=0;s<path.length-1;s++){
            var f=path[s], t=path[s+1];
            var entry=coopEvByOrg[f];var ev=null;
            if(entry&&entry.edges){
              for(var i=0;i<entry.edges.length;i++){
                var ex=entry.edges[i];
                if((ex.source===f&&ex.target===t||ex.source===t&&ex.target===f)&&ex.relationship.indexOf('cooperation')>=0){
                  ev={description:ex.descriptions?ex.descriptions[0]:'',time_period:ex.time_periods?ex.time_periods.join(', '):'',evidence_quote:ex.evidence_quote||'',source_urls:ex.source_urls||[]};
                  if(ex.source_urls)ex.source_urls.forEach(function(u){allSrc[u]=true;});
                  break;
                }
              }
            }
            steps.push({from:f,to:t,relationship:'cooperation',evidence:ev});
          }
          return {hops:path.length-1,steps:steps};
        });
        return JSON.stringify({organization_a:a,organization_b:b,direct_cooperation:false,cooperation_routes_found:routes.length,source_urls:Object.keys(allSrc),cooperation_routes:routes});
      });
    });
  }

  function tool_get_network_neighborhood(args){
    var org=args.organization||'', relType=args.relationship_type||null;
    return Promise.all([loadAdj(),loadCompact()]).then(function(){
      var match=bestMatch(org);
      if(!match)return JSON.stringify({error:'Organization not found: '+org});
      var edges=adjData[match]||[];
      if(relType)edges=edges.filter(function(e){return e.r.indexOf(relType)>=0;});

      var directSet={};
      // Direct connections with types and counts
      var directConnections=edges.map(function(e){
        directSet[e.t]=true;
        return {name:e.t,types:e.r};
      });
      directConnections.sort(function(a,b){return a.name.localeCompare(b.name);});

      // Count by type
      var typeCounts={cooperation:0,conflict:0,other:0};
      for(var i=0;i<edges.length;i++){var rs=edges[i].r;for(var j=0;j<rs.length;j++){var r=rs[j];if(typeCounts.hasOwnProperty(r))typeCounts[r]++;else typeCounts[r]=1;}}

      // Second-degree: build via with relationship types
      var secondDegree={};
      for(var i=0;i<edges.length;i++){
        var viaName=edges[i].t;
        var viaTypes=edges[i].r;  // types of the 1st-hop edge
        var nbrEdges=adjData[viaName]||[];
        for(var j=0;j<nbrEdges.length;j++){
          var target=nbrEdges[j].t;
          var targetRel=nbrEdges[j].r;  // types of the 2nd-hop edge
          if(target===match||directSet[target])continue;
          if(!secondDegree[target])secondDegree[target]={paths:0,via:[]};
          secondDegree[target].paths++;
          if(secondDegree[target].via.length<5){
            var already=null;
            for(var vi=0;vi<secondDegree[target].via.length;vi++){
              if(secondDegree[target].via[vi].name===viaName){already=true;break;}
            }
            if(!already)secondDegree[target].via.push({name:viaName,types:viaTypes,target_relationship:targetRel});
          }
        }
      }

      // Sort second-degree by path count descending
      var secondList=Object.keys(secondDegree).map(function(k){return {name:k,paths:secondDegree[k].paths,via:secondDegree[k].via};});
      secondList.sort(function(a,b){return b.paths-a.paths;});

      return JSON.stringify({
        organization:match,
        direct:{total:edges.length,type_counts:typeCounts,connections:directConnections.slice(0,50)},
        second_degree:{total:secondList.length,top:secondList.slice(0,25)}
      });
    });
  }

  function tool_get_community(args){
    var org=args.organization||null;
    var cid=args.community_id!=null?args.community_id:null;
    var keyword=args.keyword||null;
    return Promise.all([loadCommunitiesData(),loadCompact()]).then(function(results){
      var comms=results[0];
      // Annotate members with [unprofiled] when they have no data so the LLM knows
      // not to invent details about them.
      function annotateMembers(memberNames){
        if(!memberNames)return memberNames;
        return memberNames.map(function(m){
          var isProfiled=false;
          if(compactData&&compactData.orgs){
            var o=compactData.orgs[m];
            if(o&&o.profiled===true)isProfiled=true;
          }
          return isProfiled?m:m+' [unprofiled — no data]';
        });
      }
      // Single-community by id: return full detail
      if(cid!=null){
        for(var i=0;i<comms.length;i++){
          var c=comms[i];
          if(c.i===cid)return JSON.stringify({community_id:c.i,title:c.t,summary:c.m,short_summary:c.b||null,size:c.s,top_hubs:c.k||[],members:annotateMembers(c.o)});
        }
        return JSON.stringify({error:'Community '+cid+' not found.'});
      }
      // Single-org mode: find community containing that org, return full detail
      if(org){
        var match=bestMatch(org);
        if(!match)return JSON.stringify({error:'Organization not found: '+org});
        for(var i=0;i<comms.length;i++){
          var c=comms[i];
          if(c.o&&c.o.indexOf(match)>=0)return JSON.stringify({community_id:c.i,title:c.t,summary:c.m,short_summary:c.b||null,size:c.s,top_hubs:c.k||[],members:annotateMembers(c.o)});
        }
        return JSON.stringify({message:'Organization "'+match+'" not found in any community.'});
      }
      // List-all mode: short_summary only, no full summary — caller must use community_id to drill in
      // Keyword filter: only return communities whose member names, title, or summary contain the keyword
      var list=comms;
      if(keyword){
        var kw=fold(keyword);
        list=comms.filter(function(c){
          // Check title
          if(fold(c.t).indexOf(kw)>=0)return true;
          // Check short summary
          if(c.b&&fold(c.b).indexOf(kw)>=0)return true;
          // Check member names
          if(c.o){for(var mi=0;mi<c.o.length;mi++){if(fold(c.o[mi]).indexOf(kw)>=0)return true;}}
          return false;
        });
      }
      // No members in list mode — the full member list bloats the response to
      // ~108 KB, which makes subsequent API calls hit the Netlify function
      // timeout (10–26 s). Use community_id to get the full member list for a
      // specific community when you need it.
      var result=list.map(function(c){
        return {
          id:c.i,
          title:c.t,
          short_summary:c.b||null,
          size:c.s,
          top_hubs:c.k?c.k.slice(0,5):[]
        };
      });
      return JSON.stringify({total_communities:result.length,communities:result});
    });
  }

  function tool_get_triadic_signals(args){
    var org=args.organization||'', sigType=args.signal_type||null, minScore=args.min_score||5;
    return Promise.all([loadTriadicSignals(),loadCompact()]).then(function(results){
      var signals=results[0];
      var match=bestMatch(org);
      if(!match)return JSON.stringify({error:'Organization not found: '+org});
      var results=[];
      for(var i=0;i<signals.length;i++){
        var s=signals[i];
        if(s.a!==match&&s.b!==match)continue;
        if(s.total_weighted<minScore)continue;
        if(sigType==='cooperation'&&!s.is_cp)continue;
        if(sigType==='adversaries'&&!s.is_sa)continue;
        var other=s.a===match?s.b:s.a;
        results.push({
          other_organization:other,
          country_a:s.country_a,country_b:s.country_b,
          same_country:s.same_country,
          signal_type:s.signal,
          common_partners:s.cp_items||[],common_partner_count:s.cp_count,cooperation_score:s.cp_score,
          common_adversaries:s.adv_items||[],common_adversary_count:s.adv_count,adversary_score:s.adv_score,
          total_score:s.total_weighted,rank:s.rank
        });
      }
      results.sort(function(a,b){return b.total_score-a.total_score;});
      if(results.length>20)results=results.slice(0,20);
      return JSON.stringify({organization:match,signals_found:results.length,signals:results});
    });
  }

  function tool_get_bridges(args){
    var minComm=args.min_communities||3;
    return loadBridgesData().then(function(bridges){
      var results=bridges.filter(function(b){return b.d&&b.d.length>=minComm;});
      results.sort(function(a,b){return b.d.length-a.d.length;});
      if(results.length>20)results=results.slice(0,20);
      var list=results.map(function(b){
        return {
          organization:b.n,country:b.c,
          communities_spanned:b.d.length,
          total_degree:b.x,
          betweenness_rank:b.r,
          betweenness_score:b.b,
          strength_score:b.s,
          communities:b.d.map(function(c){return c;})
        };
      });
      return JSON.stringify({bridge_orgs_found:list.length,bridges:list});
    });
  }

  function tool_get_centrality(args){
    var metric=args.metric||'betweenness', orgName=args.organization||null, limit=args.limit||20, country=args.country||null;
    var minConflicts=args.min_conflicts!=null?args.min_conflicts:null;
    var maxCooperation=args.max_cooperation!=null?args.max_cooperation:null;
    if(limit<1)limit=1;if(limit>50)limit=50;
    return Promise.all([loadCentralityData(),loadCompact()]).then(function(res){
      var orgs=res[0];
      // connected_orgs = orgs with >=1 edge (the ONLY orgs that have a centrality score / rank).
      // total_orgs_in_database counts every org, connected or isolated. The two differ and must not be conflated.
      var connectedCount=orgs.length;
      var totalDb=(res[1]&&res[1].total_orgs)||null;
      var isolatedCount=(totalDb!=null)?totalDb-connectedCount:null;
      var scopeNote='Centrality is only defined for organizations that have at least one connection. '+
        connectedCount+' organizations are connected and ranked here'+
        (totalDb!=null?('; the database holds '+totalDb+' organizations in total, so '+isolatedCount+
        ' are isolated (no documented relationship) and have no centrality score and no rank'):'')+
        '. Never report the ranked count as the size of the database.';
      // Single org lookup
      if(orgName){
        var match=bestMatch(orgName);
        if(!match)return JSON.stringify({error:'Organization not found: '+orgName});
        for(var i=0;i<orgs.length;i++){
          if(orgs[i].n===match){
            var o=orgs[i];
            return JSON.stringify({
              organization:o.n,
              country:o.c||null,
              is_defunct:o.f||false,
              description:o.desc||null,
              degree:{value:o.d,rank:o.dr,out_of:connectedCount},
              betweenness:{value:o.b,rank:o.br,out_of:connectedCount},
              pagerank:{value:o.p,rank:o.pr,out_of:connectedCount},
              cooperation_degree:o.cd,
              conflict_degree:o.xd,
              ranked_among_connected_orgs:connectedCount,
              total_orgs_in_database:totalDb,
              isolated_orgs_not_ranked:isolatedCount,
              note:scopeNote
            });
          }
        }
        return JSON.stringify({message:'Organization "'+match+'" is not in the connected component — no edges to any other org.',connected_orgs_ranked:connectedCount,total_orgs_in_database:totalDb,isolated_orgs_not_ranked:isolatedCount});
      }
      // Global ranking
      var rankKey, valKey;
      if(metric==='degree'){rankKey='dr';valKey='d';}
      else if(metric==='pagerank'){rankKey='pr';valKey='p';}
      else{rankKey='br';valKey='b';}  // default: betweenness
      var sorted=orgs.slice().sort(function(a,b){return a[rankKey]-b[rankKey];});
      var countryFiltered=false;
      if(country){
        countryFiltered=true;
        var cq=fold(country);
        sorted=sorted.filter(function(o){return o.c&&fold(o.c)===cq;});
        if(sorted.length===0)return JSON.stringify({error:'No orgs found for country: '+country+'. Make sure the country name is correct.'});
      }
      // Relationship-profile filters (applied after country, before limit)
      var relFiltered=false;
      if(minConflicts!=null){
        relFiltered=true;
        sorted=sorted.filter(function(o){return (o.xd||0)>=minConflicts;});
      }
      if(maxCooperation!=null){
        relFiltered=true;
        sorted=sorted.filter(function(o){return (o.cd||0)<=maxCooperation;});
      }
      if(relFiltered && sorted.length===0)return JSON.stringify({error:'No orgs found matching the relationship filters (min_conflicts='+minConflicts+', max_cooperation='+maxCooperation+'). Try different values or remove the filters.',connected_orgs_ranked:connectedCount,total_orgs_in_database:totalDb,isolated_orgs_not_ranked:isolatedCount});
      var results=sorted.slice(0,limit).map(function(o){
        return {
          organization:o.n,
          country:o.c||null,
          is_defunct:o.f||false,
          degree:{value:o.d,rank:o.dr},
          betweenness:{value:o.b,rank:o.br},
          pagerank:{value:o.p,rank:o.pr},
          cooperation_degree:o.cd,
          conflict_degree:o.xd,
          description:o.desc||null
        };
      });
      var out={
        metric:metric,
        connected_orgs_ranked:connectedCount,
        total_orgs_in_database:totalDb,
        isolated_orgs_not_ranked:isolatedCount,
        note:scopeNote,
        results:results
      };
      if(countryFiltered){out.country=country;out.orgs_matched_in_country=sorted.length;}
      if(relFiltered){out.filtered_orgs_matched=sorted.length;}
      return JSON.stringify(out);
    });
  }

  function executeTool(name, args){
    switch(name){
      case 'get_organization':return tool_get_organization(args);
      case 'find_by_type':return tool_find_by_type(args);
      case 'get_connections':return tool_get_connections(args);
      case 'get_relationship_summary':return tool_get_relationship_summary(args);
      case 'find_by_country':return tool_find_by_country(args);
      case 'find_by_countries':return tool_find_by_countries(args);
      case 'find_paths':return tool_find_paths(args);
      case 'find_cooperation_routes':return tool_find_cooperation_routes(args);
      case 'get_network_neighborhood':return tool_get_network_neighborhood(args);
      case 'get_community':return tool_get_community(args);
      case 'get_triadic_signals':return tool_get_triadic_signals(args);
      case 'get_bridges':return tool_get_bridges(args);
      case 'get_centrality':return tool_get_centrality(args);
      default:return JSON.stringify({error:'Unknown tool: '+name});
    }
  }

  // ── System prompt ─────────────────────────────────────────────────────
  var SYSTEM_PROMPT=[
    'You are CRIMENET AI, a research assistant for CRIMENET, a knowledge graph of the global network of organized crime extracted from Wikipedia. Answer in English.',
    '',
    // <!-- crimenet-stats:start — auto-generated by tools/update_readme_stats.py; do not edit by hand -->
    'CRIMENET: 4,504 criminal organizations and 10,935 relationships from Wikipedia.',
    // <!-- crimenet-stats:end -->
    'Relationships: cooperation (alliances, joint operations, commercial dealings),',
    'conflict (wars, rivalries, clashes), other (structural ties: sub-unit, faction,',
    'wing, splinter, successor, merged into, truce). Every edge has a verbatim quote.',
    '',
    'Rules:',
    '- Use tools. Never guess org names or relationship counts.',
    '- When a tool returns type_counts or connections_total, use its exact numbers',
    '  in your answer. Never substitute your own count or estimate.',
    '- For superlative or ranking questions ("highest X", "most connected", "largest", "most powerful", "#1 in Y"): use get_centrality. get_organization now returns network_rank with degree/betweenness/pagerank values and ranks for individual orgs, but for comparative or superlative claims across orgs you must still call get_centrality to see the full ranking. Never claim an org is "the most" or "the highest" in any network metric without checking get_centrality first.',
    '  betweenness = gatekeeping/bridging power (who sits on the most paths),',
    '  pagerank = weighted popularity (quality of connections matters),',
    '  degree = raw number of connections. When the question is vague, default to',
    '  betweenness — it best captures structural importance.',
    '- You can combine get_centrality with other tools for richer answers:',
    '  get_centrality to find top orgs, then get_organization for details on',
    '  specific ones, or get_centrality filtered by country to compare regions.',
    '- For questions about relationship profiles ("which orgs have only conflicts",',
    '  "which orgs never cooperate", "orgs with at least 5 conflicts", "pure rivals"):',
    '  use get_centrality with min_conflicts and/or max_cooperation. These filter',
    '  by conflict_degree and cooperation_degree on the full connected set, then',
    '  rank the filtered orgs by your metric. For "only conflicts, no cooperation",',
    '  pass max_cooperation: 0 and min_conflicts: 1. For "what is the most hostile"',
    '  sort by degree or betweenness after filtering. The filters also combine',
    '  with country to narrow the ranking to a specific region.',
    '- For SPECIFIC named orgs ("Tell me about X", "Who are CJNG?"):',
    '  use get_organization. For CATEGORIES or types ("Russian mafia", "motorcycle',
    '  clubs", "political crime groups"): use find_by_type. The distinction:',
    '  get_organization = "I know the name", find_by_type = "find me orgs',
    '  matching this description or type". If get_organization returns empty for',
    '  what looks like a category, immediately try find_by_type.',
    '- For comparison questions ("compare X and Y", "which is most connected"):',
    '  limit yourself to 2-4 focused tool calls, then synthesize. Do NOT look up',
    '  every organization individually — compare at the aggregate level instead.',
    '- "What are X\'s clans / factions / sub-units / wings / splinters?":',
    '  get_organization for X first — read the description, which often names',
    '  the specific term (e.g. \'ndrina for \'Ndrangheta, cosca for Sicilian mafia).',
    '  Then get_connections with relationship_type "other" (structural ties).',
    '  These edges are how the graph models internal breakdowns. If no structural',
    '  edges exist, check the description — it may state the total number of',
    '  sub-units even when the individual units are not all named in Wikipedia.',
    '- "How do X and Y relate?": get_relationship_summary, then get_connections',
    '  with target_organization set. get_connections returns ALL edges between',
    '  the two orgs (from both orgs\' evidence shards). Always list every',
    '  evidence quote grouped by relationship type. Sources are added automatically.',
    '  If get_connections returns no direct edges, fall back to find_paths',
    '  (any relationship type, max 3 hops) to find indirect connections.',
    '  If that also fails, try find_cooperation_routes as a last resort.',
    '- "Do X and Y cooperate?": find_cooperation_routes.',
    '- "Allies of allies" / "X\'s network position": get_network_neighborhood with',
    '  relationship_type: "cooperation". This gives you direct coop allies (with',
    '  types) and second-degree allies with hop-by-hop relationship types.',
    '  Report the counts exactly as the tool returns them.',
    '- Multi-hop any type: find_paths. Sources are added automatically.',
    '- "Which orgs operate in both X and Y?": use find_by_countries with an',
    '  array of country names. Only returns orgs active in ALL countries.',
    '- "Which [type] are defunct?": use get_organization with is_defunct:true.',
    '- "Which orgs are still active?": use get_organization with is_defunct:false.',
    '- Potential undocumented ties: get_triadic_signals (statistical, not confirmed).',
    '- For community questions that require scanning or comparing across communities',
    '  ("most surprising", "largest", "communities about X topic", "communities',
    '  containing orgs named Y"): STEP 1: call get_community() with keyword to filter',
    '  to only communities whose member names, title, or summary contain the term.',
    '  This returns a short list you can scan. Always use keyword to narrow the search',
    '  when looking for communities containing a specific org or term — never scan 224',
    '  member lists yourself, you will miss results. STEP 2: if the keyword filter',
    '  returned too many communities, use keyword with a more specific term. STEP 3:',
    '  call get_community() with community_id for each community you want to present.',
    '  This returns the FULL summary (the detailed paragraph). You MUST do this before',
    '  presenting a community. Never present a community using only its short_summary',
    '  — the user wants the full detail. Do not embellish the summaries with your own',
    '  training knowledge. Do not add facts, anecdotes, or analysis that are not in',
    '  the full summary or the member list. The community data is what CRIMENET knows.',
    '  Present it faithfully. Never claim what a tool result says or does not say',
    '  unless you called that tool in this conversation — do not assume.',
    '- Member lists include "[unprofiled — no data]" markers on orgs that have no',
    '  profiled Wikipedia article. These orgs have no description, no country, and',
    '  no other data — only a name. Do not invent background, location, or activity',
    '  for them. If you need their details, call get_organization first; if it returns',
    '  no description, say the org is unprofiled and no information is available.',
    '- For "most surprising" or "most interesting" questions: pick',
    '  candidate communities from short_summaries in step 1, pull their full summaries',
    '  in step 2, then present the winner(s) with their actual title and full summary.',
    '- When citing facts, mention the Wikipedia article name inline where possible',
    '  (e.g. "according to the Jalisco New Generation Cartel article, CJNG broke away...").',
    '  This makes claims traceable even before the user expands the Evidence section.',
    '- Every answer ends with a structured Evidence section (collapsed by default)',
    '  showing every edge with Source/Time/Quote pills, plus a Sources list.',
    '  These are added automatically — no need to repeat sources manually.',
    '- NEVER use HTML tags in your answer. Do NOT output <details>, <summary>,',
    '  <div>, or any other HTML element. The app builds the evidence section,',
    '  sources list, and summary blocks automatically. HTML in your answer will',
    '  render as broken raw text.',
    '- Do NOT add your own evidence table, edge list, or source list at the',
    '  bottom of your answer. The app appends all edges and sources below',
    '  automatically. Duplicating them confuses the reader.',
    '- Bullet points. Concise.',
    '- Do not comment on tool output size ("that is a lot of data"). Just use it.',
    '- Always quote the exact counts from type_counts / connections_total /',
    '  signals_found fields in your answer. These are computed from the full data.',
    '  Never replace them with your own counting or approximations.',
    '- Outside scope? Say so.',
    '- If find_paths returns nothing, fall back to find_cooperation_routes.',
    '- Plan your tool calls before making them. If a question needs two tools,',
    '  call them in order. Do not repeat the same call. The app blocks exact',
    '  duplicate tool calls automatically — if you get a "Duplicate call" error,',
    '  use different parameters or proceed with the data you already have.',
    '- For "who might be working with X that we do not know about?", call',
    '  get_triadic_signals with signal_type "cooperation". For potential hidden',
    '  rivalries, use signal_type "adversaries". These are statistical signals,',
    '  not confirmed facts — always caveat them as candidate undocumented ties.'
  ].join('\n');


  // ── Agent loop ────────────────────────────────────────────────────────
  async function runAgent(userQuestion, onStatus){
    await dataReady;
    var messages=[{role:'system',content:SYSTEM_PROMPT},{role:'user',content:userQuestion}];
    var collectedSources={};  // url -> {title, url}
    var collectedEvidence={pairs:{}, paths:[], routes:[], orgPairsSeen:new Set()};
    var calledTools=new Set();  // "toolName||JSON.stringify(args)" dedup guard

    function extractSources(toolResult){
      // Parse JSON tool results and collect any source URLs found and edge evidence
      try{
        var r=JSON.parse(toolResult);
      }catch(e){return;}
      if(!r)return;
      // Direct source_urls array
      if(Array.isArray(r.source_urls))r.source_urls.forEach(function(u){collectedSources[u]={title:titleFromUrl(u),url:u};});
      // Per-edge source_urls
      var edges=r.edges||r.connections||null;
      if(edges){edges.forEach(function(e){if(e.source_urls)e.source_urls.forEach(function(u){collectedSources[u]={title:titleFromUrl(u),url:u};});});}
      // Per-path evidence
      var paths=r.paths||r.cooperation_routes||null;
      if(paths){paths.forEach(function(p){var steps=p.steps||(Array.isArray(p)?p:[]);steps.forEach(function(s){var ev=s.evidence||null;if(ev&&ev.source_urls)ev.source_urls.forEach(function(u){collectedSources[u]={title:titleFromUrl(u),url:u};});});});}
      // Organization sources (own_sources mapped as sources)
      var matches=r.matches||null;
      if(matches){matches.forEach(function(m){if(m.sources)m.sources.forEach(function(s){collectedSources[s.url]={title:s.title,url:s.url};});});}
      // get_organization single result
      if(r.sources)r.sources.forEach(function(s){collectedSources[s.url]={title:s.title,url:s.url};});

      // ── Collect edge evidence for structured rendering ──
      // get_connections pair: {organization_a, organization_b, edges[], source_urls[]}
      if(r.organization_a&&r.organization_b&&r.edges&&r.edges.length>0){
        var pairKey=[r.organization_a,r.organization_b].sort().join('||');
        if(!collectedEvidence.pairs[pairKey]){
          collectedEvidence.pairs[pairKey]={a:r.organization_a,b:r.organization_b,edges:[],summary:null};
        }
        for(var ei=0;ei<r.edges.length;ei++){
          collectedEvidence.pairs[pairKey].edges.push({
            description:r.edges[ei].description||'',
            relationship:r.edges[ei].relationship||'other',
            time_period:r.edges[ei].time_period||'',
            evidence_quote:r.edges[ei].evidence_quote||'',
            source_urls:r.edges[ei].source_urls||[]
          });
        }
      }
      // find_paths: {from, to, paths[]}
      if(r.from&&r.to&&r.paths&&r.paths.length>0){
        collectedEvidence.paths.push({from:r.from,to:r.to,paths:r.paths});
      }
      // find_cooperation_routes: {organization_a, organization_b, cooperation_routes[]}
      if(r.organization_a&&r.organization_b&&r.cooperation_routes&&r.cooperation_routes.length>0){
        collectedEvidence.routes.push({from:r.organization_a,to:r.organization_b,routes:r.cooperation_routes});
      }
      // get_relationship_summary: {organization_a, organization_b, summary}
      if(r.organization_a&&r.organization_b&&r.summary){
        var pk=[r.organization_a,r.organization_b].sort().join('||');
        if(!collectedEvidence.pairs[pk]){
          collectedEvidence.pairs[pk]={a:r.organization_a,b:r.organization_b,edges:[],summary:null};
        }
        collectedEvidence.pairs[pk].summary=r.summary;
      }
    }

    // ── Render evidence section (matching Trace a Connection format) ──
    var EDGE_LABELS=U.EDGE_META;
    var FAMILY_COLORS={};
    Object.keys(EDGE_LABELS).forEach(function(k){
      FAMILY_COLORS[k]=U.FAMILY_COLOR[EDGE_LABELS[k].family];
    });

    function renderSourcePill(url){
      return '<div class="ev-srcs"><span class="ev-srcs-label">Source:</span> <a href="'+esc(url)+'" target="_blank" rel="noopener nofollow">'+esc(titleFromUrl(url))+'</a></div>';
    }

    function renderTimeToggle(timeVal){
      var tid='evt-'+Math.random().toString(36).slice(2,10);
      var btn='<button type="button" class="ev-toggle t" data-target="'+tid+'"><span>Time</span><span class="chev">▾</span></button>';
      var reveal='<div class="fp-reveal" id="'+tid+'" hidden><span class="ev-time">'+esc(timeVal||'Unknown')+'</span></div>';
      return btn+reveal;
    }

    function renderQuoteToggle(quote){
      var qid='evq-'+Math.random().toString(36).slice(2,10);
      var btn='<button type="button" class="ev-toggle q" data-target="'+qid+'"><span>Quote</span><span class="chev">▾</span></button>';
      var reveal='<div class="fp-reveal" id="'+qid+'" hidden><blockquote class="ev-quote">'+esc(quote||'')+'</blockquote></div>';
      return btn+reveal;
    }

    function renderEdgeBlock(e){
      var desc=e.description||'';
      var html='<div class="rp-linkage">';
      html+='<div class="rp-linkage-desc">'+(desc?esc(desc):'<em style="color:var(--text-muted)">No description.</em>')+'</div>';
      // Meta row: sources + time + quote
      var metaParts='';
      if(e.source_urls&&e.source_urls.length)for(var u=0;u<e.source_urls.length;u++)metaParts+=renderSourcePill(e.source_urls[u]);
      metaParts+=renderTimeToggle(e.time_period||'');
      metaParts+=renderQuoteToggle(e.evidence_quote||'');
      html+='<div class="ev-meta">'+metaParts+'</div>';
      html+='</div>';
      return html;
    }

    function renderEvidenceSection(){
      var html='';
      var pairKeys=Object.keys(collectedEvidence.pairs);
      var hasPaths=collectedEvidence.paths.length>0;
      var hasRoutes=collectedEvidence.routes.length>0;
      if(!pairKeys.length&&!hasPaths&&!hasRoutes)return '';

      // Count total edges for the badge
      var totalEdges=0;
      pairKeys.forEach(function(pk){
        var pair=collectedEvidence.pairs[pk];
        // Deduplicate and count
        var byRel={};
        pair.edges.forEach(function(e){var r=e.relationship||'other';if(!byRel[r])byRel[r]=[];byRel[r].push(e);});
        var deduped={};
        ['cooperation','conflict','other'].forEach(function(r){
          if(!byRel[r])return;
          var seen=new Set();deduped[r]=[];
          byRel[r].forEach(function(e){var key=JSON.stringify([e.description,e.evidence_quote]);if(!seen.has(key)){seen.add(key);deduped[r].push(e);}});
        });
        Object.keys(deduped).forEach(function(r){totalEdges+=deduped[r].length;});
      });

      html+='<div class="ask-evid">';
      html+='<div class="ask-evid-head collapsed" onclick="var b=this.nextElementSibling;var opening=b.classList.contains(\'collapsed\');this.classList.toggle(\'collapsed\');b.classList.toggle(\'collapsed\');">';
      html+='Trace this answer to the original sources';
      if(totalEdges>0)html+='<span class="ask-evid-badge">'+totalEdges+' edge'+(totalEdges!==1?'s':'')+'</span>';
      html+='</div>';
      html+='<div class="ask-evid-body collapsed">';
      html+='<div class="ask-evid-subhead">Every claim below is backed by a verbatim quote from Wikipedia. Click each edge to see the exact source article, time period, and original text.</div>';

      // Direct pairs
      pairKeys.forEach(function(pk){
        var pair=collectedEvidence.pairs[pk];
        // Group edges by relationship type
        var byRel={};
        pair.edges.forEach(function(e){
          var r=e.relationship||'other';
          if(!byRel[r])byRel[r]=[];
          byRel[r].push(e);
        });
        // Deduplicate edges (same description+relationship+quote)
        var deduped={};
        ['cooperation','conflict','other'].forEach(function(r){
          if(!byRel[r])return;
          var seen=new Set();deduped[r]=[];
          byRel[r].forEach(function(e){
            var key=JSON.stringify([e.description,e.evidence_quote]);
            if(!seen.has(key)){seen.add(key);deduped[r].push(e);}
          });
          if(!deduped[r].length)delete deduped[r];
        });

        // Relationship Summary (from get_relationship_summary tool)
        if(pair.summary){
          html+='<div class="rs-heading">AI Analysis</div>';
          html+='<div class="rs-ai-note">This narrative was generated by AI based on the evidence edges listed below. It is a summary, not a primary source.</div>';
          html+='<div class="rs-summary">'+esc(pair.summary)+'</div>';
          html+='<div class="rs-evidence-divider">Verified Evidence</div>';
        }

        // Render each relationship group
        var order=['cooperation','conflict','other'];
        order.forEach(function(r){
          if(!deduped[r]||!deduped[r].length)return;
          var color=FAMILY_COLORS[r]||FAMILY_COLORS.other;
          html+='<div class="rp-section" style="--rc:'+color+'">';
          html+='<div class="rp-section-title collapsed" onclick="var b=this.nextElementSibling;this.classList.toggle(\'collapsed\');b.classList.toggle(\'collapsed\');">'+((EDGE_LABELS[r]||{}).label||r)+'<span class="rp-section-count">'+deduped[r].length+'</span></div>';
          html+='<div class="rp-section-body collapsed">';
          deduped[r].forEach(function(e){html+=renderEdgeBlock(e);});
          html+='</div></div>';
        });
      });

      // Multi-hop paths
      if(hasPaths){
        collectedEvidence.paths.forEach(function(pathResult,pi){
          if(!pathResult.paths||!pathResult.paths.length)return;
          pathResult.paths.forEach(function(path,pp){
            var hops=path.length||0;
            html+='<div class="rp-section" style="--rc:var(--fam-loose)">';
            html+='<div class="rp-section-title collapsed" onclick="var b=this.nextElementSibling;this.classList.toggle(\'collapsed\');b.classList.toggle(\'collapsed\');">Path '+(pp+1)+'<span class="rp-section-count">'+hops+' hop'+(hops!==1?'s':'')+'</span></div>';
            html+='<div class="rp-section-body collapsed">';
            for(var hi=0;hi<path.length;hi++){
              var step=path[hi];
              var ev=step.evidence||null;
              var label=(step.from||'?')+' → '+(step.to||'?');
              var rels=step.relationships?step.relationships.join(', '):'';
              if(rels)label+=' ('+rels+')';
              html+='<div class="rp-linkage">';
              html+='<div class="rp-linkage-desc"><strong>'+esc(label)+'</strong></div>';
              if(ev){
                var mp='';
                if(ev.source_urls&&ev.source_urls.length)for(var su=0;su<ev.source_urls.length;su++)mp+=renderSourcePill(ev.source_urls[su]);
                mp+=renderTimeToggle(ev.time_period||'');
                mp+=renderQuoteToggle(ev.evidence_quote||'');
                html+='<div class="ev-meta">'+mp+'</div>';
              }
              html+='</div>';
            }
            html+='</div></div>';
          });
        });
      }

      // Cooperation routes (similar structure to paths)
      if(hasRoutes){
        collectedEvidence.routes.forEach(function(routeResult,ri){
          if(!routeResult.routes||!routeResult.routes.length)return;
          routeResult.routes.forEach(function(route,rp){
            var steps=route.steps||[];
            html+='<div class="rp-section" style="--rc:var(--fam-coop)">';
            html+='<div class="rp-section-title collapsed" onclick="var b=this.nextElementSibling;this.classList.toggle(\'collapsed\');b.classList.toggle(\'collapsed\');">Cooperation Route '+(rp+1)+'<span class="rp-section-count">'+route.hops+' hop'+(route.hops!==1?'s':'')+'</span></div>';
            html+='<div class="rp-section-body collapsed">';
            for(var si=0;si<steps.length;si++){
              var st=steps[si];
              var sev=st.evidence||null;
              var slabel=(st.from||'?')+' → '+(st.to||'?');
              html+='<div class="rp-linkage">';
              html+='<div class="rp-linkage-desc"><strong>'+esc(slabel)+'</strong></div>';
              if(sev){
                var sp='';
                if(sev.source_urls&&sev.source_urls.length)for(var su2=0;su2<sev.source_urls.length;su2++)sp+=renderSourcePill(sev.source_urls[su2]);
                sp+=renderTimeToggle(sev.time_period||'');
                sp+=renderQuoteToggle(sev.evidence_quote||'');
                html+='<div class="ev-meta">'+sp+'</div>';
              }
              html+='</div>';
            }
            html+='</div></div>';
          });
        });
      }

      html+='</div></div>'; // .ask-evid-body, .ask-evid
      return html;
    }

    for(var iter=0;iter<MAX_ITERATIONS;iter++){
      if(onStatus)onStatus(iter===0?'Reasoning…':'Looking up data…');
      var payload={model:'deepseek-v4-flash',messages:messages,tools:TOOLS};

      var resp=await fetch('https://afmartins.netlify.app/.netlify/functions/crimenet-ask',{
        method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)
      });
      if(!resp.ok)throw new Error('API error: HTTP '+resp.status);
      var data=await resp.json();
      if(data.error){
        var em=data.error.message||data.error;
        if(em&&em.indexOf('rate')>=0)throw new Error('Rate limited. Wait a moment and try again.');
        throw new Error('API error');
      }
      var choice=data.choices&&data.choices[0];
      if(!choice)throw new Error('No response from API');
      var msg=choice.message;

      // Tool calls?
      if(msg.tool_calls&&msg.tool_calls.length>0){
        var assistantMsg={role:'assistant',tool_calls:msg.tool_calls};
        if(msg.content)assistantMsg.content=msg.content;
        if(msg.reasoning_content)assistantMsg.reasoning_content=msg.reasoning_content;
        messages.push(assistantMsg);
        for(var i=0;i<msg.tool_calls.length;i++){
          var tc=msg.tool_calls[i], toolName=tc.function.name, toolArgs={};
          try{toolArgs=JSON.parse(tc.function.arguments||'{}');}catch(e){}
          // Dedup guard: if this exact call was already made, feed back a warning
          // so the LLM stops retrying and moves on instead of burning iterations.
          var dedupKey=toolName+'||'+JSON.stringify(toolArgs);
          if(calledTools.has(dedupKey)){
            var dedupResult=JSON.stringify({error:'Duplicate call: '+toolName+' was already called with these exact arguments. Use a different query or proceed with the data you have.'});
            extractSources(dedupResult);
            messages.push({role:'tool',tool_call_id:tc.id,content:dedupResult});
            continue;
          }
          calledTools.add(dedupKey);
          if(onStatus)onStatus('Looking up: '+toolName.replace(/_/g,' ')+'…');
          var result=await executeTool(toolName,toolArgs);
          extractSources(result);
          messages.push({role:'tool',tool_call_id:tc.id,content:result});
        }
        continue;
      }

      // Final answer — return markdown text and HTML sections separately
      // so the caller can render markdown without escaping the evidence/sources HTML.
      // Strip any HTML tags that leaked from the LLM.
      var answer=stripHtmlTags(msg.content||'No response generated.');
      var evidHtml=renderEvidenceSection();
      var urls=Object.keys(collectedSources);
      var sourcesHtml='';
      if(urls.length>0){
        sourcesHtml+='<div class="ask-src-head">Sources</div>';
        sourcesHtml+='<div class="ask-src-list">';
        urls.forEach(function(u){
          var s=collectedSources[u];
          sourcesHtml+='<a href="'+esc(s.url)+'" target="_blank" rel="noopener nofollow">'+esc(s.title)+'</a>';
        });
        sourcesHtml+='</div>';
      }
      return {markdown:answer,evidenceHtml:evidHtml,sourcesHtml:sourcesHtml};
    }
    return {markdown:'I ran into a loop trying to answer your question. Please try rephrasing it.',evidenceHtml:'',sourcesHtml:''};
  }

  // ── UI ────────────────────────────────────────────────────────────────
  function initAsk(){
    if(initialized)return;
    initialized=true;

    var input = document.getElementById('ds-input');
    var send = document.getElementById('ds-send');
    var answers = document.getElementById('ds-answers');
    var answersScroll = document.getElementById('ds-answers-scroll');
    var centered = document.querySelector('.ds-centered-wrap');
    var searchBottom = document.getElementById('ds-search-bottom');
    var inputBottom = document.getElementById('ds-input-bottom');
    var sendBottom = document.getElementById('ds-send-bottom');
    var examplesWrap = document.querySelector('.ds-suggestions');

    var firstQuestion = true;
    var currentTurn = null;

    function collapseExamples(){
      if(examplesWrap && !examplesWrap.classList.contains('collapsed')){
        examplesWrap.classList.add('collapsed');
      }
    }

    function newTurn(question){
      // Newest turn goes on top; demote the previously-current turn.
      var prev = answers.querySelectorAll('.ask-turn.ask-current');
      for(var i=0;i<prev.length;i++) prev[i].classList.remove('ask-current');

      var turn = document.createElement('div');
      turn.className = 'ask-turn ask-current';
      turn.innerHTML =
        '<div class="ask-msg ask-user"><div class="ask-msg-label">You</div><div class="ask-msg-body">'+esc(question)+'</div></div>'+
        '<div class="ask-msg ask-assistant"><div class="ask-msg-label">CRIMENET AI</div><div class="ask-msg-body"><span class="ask-spinner"></span> Thinking…</div></div>';
      answers.insertBefore(turn, answers.firstChild);
      currentTurn = turn;

      // On first question: collapse examples and reveal the answers area.
      if(firstQuestion){
        firstQuestion = false;
        collapseExamples();
        if(answersScroll) answersScroll.classList.add('show');
        if(searchBottom) searchBottom.style.display = '';
      }

      setTimeout(function(){
        turn.scrollIntoView({behavior:'smooth',block:'start'});
      },50);
    }

    // Fill the current turn's answer body (replaces the loading spinner in place).
    function setAnswer(answer){
      if(!currentTurn) return;
      var body = currentTurn.querySelector('.ask-assistant .ask-msg-body');
      if(body) body.innerHTML = answer;
    }

    function updateStatus(text){
      if(!currentTurn) return;
      var body = currentTurn.querySelector('.ask-assistant .ask-msg-body');
      if(body) body.innerHTML = '<span class="ask-spinner"></span> '+esc(text);
    }

    function submitQuestion(q, isFollowUp){
      q = (q||'').trim(); if(!q) return;
      newTurn(q);
      send.disabled = true; input.disabled = true;
      if(sendBottom) sendBottom.disabled = true;
      if(inputBottom) inputBottom.disabled = true;

      runAgent(q, updateStatus).then(function(result){
        // Structured result: {markdown, evidenceHtml, sourcesHtml}
        var answer = simpleMarkdown(result.markdown||'')
          + result.sourcesHtml
          + result.evidenceHtml;
        setAnswer(answer);
        // Clear the bottom input if this was a follow-up
        if(isFollowUp && inputBottom) inputBottom.value = '';
      }).catch(function(err){
        setAnswer('Error: '+esc(err.message||'Failed to reach the API.'));
      }).then(function(){
        send.disabled = false; input.disabled = false;
        if(sendBottom) sendBottom.disabled = false;
        if(inputBottom) inputBottom.disabled = false;
        // Focus back on the bottom input for follow-up conversation
        if(!firstQuestion && inputBottom) inputBottom.focus();
      });
    }

    send.addEventListener('click', function(){ submitQuestion(input.value); });
    input.addEventListener('keydown', function(e){
      if(e.key === 'Enter' && !e.shiftKey){ e.preventDefault(); submitQuestion(input.value); }
    });

    // Auto-grow textarea on input
    input.addEventListener('input', function(){
      this.style.height = 'auto';
      this.style.height = Math.min(this.scrollHeight, 200) + 'px';
    });

    // Wire up example buttons
    if(examplesWrap){
      var examples = examplesWrap.querySelectorAll('.ask-example');
      for(var i = 0; i < examples.length; i++){
        examples[i].addEventListener('click', function(){
          input.value = this.dataset.prompt || '';
          input.focus();
          input.style.height = 'auto';
          input.style.height = Math.min(input.scrollHeight, 200) + 'px';
        });
      }

      // Show 2 examples per group, rest hidden with a More button
      var groups = examplesWrap.querySelectorAll('.ask-examples-group');
      for(var g = 0; g < groups.length; g++){
        var btns = groups[g].querySelectorAll('.ask-example');
        if(btns.length <= 2) continue;
        for(var ei = 2; ei < btns.length; ei++) btns[ei].classList.add('hidden-example');
        var moreBtn = document.createElement('button');
        moreBtn.className = 'ask-example-more-btn';
        moreBtn.textContent = '+ ' + (btns.length - 2) + ' more';
        (function(btn, total){
          btn.addEventListener('click', function(e){
            e.preventDefault(); e.stopPropagation();
            var all = this.parentNode.querySelectorAll('.ask-example');
            var expanded = all[2] && !all[2].classList.contains('hidden-example');
            for(var j = 2; j < all.length; j++) all[j].classList.toggle('hidden-example', expanded);
            this.textContent = expanded ? ('+ ' + (total - 2) + ' more') : 'Show less';
          });
        })(moreBtn, btns.length);
        groups[g].appendChild(moreBtn);
      }

      // Toggle examples section
      var toggleBtn = document.getElementById('ask-examples-toggle');
      if(toggleBtn){
        toggleBtn.addEventListener('click', function(e){
          e.preventDefault(); e.stopPropagation();
          var collapsed = examplesWrap.classList.toggle('collapsed');
          this.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        });
      }
    }

    // Bottom search bar for follow-up questions
    if(sendBottom && inputBottom){
      sendBottom.addEventListener('click', function(){ submitQuestion(inputBottom.value, true); });
      inputBottom.addEventListener('keydown', function(e){
        if(e.key === 'Enter' && !e.shiftKey){ e.preventDefault(); submitQuestion(inputBottom.value, true); }
      });
      inputBottom.addEventListener('input', function(){
        this.style.height = 'auto';
        this.style.height = Math.min(this.scrollHeight, 120) + 'px';
      });
    }

    // About: "About" toggles a modal box; close via X, overlay click, or Escape.
    var aboutToggle = document.getElementById('ds-about-toggle');
    var aboutModal = document.getElementById('ds-about-modal');
    var aboutClose = document.getElementById('ds-about-close');
    if(aboutToggle && aboutModal){
      function closeAbout(){
        aboutModal.classList.remove('open');
        aboutToggle.setAttribute('aria-expanded','false');
        aboutToggle.focus();
      }
      aboutToggle.addEventListener('click', function(){
        var open = aboutModal.classList.toggle('open');
        aboutToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
      });
      if(aboutClose) aboutClose.addEventListener('click', closeAbout);
      aboutModal.addEventListener('click', function(e){ if(e.target === aboutModal) closeAbout(); });
      document.addEventListener('keydown', function(e){
        if(e.key === 'Escape' && aboutModal.classList.contains('open')) closeAbout();
      });
    }

    // Evidence Time/Quote reveals — delegated toggle (mirrors browse.js).
    // The evidence section is injected after each answer, so a single
    // document-level handler covers every current and future .ev-toggle.
    document.addEventListener('click', function(ev){
      var b = ev.target.closest('.ev-toggle');
      if(!b) return;
      var t = document.getElementById(b.dataset.target);
      if(!t) return;
      var opening = t.hasAttribute('hidden');
      if(opening) t.removeAttribute('hidden'); else t.setAttribute('hidden','');
      b.classList.toggle('open', opening);
    });

    dataReady = Promise.all([loadCompact(), loadAdj()]);
  }

  document.addEventListener('DOMContentLoaded', function(){ initAsk(); });
})();
