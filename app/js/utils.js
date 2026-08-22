// ── CRIMENET shared utilities ────────────────────────────────────────────────
// Loaded by all pages that need esc, fold, FNV-1a hashing, evidence/summary
// loading, relationship labels, and spinner SVG.  Define BEFORE page scripts.
(function(){
  var U = {};

  // ── HTML escaping ────────────────────────────────────────────────────────
  U.esc = function(s){
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  };

  // ── Normalised fold for search ──────────────────────────────────────────
  U.fold = function(s){
    return (s || '').normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
  };

  // ── Wikipedia title from URL ────────────────────────────────────────────
  U.titleFromUrl = function(url){
    if (!url) return 'Wikipedia';
    try {
      var u = new URL(url);
      var m = u.searchParams.get('title');
      if (m) return decodeURIComponent(m.replace(/_/g, ' '));
      var p = u.pathname.split('/wiki/')[1];
      if (p) return decodeURIComponent(p.split('?')[0].split('#')[0].replace(/_/g, ' '));
    } catch(e) {}
    return 'Wikipedia';
  };

  // ── Spinner SVG (one definition, shared everywhere) ──────────────────────
  U.SPINNER_SVG = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="var(--amber)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="animation:spin 1s linear infinite"><line x1="12" y1="2" x2="12" y2="6"></line><line x1="12" y1="18" x2="12" y2="22"></line><line x1="4.93" y1="4.93" x2="7.76" y2="7.76"></line><line x1="16.24" y1="16.24" x2="19.07" y2="19.07"></line><line x1="2" y1="12" x2="6" y2="12"></line><line x1="18" y1="12" x2="22" y2="12"></line><line x1="4.93" y1="19.07" x2="7.76" y2="16.24"></line><line x1="16.24" y1="4.93" x2="19.07" y2="7.76"></line></svg>';

  // ── Relationship taxonomy ───────────────────────────────────────────────
  U.EDGE_META = {
    cooperation: {label: 'Cooperation', family: 'coop'},
    conflict:    {label: 'Conflict',    family: 'hostile'},
    other:       {label: 'Other',       family: 'loose'}
  };
  U.edgeLabel = function(t){ return (U.EDGE_META[t] || {label: (t||'related').replace(/_/g, ' ')}).label; };
  U.edgeFamily = function(t){ return (U.EDGE_META[t] || {family: 'loose'}).family; };

  U.FAMILY_COLOR = {
    coop:   'var(--fam-coop)',
    hostile:'var(--fam-hostile)',
    loose:  'var(--fam-loose)'
  };

  // ── FNV-1a bucket hashing (matches build_evidence_shards.py) ────────────
  U.fnvHash = function(str){
    var b = new TextEncoder().encode(str);
    var h = 0x811c9dc5;
    for (var i = 0; i < b.length; i++) {
      h ^= b[i];
      h = Math.imul(h, 0x01000193) >>> 0;
    }
    return h >>> 0;
  };

  // ── Evidence shard loading ──────────────────────────────────────────────
  U.EVIDENCE_DIR = 'data/evidence';
  U.EVIDENCE_BUCKETS = 128;

  U.evidenceBucketId = function(name){
    var h = U.fnvHash(name);
    var w = Math.max(3, String(U.EVIDENCE_BUCKETS - 1).length);
    return String(h % U.EVIDENCE_BUCKETS).padStart(w, '0');
  };

  // Returns a loader function closed over its own cache + bucket-promises map.
  // Call once per page:  var loadEvidence = CrimenetUtils.evidenceLoader();
  U.evidenceLoader = function(dir, buckets){
    dir = dir || U.EVIDENCE_DIR;
    buckets = buckets || U.EVIDENCE_BUCKETS;
    var cache = {}, bucketPromises = {};

    return function loadEvidence(orgName){
      if (orgName in cache) return Promise.resolve(cache[orgName]);
      var id = U.evidenceBucketId(orgName);
      if (!bucketPromises[id]) {
        bucketPromises[id] = fetch(dir + '/' + id + '.json')
          .then(function(r){ if (!r.ok) throw new Error('evidence bucket ' + id); return r.json(); })
          .catch(function(){ return {}; });
      }
      return bucketPromises[id].then(function(bk){
        var e = bk[orgName] || null;
        cache[orgName] = e;
        return e;
      });
    };
  };

  // ── Relationship summary shard loading ──────────────────────────────────
  U.SUMMARY_DIR = 'data/relationship_summaries';
  U.SUMMARY_BUCKETS = 128;

  U.summaryBucketId = function(a, b){
    var key = [a, b].sort().join('||');
    var h = U.fnvHash(key);
    var w = Math.max(3, String(U.SUMMARY_BUCKETS - 1).length);
    return String(h % U.SUMMARY_BUCKETS).padStart(w, '0');
  };

  U.summaryLoader = function(dir, buckets){
    dir = dir || U.SUMMARY_DIR;
    buckets = buckets || U.SUMMARY_BUCKETS;
    var cache = {}, bucketPromises = {};

    return function loadSummary(a, b){
      var key = [a, b].sort().join('||');
      if (key in cache) return Promise.resolve(cache[key]);
      var id = U.summaryBucketId(a, b);
      if (!bucketPromises[id]) {
        bucketPromises[id] = fetch(dir + '/' + id + '.json')
          .then(function(r){ if (!r.ok) throw new Error('summary bucket ' + id); return r.json(); })
          .catch(function(){ return {}; });
      }
      return bucketPromises[id].then(function(bk){
        var s = bk[key] || null;
        cache[key] = s;
        return s;
      });
    };
  };

  // ── Organisation detail page URL ────────────────────────────────────────
  U.orgUrl = function(name){
    return 'index.html?org=' + encodeURIComponent(name);
  };

  window.CrimenetUtils = U;
})();
