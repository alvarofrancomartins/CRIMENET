// ── CRIMENET Triadic Signals — shared data functions loaded by browse.html and triadic_signals.html
(function(){
  function esc(s){
    return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
  }

  function signalTag(r){
    if(r.is_cp&&r.is_sa)return'<span class="sig-tag both">Both</span>';
    if(r.is_cp)return'<span class="sig-tag cp">Only Common Partners</span>';
    if(r.is_sa)return'<span class="sig-tag sa">Only Common Adversaries</span>';
    return'';
  }

  function formatItems(items, maxShow){
    maxShow=maxShow||5;
    if(!items||items.length===0)return'<span style="color:var(--text-muted);opacity:.4">—</span>';
    var shown=items.slice(0,maxShow);
    var rest=items.slice(maxShow);
    var html='<span class="item-shown">';
    for(var i=0;i<shown.length;i++){
      if(i>0)html+=', ';
      html+='<span class="sig-item-name">'+esc(shown[i])+'</span>';
    }
    html+='</span>';
    if(rest.length>0){
      html+='<span class="sig-item-more" onclick="var p=this.parentElement;p.classList.toggle(\'expanded\');event.stopPropagation()">, +'+rest.length+' more</span>';
      html+='<span class="sig-item-rest">';
      for(var k=0;k<rest.length;k++){
        html+=', <span class="sig-item-name">'+esc(rest[k])+'</span>';
      }
      html+='</span>';
    }
    return html;
  }

  function filterAndSort(data, signal, col, dir){
    var rows=data.slice();
    if(signal!=='all'){
      rows=rows.filter(function(r){
        if(signal==='cp')return r.is_cp&&!r.is_sa;
        if(signal==='sa')return r.is_sa&&!r.is_cp;
        if(signal==='both')return r.is_cp&&r.is_sa;
        return true;
      });
    }
    var direction=dir==='asc'?1:-1;
    rows.sort(function(a,b){
      var va=a[col],vb=b[col];
      if(typeof va==='string')va=va.toLowerCase();
      if(typeof vb==='string')vb=vb.toLowerCase();
      if(va==null)va='';if(vb==null)vb='';
      if(va<vb)return-1*direction;if(va>vb)return 1*direction;
      return 0;
    });
    return rows;
  }

  window.CrimenetTriadic={
    esc:esc,
    signalTag:signalTag,
    formatItems:formatItems,
    filterAndSort:filterAndSort
  };
})();
