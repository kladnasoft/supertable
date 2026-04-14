var SEC=(function(){"use strict";
function enc(v){return encodeURIComponent(v||"");}
function esc(s){var d=document.createElement("div");d.textContent=String(s||"");return d.innerHTML;}
function toast(m){var e=document.getElementById("toast");if(!e)return;e.textContent=m;e.style.display="block";clearTimeout(toast._t);toast._t=setTimeout(function(){e.style.display="none";},2200);}
function $(id){return document.getElementById(id);}
function ctx(){var r=$("root");return{org:r?r.dataset.org:"",sup:r?r.dataset.sup:"",token:r?r.dataset.token:""};}
function show(id){$(id).style.display="flex";}
function hide(id){$(id).style.display="none";}
function rbH(type){var t=(type||"").toLowerCase();return'<span class="rb rb-'+esc(t||"meta")+'">'+esc(type||"\u2014")+"</span>";}
function sl(v){if(!v)return[];if(Array.isArray(v))return v;try{var p=JSON.parse(v);return Array.isArray(p)?p:[p];}catch(e){return v?[v]:[];}}
function fd(v){if(!v)return"\u2014";try{var n=Number(v);if(n>0)return new Date(n).toLocaleDateString();return new Date(v).toLocaleDateString();}catch(e){return String(v);}}
function debounce(fn,ms){var t;return function(){clearTimeout(t);t=setTimeout(fn,ms||150);};}
function initials(u){var dn=u.display_name||u.username||u.name||"";var parts=dn.split(/[\s._-]+/);if(parts.length>=2)return(parts[0][0]||"").toUpperCase()+(parts[1][0]||"").toUpperCase();return dn.slice(0,2).toUpperCase();}
function _h(){var c=ctx(),h={"Content-Type":"application/json"};if(c.token)h["X-Admin-Token"]=c.token;return h;}
async function GET(u){var r=await fetch(u,{credentials:"same-origin",headers:_h()});var t=await r.text();var j;try{j=JSON.parse(t);}catch(e){}if(!r.ok)throw new Error((j&&(j.detail||j.message))||t||r.status);return j;}
async function POST(u,b){var r=await fetch(u,{method:"POST",credentials:"same-origin",headers:_h(),body:JSON.stringify(b)});var t=await r.text();var j;try{j=JSON.parse(t);}catch(e){}if(!r.ok)throw new Error((j&&(j.detail||j.message))||t||r.status);return j;}
async function PUT(u,b){var r=await fetch(u,{method:"PUT",credentials:"same-origin",headers:_h(),body:JSON.stringify(b)});var t=await r.text();var j;try{j=JSON.parse(t);}catch(e){}if(!r.ok)throw new Error((j&&(j.detail||j.message))||t||r.status);return j;}
async function DEL(u){var r=await fetch(u,{method:"DELETE",credentials:"same-origin",headers:_h()});var t=await r.text();var j;try{j=JSON.parse(t);}catch(e){}if(!r.ok)throw new Error((j&&(j.detail||j.message))||t||r.status);return j;}

var tab="users",roles=[],users=[],eps=[],schemas={};
var selU=null,selR=null,selO=null;
var _editUid=null,_cloneRid=null;
var selRT=null,eRT={};
var _dirty=false;

function usersForRole(rid){var c=0;users.forEach(function(u){if(sl(u.roles).indexOf(rid)>=0)c++;});return c;}
function epsForRole(rid){var c=0;eps.forEach(function(e){if(e.role_id===rid)c++;});return c;}
function fr(rid){return roles.find(function(r){return(r.role_id||r.id)===rid;});}
function prot(u){var n=(u.username||u.name||"").toLowerCase();var c=ctx();return!!u.is_superuser||n===c.sup.toLowerCase()||n==="superuser"||n==="superadmin";}

async function lRoles(){var c=ctx();try{var d=await GET("/api/v1/rbac/roles?organization="+enc(c.org)+"&super_name="+enc(c.sup));roles=(d&&d.data&&d.data.items)||[];}catch(e){roles=[];}}
async function lUsers(){var c=ctx();try{var d=await GET("/api/v1/rbac/users/list?org="+enc(c.org)+"&sup="+enc(c.sup));users=d.users||[];if(d.roles&&d.roles.length&&!roles.length)roles=d.roles;}catch(e){users=[];}}
async function lEps(){var c=ctx();try{var d=await GET("/api/v1/odata/endpoints?organization="+enc(c.org)+"&super_name="+enc(c.sup));eps=(d&&d.data&&d.data.items)||[];}catch(e){eps=[];}}
async function lSchema(){var c=ctx();try{var d=await POST("/api/v1/tables/schemas",{organization:c.org,super_name:c.sup,role_name:"superadmin"});if(d&&d.status==="ok"&&Array.isArray(d.schema)){schemas={};d.schema.forEach(function(e){var k=Object.keys(e);if(k.length){schemas[k[0]]=(e[k[0]]||[]).map(function(col){if(typeof col==="string")return{name:col,type:""};var cn=Object.keys(col);return{name:cn[0]||"",type:col[cn[0]]||""};});}});}}catch(e){}}
async function refresh(){await Promise.all([lRoles(),lUsers(),lEps()]);renderAll();}
function tiles(){$("statRoles").textContent=roles.length;$("statUsers").textContent=users.length;var cov=0;users.forEach(function(u){if(sl(u.roles).length)cov++;});$("statCovered").textContent=cov;$("statOdata").textContent=eps.length;$("cntUsers").textContent=users.length;$("cntRoles").textContent=roles.length;$("cntOdata").textContent=eps.length;var sb=$("statusBadge");if(sb){sb.style.background="rgba(16,185,129,.08)";sb.style.color="#059669";sb.innerHTML='<i class="fas fa-shield-halved"></i> '+roles.length+" roles \u00b7 "+users.length+" users";}}

/* USERS LIST+DETAIL */
function rUL(){var w=$("listU"),q=($("searchU").value||"").toLowerCase();var items=users;if(q)items=items.filter(function(u){return(u.username||u.name||"").toLowerCase().indexOf(q)>=0||(u.display_name||"").toLowerCase().indexOf(q)>=0;});if(!items.length){w.innerHTML='<div class="md-empty">'+(users.length===0?'No users yet.':'No matching users.')+'</div>';return;}var h="";items.forEach(function(u){var uid=u.user_id||u.hash||u.id,en=u.enabled!==false;var rl=sl(u.roles),rh="";rl.slice(0,2).forEach(function(rid){var ro=fr(rid);rh+=rbH(ro?ro.role:"");});if(rl.length>2)rh+=' <span style="font-size:10px;color:var(--text-muted);">+'+(rl.length-2)+"</span>";h+='<div class="md-item'+(selU===uid?" active":"")+'" data-id="'+esc(uid)+'" style="'+(en?"":"opacity:.45;")+'"><div class="mi-av" style="background:rgba(124,58,237,.08);color:var(--purple);">'+esc(initials(u))+'</div><div class="mi-body"><div class="mi-name"><span class="dot '+(en?"dot-on":"dot-off")+'"></span>'+esc(u.display_name||u.username||u.name||"\u2014")+'</div><div class="mi-sub">'+esc(u.username||"")+(rh?" \u00b7 "+rh:"")+"</div></div></div>";});w.innerHTML=h;w.querySelectorAll(".md-item").forEach(function(el){el.addEventListener("click",function(){selU=el.dataset.id;rUL();rUD();});});}
function rUD(){var c=$("uContent"),em=$("uEmpty"),ac=$("uActs");if(!selU){c.innerHTML="";em.style.display="flex";$("uName").textContent="\u2014";$("uMeta").textContent="";ac.innerHTML="";return;}em.style.display="none";var u=users.find(function(x){return(x.user_id||x.hash||x.id)===selU;});if(!u){c.innerHTML="";return;}var pr=prot(u),en=u.enabled!==false;$("uName").textContent=u.display_name||u.username||u.name||"\u2014";$("uMeta").textContent="ID: "+selU;ac.innerHTML=pr?'<span style="font-size:10px;color:var(--text-muted);display:flex;align-items:center;gap:4px;"><i class="fas fa-lock"></i> System account</span>':'<button class="btn-sm" onclick="SEC.editUser(\''+esc(selU)+'\')"><i class="fas fa-pen"></i> Edit</button><button class="btn-sm" onclick="SEC.togUser(\''+esc(selU)+'\','+(en?"false":"true")+')"><i class="fas fa-'+(en?"pause":"play")+'"></i> '+(en?"Disable":"Enable")+'</button><button class="btn-sm btn-danger" onclick="SEC.delUser(\''+esc(selU)+'\',\''+esc((u.username||u.name||"").replace(/'/g,"\\'"))+'\')"><i class="fas fa-trash"></i> Delete</button>';var statusLabel=en?'<span style="color:var(--success);font-weight:600;"><span class="dot dot-on"></span>Active</span>':'<span style="color:var(--text-muted);font-weight:600;"><span class="dot dot-off"></span>Disabled</span>';var rl=sl(u.roles),rh='<div style="display:flex;flex-wrap:wrap;gap:6px;align-items:center;">';rl.forEach(function(rid){var ro=fr(rid),rt=ro?(ro.role||""):"",rn=ro?(ro.role_name||rid.slice(0,8)):rid.slice(0,8);rh+='<span class="rb rb-'+esc(rt.toLowerCase())+'">'+esc(rn)+' <button style="border:none;background:transparent;cursor:pointer;color:inherit;opacity:.6;font-size:11px;" onclick="SEC.rmRole(\''+esc(selU)+'\',\''+esc(rid)+'\')">&times;</button></span>';});rh+='<button class="btn-sm" style="padding:2px 8px;font-size:10px;" onclick="SEC.openAR(\''+esc(selU)+'\')"><i class="fas fa-plus"></i> add</button></div>';var ti=u.token_id?(u.token_id.slice(0,8)+"\u2026"+u.token_id.slice(-6)):"\u2014";var th='<div style="display:flex;align-items:center;gap:8px;"><span style="font-family:var(--mono);font-size:11px;">'+esc(ti)+"</span>";if(!pr)th+=' <button class="btn-sm" style="padding:2px 8px;font-size:10px;" onclick="SEC.regenTok(\''+esc(selU)+'\')"><i class="fas fa-rotate"></i> Regenerate</button>';th+="</div>";c.innerHTML='<div class="config-grid"><div class="config-card"><div class="cl">Status</div><div class="cv">'+statusLabel+'</div></div><div class="config-card"><div class="cl">Username</div><div class="cv" style="font-family:var(--mono);">'+esc(u.username||u.name||"\u2014")+(pr?' <span class="rb rb-superadmin">system</span>':"")+'</div></div>'+(u.display_name?'<div class="config-card"><div class="cl">Display name</div><div class="cv">'+esc(u.display_name)+'</div></div>':"")+'<div class="config-card"><div class="cl">Created</div><div class="cv">'+esc(fd(u.created_ms||u.created_at))+'</div></div><div class="config-card full"><div class="cl">Assigned roles</div><div class="cv" style="font-family:var(--font);">'+rh+'</div></div>'+(pr?'':'<div class="config-card full"><div class="cl"><i class="fas fa-key" style="color:var(--amber);margin-right:4px;"></i>Login token</div><div class="cv">'+th+'</div></div>')+'</div>';}

/* ROLES LIST */
function tblSum(r){var t=r.tables;if(Array.isArray(t))return(t.length===1&&t[0]==="*")?"all":t.length;if(typeof t==="object"&&t){var k=Object.keys(t);if(k.length===1&&k[0]==="*")return"all";return k.filter(function(x){return x!=="*";}).length;}return"all";}
function rRL(){var w=$("listR"),q=($("searchR").value||"").toLowerCase();var items=roles;if(q)items=items.filter(function(r){return(r.role_name||"").toLowerCase().indexOf(q)>=0||(r.role||"").toLowerCase().indexOf(q)>=0;});if(!items.length){w.innerHTML='<div class="md-empty">No roles found.</div>';return;}var h="";items.forEach(function(r){var rid=r.role_id||r.id,en=r.enabled!==false,ts=tblSum(r);h+='<div class="md-item'+(selR===rid?" active":"")+'" data-id="'+esc(rid)+'" style="'+(en?"":"opacity:.45;")+'"><div class="mi-av" style="background:rgba(28,150,202,.08);color:var(--primary);"><i class="fas fa-shield-halved" style="font-size:12px;"></i></div><div class="mi-body"><div class="mi-name"><span class="dot '+(en?"dot-on":"dot-off")+'"></span>'+esc(r.role_name||rid)+'</div><div class="mi-sub">'+esc(r.role||"reader")+" \u00b7 "+(ts==="all"?"all tables":ts+" table(s)")+"</div></div></div>";});w.innerHTML=h;w.querySelectorAll(".md-item").forEach(function(el){el.addEventListener("click",function(){selR=el.dataset.id;selRT=null;_loadRoleIntoEditor();rRL();rRD();});});}

/* ROLES DETAIL — SPLIT EDITOR */
function _loadRoleIntoEditor(){var r=selR?fr(selR):null;eRT={};selRT=null;_dirty=false;if(!r)return;var t=r.tables;if(typeof t==="object"&&!Array.isArray(t)&&t){Object.keys(t).forEach(function(tn){if(tn==="*")return;var e=t[tn],cols=e.columns||["*"],flt=e.filters||["*"];var ac=Array.isArray(cols)&&cols.length===1&&cols[0]==="*";var af=Array.isArray(flt)&&flt.length===1&&flt[0]==="*";eRT[tn]={columns:ac?[]:cols.slice(),allCols:ac,filters:af?[]:(Array.isArray(flt)?flt.slice():[])};});}var tn=Object.keys(eRT);if(tn.length)selRT=tn[0];}
function _markDirty(){if(_dirty)return;_dirty=true;var btn=$("btnRoleSave");if(btn){btn.classList.add("btn-primary");btn.innerHTML='<i class="fas fa-save"></i> Save';}}
function _isWild(r){var t=r.tables;if(!t)return true;if(typeof t==="object"&&!Array.isArray(t)){var k=Object.keys(t);if(k.length===0||k.length===1&&k[0]==="*")return true;}return false;}

function rRD(){
  var c=$("rContent"),em=$("rEmpty"),ac=$("rActs");
  if(!selR){c.innerHTML="";em.style.display="flex";$("rName").textContent="\u2014";$("rMeta").textContent="";ac.innerHTML="";return;}
  em.style.display="none";
  var r=roles.find(function(x){return(x.role_id||x.id)===selR;});if(!r){c.innerHTML="";return;}
  var sa=(r.role||"").toLowerCase()==="superadmin",en=r.enabled!==false;
  var uCnt=usersForRole(selR),eCnt=epsForRole(selR);
  $("rName").textContent=r.role_name||"\u2014";$("rMeta").textContent="ID: "+selR;
  ac.innerHTML=sa?'<span style="font-size:10px;color:var(--text-muted);display:flex;align-items:center;gap:4px;"><i class="fas fa-lock"></i> System role</span>'
    :'<button class="btn-sm" id="btnRoleSave" onclick="SEC.saveRoleTables()"><i class="fas fa-pen"></i> Edit</button><button class="btn-sm" onclick="SEC.cloneRole(\''+esc(selR)+'\')"><i class="fas fa-copy"></i> Clone</button><button class="btn-sm" onclick="SEC.togRole(\''+esc(selR)+'\','+(en?"false":"true")+','+uCnt+','+eCnt+')"><i class="fas fa-'+(en?"pause":"play")+'"></i> '+(en?"Disable":"Enable")+'</button><button class="btn-sm btn-danger" onclick="SEC.delRole(\''+esc(selR)+'\','+uCnt+','+eCnt+')"><i class="fas fa-trash"></i></button>';
  var statusLabel=en?'<span style="color:var(--success);font-weight:600;"><span class="dot dot-on"></span>Active</span>':'<span style="color:var(--text-muted);font-weight:600;"><span class="dot dot-off"></span>Disabled</span>';
  var usersHtml='<div style="display:flex;flex-wrap:wrap;gap:4px;align-items:center;">';
  if(uCnt>0){users.forEach(function(u){if(sl(u.roles).indexOf(selR)>=0){var uid=u.user_id||u.hash||u.id;usersHtml+='<span style="display:inline-flex;align-items:center;gap:3px;padding:2px 8px;border-radius:999px;background:rgba(124,58,237,.06);border:1px solid rgba(124,58,237,.15);font-size:10px;color:var(--purple);"><i class="fas fa-user" style="font-size:8px;"></i>'+esc(u.display_name||u.username||u.name||"\u2014")+' <button style="border:none;background:transparent;cursor:pointer;color:inherit;opacity:.5;font-size:11px;" onclick="SEC.rmUserFromRole(\''+esc(uid)+'\',\''+esc(selR)+'\')">&times;</button></span>';}});}
  usersHtml+='<button class="btn-sm" style="padding:2px 8px;font-size:10px;" onclick="SEC.openAddUserToRole(\''+esc(selR)+'\')"><i class="fas fa-plus"></i> add</button></div>';
  var h='<div class="config-grid" style="grid-template-columns:1fr 1fr 1fr;"><div class="config-card"><div class="cl">Status</div><div class="cv">'+statusLabel+'</div></div><div class="config-card"><div class="cl">Type</div><div class="cv">'+rbH(r.role)+'</div></div><div class="config-card"><div class="cl">OData</div><div class="cv">'+eCnt+'</div></div></div><div class="config-grid" style="margin-top:10px;grid-template-columns:1fr;"><div class="config-card"><div class="cl"><i class="fas fa-users" style="color:var(--purple);margin-right:3px;"></i>Assigned users</div><div class="cv" style="font-family:var(--font);font-weight:400;">'+usersHtml+'</div></div></div>';
  if(sa){h+='<div style="padding:14px;text-align:center;color:var(--text-muted);font-size:12px;margin-top:10px;"><i class="fas fa-infinity" style="color:var(--primary);margin-right:4px;"></i> Superadmin has unrestricted access.</div>';c.innerHTML=h;return;}
  // 3-column split: available | added | editor
  h+='<div class="re-split" id="reSplit">'
    +'<div class="re-col re-col-avail"><div class="re-col-hdr"><span>Available</span><span class="re-cnt" id="reAvailCnt">0</span><button class="btn-sm" style="padding:1px 6px;font-size:9px;margin-left:auto;" onclick="SEC.reAddAll()"><i class="fas fa-plus"></i> All</button></div><div class="re-col-search"><input id="reSearchAvail" placeholder="Filter\u2026"/></div><div class="re-col-items" id="reAvailList"></div></div>'
    +'<div class="re-col re-col-added"><div class="re-col-hdr"><span>Added</span><span class="re-cnt" id="reAddedCnt">0</span><button class="btn-sm" style="padding:1px 6px;font-size:9px;margin-left:auto;" onclick="SEC.reRemoveAll()"><i class="fas fa-minus"></i> All</button></div><div class="re-col-search"><input id="reSearchAdded" placeholder="Filter\u2026"/></div><div class="re-col-items" id="reAddedList"></div></div>'
    +'<div class="re-col re-col-editor" id="reEditor"></div>'
    +'</div>';
  c.innerHTML=h;reAvailList();reAddedList();reEditor();
  $("reSearchAvail").addEventListener("input",debounce(reAvailList,120));
  $("reSearchAdded").addEventListener("input",debounce(reAddedList,120));
}

function reAvailList(){
  var w=$("reAvailList"),cnt=$("reAvailCnt");if(!w)return;
  var cur=Object.keys(eRT),all=Object.keys(schemas).sort();
  var avail=all.filter(function(t){return cur.indexOf(t)<0;});
  var q=(($("reSearchAvail")||{}).value||"").toLowerCase();
  if(q)avail=avail.filter(function(t){return t.toLowerCase().indexOf(q)>=0;});
  if(cnt)cnt.textContent=all.length-cur.length;
  if(!avail.length){w.innerHTML='<div class="re-empty"><i class="fas fa-'+(cur.length===all.length?'check-circle':'search')+'"></i><div>'+(cur.length===all.length?'All added':(q?'No match':'None'))+'</div></div>';return;}
  var h="";avail.forEach(function(t){
    h+='<div class="re-ti re-ti-avail" data-t="'+esc(t)+'">'
      +'<div class="re-ti-ic"><i class="fas fa-table"></i></div>'
      +'<span class="re-ti-nm">'+esc(t)+'</span>'
      +'<span class="re-ti-arr">&rarr;</span></div>';
  });
  w.innerHTML=h;
  w.querySelectorAll(".re-ti-avail").forEach(function(el){el.addEventListener("click",function(){
    var t=el.dataset.t;eRT[t]={columns:[],allCols:true,filters:[]};selRT=t;_markDirty();
    reAvailList();reAddedList();reEditor();
  });});
}

function reAddedList(){
  var w=$("reAddedList"),cnt=$("reAddedCnt");if(!w)return;
  var tn=Object.keys(eRT);
  if(cnt)cnt.textContent=tn.length;
  var q=(($("reSearchAdded")||{}).value||"").toLowerCase();
  var filtered=tn;if(q)filtered=tn.filter(function(t){return t.toLowerCase().indexOf(q)>=0;});
  if(!tn.length){w.innerHTML='<div class="re-empty"><i class="fas fa-table"></i><div>Click to add</div></div>';return;}
  if(!filtered.length){w.innerHTML='<div class="re-empty"><i class="fas fa-search"></i><div>No match</div></div>';return;}
  var h="";filtered.forEach(function(n){
    var e=eRT[n],cc=e.allCols?"*":e.columns.length;
    h+='<div class="re-ti re-ti-added'+(selRT===n?" active":"")+'" data-t="'+esc(n)+'">'
      +'<div class="re-ti-ic"><i class="fas fa-table"></i></div>'
      +'<span class="re-ti-nm">'+esc(n)+'</span>'
      +'<span class="re-ti-cnt">'+cc+'</span>'
      +'<span class="re-ti-x" data-t="'+esc(n)+'">&times;</span></div>';
  });
  w.innerHTML=h;
  w.querySelectorAll(".re-ti-added").forEach(function(el){el.addEventListener("click",function(ev){
    if(ev.target.closest(".re-ti-x")){
      var tn=ev.target.closest(".re-ti-x").dataset.t;delete eRT[tn];_markDirty();
      if(selRT===tn){var k=Object.keys(eRT);selRT=k.length?k[0]:null;}
      reAvailList();reAddedList();reEditor();return;
    }
    selRT=el.dataset.t;reAddedList();reEditor();
  });});
}

function reEditor(){
  var w=$("reEditor");if(!w)return;
  if(!selRT||!eRT[selRT]){
    w.innerHTML='<div class="re-empty"><i class="fas fa-columns" style="font-size:18px;"></i>'
      +'<div style="display:flex;flex-direction:column;gap:6px;margin-top:4px;">'
      +'<div class="re-step"><div class="re-step-n">1</div><div class="re-step-t">Click a table from Available to add it</div></div>'
      +'<div class="re-step"><div class="re-step-n">2</div><div class="re-step-t">Select it in Added to edit columns and filters</div></div>'
      +'<div class="re-step"><div class="re-step-n">3</div><div class="re-step-t">Click Save when done</div></div>'
      +'</div></div>';return;
  }
  var n=selRT,e=eRT[n],sc=schemas[n]||[];
  var cc=e.allCols?"all":e.columns.length,fc=e.filters.length;
  var h='<div class="re-editor-hdr"><i class="fas fa-table" style="font-size:10px;color:var(--primary);"></i><span class="tn">'+esc(n)+'</span><span class="ts" id="reStats">'+cc+' cols \u00b7 '+fc+' filter(s)</span></div>'
    +'<div class="re-editor-body" id="reEdBody">'
    +'<div class="re-col-bar"><span class="lb">Columns</span><span class="re-sel" data-a="all">all</span><span class="re-sel" data-a="none">none</span></div>'
    +'<div class="re-chips" id="reChips">';
  if(sc.length){sc.forEach(function(s){var on=e.allCols||e.columns.indexOf(s.name)>=0;h+='<span class="col-chip '+(on?"on":"off")+'" data-c="'+esc(s.name)+'">'+esc(s.name)+'</span>';});}
  else h+='<span style="font-size:11px;color:var(--text-muted);">'+(e.allCols?"All columns":"Cols: "+e.columns.join(", "))+'</span>';
  h+='</div><div class="re-flt-bar"><span class="lb">Filters</span><button class="btn-sm" style="padding:1px 8px;font-size:10px;" data-a="add-f"><i class="fas fa-plus"></i> add</button></div><div id="reFilters" style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:6px;">';
  if(fc){e.filters.forEach(function(f,fi){if(typeof f==="object"){Object.keys(f).forEach(function(fk){var fv=f[fk]||{};h+='<div class="te-filter-pill" data-fi="'+fi+'"><span style="font-weight:600;color:#4338ca;">'+esc(fk)+'</span><span style="color:#94a3b8;">'+esc(fv.operation||"=")+'</span><span style="font-family:var(--mono);color:#4f46e5;font-weight:600;">'+esc(fv.value||"")+'</span><span class="te-filter-x" data-fi="'+fi+'">&times;</span></div>';});}});}
  h+='</div><div id="reFilterEdit"></div></div>';w.innerHTML=h;
}

function _showFilterEditRow(){
  var w=$("reFilterEdit");if(!w||!selRT||!eRT[selRT])return;
  var sc=schemas[selRT]||[];
  var co="";sc.forEach(function(s){co+='<option value="'+esc(s.name)+'">'+esc(s.name)+'</option>';});
  if(!co)co='<option value="column">column</option>';
  var ops='<option value="=">=</option><option value="!=">!=</option><option value="<">&lt;</option><option value=">">&gt;</option><option value="<="><=</option><option value=">=">>=</option><option value="LIKE">LIKE</option><option value="IN">IN</option>';
  w.innerHTML='<div style="display:flex;align-items:center;gap:6px;padding:4px 0;"><select id="reFCol" style="padding:4px 6px;border:1px solid var(--border);border-radius:4px;font-size:11px;font-family:var(--font);outline:none;min-width:100px;">'+co+'</select><select id="reFOp" style="padding:4px 6px;border:1px solid var(--border);border-radius:4px;font-size:11px;font-family:var(--font);outline:none;">'+ops+'</select><input id="reFVal" placeholder="value" style="flex:1;min-width:60px;padding:4px 6px;border:1px solid var(--border);border-radius:4px;font-size:11px;font-family:var(--font);outline:none;"/><button class="btn-sm btn-primary" style="padding:2px 8px;font-size:10px;" data-a="save-f"><i class="fas fa-check"></i></button><button class="btn-sm" style="padding:2px 6px;font-size:10px;" data-a="cancel-f"><i class="fas fa-times"></i></button></div>';
  var inp=$("reFVal");if(inp)inp.focus();
}

function _saveFilterFromRow(){
  if(!selRT||!eRT[selRT])return;
  var col=$("reFCol"),op=$("reFOp"),val=$("reFVal");
  if(!col||!val)return;
  var cv=col.value,ov=op.value,vv=(val.value||"").trim();
  if(!cv){return;}
  eRT[selRT].filters.push({[cv]:{operation:ov,type:"value",value:vv}});
  _markDirty();reEditor();
}

function reAddAll(){Object.keys(schemas).sort().forEach(function(t){if(!eRT[t])eRT[t]={columns:[],allCols:true,filters:[]};});var k=Object.keys(eRT);if(!selRT&&k.length)selRT=k[0];_markDirty();reAvailList();reAddedList();reEditor();}
function reRemoveAll(){eRT={};selRT=null;_markDirty();reAvailList();reAddedList();reEditor();}

/* Delegated events for column editor — only ONE table rendered at a time */
document.addEventListener("click",function(ev){
  var tgt=ev.target,chip=tgt.closest(".col-chip");
  if(chip&&chip.closest("#reChips")&&selRT&&eRT[selRT]){var e=eRT[selRT],cn=chip.dataset.c,sc=schemas[selRT]||[];if(e.allCols){e.columns=sc.map(function(s){return s.name;}).filter(function(nn){return nn!==cn;});e.allCols=false;}else{var i=e.columns.indexOf(cn);if(i>=0)e.columns.splice(i,1);else e.columns.push(cn);if(sc.length&&e.columns.length>=sc.length){e.allCols=true;e.columns=[];}}if(e.allCols){document.querySelectorAll("#reChips .col-chip").forEach(function(c){c.classList.add("on");c.classList.remove("off");});}else{var on=e.columns.indexOf(cn)>=0;chip.classList.toggle("on",on);chip.classList.toggle("off",!on);}_markDirty();_reUpd();return;}
  var sa=tgt.closest(".re-sel");if(sa&&selRT&&eRT[selRT]){var e=eRT[selRT];if(sa.dataset.a==="all"){e.allCols=true;e.columns=[];document.querySelectorAll("#reChips .col-chip").forEach(function(c){c.classList.add("on");c.classList.remove("off");});}else{e.allCols=false;e.columns=[];document.querySelectorAll("#reChips .col-chip").forEach(function(c){c.classList.remove("on");c.classList.add("off");});}_markDirty();_reUpd();return;}
  var af=tgt.closest('[data-a="add-f"]');if(af&&selRT&&eRT[selRT]){_showFilterEditRow();return;}
  var sf=tgt.closest('[data-a="save-f"]');if(sf){_saveFilterFromRow();return;}
  var cf=tgt.closest('[data-a="cancel-f"]');if(cf){var w=$("reFilterEdit");if(w)w.innerHTML="";return;}
  var fx=tgt.closest(".te-filter-x");if(fx&&selRT&&eRT[selRT]){eRT[selRT].filters.splice(parseInt(fx.dataset.fi),1);_markDirty();reEditor();return;}
});
/* Enter key in filter value input saves */
document.addEventListener("keydown",function(ev){if(ev.key==="Enter"&&ev.target.id==="reFVal"){ev.preventDefault();_saveFilterFromRow();}});
function _reUpd(){var h=$("reStats");if(!h||!selRT||!eRT[selRT])return;var e=eRT[selRT];h.textContent=(e.allCols?"all":e.columns.length)+" cols \u00b7 "+e.filters.length+" filter(s)";var ti=document.querySelector('.re-ti-added.active .re-ti-cnt');if(ti)ti.textContent=e.allCols?"*":e.columns.length;}

function renderAll(){tiles();rUL();rUD();rRL();rRD();rOL();rOD();}

/* USER CRUD */
async function createU(){var un=($("edUname").value||"").trim();if(!un){$("edUErr").innerHTML='<div class="flash-error">Username is required.</div>';return;}if(!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(un)){$("edUErr").innerHTML='<div class="flash-error">Invalid username.</div>';return;}var dn=($("edDname").value||"").trim(),sc=document.querySelector("#rtgUser .role-type-card.selected");if(!sc){$("edUErr").innerHTML='<div class="flash-error">Select a role type.</div>';return;}var rt=sc.getAttribute("data-role"),c=ctx();try{var res=await POST("/api/v1/rbac/users/create",{username:un,display_name:dn,role_type:rt,org:c.org,sup:c.sup});hide("mUser");if(res.token){$("tokU").textContent=un;$("tokV").textContent=res.token;show("mTok");}else toast("Created");await refresh();}catch(e){$("edUErr").innerHTML='<div class="flash-error">'+esc(e.message)+"</div>";}}
async function delUser(uid,nm){if(!confirm('Delete user "'+nm+'"?'))return;var c=ctx();try{await DEL("/api/v1/rbac/users/"+enc(uid)+"?org="+enc(c.org)+"&sup="+enc(c.sup));selU=null;toast("Deleted");await refresh();}catch(e){toast(e.message);}}
async function togUser(uid,en){if(en==="false"||en===false){if(!confirm("Disable this user?"))return;en=false;}else en=true;var c=ctx();try{await POST("/api/v1/rbac/users/"+enc(uid)+"/toggle",{org:c.org,sup:c.sup,enabled:en});toast(en?"Enabled":"Disabled");await refresh();}catch(e){toast(e.message);}}
function editUser(uid){var u=users.find(function(x){return(x.user_id||x.hash||x.id)===uid;});if(!u)return;_editUid=uid;$("edEUname").value=u.username||u.name||"";$("edEDname").value=u.display_name||"";$("edEUErr").innerHTML="";show("mEditUser");}
async function saveEditUser(){if(!_editUid)return;var un=($("edEUname").value||"").trim(),dn=($("edEDname").value||"").trim();if(!un){$("edEUErr").innerHTML='<div class="flash-error">Username required.</div>';return;}var c=ctx();try{await PUT("/api/v1/rbac/users/"+enc(_editUid)+"/edit",{org:c.org,sup:c.sup,username:un,display_name:dn});hide("mEditUser");toast("Saved");await refresh();}catch(e){$("edEUErr").innerHTML='<div class="flash-error">'+esc(e.message)+"</div>";}}
async function rmRole(uid,rid){var c=ctx();try{await DEL("/api/v1/rbac/users/"+enc(uid)+"/roles/"+enc(rid)+"?org="+enc(c.org)+"&sup="+enc(c.sup));await refresh();}catch(e){toast(e.message);}}
function openAR(uid){var u=users.find(function(x){return(x.user_id||x.hash||x.id)===uid;}),cr=sl(u?u.roles:[]),l=$("rpList");l.innerHTML="";$("rpErr").innerHTML="";roles.forEach(function(r){var rid=r.role_id||r.id||"",al=cr.indexOf(rid)>=0;var d=document.createElement("div");d.className="role-picker-item"+(al?" already":"");d.innerHTML='<div>'+rbH(r.role)+' <span style="font-size:11px;color:var(--text-muted);margin-left:4px;">'+esc((r.role_name||"").slice(0,30))+"</span></div><span style=\"font-size:10px;color:var(--text-muted);\">"+(al?"assigned":"")+"</span>";if(!al)d.addEventListener("click",function(){addUR(uid,rid);});l.appendChild(d);});show("mAddRole");}
async function addUR(uid,rid){var c=ctx();try{await POST("/api/v1/rbac/users/"+enc(uid)+"/roles",{org:c.org,sup:c.sup,role_id:rid});hide("mAddRole");await refresh();}catch(e){$("rpErr").innerHTML='<div class="flash-error">'+esc(e.message)+"</div>";}}
async function regenTok(uid){if(!confirm("Regenerate token?"))return;var c=ctx();try{var res=await POST("/api/v1/rbac/users/"+enc(uid)+"/regenerate-token",{org:c.org,sup:c.sup});$("tokU").textContent=res.username||"";$("tokV").textContent=res.token||"";show("mTok");await refresh();}catch(e){toast(e.message);}}

/* ROLE ↔ USER assignment from role detail */
function openAddUserToRole(rid){
  var l=$("ruList");l.innerHTML="";$("ruErr").innerHTML="";
  var assigned=[];users.forEach(function(u){if(sl(u.roles).indexOf(rid)>=0)assigned.push(u.user_id||u.hash||u.id);});
  users.forEach(function(u){
    var uid=u.user_id||u.hash||u.id,al=assigned.indexOf(uid)>=0;
    var d=document.createElement("div");d.className="role-picker-item"+(al?" already":"");
    d.innerHTML='<div style="display:flex;align-items:center;gap:6px;"><span style="width:22px;height:22px;border-radius:50%;background:rgba(124,58,237,.08);color:var(--purple);display:inline-flex;align-items:center;justify-content:center;font-size:9px;font-weight:600;">'+esc(initials(u))+'</span><span style="font-size:11px;font-weight:600;">'+esc(u.display_name||u.username||u.name||"")+'</span></div><span style="font-size:10px;color:var(--text-muted);">'+(al?"assigned":"")+"</span>";
    if(!al)d.addEventListener("click",function(){_addUserToRole(uid,rid);});
    l.appendChild(d);
  });show("mAddUserToRole");
}
async function _addUserToRole(uid,rid){var c=ctx();try{await POST("/api/v1/rbac/users/"+enc(uid)+"/roles",{org:c.org,sup:c.sup,role_id:rid});hide("mAddUserToRole");toast("Assigned");await refresh();}catch(e){$("ruErr").innerHTML='<div class="flash-error">'+esc(e.message)+"</div>";}}
async function rmUserFromRole(uid,rid){var c=ctx();try{await DEL("/api/v1/rbac/users/"+enc(uid)+"/roles/"+enc(rid)+"?org="+enc(c.org)+"&sup="+enc(c.sup));toast("Removed");await refresh();}catch(e){toast(e.message);}}

/* ROLE CRUD */
function openNR(){$("mRTitle").innerHTML='<i class="fas fa-shield-halved" style="color:var(--primary);"></i> New Role';var c=ctx();$("mROrg").textContent=c.org;$("mRSup").textContent=c.sup;$("edRN").value="";document.querySelectorAll("#rtgRole .role-type-card").forEach(function(c){c.classList.remove("selected");});document.querySelector('#rtgRole .role-type-card[data-role="reader"]').classList.add("selected");$("edRErr").innerHTML="";show("mRole");}
async function saveR(){var c=ctx(),nm=($("edRN").value||"").trim();if(!nm){$("edRErr").innerHTML='<div class="flash-error">Name required.</div>';return;}var sc=document.querySelector("#rtgRole .role-type-card.selected"),rt=sc?sc.getAttribute("data-role"):"reader";try{await POST("/api/v1/rbac/roles",{organization:c.org,super_name:c.sup,role_name:nm,role:rt,tables:{"*":{columns:["*"],filters:["*"]}}});hide("mRole");toast("Created \u2014 select to configure tables");await refresh();}catch(e){$("edRErr").innerHTML='<div class="flash-error">'+esc(e.message)+"</div>";}}
async function saveRoleTables(){if(!selR||!_dirty)return;var r=fr(selR);if(!r)return;var c=ctx(),to={};var tn=Object.keys(eRT);if(!tn.length)to={"*":{columns:["*"],filters:["*"]}};else tn.forEach(function(n){var e=eRT[n];to[n]={columns:e.allCols?["*"]:e.columns.slice(),filters:e.filters.length?e.filters:["*"]};});try{await PUT("/api/v1/rbac/roles/"+enc(selR),{organization:c.org,super_name:c.sup,role_name:r.role_name,role:r.role,tables:to});_dirty=false;toast("Saved");await refresh();}catch(e){toast("Save failed: "+e.message);}}
async function delRole(rid,uCnt,eCnt){var msg="Delete this role?";if(uCnt)msg+="\n\n\u26a0 "+uCnt+" user(s) affected.";if(eCnt)msg+="\n\u26a0 "+eCnt+" OData endpoint(s) affected.";if(!confirm(msg))return;var c=ctx();try{await DEL("/api/v1/rbac/roles/"+enc(rid)+"?organization="+enc(c.org)+"&super_name="+enc(c.sup));selR=null;toast("Deleted");await refresh();}catch(e){toast(e.message);}}
async function togRole(rid,en,uCnt,eCnt){if(en==="false"||en===false){var msg="Disable?";if(uCnt)msg+=" "+uCnt+" user(s) affected.";if(!confirm(msg))return;en=false;}else en=true;var c=ctx();try{await POST("/api/v1/rbac/roles/"+enc(rid)+"/toggle",{org:c.org,sup:c.sup,enabled:en});toast(en?"Enabled":"Disabled");await refresh();}catch(e){toast(e.message);}}
function cloneRole(rid){_cloneRid=rid;var r=fr(rid);$("edCloneName").value=(r?r.role_name||"":"")+"_copy";$("edCloneErr").innerHTML="";show("mClone");}
async function saveClone(){if(!_cloneRid)return;var nm=($("edCloneName").value||"").trim();if(!nm){$("edCloneErr").innerHTML='<div class="flash-error">Name required.</div>';return;}var c=ctx();try{await POST("/api/v1/rbac/roles/"+enc(_cloneRid)+"/clone",{org:c.org,sup:c.sup,role_name:nm});hide("mClone");toast("Cloned");await refresh();}catch(e){$("edCloneErr").innerHTML='<div class="flash-error">'+esc(e.message)+"</div>";}}

/* ODATA LIST+DETAIL */
function rOL(){var w=$("listO"),q=($("searchO").value||"").toLowerCase();var items=eps;if(q)items=items.filter(function(e){return(e.label||"").toLowerCase().indexOf(q)>=0||(e.role_name||"").toLowerCase().indexOf(q)>=0;});if(!items.length){w.innerHTML='<div class="md-empty">No endpoints.</div>';return;}var h="";items.forEach(function(ep){var eid=ep.endpoint_id||"",en=!!ep.enabled;h+='<div class="md-item'+(selO===eid?" active":"")+'" data-id="'+esc(eid)+'"><div class="mi-av" style="background:rgba(16,185,129,.08);color:var(--success);"><i class="fas fa-plug" style="font-size:12px;"></i></div><div class="mi-body"><div class="mi-name"><span class="dot '+(en?"dot-on":"dot-off")+'"></span>'+esc(ep.label||"\u2014")+'</div><div class="mi-sub">'+esc(ep.role_name||"\u2014")+"</div></div></div>";});w.innerHTML=h;w.querySelectorAll(".md-item").forEach(function(el){el.addEventListener("click",function(){selO=el.dataset.id;rOL();rOD();});});}
function rOD(){var c=$("oContent"),em=$("oEmpty"),ac=$("oActs");if(!selO){c.innerHTML="";em.style.display="flex";$("oName").textContent="\u2014";$("oMeta").textContent="";ac.innerHTML="";return;}em.style.display="none";var ep=eps.find(function(x){return x.endpoint_id===selO;});if(!ep){c.innerHTML="";return;}var en=!!ep.enabled,roleObj=fr(ep.role_id),roleOff=roleObj&&roleObj.enabled===false;$("oName").textContent=ep.label||"\u2014";$("oMeta").textContent="ID: "+selO.slice(0,12)+"\u2026";ac.innerHTML='<button class="btn-sm" onclick="SEC.regenEp(\''+esc(selO)+'\')"><i class="fas fa-rotate"></i></button><button class="btn-sm" onclick="SEC.togEp(\''+esc(selO)+'\','+(en?"false":"true")+')"><i class="fas fa-'+(en?"pause":"play")+'"></i></button><button class="btn-sm btn-danger" onclick="SEC.delEp(\''+esc(selO)+'\')"><i class="fas fa-trash"></i></button>';var sl2=en?'<span style="color:var(--success);font-weight:600;"><span class="dot dot-on"></span>Active</span>':'<span style="color:var(--text-muted);font-weight:600;"><span class="dot dot-off"></span>Disabled</span>';if(roleOff)sl2+=' <span style="padding:2px 6px;border-radius:3px;background:var(--warn-bg);font-size:10px;font-weight:700;color:#92400e;">role off</span>';var host=window.location.origin,url=ep.odata_url||"";c.innerHTML='<div class="config-grid"><div class="config-card"><div class="cl">Status</div><div class="cv">'+sl2+'</div></div><div class="config-card"><div class="cl">Role</div><div class="cv">'+rbH((roleObj||{}).role||"reader")+' '+esc(ep.role_name||"\u2014")+'</div></div><div class="config-card"><div class="cl">Token</div><div class="cv" style="font-family:var(--mono);font-size:11px;">'+esc(ep.token_prefix||"\u2014")+'</div></div><div class="config-card"><div class="cl">Last access</div><div class="cv">'+esc(ep.last_accessed_at||"Never")+'</div></div></div><div class="odata-url-box"><div class="ou-label"><i class="fas fa-link" style="font-size:10px;"></i> OData URL</div><div class="ou-val"><code>'+esc(host+url)+'</code><button class="odata-copy" id="cpOU">Copy</button></div></div>';$("cpOU").addEventListener("click",function(){navigator.clipboard.writeText(host+url);this.textContent="Copied!";var b=this;setTimeout(function(){b.textContent="Copy";},1500);});}
function openNE(){$("edEpL").value="";$("edEpErr").innerHTML="";var s=$("edEpR");s.innerHTML="";roles.forEach(function(r){var o=document.createElement("option");o.value=(r.role_id||r.id)+"|"+(r.role_name||"");o.textContent=esc(r.role_name||"")+" ("+esc(r.role||"")+")";s.appendChild(o);});if(!roles.length)s.innerHTML='<option value="">No roles</option>';show("mEp");}
async function saveEp(){var l=($("edEpL").value||"").trim(),rv=$("edEpR").value||"";if(!rv){$("edEpErr").innerHTML='<div class="flash-error">Select a role.</div>';return;}var p=rv.split("|"),c=ctx();try{var r=await POST("/api/v1/odata/endpoints",{organization:c.org,super_name:c.sup,label:l,role_id:p[0]||"",role_name:p[1]||""});hide("mEp");$("epTokV").textContent=(r&&r.token)||"";$("epTokU").textContent=(r&&r.data&&r.data.odata_url)?window.location.origin+r.data.odata_url:"";show("mEpTok");await lEps();renderAll();}catch(e){$("edEpErr").innerHTML='<div class="flash-error">'+esc(e.message)+"</div>";}}
async function togEp(eid,en){en=(en==="true"||en===true);var c=ctx();try{await PUT("/api/v1/odata/endpoints/"+enc(eid),{organization:c.org,super_name:c.sup,enabled:en});toast(en?"Enabled":"Disabled");await lEps();renderAll();}catch(e){toast(e.message);}}
async function regenEp(eid){if(!confirm("Regenerate?"))return;var c=ctx();try{var r=await POST("/api/v1/odata/endpoints/"+enc(eid)+"/regenerate",{organization:c.org,super_name:c.sup});$("epTokV").textContent=(r&&r.token)||"";$("epTokU").textContent=(r&&r.data&&r.data.odata_url)?window.location.origin+r.data.odata_url:"";show("mEpTok");await lEps();renderAll();}catch(e){toast(e.message);}}
async function delEp(eid){if(!confirm("Delete?"))return;var c=ctx();try{await DEL("/api/v1/odata/endpoints/"+enc(eid)+"?organization="+enc(c.org)+"&super_name="+enc(c.sup));selO=null;toast("Deleted");await lEps();renderAll();}catch(e){toast(e.message);}}

/* WIRING */
document.querySelectorAll(".section-node[data-tab]").forEach(function(b){b.addEventListener("click",function(){tab=b.getAttribute("data-tab");document.querySelectorAll(".section-node[data-tab]").forEach(function(x){x.classList.toggle("active",x===b);});document.querySelectorAll(".pane").forEach(function(p){p.classList.toggle("active",p.id==="pane-"+tab);});$("btnNewLabel").textContent={users:"Create User",roles:"New Role",odata:"New Endpoint"}[tab]||"New";});});
$("btnNew").addEventListener("click",function(){if(tab==="users"){$("edUname").value="";$("edDname").value="";document.querySelectorAll("#rtgUser .role-type-card").forEach(function(c){c.classList.remove("selected");});document.querySelector('#rtgUser .role-type-card[data-role="reader"]').classList.add("selected");$("edUErr").innerHTML="";show("mUser");}else if(tab==="roles")openNR();else if(tab==="odata")openNE();});
$("btnSaveU").addEventListener("click",createU);
$("btnSaveR").addEventListener("click",saveR);
$("btnSaveEp").addEventListener("click",saveEp);
$("btnSaveEU").addEventListener("click",saveEditUser);
$("btnSaveClone").addEventListener("click",saveClone);
var _dU=debounce(rUL,150),_dR=debounce(rRL,150),_dO=debounce(rOL,150);
$("searchU").addEventListener("input",_dU);$("searchR").addEventListener("input",_dR);$("searchO").addEventListener("input",_dO);
$("btnCpTok").addEventListener("click",function(){var t=$("tokV").textContent||"",b=this;navigator.clipboard.writeText(t).then(function(){b.innerHTML='<i class="fas fa-check"></i> Copied';setTimeout(function(){b.innerHTML='<i class="fas fa-copy"></i> Copy';},1500);});});
$("btnCpEpTok").addEventListener("click",function(){var t=$("epTokV").textContent||"",b=this;navigator.clipboard.writeText(t).then(function(){b.innerHTML='<i class="fas fa-check"></i> Copied';setTimeout(function(){b.innerHTML='<i class="fas fa-copy"></i> Copy';},1500);});});
var _allModals=["mUser","mTok","mEditUser","mAddRole","mRole","mAddUserToRole","mClone","mEp","mEpTok"];
document.addEventListener("keydown",function(ev){if(ev.key!=="Escape")return;_allModals.forEach(function(id){var e=$(id);if(e&&e.style.display==="flex")e.style.display="none";});});
_allModals.forEach(function(id){var e=$(id);if(e)e.addEventListener("click",function(ev){if(ev.target===e)e.style.display="none";});});
var ln=document.getElementById("link-security")||document.getElementById("link-rbac");if(ln)ln.classList.add("active");
window.ccOnSuperChange=function(v){if(!v)return;var p=v.split("/");if(p.length<2)return;var r=$("root");if(r){r.dataset.org=p[0].trim();r.dataset.sup=p[1].trim();}selU=null;selR=null;selO=null;selRT=null;eRT={};lSchema().then(function(){refresh();});};
var _c=ctx();if(_c.org&&_c.sup)lSchema().then(function(){refresh();});
return{hide:hide,srt:function(card,gid){document.querySelectorAll("#"+gid+" .role-type-card").forEach(function(c){c.classList.remove("selected");});card.classList.add("selected");},delUser:delUser,togUser:togUser,editUser:editUser,rmRole:rmRole,openAR:openAR,regenTok:regenTok,saveRoleTables:saveRoleTables,cloneRole:cloneRole,delRole:delRole,togRole:togRole,reAddAll:reAddAll,reRemoveAll:reRemoveAll,openAddUserToRole:openAddUserToRole,rmUserFromRole:rmUserFromRole,togEp:togEp,regenEp:regenEp,delEp:delEp};
})();
window.addEventListener("supertable-changed",function(e){var d=(e&&e.detail)||{};if(!d.org||!d.sup)return;if(typeof window.ccOnSuperChange==="function")window.ccOnSuperChange(d.org+"/"+d.sup);else{var u=new URL("/ui/security",window.location.origin);u.searchParams.set("org",d.org);u.searchParams.set("sup",d.sup);window.location.href=u.toString();}});
