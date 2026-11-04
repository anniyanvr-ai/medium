"""Live progress dashboard (stdlib only).

    python -m s3mover dashboard --state-dir ./state --port 8765

Reads the job SQLite files READ-ONLY, so it can run in the mover process,
in a separate process, or on another host that mounts the state dir.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .state import snapshot

log = logging.getLogger("s3mover.dashboard")


def _pid_alive(pid):
    """True/False when it can be checked on this host (psutil), None otherwise.
    (os.kill(pid, 0) is NOT safe on Windows - it terminates the process.)"""
    if not pid:
        return None
    try:
        import psutil
    except ImportError:
        return None
    try:
        p = psutil.Process(int(pid))
        return p.is_running() and "python" in (p.name() or "").lower()
    except Exception:
        return False


def list_jobs(state_dir: Path) -> list[dict]:
    jobs = []
    for db in sorted(state_dir.glob("*.db")):
        try:
            snap = snapshot(db, sample_window=0)
            p = snap["progress"]
            phase = p.get("status") or p.get("phase") or "unknown"
            upd = snap["progress_updated"] or 0
            if phase in ("scanning", "transferring", "verifying", "planning", "starting") and time.time() - upd > 6:
                phase = "interrupted"
            jobs.append({"job": db.stem, "phase": phase,
                         "updated": snap["progress_updated"], "mode": snap["meta"].get("mode"),
                         "action": snap["meta"].get("action"), "dst_type": snap["meta"].get("dst_type")})
        except Exception as exc:  # corrupt / foreign file
            jobs.append({"job": db.stem, "phase": f"unreadable: {exc}", "updated": None})
    jobs.sort(key=lambda j: j["updated"] or 0, reverse=True)
    return jobs


def make_handler(state_dir: Path):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # keep the console quiet
            log.debug(fmt, *args)

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            u = urlparse(self.path)
            try:
                if u.path in ("/", "/index.html"):
                    self._send(200, PAGE.encode(), "text/html; charset=utf-8")
                elif u.path == "/api/jobs":
                    self._send(200, json.dumps(list_jobs(state_dir)).encode(), "application/json")
                elif u.path == "/api/status":
                    job = parse_qs(u.query).get("job", [""])[0]
                    db = state_dir / f"{Path(job).name}.db"
                    if not job or not db.exists():
                        self._send(404, b'{"error":"unknown job"}', "application/json")
                        return
                    snap = snapshot(db)
                    snap["server_time"] = time.time()
                    snap["pid_alive"] = _pid_alive((snap.get("progress") or {}).get("pid"))
                    self._send(200, json.dumps(snap, default=str).encode(), "application/json")
                else:
                    self._send(404, b"not found", "text/plain")
            except Exception as exc:
                self._send(500, json.dumps({"error": str(exc)}).encode(), "application/json")

    return Handler


def serve(state_dir: str | Path, host: str = "127.0.0.1", port: int = 8765,
          background: bool = False) -> ThreadingHTTPServer:
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    httpd = ThreadingHTTPServer((host, port), make_handler(state_dir))
    httpd.daemon_threads = True
    log.info("dashboard: http://%s:%d/", host, port)
    if background:
        threading.Thread(target=httpd.serve_forever, name="dashboard", daemon=True).start()
    else:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
    return httpd


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>s3mover · migration progress</title>
<style>
:root{color-scheme:light;
 --plane:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;
 --grid:#e1e0d9;--axis:#c3c2b7;--ring:rgba(11,11,11,.10);--s1:#2a78d6;--track:#ecebe6;
 --good:#0ca30c;--warn:#fab219;--serious:#ec835a;--crit:#d03b3b;--goodtext:#006300}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])){color-scheme:dark;
 --plane:#0d0d0d;--surface:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--muted:#898781;
 --grid:#2c2c2a;--axis:#383835;--ring:rgba(255,255,255,.10);--s1:#3987e5;--track:#2c2c2a;--goodtext:#0ca30c}}
:root[data-theme="dark"]{color-scheme:dark;--plane:#0d0d0d;--surface:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;
 --muted:#898781;--grid:#2c2c2a;--axis:#383835;--ring:rgba(255,255,255,.10);--s1:#3987e5;--track:#2c2c2a;--goodtext:#0ca30c}
*{box-sizing:border-box}
body{margin:0;background:var(--plane);color:var(--ink);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
.wrap{max-width:1240px;margin:0 auto;padding:20px 16px 40px}
header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;margin-bottom:16px}
h1{font-size:18px;margin:0;font-weight:650;letter-spacing:-.01em}
.sub{color:var(--ink2);font-size:13px}
select,button{font:inherit;color:var(--ink);background:var(--surface);border:1px solid var(--ring);border-radius:8px;padding:6px 10px}
.card{background:var(--surface);border:1px solid var(--ring);border-radius:12px;padding:16px}
.grid{display:grid;gap:12px}
.tiles{grid-template-columns:repeat(auto-fit,minmax(150px,1fr));margin:12px 0}
.tile .lbl{color:var(--ink2);font-size:12px}
.tile .val{font-size:24px;font-weight:650;margin-top:2px}
.tile .note{color:var(--muted);font-size:12px;margin-top:2px}
.badge{display:inline-flex;gap:6px;align-items:center;font-weight:600;font-size:13px;padding:3px 10px;border-radius:999px;border:1px solid var(--ring)}
.dot{width:8px;height:8px;border-radius:50%}
.bar{height:10px;background:var(--track);border-radius:999px;overflow:hidden;display:flex;gap:2px}
.bar > span{display:block;height:100%}
.meta{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:4px 24px;font-size:13px;margin-top:12px}
.meta b{font-weight:600;color:var(--ink2);font-weight:500;margin-right:6px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:var(--ink2);font-weight:500;border-bottom:1px solid var(--axis);padding:6px 8px;white-space:nowrap}
td{border-bottom:1px solid var(--grid);padding:6px 8px;font-variant-numeric:tabular-nums;vertical-align:middle}
td.k{font-family:ui-monospace,Consolas,monospace;font-size:12px;word-break:break-all}
.two{grid-template-columns:2fr 1fr}
@media (max-width:860px){.two{grid-template-columns:1fr}}
h2{font-size:14px;margin:0 0 10px;font-weight:600}
.chart{position:relative;height:200px}
svg{display:block;width:100%;height:100%;overflow:visible}
.tip{position:absolute;pointer-events:none;background:var(--surface);border:1px solid var(--ring);border-radius:8px;padding:6px 8px;font-size:12px;box-shadow:0 4px 14px rgba(0,0,0,.12);display:none;white-space:nowrap}
.mini{height:6px;min-width:80px}
.empty{color:var(--muted);font-size:13px;padding:8px 0}
.ok{color:var(--goodtext)}
.chip{display:inline-block;border:1px solid var(--ring);border-radius:999px;padding:1px 8px;margin:2px 2px;font-size:12px;font-variant-numeric:tabular-nums;white-space:nowrap}
.scroll{overflow-x:auto}
</style></head>
<body><div class="wrap">
<header>
  <div><h1 id="title">s3mover</h1><div class="sub" id="subtitle">loading…</div></div>
  <div style="display:flex;gap:8px;align-items:center">
    <span class="badge" id="phase"><span class="dot"></span><span>—</span></span>
    <select id="job" aria-label="Job"></select>
    <button id="theme" title="Toggle theme">◐</button>
  </div>
</header>

<div class="card">
  <div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;flex-wrap:wrap">
    <div><span style="font-size:32px;font-weight:650" id="pct">–</span>
      <span class="sub" id="pctnote"></span></div>
    <div class="sub" id="eta"></div>
  </div>
  <div class="bar" id="bar" style="margin-top:10px" role="img" aria-label="progress"></div>
  <div class="sub" style="margin-top:6px;display:flex;gap:16px;flex-wrap:wrap" id="legend"></div>
  <div class="meta" id="meta"></div>
</div>

<div class="grid tiles" id="tiles"></div>

<div class="grid two">
  <div class="card"><h2>Objects completed per minute</h2>
    <div class="chart" id="chart"><div class="tip" id="tip"></div></div></div>
  <div class="card"><h2>Integrity verification</h2><div id="verify"></div></div>
</div>

<div class="card" style="margin-top:12px"><h2>Shards</h2><div class="scroll"><table id="shards"></table></div></div>
<div class="card" style="margin-top:12px"><h2>Objects per month (date used by the filter, all listed objects)</h2><div class="sub" style="margin-bottom:8px">Compare with the window above: a folder with "Matched 0" has all its objects in months outside the window.</div><div class="scroll"><table id="months"></table></div></div>
<div class="card" style="margin-top:12px"><h2>Recent errors &amp; retries</h2><div class="scroll"><table id="errors"></table></div></div>
<div class="card" style="margin-top:12px"><h2>Runs (resume history)</h2><div class="scroll"><table id="runs"></table></div></div>
</div>
<script>
const $=id=>document.getElementById(id);
const fmtN=n=>n==null?'–':Number(n).toLocaleString();
function fmtB(b){if(b==null)return'–';const u=['B','KB','MB','GB','TB','PB'];let i=0;b=Number(b);while(b>=1024&&i<u.length-1){b/=1024;i++}return (i?b.toFixed(b<10?2:1):b)+' '+u[i]}
function fmtD(s){if(s==null)return'–';s=Math.round(s);const h=Math.floor(s/3600),m=Math.floor(s%3600/60),x=s%60;return h?`${h}h ${m}m`:m?`${m}m ${x}s`:`${x}s`}
const fmtT=t=>t?new Date(t*1000).toLocaleString():'–';
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const PHASE={complete:['var(--good)','✓ Complete'],incomplete:['var(--crit)','✕ Incomplete'],interrupted:['var(--warn)','❚❚ Interrupted — resume to continue'],
 failed:['var(--crit)','✕ Failed'],scanning:['var(--s1)','Scanning'],transferring:['var(--s1)','Transferring'],verifying:['var(--s1)','Verifying'],
 planning:['var(--s1)','Planning'],planned:['var(--good)','✓ Planned (dry run)'],starting:['var(--s1)','Starting']};
let job=new URLSearchParams(location.search).get('job')||'';

$('theme').onclick=()=>{const r=document.documentElement;const dark=r.dataset.theme?r.dataset.theme==='dark':matchMedia('(prefers-color-scheme: dark)').matches;r.dataset.theme=dark?'light':'dark';};

async function loadJobs(){
  const jobs=await (await fetch('api/jobs')).json();
  const sel=$('job');sel.innerHTML=jobs.map(j=>`<option value="${esc(j.job)}">${esc(j.job)} · ${esc(j.phase)}</option>`).join('');
  if(!job&&jobs.length)job=jobs[0].job; sel.value=job;
  sel.onchange=()=>{job=sel.value;history.replaceState(null,'','?job='+encodeURIComponent(job));tick();};
}

function tile(lbl,val,note){return `<div class="card tile"><div class="lbl">${lbl}</div><div class="val">${val}</div><div class="note">${note||''}</div></div>`}

function render(s){
  const p=s.progress||{}; const m=s.meta||{};
  // fresh progress = running; if a long step is silent, a live process id still counts as running
  const live=(p.updated&&(s.server_time-p.updated)<6)||(s.pid_alive===true&&!p.status);
  let phase=p.status||p.phase||'starting';
  if(!p.status&&!live&&['scanning','transferring','verifying','planning','starting'].includes(phase))phase='interrupted';
  const [col,label]=PHASE[phase]||['var(--muted)',phase];
  $('phase').innerHTML=`<span class="dot" style="background:${col}"></span><span>${label}${p.paused?' · paused':''}</span>`;
  $('title').textContent=`s3mover · ${m.job||job}`;
  $('subtitle').textContent=`${(p.src_type||m.src_type||'').toUpperCase()} → ${(p.dst_type||m.dst_type||'').toUpperCase()} · ${m.action||''} · mode: ${m.mode||''} · ${m.window||''}`;
  const matched=p.matched||0, done=p.complete||0, failed=p.failed||0;
  const pct=matched?100*done/matched:(phase==='complete'?100:0);
  $('pct').textContent=pct.toFixed(pct>=99.995||pct===0?0:2)+'%';
  $('pctnote').textContent=` ${fmtN(done)} of ${fmtN(matched)} objects · ${fmtB(p.complete_bytes)} of ${fmtB(p.matched_bytes)}`+(p.shards_scanned<p.shards_total?' (still scanning — total grows)':'');
  $('eta').textContent=phase==='complete'?`finished in ${fmtD(p.elapsed_s)}`:
    (live&&['planning','verifying'].includes(phase)&&p.activity?p.activity:(live&&p.eta_s?`ETA ${fmtD(p.eta_s)}`:''));
  const segs=[['copied','var(--s1)','Copied'],['moved','var(--s1)','Moved'],['skipped_existing','var(--good)','Already at destination'],['failed','var(--crit)','Failed']];
  $('bar').innerHTML=matched?segs.filter(x=>p[x[0]]).map(x=>`<span style="width:${100*p[x[0]]/matched}%;background:${x[1]}" title="${x[2]}: ${fmtN(p[x[0]])}"></span>`).join(''):'';
  $('legend').innerHTML=segs.filter(x=>p[x[0]]).map(x=>`<span><span class="dot" style="display:inline-block;background:${x[1]};margin-right:6px"></span>${x[2]} ${fmtN(p[x[0]])}</span>`).join('')+`<span>Pending ${fmtN(p.pending)}</span>`;
  $('meta').innerHTML=[['Source',p.source||m.source],['Destination',p.destination||m.destination],['Window (UTC)',m.window],
    ['Started',fmtT(p.started)],['Last update',fmtT(p.updated)+(live?'':' (not running)')],['Process',p.pid?`pid ${p.pid}, ${p.workers} workers`:'–']]
    .map(([k,v])=>`<div><b>${k}</b>${esc(v||'–')}</div>`).join('');
  $('tiles').innerHTML=[
    tile('Listed at source',fmtN(p.listed),`${p.shards_scanned??0}/${p.shards_total??0} shards scanned`),
    tile('Matched filter',fmtN(matched),fmtB(p.matched_bytes)),
    tile(m.action==='move'?'Moved':'Copied',fmtN((p.copied||0)+(p.moved||0)),`+${fmtN(p.skipped_existing)} already present`),
    tile('Failed',`<span style="color:${failed?'var(--crit)':'inherit'}">${fmtN(failed)}</span>`,failed?'✕ re-run to retry':'none'),
    live?tile('Throughput',`${(p.rate_obj_s||0).toFixed(1)}/s`,`${(p.rate_mb_s||0).toFixed(1)} MB/s · ${fmtN(p.inflight)} in flight`)
        :tile('Throughput','–',phase==='complete'?`run took ${fmtD(p.elapsed_s)}`:'not running'),
    tile('Rate limits (req/s)',`${p.src_rps_limit??'–'} / ${p.dst_rps_limit??'–'}`,`src / dst · ${fmtN(p.throttle_events)} back-offs`),
    tile('Memory budget',`${p.mem_budget_used_mb??0} MB`,`of ${p.mem_budget_max_mb??'–'} MB max in flight`),
  ].join('');
  drawChart(s.samples||[]);
  renderVerify(p.verify, s.verify||[]);
  const sp=p.shard_progress||{};
  // with date-folder pruning there can be one shard per day folder: show the busiest 300
  function shardRows(all,sp){if(all.length<=300)return all;
    const score=r=>{const d=sp[r.id]||[0,0];return (r.status!=='scanned'?1e12:0)+(r.matched-d[0])*1e3+r.matched};
    const top=[...all].sort((a,b)=>score(b)-score(a)).slice(0,300);
    const rest=all.length-300;setTimeout(()=>{const t=$('shards');if(t)t.insertAdjacentHTML('beforeend',`<tr><td colspan="10" class="sub">… ${fmtN(rest)} more folders (fully done or nothing in window) not shown</td></tr>`)},0);
    return top.sort((a,b)=>a.id-b.id);}
  const fmtDay=t=>t?new Date(t*1000).toISOString().slice(0,10):'–';
  $('shards').innerHTML='<tr><th>Bucket</th><th>Prefix</th><th>Scan</th><th style="text-align:right">Listed</th><th style="text-align:right">Matched</th><th style="text-align:right">Size</th><th style="text-align:right" title="LastModified (or key date) earlier than the window start">Before window</th><th style="text-align:right" title="later than the window end">After window</th><th title="oldest – newest date seen while listing">Dates found</th><th>Done</th></tr>'+
    shardRows(s.shards||[],sp).map(r=>{const d=(sp[r.id]||[0,0]);const pc=r.matched?100*d[0]/r.matched:(r.status==='scanned'?100:0);
      const why=(r.listed>0&&r.matched===0)?` title="0 matched: ${fmtN(r.before_window)} before and ${fmtN(r.after_window)} after the window${r.other_excluded?', '+fmtN(r.other_excluded)+' regex/no-date':''}"`:'';
      return `<tr${why}><td>${esc(r.src_bucket)}${r.dst_bucket&&r.dst_bucket!==r.src_bucket?' → '+esc(r.dst_bucket):''}</td><td class="k">${esc(r.prefix||(r.delimited?'(root objects)':'(all)'))}</td>
      <td>${r.status==='scanned'?'<span class="ok">✓ scanned</span>':esc(r.status)}</td><td style="text-align:right">${fmtN(r.listed)}</td><td style="text-align:right">${fmtN(r.matched)}</td><td style="text-align:right">${fmtB(r.matched_bytes)}</td>
      <td style="text-align:right">${fmtN(r.before_window)}</td><td style="text-align:right">${fmtN(r.after_window)}</td><td style="white-space:nowrap">${fmtDay(r.min_mtime)} → ${fmtDay(r.max_mtime)}</td>
      <td>${(r.matched===0&&r.status==='scanned')?'<span class="sub">nothing in window</span>':`<div style="display:flex;gap:8px;align-items:center"><div class="bar mini" style="flex:1"><span style="width:${pc}%;background:${d[1]?'var(--crit)':'var(--s1)'}"></span></div><span style="min-width:48px;text-align:right">${pc.toFixed(0)}%</span></div>`}</td></tr>`}).join('');
  // date distribution: answers "why did this folder match 0?"
  const win=(m.window||'');
  $('months').innerHTML=(s.shards||[]).filter(r=>r.months&&r.months!=='{}').slice(0,200).map(r=>{let mo={};try{mo=JSON.parse(r.months)}catch(e){}
      return `<tr><td class="k" style="white-space:nowrap;width:1%">${esc(r.src_bucket)}/${esc(r.prefix||'(root)')}</td><td>${Object.entries(mo).map(([k,v])=>`<span class="chip">${k}: <b>${fmtN(v)}</b></span>`).join(' ')}</td></tr>`}).join('')
    ||'<tr><td class="empty">Filled in while folders are listed.</td></tr>';
  $('errors').innerHTML=(s.errors||[]).length?'<tr><th>Time</th><th>Bucket</th><th>Key</th><th>Attempt</th><th>Message</th></tr>'+s.errors.map(e=>`<tr><td>${new Date(e.ts*1000).toLocaleTimeString()}</td><td>${esc(e.bucket)}</td><td class="k">${esc(e.key)}</td><td>${e.attempt}</td><td class="k">${esc(e.msg)}</td></tr>`).join(''):'<tr><td class="empty">No errors.</td></tr>';
  const newest=Math.max(0,...(s.runs||[]).map(r=>r.id));
  window._live=live;
  $('runs').innerHTML='<tr><th>#</th><th>Started</th><th>Ended</th><th>Status</th><th style="text-align:right">Completed in run</th><th style="text-align:right">Bytes in run</th></tr>'+(s.runs||[]).map(r=>{let sm={};try{sm=JSON.parse(r.summary||'{}')}catch(e){}
    let st=r.status;if(!r.ended&&(r.id!==newest||!live))st='✕ killed / crashed — no clean shutdown';
    else if(st==='complete')st='✓ complete';else if(st==='interrupted')st='❚❚ interrupted (graceful)';
    return `<tr><td>${r.id}</td><td>${fmtT(r.started)}</td><td>${fmtT(r.ended)}</td><td>${esc(st)}</td><td style="text-align:right">${fmtN(sm.completed_this_run)}</td><td style="text-align:right">${fmtB(sm.transferred_bytes_this_run)}</td></tr>`}).join('');
}

function renderVerify(v,rows){
  const el=$('verify');
  if(!v&&!rows.length){el.innerHTML='<div class="empty">Runs automatically after the transfer finishes (destination listing vs. expected set).</div>';return}
  if(!v){v={expected:0,ok:0,missing:0,mismatched:0,extra:0,samples:[]};rows.forEach(r=>{for(const k of ['expected','ok','missing','mismatched','extra'])v[k]+=r[k]})}
  const good=v.missing===0&&v.mismatched===0;
  const pc=v.expected?100*v.ok/v.expected:100;
  el.innerHTML=`<div style="font-size:28px;font-weight:650">${pc.toFixed(pc===100?0:4)}%</div>
   <div class="badge" style="margin:6px 0 10px"><span class="dot" style="background:${good?'var(--good)':'var(--crit)'}"></span>${good?'✓ All objects present with matching size':'✕ Destination incomplete'}</div>
   <table><tr><td>Expected</td><td style="text-align:right">${fmtN(v.expected)}</td></tr><tr><td>Verified OK</td><td style="text-align:right">${fmtN(v.ok)}</td></tr>
   <tr><td>Missing</td><td style="text-align:right">${fmtN(v.missing)}</td></tr><tr><td>Size mismatch</td><td style="text-align:right">${fmtN(v.mismatched)}</td></tr>
   <tr><td>Other objects at destination</td><td style="text-align:right">${fmtN(v.extra)}</td></tr></table>`+
   ((v.samples||[]).length?'<div class="sub" style="margin-top:8px">Examples: '+v.samples.slice(0,5).map(x=>`<code>${esc(x.key)}</code> (${esc(x.problem)})`).join(', ')+'</div>':'');
}

function drawChart(samples){
  const box=$('chart'), tip=$('tip');
  box.querySelectorAll('svg,.empty').forEach(n=>n.remove());
  // per-minute buckets from cumulative counters (counters reset per run -> clamp at 0)
  const pts=[];let j=0;
  for(let i=1;i<samples.length;i++){const b=samples[i];
    if(b.done_objects<samples[i-1].done_objects){j=i;continue}            // new run: counters restart
    while(j<i-1&&b.ts-samples[j+1].ts>=15)j++;
    const a=samples[j];const dt=b.ts-a.ts;if(dt<=0||b.done_objects<a.done_objects)continue;
    pts.push({t:b.ts,v:(b.done_objects-a.done_objects)*60/dt,mb:(b.done_bytes-a.done_bytes)/dt/1048576});}
  if(pts.length<2){box.insertAdjacentHTML('beforeend','<div class="empty">Collecting samples…</div>');return}
  const W=box.clientWidth,H=box.clientHeight,L=52,R=8,T=8,B=24;
  const t0=pts[0].t,t1=pts[pts.length-1].t,vmax=Math.max(1,...pts.map(p=>p.v));
  const nice=x=>{const e=Math.pow(10,Math.floor(Math.log10(x)));const f=x/e;return (f<=1?1:f<=2?2:f<=5?5:10)*e};
  const ymax=nice(vmax*1.1);
  const X=t=>L+(W-L-R)*(t1===t0?0:(t-t0)/(t1-t0)), Y=v=>T+(H-T-B)*(1-v/ymax);
  let g='';for(let i=0;i<=4;i++){const v=ymax*i/4,y=Y(v);g+=`<line x1="${L}" x2="${W-R}" y1="${y}" y2="${y}" stroke="var(${i?'--grid':'--axis'})"/><text x="${L-8}" y="${y+4}" text-anchor="end" font-size="11" fill="var(--muted)">${v>=1000?(v/1000).toFixed(v%1000?1:0)+'k':Math.round(v)}</text>`}
  for(let i=0;i<=4;i++){const t=t0+(t1-t0)*i/4;g+=`<text x="${X(t)}" y="${H-6}" text-anchor="${i===0?'start':i===4?'end':'middle'}" font-size="11" fill="var(--muted)">${new Date(t*1000).toLocaleTimeString([], t1-t0<900?{hour:'2-digit',minute:'2-digit',second:'2-digit'}:{hour:'2-digit',minute:'2-digit'})}</text>`}
  const d=pts.map((p,i)=>`${i?'L':'M'}${X(p.t).toFixed(1)},${Y(p.v).toFixed(1)}`).join('');
  const svg=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="objects per minute over time">${g}
   <path d="${d} L${X(t1)},${Y(0)} L${X(t0)},${Y(0)}Z" fill="var(--s1)" opacity=".10"/>
   <path d="${d}" fill="none" stroke="var(--s1)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
   <line id="xh" y1="${T}" y2="${H-B}" stroke="var(--axis)" visibility="hidden"/><circle id="xd" r="4" fill="var(--s1)" stroke="var(--surface)" stroke-width="2" visibility="hidden"/>
   <rect x="${L}" y="${T}" width="${W-L-R}" height="${H-T-B}" fill="transparent" id="hit"/></svg>`;
  box.insertAdjacentHTML('afterbegin',svg);
  const hit=box.querySelector('#hit'),xh=box.querySelector('#xh'),xd=box.querySelector('#xd');
  hit.onmousemove=e=>{const r=box.getBoundingClientRect();const x=e.clientX-r.left;let best=pts[0];for(const p of pts)if(Math.abs(X(p.t)-x)<Math.abs(X(best.t)-x))best=p;
    xh.setAttribute('x1',X(best.t));xh.setAttribute('x2',X(best.t));xh.setAttribute('visibility','visible');xd.setAttribute('cx',X(best.t));xd.setAttribute('cy',Y(best.v));xd.setAttribute('visibility','visible');
    tip.style.display='block';tip.innerHTML=`<b>${Math.round(best.v).toLocaleString()}</b> objects/min<br><span style="color:var(--ink2)">${best.mb.toFixed(2)} MB/s · ${new Date(best.t*1000).toLocaleTimeString()}</span>`;
    const tx=Math.min(X(best.t)+12,W-tip.offsetWidth-4);tip.style.left=tx+'px';tip.style.top=Math.max(0,Y(best.v)-40)+'px';};
  hit.onmouseleave=()=>{tip.style.display='none';xh.setAttribute('visibility','hidden');xd.setAttribute('visibility','hidden')};
}

async function tick(){
  if(!job){await loadJobs();if(!job){$('subtitle').textContent='No jobs in state directory yet.';return}}
  try{const r=await fetch('api/status?job='+encodeURIComponent(job));if(r.ok)render(await r.json());}catch(e){$('subtitle').textContent='dashboard server unreachable';}
}
loadJobs().then(tick);setInterval(tick,2000);setInterval(loadJobs,15000);
</script></body></html>
"""
