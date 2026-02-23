"""
Simple web UI for the Assessment API.

Serves an HTML dashboard at ``/ui`` (or ``/<base_path>/ui`` when a subpath
is configured) that provides a browser-based interface to all API features.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["ui"])


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def ui_page(request: Request):
    """Serve the single-page assessment UI."""
    return HTMLResponse(content=_build_html(), status_code=200)


def _build_html() -> str:
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Assessment API – Dashboard</title>
<style>
:root{--bg:#f5f7fa;--card:#fff;--primary:#4361ee;--primary-hover:#3a56d4;--danger:#ef476f;--success:#06d6a0;--warn:#ffd166;--text:#212529;--muted:#6c757d;--border:#dee2e6;--radius:8px;--disabled:#a0aec0}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.6;padding:0}
header{background:var(--primary);color:#fff;padding:1rem 2rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.5rem}
header h1{font-size:1.3rem;font-weight:600}
header .info{font-size:.85rem;opacity:.85}
.container{max-width:1200px;margin:1.5rem auto;padding:0 1rem}
.tabs{display:flex;gap:4px;margin-bottom:1rem;flex-wrap:wrap}
.tab{padding:.5rem 1rem;border:none;background:var(--card);cursor:pointer;border-radius:var(--radius) var(--radius) 0 0;font-size:.9rem;color:var(--muted);border-bottom:2px solid transparent}
.tab.active{color:var(--primary);border-bottom-color:var(--primary);font-weight:600}
.panel{display:none;background:var(--card);border-radius:0 var(--radius) var(--radius) var(--radius);padding:1.5rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.panel.active{display:block}
label{display:block;font-weight:500;margin:.6rem 0 .25rem;font-size:.9rem}
input[type=text],input[type=file],select,textarea{width:100%;padding:.5rem .7rem;border:1px solid var(--border);border-radius:var(--radius);font-size:.9rem}
textarea{resize:vertical;min-height:60px}
.row{display:flex;gap:1rem;flex-wrap:wrap}
.row>*{flex:1;min-width:200px}
button,.btn{padding:.55rem 1.2rem;border:none;border-radius:var(--radius);cursor:pointer;font-size:.9rem;font-weight:500;transition:background .15s,opacity .15s}
.btn-primary{background:var(--primary);color:#fff}.btn-primary:hover:not(:disabled){background:var(--primary-hover)}
.btn-sm{padding:.35rem .8rem;font-size:.82rem}
.btn-danger{background:var(--danger);color:#fff}
.btn-outline{background:transparent;border:1px solid var(--border);color:var(--text)}.btn-outline:hover:not(:disabled){background:var(--bg)}
button:disabled,.btn:disabled{opacity:.55;cursor:not-allowed}
.btn-bar{margin-top:1rem}
table{width:100%;border-collapse:collapse;font-size:.88rem;margin-top:.8rem}
th,td{text-align:left;padding:.55rem .7rem;border-bottom:1px solid var(--border)}
th{background:var(--bg);font-weight:600;position:sticky;top:0}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.78rem;font-weight:600;text-transform:uppercase}
.badge-pending{background:#e2e3e5;color:#383d41}
.badge-uploading,.badge-parsing,.badge-processing,.badge-awaiting_documents{background:#fff3cd;color:#856404}
.badge-completed,.badge-success{background:#d4edda;color:#155724}
.badge-failed{background:#f8d7da;color:#721c24}
.badge-running{background:#fff3cd;color:#856404}
.toast{position:fixed;bottom:1.5rem;right:1.5rem;padding:.8rem 1.2rem;border-radius:var(--radius);color:#fff;font-size:.9rem;z-index:999;opacity:0;transition:opacity .3s}
.toast.show{opacity:1}
.toast-ok{background:var(--success)}.toast-err{background:var(--danger)}
#jwt-bar{display:flex;gap:.5rem;align-items:center;margin-bottom:1rem;flex-wrap:wrap}
#jwt-bar input{max-width:400px}
.detail-grid{display:grid;grid-template-columns:160px 1fr;gap:.3rem .8rem;font-size:.9rem;margin:.8rem 0}
.detail-grid dt{font-weight:600;color:var(--muted)}
.detail-grid dd{word-break:break-all}
.result-card{background:var(--bg);border-radius:var(--radius);padding:.8rem 1rem;margin:.5rem 0}
.result-card h4{font-size:.92rem;margin-bottom:.3rem}
.hidden{display:none}
.spinner{display:inline-block;width:14px;height:14px;border:2px solid rgba(255,255,255,.4);border-top-color:#fff;border-radius:50%;animation:spin .6s linear infinite;vertical-align:middle;margin-left:6px}
.spinner-dark{border-color:var(--border);border-top-color:var(--primary)}
@keyframes spin{to{transform:rotate(360deg)}}
.empty{text-align:center;padding:2rem;color:var(--muted)}
/* Reference cards */
.ref-list{margin-top:.5rem}
.ref-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:.6rem .8rem;margin:.4rem 0;font-size:.85rem}
.ref-card-header{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap;margin-bottom:.3rem}
.ref-type-badge{display:inline-block;padding:1px 6px;border-radius:4px;font-size:.72rem;font-weight:700;text-transform:uppercase;background:#e8eaf6;color:#283593}
.ref-type-badge.pdf{background:#ffebee;color:#c62828}
.ref-type-badge.excel{background:#e8f5e9;color:#2e7d32}
.ref-type-badge.docx{background:#e3f2fd;color:#1565c0}
.ref-type-badge.ppt{background:#fff3e0;color:#e65100}
.ref-meta{font-size:.78rem;color:var(--muted)}
.ref-snippet{margin-top:.3rem;font-size:.82rem;color:#555;white-space:pre-wrap;word-break:break-word;max-height:100px;overflow:auto}
.ref-links{margin-top:.3rem;display:flex;gap:.6rem;flex-wrap:wrap;font-size:.8rem}
.ref-links a{color:var(--primary);text-decoration:none;cursor:pointer}
.ref-links a:hover{text-decoration:underline}
/* Image modal */
.modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.6);z-index:1000;display:flex;align-items:center;justify-content:center}
.modal-content{background:#fff;border-radius:var(--radius);padding:1rem;max-width:90vw;max-height:90vh;overflow:auto;position:relative}
.modal-content img{max-width:100%;max-height:80vh;display:block;margin:0 auto}
.modal-close{position:absolute;top:.5rem;right:.5rem;background:var(--danger);color:#fff;border:none;border-radius:50%;width:28px;height:28px;cursor:pointer;font-size:1rem;line-height:1}
/* Auto-refresh toggle */
.auto-refresh{display:flex;align-items:center;gap:.5rem;font-size:.85rem}
.auto-refresh label{margin:0;font-weight:normal}
.json-error{border-color:var(--danger) !important;background-color:#ffebee !important}
@media(max-width:600px){.row{flex-direction:column}.tabs{gap:2px}}
</style>
</head>
<body>
<header>
  <h1>&#128202; Assessment API Dashboard</h1>
  <span class="info" id="hdr-info"></span>
</header>

<div class="container">
  <!-- JWT Token Bar -->
  <div id="jwt-bar">
    <label style="margin:0;white-space:nowrap">JWT Token:</label>
    <input type="text" id="jwt-input" placeholder="Paste JWT token (leave empty if auth disabled)"/>
    <button class="btn btn-outline btn-sm" onclick="saveJwt()">Save</button>
    <span id="jwt-status" style="font-size:.82rem"></span>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <button class="tab active" data-tab="tasks" onclick="switchTab(this)">&#128203; Tasks</button>
    <button class="tab" data-tab="single" onclick="switchTab(this)">&#9889; Single-call</button>
    <button class="tab" data-tab="dataset" onclick="switchTab(this)">&#128451; From Dataset</button>
    <button class="tab" data-tab="session" onclick="switchTab(this)">&#128257; Two-phase</button>
    <button class="tab" data-tab="upload" onclick="switchTab(this)">&#128228; Upload Docs</button>
    <button class="tab" data-tab="manage" onclick="switchTab(this)">&#128193; Manage Data</button>
    <button class="tab" data-tab="health" onclick="switchTab(this)">&#128154; Health</button>
  </div>

  <!-- TASKS PANEL -->
  <div class="panel active" id="panel-tasks">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:.5rem">
      <h3>Assessment Tasks</h3>
      <div style="display:flex;align-items:center;gap:.8rem;flex-wrap:wrap">
        <div class="auto-refresh">
          <label><input type="checkbox" id="auto-refresh-cb" onchange="toggleAutoRefresh()"/> Auto-refresh</label>
          <select id="auto-refresh-interval" onchange="toggleAutoRefresh()" style="width:auto;padding:2px 4px;font-size:.82rem">
            <option value="5">5s</option>
            <option value="10" selected>10s</option>
            <option value="30">30s</option>
            <option value="60">60s</option>
          </select>
        </div>
        <button class="btn btn-primary btn-sm" id="btn-refresh-tasks" onclick="loadTasks()">&#128260; Refresh</button>
      </div>
    </div>
    <div id="tasks-body"><p class="empty">Click Refresh to load tasks</p></div>
  </div>

  <!-- SINGLE-CALL PANEL -->
  <div class="panel" id="panel-single">
    <h3>Single-call Assessment</h3>
    <p style="color:var(--muted);font-size:.88rem;margin:.4rem 0">Upload questions Excel + all evidence docs in one request.</p>
    <label>Questions Excel File *</label>
    <input type="file" id="single-q" accept=".xlsx,.xls" onchange="onSingleFieldChange()"/>
    <label>Evidence Documents *</label>
    <input type="file" id="single-ev" multiple onchange="onSingleFieldChange()"/>
    <div class="row">
      <div><label>Dataset Name (optional)</label><input type="text" id="single-ds" placeholder="Auto-generated if empty" oninput="onSingleFieldChange()"/></div>
      <div><label>Chat Name (optional)</label><input type="text" id="single-chat" placeholder="Auto-generated if empty" oninput="onSingleFieldChange()"/></div>
    </div>
    <div class="row">
      <div><label>Dataset Options (JSON, optional)</label><textarea id="single-ds-opts" placeholder='{"permission": "me"}' oninput="onSingleFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
      <div><label>Chat Options (JSON, optional)</label><textarea id="single-chat-opts" placeholder='{"prompt": {"system": "..."}}' oninput="onSingleFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
    </div>
    <div class="row">
      <div><label>Question ID Column (optional)</label><input type="text" id="single-qid-col" placeholder="A" oninput="onSingleFieldChange()" style="width:80px"/></div>
      <div><label>Question Column (optional)</label><input type="text" id="single-q-col" placeholder="B" oninput="onSingleFieldChange()" style="width:80px"/></div>
    </div>
    <div class="row">
      <div><label>Vendor Resp. Column (optional)</label><input type="text" id="single-v-res-col" placeholder="C" oninput="onSingleFieldChange()" style="width:80px"/></div>
      <div><label>Vendor Comm. Column (optional)</label><input type="text" id="single-v-com-col" placeholder="D" oninput="onSingleFieldChange()" style="width:80px"/></div>
    </div>
    <label style="display:flex;align-items:center;gap:.5rem;margin-top:.5rem"><input type="checkbox" id="single-v-process" onchange="onSingleFieldChange()"/> Process vendor response &amp; comments</label>
    <div class="btn-bar"><button class="btn btn-primary" id="btn-single" onclick="submitSingle()">&#128640; Start Assessment</button></div>
    <div id="single-result" class="hidden" style="margin-top:1rem"></div>
  </div>

  <!-- FROM DATASET PANEL -->
  <div class="panel" id="panel-dataset">
    <h3>Assessment from Existing Dataset</h3>
    <p style="color:var(--muted);font-size:.88rem;margin:.4rem 0">Use already-uploaded &amp; parsed RAGFlow datasets.</p>
    <label>Questions Excel File *</label>
    <input type="file" id="ds-q" accept=".xlsx,.xls" onchange="onDatasetFieldChange()"/>
    <label>Dataset IDs * (comma-separated)</label>
    <input type="text" id="ds-ids" placeholder="e.g. abc123,def456" oninput="onDatasetFieldChange()"/>
    <label>Chat Name (optional)</label>
    <input type="text" id="ds-chat" placeholder="Auto-generated if empty" oninput="onDatasetFieldChange()"/>
    <div class="row">
      <div><label>Dataset Options (JSON, optional)</label><textarea id="ds-opts" placeholder='{"permission": "me"}' oninput="onDatasetFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
      <div><label>Chat Options (JSON, optional)</label><textarea id="ds-chat-opts" placeholder='{"prompt": {"system": "..."}}' oninput="onDatasetFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
    </div>
    <div class="row">
      <div><label>Question ID Column (optional)</label><input type="text" id="ds-qid-col" placeholder="A" oninput="onDatasetFieldChange()" style="width:80px"/></div>
      <div><label>Question Column (optional)</label><input type="text" id="ds-q-col" placeholder="B" oninput="onDatasetFieldChange()" style="width:80px"/></div>
    </div>
    <div class="row">
      <div><label>Vendor Resp. Column (optional)</label><input type="text" id="ds-v-res-col" placeholder="C" oninput="onDatasetFieldChange()" style="width:80px"/></div>
      <div><label>Vendor Comm. Column (optional)</label><input type="text" id="ds-v-com-col" placeholder="D" oninput="onDatasetFieldChange()" style="width:80px"/></div>
    </div>
    <label style="display:flex;align-items:center;gap:.5rem;margin-top:.5rem"><input type="checkbox" id="ds-v-process" onchange="onDatasetFieldChange()"/> Process vendor response &amp; comments</label>
    <div class="btn-bar"><button class="btn btn-primary" id="btn-dataset" onclick="submitFromDataset()">&#128640; Start Assessment</button></div>
    <div id="ds-result" class="hidden" style="margin-top:1rem"></div>
  </div>

  <!-- TWO-PHASE PANEL -->
  <div class="panel" id="panel-session">
    <h3>Two-phase Workflow</h3>
    <p style="color:var(--muted);font-size:.88rem;margin:.4rem 0">1) Create session &rarr; 2) Upload docs &rarr; 3) Start</p>
    <fieldset style="border:1px solid var(--border);border-radius:var(--radius);padding:1rem;margin:.8rem 0">
      <legend style="font-weight:600">Phase 1: Create Session</legend>
      <label>Questions Excel File *</label>
      <input type="file" id="sess-q" accept=".xlsx,.xls" onchange="onSessCreateFieldChange()"/>
      <label>Dataset Name (optional)</label>
      <input type="text" id="sess-ds" placeholder="Auto-generated" oninput="onSessCreateFieldChange()"/>
      <div class="row">
        <div><label>Dataset Options (JSON, optional)</label><textarea id="sess-ds-opts" placeholder='{"permission": "me"}' oninput="onSessCreateFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
        <div><label>Chat Options (JSON, optional)</label><textarea id="sess-chat-opts" placeholder='{"prompt": {"system": "..."}}' oninput="onSessCreateFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
      </div>
      <div class="row">
        <div><label>Question ID Column (optional)</label><input type="text" id="sess-qid-col" placeholder="A" oninput="onSessCreateFieldChange()" style="width:80px"/></div>
        <div><label>Question Column (optional)</label><input type="text" id="sess-q-col" placeholder="B" oninput="onSessCreateFieldChange()" style="width:80px"/></div>
      </div>
      <div class="row">
        <div><label>Vendor Resp. Column (optional)</label><input type="text" id="sess-v-res-col" placeholder="C" oninput="onSessCreateFieldChange()" style="width:80px"/></div>
        <div><label>Vendor Comm. Column (optional)</label><input type="text" id="sess-v-com-col" placeholder="D" oninput="onSessCreateFieldChange()" style="width:80px"/></div>
      </div>
      <div class="btn-bar"><button class="btn btn-primary" id="btn-sess-create" onclick="createSession()">Create Session</button></div>
    </fieldset>
    <fieldset style="border:1px solid var(--border);border-radius:var(--radius);padding:1rem;margin:.8rem 0">
      <legend style="font-weight:600">Phase 2: Upload Documents</legend>
      <label>Task ID *</label>
      <input type="text" id="sess-tid" placeholder="From Phase 1" oninput="onSessUploadFieldChange()"/>
      <label>Evidence Documents *</label>
      <input type="file" id="sess-files" multiple onchange="onSessUploadFieldChange()"/>
      <div class="btn-bar"><button class="btn btn-primary" id="btn-sess-upload" onclick="uploadSessionDocs()">Upload Documents</button></div>
    </fieldset>
    <fieldset style="border:1px solid var(--border);border-radius:var(--radius);padding:1rem;margin:.8rem 0">
      <legend style="font-weight:600">Phase 3: Start Assessment</legend>
      <label>Task ID *</label>
      <input type="text" id="sess-start-tid" placeholder="Same task ID" oninput="onSessStartFieldChange()"/>
      <label>Chat Name (optional)</label>
      <input type="text" id="sess-start-chat" oninput="onSessStartFieldChange()"/>
      <div class="row">
        <div><label>Dataset Options (JSON, optional)</label><textarea id="sess-start-ds-opts" placeholder='{"permission": "me"}' oninput="onSessStartFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
        <div><label>Chat Options (JSON, optional)</label><textarea id="sess-start-chat-opts" placeholder='{"prompt": {"system": "..."}}' oninput="onSessStartFieldChange(); validateJsonInput(this)" rows="2"></textarea></div>
      </div>
      <label style="display:flex;align-items:center;gap:.5rem;margin-top:.5rem"><input type="checkbox" id="sess-v-process" onchange="onSessStartFieldChange()"/> Process vendor response &amp; comments</label>
      <div class="btn-bar"><button class="btn btn-primary" id="btn-sess-start" onclick="startSession()">&#128640; Start Assessment</button></div>
    </fieldset>
    <div id="sess-result" class="hidden" style="margin-top:1rem"></div>
  </div>

  <!-- UPLOAD DOCS PANEL -->
  <div class="panel" id="panel-upload">
    <h3>Upload Documents to Dataset</h3>
    <label>Dataset ID *</label>
    <input type="text" id="up-dsid" placeholder="Existing RAGFlow dataset ID" oninput="onUploadFieldChange()"/>
    <label>Documents *</label>
    <input type="file" id="up-files" multiple onchange="onUploadFieldChange()"/>
    <label><input type="checkbox" id="up-parse" checked/> Trigger parsing after upload</label>
    <div class="btn-bar"><button class="btn btn-primary" id="btn-upload" onclick="uploadDocs()">&#128228; Upload</button></div>
    <div id="up-result" class="hidden" style="margin-top:1rem"></div>
  </div>

  <!-- MANAGE DATA PANEL -->
  <div class="panel" id="panel-manage">
    <h3>Manage Datasets</h3>
    <div style="display:flex; gap:0.5rem; margin-bottom:0.5rem">
      <button class="btn btn-primary btn-sm" onclick="loadDatasets()">&#128260; Refresh List</button>
      <button class="btn btn-danger btn-sm" onclick="deleteSelectedDatasets()">Delete Selected</button>
    </div>
    <div id="manage-datasets-list" style="max-height:300px; overflow:auto; border:1px solid var(--border); border-radius:var(--radius); padding:0.5rem; margin-bottom:0.5rem">
      <p class="empty">Click Refresh to load datasets</p>
    </div>
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1.5rem">
      <span id="ds-page-info" style="font-size:0.85rem; color:var(--muted)">Page 1</span>
      <div style="display:flex; gap:0.5rem">
        <button class="btn btn-outline btn-sm" id="btn-ds-prev" onclick="changeDsPage(-1)" disabled>Previous</button>
        <button class="btn btn-outline btn-sm" id="btn-ds-next" onclick="changeDsPage(1)" disabled>Next</button>
      </div>
    </div>

    <h3>Manage Documents</h3>
    <label>Dataset ID</label>
    <div style="display:flex; gap:0.5rem; margin-bottom:0.5rem">
      <input type="text" id="manage-doc-dsid" placeholder="Dataset ID" style="flex:1" />
      <button class="btn btn-primary btn-sm" onclick="loadDocuments(1)">Load Documents</button>
    </div>
    <div style="display:flex; gap:0.5rem; margin-bottom:0.5rem">
      <button class="btn btn-danger btn-sm" onclick="deleteSelectedDocuments()">Delete Selected Documents</button>
    </div>
    <div id="manage-documents-list" style="max-height:300px; overflow:auto; border:1px solid var(--border); border-radius:var(--radius); padding:0.5rem; margin-bottom:0.5rem">
      <p class="empty">Enter Dataset ID and click Load</p>
    </div>
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1.5rem">
      <span id="doc-page-info" style="font-size:0.85rem; color:var(--muted)">Page 1</span>
      <div style="display:flex; gap:0.5rem">
        <button class="btn btn-outline btn-sm" id="btn-doc-prev" onclick="changeDocPage(-1)" disabled>Previous</button>
        <button class="btn btn-outline btn-sm" id="btn-doc-next" onclick="changeDocPage(1)" disabled>Next</button>
      </div>
    </div>
  </div>

  <!-- HEALTH PANEL -->
  <div class="panel" id="panel-health">
    <h3>Health Check</h3>
    <button class="btn btn-primary btn-sm" id="btn-health" onclick="checkHealth()">&#128154; Check</button>
    <pre id="health-result" style="margin-top:1rem;background:var(--bg);padding:1rem;border-radius:var(--radius);font-size:.88rem"></pre>
  </div>

  <!-- TASK DETAIL MODAL -->
  <div id="task-detail" class="hidden" style="margin-top:1rem">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:.5rem;margin-bottom:.5rem">
      <h3 id="detail-title">Task Details</h3>
      <div>
        <button class="btn btn-outline btn-sm" onclick="closeDetail()">&#10005; Close</button>
        <button class="btn btn-primary btn-sm" onclick="refreshDetail()">&#128260; Refresh</button>
        <button class="btn btn-outline btn-sm" onclick="downloadExcel()">&#128229; Excel</button>
      </div>
    </div>
    <div id="detail-body"></div>
    <h4 style="margin-top:1rem">Results</h4>
    <div id="detail-results"></div>
    <div id="detail-pagination" style="margin-top:.5rem;display:flex;gap:.5rem;align-items:center"></div>
  </div>
</div>

<div class="toast" id="toast"></div>
<!-- Image modal container -->
<div id="img-modal" class="hidden"></div>

<script>
function validateJsonInput(el) {
  const val = el.value.trim();
  el.classList.remove('json-error');
  if (val) {
    try {
      JSON.parse(val);
    } catch (e) {
      el.classList.add('json-error');
    }
  }
}

const API = (function(){
  const path = window.location.pathname;
  const uiIdx = path.lastIndexOf('/ui');
  const base = uiIdx > 0 ? path.substring(0, uiIdx) : '';
  return base + '/api/v1';
})();
const BASE_PATH = (function(){
  const path = window.location.pathname;
  const uiIdx = path.lastIndexOf('/ui');
  return uiIdx > 0 ? path.substring(0, uiIdx) : '';
})();
const HEALTH_URL = (function(){
  const path = window.location.pathname;
  const uiIdx = path.lastIndexOf('/ui');
  return uiIdx > 0 ? path.substring(0, uiIdx) + '/health' : '/health';
})();

let JWT = localStorage.getItem('assessment_jwt') || '';
document.getElementById('jwt-input').value = JWT;
updateJwtStatus();

function headers(){const h={};if(JWT)h['Authorization']='Bearer '+JWT;return h;}
function saveJwt(){JWT=document.getElementById('jwt-input').value.trim();localStorage.setItem('assessment_jwt',JWT);updateJwtStatus();toast('JWT saved','ok');}
function updateJwtStatus(){document.getElementById('jwt-status').textContent=JWT?'\u2714 Token set':'No token (auth disabled?)';}

function toast(msg,type){const t=document.getElementById('toast');t.textContent=msg;t.className='toast show toast-'+(type||'ok');setTimeout(()=>t.className='toast',3000);}

function switchTab(btn){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('panel-'+btn.dataset.tab).classList.add('active');
  document.getElementById('task-detail').classList.add('hidden');
}

function badgeClass(state){return 'badge badge-'+(state||'pending');}

/* ------------------------------------------------------------------ */
/* Button state helpers                                                */
/* ------------------------------------------------------------------ */
function btnLoading(id, text){
  const b=document.getElementById(id);
  if(!b)return;
  b.disabled=true;
  b._origHTML=b.innerHTML;
  b.innerHTML=escHtml(text)+'<span class="spinner"></span>';
}
function btnDone(id){
  const b=document.getElementById(id);
  if(!b)return;
  if(b._origHTML) b.innerHTML=b._origHTML;
}
function btnReset(id){
  const b=document.getElementById(id);
  if(!b)return;
  b.disabled=false;
  if(b._origHTML) b.innerHTML=b._origHTML;
}
function btnError(id){
  btnReset(id);
}

/* ------------------------------------------------------------------ */
/* Field-change listeners                                              */
/* ------------------------------------------------------------------ */
function onSingleFieldChange(){ btnReset('btn-single'); }
function onDatasetFieldChange(){ btnReset('btn-dataset'); }
function onSessCreateFieldChange(){ btnReset('btn-sess-create'); }
function onSessUploadFieldChange(){ btnReset('btn-sess-upload'); }
function onSessStartFieldChange(){ btnReset('btn-sess-start'); }
function onUploadFieldChange(){ btnReset('btn-upload'); }
function onRetryFieldChange(){ btnReset('btn-retry-start'); }

/* ------------------------------------------------------------------ */
/* Auto-refresh                                                        */
/* ------------------------------------------------------------------ */
let _autoRefreshTimer = null;
function toggleAutoRefresh(){
  if(_autoRefreshTimer){clearInterval(_autoRefreshTimer);_autoRefreshTimer=null;}
  const cb=document.getElementById('auto-refresh-cb');
  if(cb.checked){
    const secs=parseInt(document.getElementById('auto-refresh-interval').value)||10;
    loadTasks();
    _autoRefreshTimer=setInterval(loadTasks, secs*1000);
  }
}

/* ------------------------------------------------------------------ */
/* Image modal                                                         */
/* ------------------------------------------------------------------ */
function showImageModal(url){
  const m=document.getElementById('img-modal');
  m.className='modal-overlay';
  m.innerHTML='<div class="modal-content"><button class="modal-close" onclick="closeImageModal()">&times;</button><img src="'+escAttr(url)+'" alt="Reference image"/></div>';
  m.onclick=function(e){if(e.target===m)closeImageModal();};
}
function closeImageModal(){
  const m=document.getElementById('img-modal');
  m.className='hidden';
  m.innerHTML='';
}

// ----- TASKS -----
let _taskPage=1;
let _taskPageSize=10;

async function loadTasks(){
  const btn=document.getElementById('btn-refresh-tasks');
  btn.disabled=true;
  const body=document.getElementById('tasks-body');
  body.innerHTML='<p>Loading\u2026<span class="spinner spinner-dark"></span></p>';
  try{
    let r;
    try{
      r=await fetch(API+'/assessments?page='+_taskPage+'&page_size='+_taskPageSize,{headers:headers()});
    }catch(netErr){
      throw new Error('Network error: could not reach the server.');
    }
    if(!r.ok){const txt=await r.text();throw new Error('Server error ('+r.status+'): '+(txt.substring(0,300)||r.statusText));}
    const resp=await r.json();
    const tasks = resp.tasks || [];
    
    if(!tasks.length && _taskPage === 1){body.innerHTML='<p class="empty">No tasks found</p>';return;}
    
    let html='<table><tr><th>Task ID</th><th>State</th><th>Stage</th><th>Progress</th><th>Dataset</th><th>Chat</th><th>Created</th><th>Actions</th></tr>';
    tasks.forEach(t=>{
      const dsId=t.dataset_id||'\u2014';
      const chId=t.chat_id||'\u2014';
      html+='<tr><td style="font-family:monospace;font-size:.82rem">'+t.task_id.substring(0,12)+'\u2026</td>'
        +'<td><span class="'+badgeClass(t.state)+'">'+t.state+'</span></td>'
        +'<td>'+t.pipeline_stage+'</td>'
        +'<td>'+t.questions_processed+'/'+t.total_questions+'</td>'
        +'<td style="font-family:monospace;font-size:.78rem">'+(dsId.length>12?dsId.substring(0,12)+'\u2026':dsId)+'</td>'
        +'<td style="font-family:monospace;font-size:.78rem">'+(chId.length>12?chId.substring(0,12)+'\u2026':chId)+'</td>'
        +'<td style="font-size:.82rem">'+new Date(t.created_at).toLocaleString()+'</td>'
        +'<td><button class="btn btn-outline btn-sm" onclick="viewTask(\''+t.task_id+'\')">View</button></td></tr>';
    });
    html+='</table>';
    
    // Pagination Controls
    html+='<div style="margin-top:10px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:1rem">';
    
    // Prev/Next
    let pInfo = '<div><span style="font-size:.9rem;margin-right:10px">Page '+resp.page+' of '+resp.total_pages+' ('+resp.total+' total)</span>';
    if(resp.page>1) pInfo+='<button class="btn btn-outline btn-sm" onclick="_taskPage--;loadTasks()">\u2190 Prev</button> ';
    if(resp.page<resp.total_pages) pInfo+='<button class="btn btn-outline btn-sm" onclick="_taskPage++;loadTasks()">Next \u2192</button>';
    pInfo+='</div>';
    
    // Page Size
    let pSize = '<div style="display:flex;align-items:center;gap:.5rem;font-size:.85rem"><span>Per page:</span>';
    [10, 20, 50, 100].forEach(sz => {
        const cls = (sz===_taskPageSize) ? 'btn-primary' : 'btn-outline';
        pSize += `<button class="btn ${cls} btn-sm" onclick="_taskPageSize=${sz};_taskPage=1;loadTasks()">${sz}</button>`;
    });
    pSize += '</div>';
    
    html+=pInfo+pSize+'</div>';
    
    body.innerHTML=html;
  }catch(e){body.innerHTML='<p style="color:var(--danger)">Error: '+escHtml(e.message)+'</p>';}
  finally{btn.disabled=false;}
}

let _detailTaskId='';
let _detailPage=1;
let _detailPageSize=20;

async function viewTask(tid){
  _detailTaskId=tid;_detailPage=1;_detailPageSize=20;
  document.getElementById('task-detail').classList.remove('hidden');
  document.getElementById('detail-title').textContent='Task '+tid.substring(0,12)+'\u2026';
  await refreshDetail();
}
function closeDetail(){document.getElementById('task-detail').classList.add('hidden');}

async function refreshDetail(){
  if(!_detailTaskId)return;
  try{
    let r;
    try{
      r=await fetch(API+'/assessments/'+_detailTaskId,{headers:headers()});
    }catch(netErr){
      throw new Error('Network error: could not reach the server.');
    }
    if(!r.ok){const txt=await r.text();throw new Error('Server error ('+r.status+'): '+(txt.substring(0,300)||r.statusText));}
    const s=await r.json();
    let html='<dl class="detail-grid">';
    html+='<dt>Task ID</dt><dd style="font-family:monospace">'+s.task_id+'</dd>';
    html+='<dt>State</dt><dd><span class="'+badgeClass(s.state)+'">'+s.state+'</span></dd>';
    html+='<dt>Stage</dt><dd>'+s.pipeline_stage+'</dd>';
    html+='<dt>Progress</dt><dd>'+s.questions_processed+'/'+s.total_questions+'</dd>';
    html+='<dt>Message</dt><dd>'+(s.progress_message||'\u2014')+'</dd>';
    if(s.error)html+='<dt>Error</dt><dd style="color:var(--danger)">'+escHtml(s.error)+'</dd>';
    html+='<dt>Created</dt><dd>'+new Date(s.created_at).toLocaleString()+'</dd>';
    html+='<dt>Updated</dt><dd>'+new Date(s.updated_at).toLocaleString()+'</dd>';
    // RAGFlow resource IDs
    if(s.dataset_id)html+='<dt>Dataset ID</dt><dd style="font-family:monospace;font-size:.85rem">'+escHtml(s.dataset_id)+'</dd>';
    if(s.chat_id)html+='<dt>Chat ID</dt><dd style="font-family:monospace;font-size:.85rem">'+escHtml(s.chat_id)+'</dd>';
    if(s.session_id)html+='<dt>Session ID</dt><dd style="font-family:monospace;font-size:.85rem">'+escHtml(s.session_id)+'</dd>';
    if(s.document_ids&&s.document_ids.length)html+='<dt>Document IDs</dt><dd style="font-family:monospace;font-size:.85rem">'+s.document_ids.map(escHtml).join(', ')+'</dd>';
    html+='</dl>';
    // Per-document parsing statuses
    if(s.document_statuses&&s.document_statuses.length){
      html+='<h4 style="margin-top:1rem">Document Parsing Status</h4>';
      html+='<table><tr><th>Document</th><th>Status</th><th>Progress</th><th>Message</th></tr>';
      s.document_statuses.forEach(function(ds){
        var stBadge='badge-pending';
        if(ds.status==='success')stBadge='badge-completed';
        else if(ds.status==='failed')stBadge='badge-failed';
        else if(ds.status==='timeout')stBadge='badge-failed';
        else if(ds.status==='not_found')stBadge='badge-failed';
        else if(ds.status==='running')stBadge='badge-processing';
        html+='<tr>';
        html+='<td>'+escHtml(ds.document_name||ds.document_id)+'</td>';
        html+='<td><span class="badge '+stBadge+'">'+escHtml(ds.status)+'</span></td>';
        html+='<td>'+(ds.progress*100).toFixed(0)+'%</td>';
        html+='<td>'+escHtml(ds.message||'')+'</td>';
        html+='</tr>';
      });
      html+='</table>';
    }
    // Retry panel for failed or awaiting_documents sessions that have a dataset
    if((s.state==='failed'||s.state==='awaiting_documents')&&s.dataset_id){
      html+='<div style="margin-top:1rem;padding:1rem;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg)">';
      html+='<h4 style="margin-bottom:.5rem">&#128260; Retry / Upload More Documents</h4>';
      if(s.state==='failed')html+='<p style="font-size:.88rem;color:var(--muted);margin-bottom:.5rem">This session failed. You can upload additional or replacement documents and re-start the assessment without losing the existing dataset.</p>';
      html+='<label>Additional Evidence Documents</label>';
      html+='<input type="file" id="retry-files" multiple/>';
      html+='<label style="display:flex;align-items:center;gap:.5rem;margin:.5rem 0"><input type="checkbox" id="retry-v-process" onchange="onRetryFieldChange()"/> Process vendor response &amp; comments</label>';
      html+='<div class="btn-bar" style="display:flex;gap:.5rem;flex-wrap:wrap">';
      html+='<button class="btn btn-primary btn-sm" id="btn-retry-upload" onclick="retryUploadDocs()">&#128228; Upload Documents</button>';
      html+='<button class="btn btn-primary btn-sm" id="btn-retry-start" onclick="retryStartAssessment()">&#128640; Start Assessment</button>';
      html+='</div>';
      html+='<div id="retry-result" class="hidden" style="margin-top:.5rem"></div>';
      html+='</div>';
    }
    document.getElementById('detail-body').innerHTML=html;
    await loadResults();
  }catch(e){document.getElementById('detail-body').innerHTML='<p style="color:var(--danger)">'+escHtml(e.message)+'</p>';}
}

async function loadResults(){
  const c=document.getElementById('detail-results');
  const p=document.getElementById('detail-pagination');
  try{
    let r;
    try{
      r=await fetch(API+'/assessments/'+_detailTaskId+'/results?page='+_detailPage+'&page_size='+_detailPageSize,{headers:headers()});
    }catch(netErr){
      throw new Error('Network error: could not reach the server.');
    }
    if(!r.ok){c.innerHTML='<p class="empty">Results not available yet</p>';p.innerHTML='';return;}
    const d=await r.json();
    if(!d.results||!d.results.length){c.innerHTML='<p class="empty">No results yet</p>';p.innerHTML='';return;}
    let html='';
    d.results.forEach(q=>{
      html+='<div class="result-card"><h4>Q'+escHtml(String(q.question_serial_no))+': '+escHtml(q.question)+'</h4>';
      html+='<div><strong>Answer:</strong> <span class="badge '+(q.ai_response==='Yes'?'badge-completed':q.ai_response==='No'?'badge-failed':'badge-pending')+'">'+escHtml(q.ai_response)+'</span></div>';
      if(q.details)html+='<div style="margin-top:.3rem"><strong>Details:</strong> '+escHtml(q.details).substring(0,500)+'</div>';
      if(q.references&&q.references.length){
        html+='<div class="ref-list"><strong>References ('+q.references.length+'):</strong>';
        q.references.forEach(function(ref){
          html+=buildRefCard(ref);
        });
        html+='</div>';
      }
      html+='</div>';
    });
    c.innerHTML=html;
    
    // Pagination Controls
    let ph='<div style="margin-top:10px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:1rem">';
    
    // Prev/Next
    ph+='<div><span style="font-size:.9rem;margin-right:10px">Page '+d.page+' of '+d.total_pages+'</span>';
    if(d.page>1)ph+='<button class="btn btn-outline btn-sm" onclick="_detailPage--;loadResults()">\u2190 Prev</button> ';
    if(d.page<d.total_pages)ph+='<button class="btn btn-outline btn-sm" onclick="_detailPage++;loadResults()">Next \u2192</button>';
    ph+='</div>';

    // Page Size
    let pSize = '<div style="display:flex;align-items:center;gap:.5rem;font-size:.85rem"><span>Per page:</span>';
    [10, 20, 50, 100].forEach(sz => {
        const cls = (sz===_detailPageSize) ? 'btn-primary' : 'btn-outline';
        pSize += `<button class="btn ${cls} btn-sm" onclick="_detailPageSize=${sz};_detailPage=1;loadResults()">${sz}</button>`;
    });
    pSize += '</div>';

    ph+=pSize+'</div>';

    p.innerHTML=ph;
  }catch(e){c.innerHTML='<p style="color:var(--danger)">'+escHtml(e.message)+'</p>';p.innerHTML='';}
}

function buildRefCard(ref){
  let html='<div class="ref-card">';
  html+='<div class="ref-card-header">';
  if(ref.document_type){
    const cls=ref.document_type.toLowerCase();
    html+='<span class="ref-type-badge '+escAttr(cls)+'">'+escHtml(ref.document_type)+'</span>';
  }
  html+='<strong>'+escHtml(ref.document_name||'Unknown document')+'</strong>';
  // Meta info
  let meta=[];
  if(ref.page_number!=null) meta.push('Page '+ref.page_number);
  if(ref.chunk_index!=null) meta.push('Chunk '+ref.chunk_index);
  if(ref.coordinates&&ref.coordinates.length===4) meta.push('Coords: ['+ref.coordinates.map(function(c){return c.toFixed(1)}).join(', ')+']');
  if(meta.length) html+='<span class="ref-meta">'+escHtml(meta.join(' \u2022 '))+'</span>';
  html+='</div>';
  // Snippet
  if(ref.snippet){
    html+='<div class="ref-snippet">'+escHtml(ref.snippet)+'</div>';
  }
  // Links
  let links=[];
  if(ref.document_url){
    const fullUrl=BASE_PATH+ref.document_url;
    links.push('<a href="'+escAttr(fullUrl)+'" target="_blank" title="Download document">&#128196; Document</a>');
  }
  if(ref.image_url){
    const fullImgUrl=BASE_PATH+ref.image_url;
    links.push('<a href="javascript:void(0)" onclick="showImageModal(\''+escAttr(fullImgUrl)+'\')" title="View image">&#128444; Image</a>');
  }
  if(ref.page_number!=null && ref.document_url){
    const pdfUrl=BASE_PATH+ref.document_url;
    links.push('<a href="'+escAttr(pdfUrl)+'" target="_blank" title="Open at page">&#128279; Page '+ref.page_number+'</a>');
  }
  if(links.length){
    html+='<div class="ref-links">'+links.join('')+'</div>';
  }
  html+='</div>';
  return html;
}

function downloadExcel(){
  if(!_detailTaskId)return;
  const url=API+'/assessments/'+_detailTaskId+'/results/excel';
  if(JWT){
    fetch(url,{headers:headers()}).then(r=>{
      if(!r.ok){
        return r.text().then(txt=>{throw new Error('Download failed ('+r.status+'): '+(txt.substring(0,200)||r.statusText));});
      }
      return r.blob();
    }).then(b=>{
      const a=document.createElement('a');a.href=URL.createObjectURL(b);
      a.download='assessment_'+_detailTaskId.substring(0,8)+'.xlsx';a.click();
    }).catch(e=>toast(e.message,'err'));
  }else{window.open(url);}
}

// ----- SINGLE CALL -----
async function submitSingle(){
  const qf=document.getElementById('single-q').files[0];
  const evf=document.getElementById('single-ev').files;
  if(!qf){toast('Select a questions file','err');return;}
  if(!evf.length){toast('Select evidence documents','err');return;}
  btnLoading('btn-single','Starting\u2026');
  const fd=new FormData();
  fd.append('questions_file',qf);
  for(const f of evf)fd.append('evidence_files',f);
  const ds=document.getElementById('single-ds').value.trim();
  const cn=document.getElementById('single-chat').value.trim();
  const dso=document.getElementById('single-ds-opts').value.trim();
  const cno=document.getElementById('single-chat-opts').value.trim();
  if(ds)fd.append('dataset_name',ds);
  if(cn)fd.append('chat_name',cn);
  if(dso)fd.append('dataset_options',dso);
  if(cno)fd.append('chat_options',cno);
  const qidCol=document.getElementById('single-qid-col').value.trim();
  const qCol=document.getElementById('single-q-col').value.trim();
  if(qidCol)fd.append('question_id_column',qidCol);
  if(qCol)fd.append('question_column',qCol);
  const vResCol=document.getElementById('single-v-res-col').value.trim();
  const vComCol=document.getElementById('single-v-com-col').value.trim();
  if(vResCol)fd.append('vendor_response_column',vResCol);
  if(vComCol)fd.append('vendor_comment_column',vComCol);
  fd.append('process_vendor_response',document.getElementById('single-v-process').checked?'true':'false');
  const ok=await postForm(API+'/assessments',fd,'single-result','btn-single');
  if(ok) btnDone('btn-single'); else btnError('btn-single');
}

// ----- FROM DATASET -----
async function submitFromDataset(){
  const qf=document.getElementById('ds-q').files[0];
  const ids=document.getElementById('ds-ids').value.trim();
  if(!qf){toast('Select a questions file','err');return;}
  if(!ids){toast('Enter dataset IDs','err');return;}
  btnLoading('btn-dataset','Starting\u2026');
  const fd=new FormData();
  fd.append('questions_file',qf);
  fd.append('dataset_ids',ids);
  const cn=document.getElementById('ds-chat').value.trim();
  const dso=document.getElementById('ds-opts').value.trim();
  const cno=document.getElementById('ds-chat-opts').value.trim();
  if(cn)fd.append('chat_name',cn);
  if(dso)fd.append('dataset_options',dso);
  if(cno)fd.append('chat_options',cno);
  const qidCol=document.getElementById('ds-qid-col').value.trim();
  const qCol=document.getElementById('ds-q-col').value.trim();
  if(qidCol)fd.append('question_id_column',qidCol);
  if(qCol)fd.append('question_column',qCol);
  const vResCol=document.getElementById('ds-v-res-col').value.trim();
  const vComCol=document.getElementById('ds-v-com-col').value.trim();
  if(vResCol)fd.append('vendor_response_column',vResCol);
  if(vComCol)fd.append('vendor_comment_column',vComCol);
  fd.append('process_vendor_response',document.getElementById('ds-v-process').checked?'true':'false');
  const ok=await postForm(API+'/assessments/from-dataset',fd,'ds-result','btn-dataset');
  if(ok) btnDone('btn-dataset'); else btnError('btn-dataset');
}

// ----- TWO-PHASE -----
async function createSession(){
  const qf=document.getElementById('sess-q').files[0];
  if(!qf){toast('Select a questions file','err');return;}
  btnLoading('btn-sess-create','Creating\u2026');
  const fd=new FormData();fd.append('questions_file',qf);
  const ds=document.getElementById('sess-ds').value.trim();
  const dso=document.getElementById('sess-ds-opts').value.trim();
  const cno=document.getElementById('sess-chat-opts').value.trim();
  if(ds)fd.append('dataset_name',ds);
  if(dso)fd.append('dataset_options',dso);
  if(cno)fd.append('chat_options',cno);
  const qidCol=document.getElementById('sess-qid-col').value.trim();
  const qCol=document.getElementById('sess-q-col').value.trim();
  if(qidCol)fd.append('question_id_column',qidCol);
  if(qCol)fd.append('question_column',qCol);
  const vResCol=document.getElementById('sess-v-res-col').value.trim();
  const vComCol=document.getElementById('sess-v-com-col').value.trim();
  if(vResCol)fd.append('vendor_response_column',vResCol);
  if(vComCol)fd.append('vendor_comment_column',vComCol);
  const r=await postForm(API+'/assessments/sessions',fd,'sess-result','btn-sess-create');
  if(r&&r.task_id){
    document.getElementById('sess-tid').value=r.task_id;
    document.getElementById('sess-start-tid').value=r.task_id;
    btnDone('btn-sess-create');
  } else { btnError('btn-sess-create'); }
}
async function uploadSessionDocs(){
  const tid=document.getElementById('sess-tid').value.trim();
  const files=document.getElementById('sess-files').files;
  if(!tid){toast('Enter task ID','err');return;}
  if(!files.length){toast('Select files','err');return;}
  btnLoading('btn-sess-upload','Uploading\u2026');
  const fd=new FormData();
  for(const f of files)fd.append('files',f);
  const ok=await postForm(API+'/assessments/sessions/'+tid+'/documents',fd,'sess-result','btn-sess-upload');
  if(ok) btnDone('btn-sess-upload'); else btnError('btn-sess-upload');
}
async function startSession(){
  const tid=document.getElementById('sess-start-tid').value.trim();
  if(!tid){toast('Enter task ID','err');return;}
  btnLoading('btn-sess-start','Starting\u2026');
  const fd=new FormData();
  const cn=document.getElementById('sess-start-chat').value.trim();
  const dso=document.getElementById('sess-start-ds-opts').value.trim();
  const cno=document.getElementById('sess-start-chat-opts').value.trim();
  if(cn)fd.append('chat_name',cn);
  if(dso)fd.append('dataset_options',dso);
  if(cno)fd.append('chat_options',cno);
  fd.append('process_vendor_response',document.getElementById('sess-v-process').checked?'true':'false');
  const ok=await postForm(API+'/assessments/sessions/'+tid+'/start',fd,'sess-result','btn-sess-start');
  if(ok) btnDone('btn-sess-start'); else btnError('btn-sess-start');
}

// ----- UPLOAD DOCS -----
async function uploadDocs(){
  const dsid=document.getElementById('up-dsid').value.trim();
  const files=document.getElementById('up-files').files;
  if(!dsid){toast('Enter dataset ID','err');return;}
  if(!files.length){toast('Select files','err');return;}
  btnLoading('btn-upload','Uploading\u2026');
  const fd=new FormData();
  fd.append('dataset_id',dsid);
  for(const f of files)fd.append('files',f);
  fd.append('parse',document.getElementById('up-parse').checked?'true':'false');
  const ok=await postForm(API+'/documents/upload',fd,'up-result','btn-upload');
  if(ok) btnDone('btn-upload'); else btnError('btn-upload');
}

// ----- HEALTH -----
async function checkHealth(){
  const btn=document.getElementById('btn-health');
  btn.disabled=true;
  const el=document.getElementById('health-result');
  try{
    let r;
    try{
      r=await fetch(HEALTH_URL);
    }catch(netErr){
      throw new Error('Network error: could not reach the server.');
    }
    if(!r.ok)throw new Error('Health check failed with status '+r.status);
    const ct=r.headers.get('content-type')||'';
    if(!ct.includes('application/json')){
      const txt=await r.text();
      throw new Error('Unexpected response: '+txt.substring(0,200));
    }
    const d=await r.json();
    el.textContent=JSON.stringify(d,null,2);
    document.getElementById('hdr-info').textContent='RAGFlow: '+d.ragflow_url+' | Auth: '+(d.auth_enabled?'ON':'OFF');
  }catch(e){el.textContent='Error: '+e.message;}
  finally{btn.disabled=false;}
}

// ----- HELPERS -----
async function postForm(url,fd,resultElId,btnId){
  const el=document.getElementById(resultElId);
  el.classList.remove('hidden');
  el.innerHTML='<p>Submitting\u2026<span class="spinner spinner-dark"></span></p>';
  try{
    let r;
    try{
      r=await fetch(url,{method:'POST',headers:headers(),body:fd});
    }catch(netErr){
      throw new Error('Network error: could not reach the server. Please check your connection.');
    }
    let d;
    const ct=r.headers.get('content-type')||'';
    if(ct.includes('application/json')){
      d=await r.json();
    }else{
      const txt=await r.text();
      if(!r.ok)throw new Error('Server error ('+r.status+'): '+(txt.substring(0,300)||r.statusText));
      d={raw:txt};
    }
    if(!r.ok)throw new Error(d.detail||JSON.stringify(d));
    el.innerHTML='<pre style="background:var(--bg);padding:1rem;border-radius:var(--radius);font-size:.85rem;overflow:auto">'+escHtml(JSON.stringify(d,null,2))+'</pre>';
    toast('Success!','ok');
    return d;
  }catch(e){el.innerHTML='<p style="color:var(--danger)">Error: '+escHtml(e.message)+'</p>';toast(e.message,'err');return null;}
}
async function retryUploadDocs(){
  if(!_detailTaskId)return;
  const filesEl=document.getElementById('retry-files');
  if(!filesEl||!filesEl.files.length){toast('Select files to upload','err');return;}
  btnLoading('btn-retry-upload','Uploading\u2026');
  const fd=new FormData();
  for(const f of filesEl.files)fd.append('files',f);
  const ok=await postForm(API+'/assessments/sessions/'+_detailTaskId+'/documents',fd,'retry-result','btn-retry-upload');
  if(ok){btnDone('btn-retry-upload');await refreshDetail();}else{btnError('btn-retry-upload');}
}
async function retryStartAssessment(){
  if(!_detailTaskId)return;
  btnLoading('btn-retry-start','Starting\u2026');
  const fd=new FormData();
  fd.append('process_vendor_response',document.getElementById('retry-v-process').checked?'true':'false');
  const ok=await postForm(API+'/assessments/sessions/'+_detailTaskId+'/start',fd,'retry-result','btn-retry-start');
  if(ok){btnDone('btn-retry-start');toast('Assessment re-started!','ok');await refreshDetail();}else{btnError('btn-retry-start');}
}
function escHtml(s){const d=document.createElement('div');d.textContent=s||'';return d.innerHTML;}
function escAttr(s){return (s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

// Auto-check health on load
checkHealth();
/* ------------------------------------------------------------------ */
/* Manage Data                                                         */
/* ------------------------------------------------------------------ */
let dsPage=1, dsPageSize=10, dsTotal=0;
let docPage=1, docPageSize=10, docTotal=0;

async function loadDatasets(page=1){
  dsPage = page;
  const div = document.getElementById('manage-datasets-list');
  div.innerHTML = '<span class="spinner spinner-dark"></span> Loading...';
  try {
    const res = await fetch(`${API}/datasets?page=${dsPage}&page_size=${dsPageSize}`, { headers: headers() });
    if(!res.ok) throw new Error('Failed to load datasets');
    const data = await res.json();
    
    // Support both raw list and paginated response
    let list=[], total=0;
    if(Array.isArray(data)){
      list = data;
      total = list.length; // Can't define total
    } else {
      list = data.items || [];
      total = data.total;
    }
    dsTotal = total;

    if(list.length === 0) {
      div.innerHTML = '<p class="empty">No datasets found</p>';
      updatePagination('ds', dsPage, dsPageSize, dsTotal, 0);
      return;
    }
    let html = '<table><thead><tr><th><input type="checkbox" onchange="toggleAll(this, \'ds-cb\')"/></th><th>ID</th><th>Name</th><th>Docs</th></tr></thead><tbody>';
    list.forEach(d => {
      html += `<tr>
        <td><input type="checkbox" class="ds-cb" value="${d.id}"/></td>
        <td>${d.id}</td>
        <td>${d.name}</td>
        <td>${d.document_count}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    div.innerHTML = html;
    updatePagination('ds', dsPage, dsPageSize, dsTotal, list.length);
  } catch(e) {
    div.innerHTML = `<p class="error">${escHtml(e.message)}</p>`;
  }
}

function changeDsPage(delta) {
  const newPage = dsPage + delta;
  if (newPage < 1) return;
  loadDatasets(newPage);
}

async function deleteSelectedDatasets(){
  const ids = Array.from(document.querySelectorAll('.ds-cb:checked')).map(cb => cb.value);
  if(ids.length === 0) return toast('No datasets selected', 'err');
  if(!confirm(`Delete ${ids.length} datasets? This cannot be undone.`)) return;
  
  try {
    const res = await fetch(API + '/datasets', {
      method: 'DELETE',
      headers: {...headers(), 'Content-Type': 'application/json'},
      body: JSON.stringify({ids: ids})
    });
    if(!res.ok) throw new Error('Failed to delete datasets');
    toast('Datasets deleted', 'ok');
    loadDatasets(1); // Reload first page
  } catch(e) {
    toast(e.message, 'err');
  }
}

async function loadDocuments(page=1){
  const dsId = document.getElementById('manage-doc-dsid').value.trim();
  if(!dsId) return toast('Please enter Dataset ID', 'err');
  
  docPage = page;
  const div = document.getElementById('manage-documents-list');
  div.innerHTML = '<span class="spinner spinner-dark"></span> Loading...';
  try {
    const res = await fetch(`${API}/datasets/${dsId}/documents?page=${docPage}&page_size=${docPageSize}`, { headers: headers() });
    if(!res.ok) throw new Error('Failed to load documents');
    const data = await res.json();
    
    let list=[], total=0;
    if(Array.isArray(data)){
      list = data;
      total = list.length; 
    } else {
      list = data.items || [];
      total = data.total;
    }
    docTotal = total;

    if(!list || list.length === 0) {
      div.innerHTML = '<p class="empty">No documents found</p>';
      updatePagination('doc', docPage, docPageSize, docTotal, 0);
      return;
    }
    let html = '<table><thead><tr><th><input type="checkbox" onchange="toggleAll(this, \'doc-cb\')"/></th><th>ID</th><th>Name</th><th>Status</th></tr></thead><tbody>';
    list.forEach(d => {
      html += `<tr>
        <td><input type="checkbox" class="doc-cb" value="${d.id}"/></td>
        <td>${d.id}</td>
        <td>${d.name}</td>
        <td><span class="${badgeClass(d.status)}">${d.status}</span></td>
      </tr>`;
    });
    html += '</tbody></table>';
    div.innerHTML = html;
    updatePagination('doc', docPage, docPageSize, docTotal, list.length);
  } catch(e) {
    div.innerHTML = `<p class="error">${escHtml(e.message)}</p>`;
  }
}

function changeDocPage(delta) {
  const newPage = docPage + delta;
  if (newPage < 1) return;
  loadDocuments(newPage);
}

async function deleteSelectedDocuments(){
  const dsId = document.getElementById('manage-doc-dsid').value.trim();
  if(!dsId) return toast('Please enter Dataset ID', 'err');

  const ids = Array.from(document.querySelectorAll('.doc-cb:checked')).map(cb => cb.value);
  if(ids.length === 0) return toast('No documents selected', 'err');
  if(!confirm(`Delete ${ids.length} documents? This cannot be undone.`)) return;

  try {
    const res = await fetch(`${API}/datasets/${dsId}/documents`, {
      method: 'DELETE',
      headers: {...headers(), 'Content-Type': 'application/json'},
      body: JSON.stringify({ids: ids})
    });
    if(!res.ok) throw new Error('Failed to delete documents');
    toast('Documents deleted', 'ok');
    loadDocuments(docPage); // Reload current page
  } catch(e) {
    toast(e.message, 'err');
  }
}

function updatePagination(type, page, pageSize, total, currentCount) {
    const info = document.getElementById(type + '-page-info');
    const btnPrev = document.getElementById('btn-' + type + '-prev');
    const btnNext = document.getElementById('btn-' + type + '-next');
    if(!info || !btnPrev || !btnNext) return;
    
    btnPrev.disabled = page <= 1;
    
    if (total === null || total === undefined || total === 0 && currentCount > 0) {
        // Unknown total
        info.textContent = `Page ${page}`;
        // If we got full page, assume there might be more
        btnNext.disabled = currentCount < pageSize;
    } else {
        const totalPages = Math.ceil(total / pageSize) || 1;
        info.textContent = `Page ${page} of ${totalPages} (Total: ${total})`;
        btnNext.disabled = page >= totalPages;
    }
}

function toggleAll(source, className) {
  const checkboxes = document.querySelectorAll('.' + className);
  for(var i=0, n=checkboxes.length;i<n;i++) {
    checkboxes[i].checked = source.checked;
  }
}

</script>
</body>
</html>"""
