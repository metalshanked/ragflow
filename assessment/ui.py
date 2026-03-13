"""
Simple web UI for AI Assessments.

Serves an HTML UI at ``/ui`` (or ``/<base_path>/ui`` when a subpath
is configured) that provides a browser-based interface to all API features.
"""

from __future__ import annotations

import base64

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

router = APIRouter(tags=["ui"])

APP_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128" role="img" aria-label="Assessment">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#4361ee"/>
      <stop offset="100%" stop-color="#2ec4b6"/>
    </linearGradient>
  </defs>
  <rect width="128" height="128" rx="28" fill="url(#bg)"/>
  <path d="M30 94V34h52l16 16v44H30z" fill="#fff" opacity=".98"/>
  <path d="M82 34v18h16" fill="#dfe7ff"/>
  <path d="M46 82l10-12 10 8 18-24" fill="none" stroke="#4361ee" stroke-width="9" stroke-linecap="round" stroke-linejoin="round"/>
  <circle cx="46" cy="82" r="4.5" fill="#2ec4b6"/>
  <circle cx="66" cy="78" r="4.5" fill="#2ec4b6"/>
  <circle cx="84" cy="54" r="4.5" fill="#2ec4b6"/>
</svg>"""

APP_FAVICON_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAACBElEQVR42sWWa0tUURSG9y+a+/0+0y8xEiOSQhRFFEMSRZQikiKURJQiFEW832acNLOLpd2vVpqjWf6Ft3Xmg1DOWnNm3HBeWJ+fZ5+z91pLnak4QursH6TO/Uay8hDJql9Inj9A4sI+EhdzSFTvIX7pJ+KXdxGv2UWsdgexuh+I1X9HtOEboo3biDZ9RaT5CyItnxG58gnhVqqrHxFu+4BQ+3uEOt4h1PkWwa43CHa/RvDaKwSubyFwYwvKSnjg5iYJWAj397yEshLuv/WCBCyE+29vkIAA15HtHFi4785zKOnkOuB5AQbu631GAsJn1wHPCzBwX99TEhD+uQ64URzce/cJlHThdMDzAgzc279OAsJt/z+2lanjKkmAgXsHHpOA8NQ4gVLDwT2DJCC980IC5YSDe4bWSEBoMlKMIWaUKQEG7rn3iASEDmdGwIwEB3ffX4WS2muxmJXg4O4HKyQg9HYzMSPBwd3DD0lAGCxmU0yCg7tGslDSVCslkgQHd40uk4AwUksNJ8HBneNpKGmZKDcnBBi4c8IQEDaZ0+QfAQbunFwiAWGN0hUO7phehJJ2OG0CDNwxs0ACwgKpTYCBO+bmSUDYXnWFg9sX5qDMrM7SPJeajHRyA25fnCUBC+H29AwJWAi3ZaahrITbslMkYCHcWPH+AiMzXFUS3KaoAAAAAElFTkSuQmCC"
)
APP_FAVICON_ICO = base64.b64decode(
    "AAABAAEAICAAAAEAIAAxAgAAFgAAAIlQTkcNChoKAAAADUlIRFIAAAAgAAAAIAgGAAAAc3p69AAAAgRJREFUeNrFlmtLVFEUhvcvmvv9PtMvMRIjkkIURRRDEkWUIpIilESUIhRFvN9mnDSzi6Xdr1aao1n+hbd15oNQzlpzZtxwXlifn2efs/daS52pOELq7B+kzv1GsvIQyapfSJ4/QOLCPhIXc0hU7yF+6Sfil3cRr9lFrHYHsbofiNV/R7ThG6KN24g2fUWk+QsiLZ8RufIJ4Vaqqx8RbvuAUPt7hDreIdT5FsGuNwh2v0bw2isErm8hcGMLykp44OYmCVgI9/e8hLIS7r/1ggQshPtvb5CAANeR7RxYuO/Ocyjp5DrgeQEG7ut9RgLCZ9cBzwswcF/fUxIQ/rkOuFEc3Hv3CZR04XTA8wIM3Nu/TgLCbf8/tpWp4ypJgIF7Bx6TgPDUOIFSw8E9gyQgvfNCAuWEg3uG1khAaDJSjCFmlCkBBu6594gEhA5nRsCMBAd331+FktprsZiV4ODuByskIPR2MzEjwcHdww9JQBgsZlNMgoO7RrJQ0lQrJZIEB3eNLpOAMFJLDSfBwZ3jaShpmSg3JwQYuHPCEBA2mdPkHwEG7pxcIgFhjdIVDu6YXoSSdjhtAgzcMbNAAsICqU2AgTvm5klA2F51hYPbF+agzKzO0jyXmox0cgNuX5wlAQvh9vQMCVgIt2WmoayE27JTJGAh3Fjx/gIjM1xVEtymqAAAAABJRU5ErkJggg=="
)

def _ui_asset_href(request: Request, asset_name: str) -> str:
    _ = request
    return asset_name


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def ui_page(request: Request):
    """Serve the single-page assessment UI."""
    return HTMLResponse(
        content=_build_html(
            favicon_href=_ui_asset_href(request, "favicon.ico"),
        ),
        status_code=200,
    )


@router.get("/icon.svg", include_in_schema=False)
async def app_icon() -> Response:
    """Serve the app icon."""
    return Response(
        content=APP_ICON_SVG,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.get("/favicon.png", include_in_schema=False)
async def favicon_png() -> Response:
    """Serve a PNG favicon for browsers that ignore SVG favicons."""
    return Response(
        content=APP_FAVICON_PNG,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    """Serve a browser-friendly favicon."""
    return Response(
        content=APP_FAVICON_ICO,
        media_type="image/x-icon",
        headers={"Cache-Control": "public, max-age=86400"},
    )


def _build_html(*, favicon_href: str) -> str:
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<link rel="icon" href="__FAVICON_HREF__" sizes="any" type="image/x-icon"/>
<link rel="shortcut icon" href="__FAVICON_HREF__" type="image/x-icon"/>
<link rel="apple-touch-icon" href="__FAVICON_HREF__"/>
<title>AI Assessments</title>
<style>
:root{--bg:#f5f7fa;--card:#fff;--primary:#4361ee;--primary-hover:#3a56d4;--danger:#ef476f;--success:#06d6a0;--warn:#ffd166;--text:#212529;--muted:#6c757d;--border:#dee2e6;--radius:8px;--disabled:#a0aec0}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.6;padding:0}
header{background:var(--primary);color:#fff;padding:1rem 2rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.75rem}
header h1{font-size:1.3rem;font-weight:600;display:flex;align-items:center;gap:.65rem}
header h1 img{width:28px;height:28px;display:block;flex:0 0 auto}
header .info{font-size:.85rem;opacity:.85}
.header-actions{display:flex;align-items:center;gap:.75rem;flex-wrap:wrap;justify-content:flex-end}
.session-pill{display:inline-flex;align-items:center;padding:.35rem .7rem;border-radius:999px;background:rgba(255,255,255,.18);font-size:.82rem}
.container{max-width:1200px;margin:1.5rem auto;padding:0 1rem}
.tabs{display:flex;gap:4px;margin-bottom:1rem;flex-wrap:wrap}
.tab{padding:.5rem 1rem;border:none;background:var(--card);cursor:pointer;border-radius:var(--radius) var(--radius) 0 0;font-size:.9rem;color:var(--muted);border-bottom:2px solid transparent}
.tab.active{color:var(--primary);border-bottom-color:var(--primary);font-weight:600}
.panel{display:none;background:var(--card);border-radius:0 var(--radius) var(--radius) var(--radius);padding:1.5rem;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.panel.active{display:block}
label{display:block;font-weight:500;margin:.6rem 0 .25rem;font-size:.9rem}
input[type=text],input[type=password],input[type=file],select,textarea{width:100%;padding:.5rem .7rem;border:1px solid var(--border);border-radius:var(--radius);font-size:.9rem}
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
.login-screen{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;padding:1.5rem;background:linear-gradient(180deg,#eef4ff 0%,#f5f7fa 48%,#dfe9ff 100%);z-index:1100}
.login-card{width:min(460px,100%);background:rgba(255,255,255,.96);border:1px solid rgba(67,97,238,.12);border-radius:20px;box-shadow:0 24px 60px rgba(36,55,99,.16);padding:2rem}
.login-card h2{font-size:1.4rem;margin-bottom:.35rem}
.login-card p{color:var(--muted);margin-bottom:1rem}
.login-mark{display:flex;align-items:center;gap:.9rem;margin-bottom:1.25rem}
.login-mark img{width:52px;height:52px;display:block}
.login-field{display:flex;flex-direction:column;gap:.3rem;margin-bottom:.85rem}
.login-status{font-size:.84rem;color:var(--muted);margin-top:.85rem;min-height:1.2rem}
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
.link-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.85rem;margin-top:1rem}
.link-card{display:block;background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:.9rem 1rem;color:var(--text);text-decoration:none;cursor:pointer}
button.link-card{width:100%;text-align:left;font:inherit}
.link-card:hover{border-color:var(--primary);box-shadow:0 4px 16px rgba(67,97,238,.08)}
.link-card strong{display:block;margin-bottom:.25rem}
.link-card code{display:block;font-size:.8rem;color:var(--muted);word-break:break-all}
.api-link-status{font-size:.84rem;color:var(--muted);margin-top:1rem;min-height:1.2rem}
.api-link-result{margin-top:.75rem;background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:1rem;font-size:.84rem;overflow:auto;max-height:360px;white-space:pre-wrap;word-break:break-word}
/* Reference modal */
.modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.6);z-index:1000;display:flex;align-items:center;justify-content:center}
.modal-content{background:#fff;border-radius:var(--radius);padding:1rem;max-width:90vw;max-height:90vh;overflow:auto;position:relative}
.modal-content img{max-width:100%;max-height:80vh;display:block;margin:0 auto}
.modal-close{position:absolute;top:.5rem;right:.5rem;background:var(--danger);color:#fff;border:none;border-radius:999px;width:32px;height:32px;padding:0;cursor:pointer;font-size:1.15rem;line-height:1;display:flex;align-items:center;justify-content:center}
.modal-content.modal-document{width:min(96vw,1200px);max-width:min(96vw,1200px);padding:1rem 1rem 1.25rem}
.modal-title{font-size:1rem;font-weight:600;padding-right:2rem}
.modal-body{margin-top:.85rem}
.modal-body iframe{width:100%;height:78vh;border:1px solid var(--border);border-radius:var(--radius);background:#fff}
.modal-body pre{white-space:pre-wrap;word-break:break-word;background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:.9rem;max-height:72vh;overflow:auto}
.modal-toolbar{display:flex;align-items:center;gap:.6rem;flex-wrap:wrap;margin-bottom:.75rem}
.modal-toolbar a{color:var(--primary);text-decoration:none;font-size:.84rem}
.modal-toolbar a:hover{text-decoration:underline}
.reference-html{background:#fff;border:1px solid var(--border);border-radius:var(--radius);padding:.9rem;max-height:72vh;overflow:auto}
.reference-html table{margin-top:0}
.reference-html img{max-width:100%;height:auto}
.reference-score{font-size:.78rem;color:var(--muted)}
.reference-empty{font-size:.84rem;color:var(--muted)}
/* Auto-refresh toggle */
.auto-refresh{display:flex;align-items:center;gap:.5rem;font-size:.85rem}
.auto-refresh label{margin:0;font-weight:normal}
.json-error{border-color:var(--danger) !important;background-color:#ffebee !important}
@media(max-width:600px){.row{flex-direction:column}.tabs{gap:2px}.header-actions{justify-content:flex-start}}
</style>
</head>
<body>
<div class="login-screen hidden" id="login-screen">
  <div class="login-card">
    <div class="login-mark">
      <img src="__FAVICON_HREF__" alt=""/>
      <div>
        <h2>AI Assessments</h2>
        <p>Sign in with your LDAP account to access the app.</p>
      </div>
    </div>
    <div class="login-field">
      <label style="margin:0">Username</label>
      <input type="text" id="auth-username" placeholder="username" onkeydown="if(event.key==='Enter'){loginLdap();}"/>
    </div>
    <div class="login-field">
      <label style="margin:0">Password</label>
      <input type="password" id="auth-password" placeholder="password" onkeydown="if(event.key==='Enter'){loginLdap();}"/>
    </div>
    <button class="btn btn-primary" id="btn-login" onclick="loginLdap()">Sign In</button>
    <div class="login-status" id="login-status"></div>
  </div>
</div>
<header>
  <h1><img src="__FAVICON_HREF__" alt=""/>AI Assessments</h1>
  <div class="header-actions">
    <span class="info" id="hdr-info"></span>
    <span class="session-pill hidden" id="session-summary"></span>
    <button class="btn btn-outline btn-sm hidden" id="btn-logout" onclick="logout()">Logout</button>
  </div>
</header>

<div class="container" id="app-shell">
  <!-- Tabs -->
  <div class="tabs">
    <button class="tab active" data-tab="tasks" onclick="switchTab(this)">&#128203; Tasks</button>
    <button class="tab" data-tab="single" onclick="switchTab(this)">&#9889; Single-call</button>
    <button class="tab" data-tab="dataset" onclick="switchTab(this)">&#128451; From Dataset</button>
    <button class="tab" data-tab="session" onclick="switchTab(this)">&#128257; Two-phase</button>
    <button class="tab" data-tab="upload" onclick="switchTab(this)">&#128228; Upload Docs</button>
    <button class="tab" data-tab="manage" onclick="switchTab(this)">&#128193; Manage Data</button>
    <button class="tab" data-tab="health" onclick="switchTab(this)">&#128154; Health</button>
    <button class="tab" data-tab="api" onclick="switchTab(this)">&#128279; API Docs</button>
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
        <button class="btn btn-danger btn-sm" id="btn-delete-all-tasks" onclick="deleteAllTasks()">Delete All Tasks</button>
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
    <label style="display:flex;align-items:center;gap:.5rem;margin-top:.5rem"><input type="checkbox" id="single-reuse-existing-ds" checked onchange="onSingleFieldChange()"/> Reuse existing dataset by name (upsert mode)</label>
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
      <label style="display:flex;align-items:center;gap:.5rem;margin-top:.5rem"><input type="checkbox" id="sess-reuse-existing-ds" checked onchange="onSessCreateFieldChange()"/> Reuse existing dataset by name (upsert mode)</label>
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

  <!-- API DOCS PANEL -->
  <div class="panel" id="panel-api">
    <h3>API Docs & Links</h3>
    <p style="color:var(--muted);font-size:.88rem;margin:.4rem 0">Quick links to the generated API documentation and common endpoints. Protected endpoint cards use the current UI session token automatically.</p>
    <div class="link-grid">
      <a class="link-card" id="link-docs" target="_blank" rel="noreferrer">
        <strong>Swagger UI</strong>
        <code></code>
      </a>
      <a class="link-card" id="link-redoc" target="_blank" rel="noreferrer">
        <strong>ReDoc</strong>
        <code></code>
      </a>
      <a class="link-card" id="link-openapi" target="_blank" rel="noreferrer">
        <strong>OpenAPI JSON</strong>
        <code></code>
      </a>
      <a class="link-card" id="link-health" target="_blank" rel="noreferrer">
        <strong>Health</strong>
        <code></code>
      </a>
      <button class="link-card" id="link-assessments" type="button" onclick="runApiLink('link-assessments')">
        <strong>Assessments List</strong>
        <code></code>
      </button>
      <button class="link-card" id="link-datasets" type="button" onclick="runApiLink('link-datasets')">
        <strong>Native Datasets</strong>
        <code></code>
      </button>
    </div>
    <div class="api-link-status" id="api-link-status">Click a protected endpoint card to run it with the current UI session token.</div>
    <pre class="api-link-result" id="api-link-result">No API endpoint response loaded yet.</pre>
  </div>

  <!-- TASK DETAIL MODAL -->
  <div id="task-detail" class="hidden" style="margin-top:1rem">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:.5rem;margin-bottom:.5rem">
      <h3 id="detail-title">Task Details</h3>
      <div style="display:flex;align-items:center;gap:.6rem;flex-wrap:wrap">
        <div class="auto-refresh">
          <label><input type="checkbox" id="detail-auto-refresh-cb" onchange="toggleDetailAutoRefresh()"/> Auto-refresh</label>
          <select id="detail-auto-refresh-interval" onchange="toggleDetailAutoRefresh()" style="width:auto;padding:2px 4px;font-size:.82rem">
            <option value="5">5s</option>
            <option value="10" selected>10s</option>
            <option value="30">30s</option>
            <option value="60">60s</option>
          </select>
        </div>
        <button class="btn btn-outline btn-sm" onclick="closeDetail()">&#10005; Close</button>
        <button class="btn btn-primary btn-sm" onclick="refreshDetail()">&#128260; Refresh</button>
        <button class="btn btn-danger btn-sm" id="btn-delete-detail-task" onclick="deleteCurrentTask()">Delete Task</button>
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

function initApiLinks(){
  const links = {
    'link-docs': BASE_PATH + '/docs',
    'link-redoc': BASE_PATH + '/redoc',
    'link-openapi': BASE_PATH + '/openapi.json',
    'link-health': HEALTH_URL,
    'link-assessments': API + '/assessments',
    'link-datasets': API + '/native/datasets',
  };
  const protectedLinks = {'link-assessments': true, 'link-datasets': true};
  Object.keys(links).forEach(function(id){
    const el = document.getElementById(id);
    if(!el)return;
    if(el.tagName === 'A'){
      el.href = links[id];
    }
    el.dataset.url = links[id];
    if(protectedLinks[id]){
      el.dataset.auth = 'required';
    }
    const codeEl = el.querySelector('code');
    if(codeEl)codeEl.textContent = links[id];
  });
}

let ACCESS_TOKEN = localStorage.getItem('assessment_access_token') || '';
let REFRESH_TOKEN = localStorage.getItem('assessment_refresh_token') || '';
let ACCESS_TOKEN_EXPIRES_AT = parseInt(localStorage.getItem('assessment_access_token_expires_at') || '0', 10) || 0;
let REFRESH_TOKEN_EXPIRES_AT = parseInt(localStorage.getItem('assessment_refresh_token_expires_at') || '0', 10) || 0;
let AUTH_USERNAME = localStorage.getItem('assessment_auth_username') || '';
let AUTH_ROLES = (localStorage.getItem('assessment_auth_roles') || '').split(',').filter(Boolean);
let AUTH_MODE = 'disabled';
let _sessionRefreshTimer = null;
let _sessionLogoutTimer = null;
let _sessionRefreshPromise = null;

document.getElementById('auth-username').value = AUTH_USERNAME;
updateAuthUi();

function headers(){const h={};if(ACCESS_TOKEN)h['Authorization']='Bearer '+ACCESS_TOKEN;return h;}
function _epochNow(){return Math.floor(Date.now()/1000);}
function _isAccessTokenExpired(skewSeconds){return !!ACCESS_TOKEN_EXPIRES_AT && ACCESS_TOKEN_EXPIRES_AT <= (_epochNow() + (skewSeconds || 0));}
function _isRefreshTokenExpired(skewSeconds){return !!REFRESH_TOKEN_EXPIRES_AT && REFRESH_TOKEN_EXPIRES_AT <= (_epochNow() + (skewSeconds || 0));}
function _clearSessionTimers(){
  if(_sessionRefreshTimer){clearTimeout(_sessionRefreshTimer);_sessionRefreshTimer=null;}
  if(_sessionLogoutTimer){clearTimeout(_sessionLogoutTimer);_sessionLogoutTimer=null;}
}
function _setStoredInt(key, value){
  if(value && value > 0){localStorage.setItem(key, String(value));}
  else{localStorage.removeItem(key);}
}
function _persistSession(){
  if(ACCESS_TOKEN){localStorage.setItem('assessment_access_token', ACCESS_TOKEN);}else{localStorage.removeItem('assessment_access_token');}
  if(REFRESH_TOKEN){localStorage.setItem('assessment_refresh_token', REFRESH_TOKEN);}else{localStorage.removeItem('assessment_refresh_token');}
  if(AUTH_USERNAME){localStorage.setItem('assessment_auth_username', AUTH_USERNAME);}else{localStorage.removeItem('assessment_auth_username');}
  localStorage.setItem('assessment_auth_roles', AUTH_ROLES.join(','));
  _setStoredInt('assessment_access_token_expires_at', ACCESS_TOKEN_EXPIRES_AT);
  _setStoredInt('assessment_refresh_token_expires_at', REFRESH_TOKEN_EXPIRES_AT);
}
function _setLoginStatus(message){
  const loginStatus=document.getElementById('login-status');
  if(loginStatus)loginStatus.textContent=message||'';
}
function setApiLinkResult(message, isError){
  const statusEl=document.getElementById('api-link-status');
  const resultEl=document.getElementById('api-link-result');
  if(statusEl){
    statusEl.textContent=isError ? 'API request failed.' : 'API request completed.';
    statusEl.style.color=isError ? 'var(--danger)' : 'var(--muted)';
  }
  if(resultEl)resultEl.textContent=message;
}
function clearSession(message, showToast){
  ACCESS_TOKEN='';
  REFRESH_TOKEN='';
  ACCESS_TOKEN_EXPIRES_AT=0;
  REFRESH_TOKEN_EXPIRES_AT=0;
  AUTH_USERNAME='';
  AUTH_ROLES=[];
  _clearSessionTimers();
  _persistSession();
  updateAuthUi();
  if(message){_setLoginStatus(message);}
  if(showToast && message){toast(message,'err');}
}
function scheduleSessionTimers(){
  _clearSessionTimers();
  if(AUTH_MODE !== 'ldap'){return;}
  if(REFRESH_TOKEN && _isRefreshTokenExpired()){
    clearSession('Session expired. Please sign in again.', true);
    return;
  }
  if(REFRESH_TOKEN && REFRESH_TOKEN_EXPIRES_AT){
    const logoutDelay=Math.max(0, (REFRESH_TOKEN_EXPIRES_AT - _epochNow()) * 1000);
    _sessionLogoutTimer=setTimeout(function(){
      clearSession('Session expired. Please sign in again.', true);
    }, logoutDelay);
  }
  if(!REFRESH_TOKEN || !ACCESS_TOKEN_EXPIRES_AT){return;}
  const leadSeconds=Math.min(60, Math.max(15, Math.floor((ACCESS_TOKEN_EXPIRES_AT - _epochNow()) / 4) || 15));
  const refreshDelay=Math.max(0, (ACCESS_TOKEN_EXPIRES_AT - _epochNow() - leadSeconds) * 1000);
  _sessionRefreshTimer=setTimeout(function(){
    void refreshSessionToken({showFeedback:false, keepLoginMessage:false, showToastOnFailure:false});
  }, refreshDelay);
}
async function ensureActiveSession(showToastOnFailure){
  if(AUTH_MODE !== 'ldap'){return !!ACCESS_TOKEN;}
  if(!ACCESS_TOKEN && !REFRESH_TOKEN){
    if(showToastOnFailure){toast('Sign in required','err');}
    return false;
  }
  if(_isRefreshTokenExpired()){
    clearSession('Session expired. Please sign in again.', !!showToastOnFailure);
    return false;
  }
  if(!ACCESS_TOKEN || _isAccessTokenExpired(30)){
    return await refreshSessionToken({showFeedback:false, keepLoginMessage:false, showToastOnFailure:!!showToastOnFailure});
  }
  scheduleSessionTimers();
  return true;
}

function _storeSession(data){
  ACCESS_TOKEN = data.access_token || ACCESS_TOKEN;
  REFRESH_TOKEN = data.refresh_token || REFRESH_TOKEN;
  AUTH_USERNAME = data.username || AUTH_USERNAME || '';
  AUTH_ROLES = Array.isArray(data.roles) ? data.roles : AUTH_ROLES;
  const now=_epochNow();
  if(typeof data.expires_in === 'number' && data.expires_in > 0){
    ACCESS_TOKEN_EXPIRES_AT = now + data.expires_in;
  }
  if(typeof data.refresh_expires_in === 'number' && data.refresh_expires_in > 0){
    REFRESH_TOKEN_EXPIRES_AT = now + data.refresh_expires_in;
  }
  _persistSession();
  scheduleSessionTimers();
  updateAuthUi();
}

function updateAuthUi(){
  const loginScreen=document.getElementById('login-screen');
  const loginStatus=document.getElementById('login-status');
  const appShell=document.getElementById('app-shell');
  const sessionSummary=document.getElementById('session-summary');
  const logoutBtn=document.getElementById('btn-logout');
  const requiresLogin = AUTH_MODE === 'ldap';
  const signedIn = (!!ACCESS_TOKEN || !!REFRESH_TOKEN) && !_isRefreshTokenExpired();

  if(sessionSummary){
    if(signedIn){
      const roleText = AUTH_ROLES.length ? ' ['+AUTH_ROLES.join(', ')+']' : '';
      sessionSummary.textContent=(AUTH_USERNAME||'user')+roleText;
      sessionSummary.classList.remove('hidden');
    }else{
      sessionSummary.textContent='';
      sessionSummary.classList.add('hidden');
    }
  }
  if(logoutBtn){
    logoutBtn.classList.toggle('hidden', !signedIn);
  }
  if(loginScreen){
    loginScreen.classList.toggle('hidden', !(requiresLogin && !signedIn));
  }
  if(appShell){
    appShell.classList.toggle('hidden', requiresLogin && !signedIn);
  }
  if(loginStatus){
    if(requiresLogin && !signedIn){
      if(!loginStatus.textContent){
        loginStatus.textContent='Sign in with LDAP to continue.';
      }
    }else if(signedIn){
      loginStatus.textContent='';
    }
  }
}

async function loginLdap(){
  const username=document.getElementById('auth-username').value.trim();
  const password=document.getElementById('auth-password').value;
  if(!username || !password){toast('Enter LDAP username and password','err');return;}
  btnLoading('btn-login','Logging in\u2026');
  try{
    const r=await fetch(API+'/auth/token',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({username,password}),
    });
    const data=await r.json().catch(()=>({detail:'Invalid server response'}));
    if(!r.ok)throw new Error(data.detail||('Login failed ('+r.status+')'));
    _storeSession(data);
    document.getElementById('auth-password').value='';
    _setLoginStatus('');
    toast('Login successful','ok');
  }catch(e){
    _setLoginStatus(e.message);
    toast(e.message,'err');
  }finally{
    btnReset('btn-login');
  }
}

async function refreshSessionToken(options){
  const opts=options||{};
  if(_sessionRefreshPromise)return _sessionRefreshPromise;
  if(!REFRESH_TOKEN){
    clearSession('Session expired. Please sign in again.', !!opts.showToastOnFailure);
    return false;
  }
  if(_isRefreshTokenExpired()){
    clearSession('Session expired. Please sign in again.', !!opts.showToastOnFailure);
    return false;
  }
  _sessionRefreshPromise=(async function(){
    try{
      const r=await fetch(API+'/auth/refresh',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({refresh_token:REFRESH_TOKEN}),
      });
      const data=await r.json().catch(()=>({detail:'Invalid server response'}));
      if(!r.ok)throw new Error(data.detail||('Refresh failed ('+r.status+')'));
      _storeSession(data);
      if(opts.showFeedback)toast('Session refreshed','ok');
      if(opts.keepLoginMessage === false)_setLoginStatus('');
      return true;
    }catch(e){
      clearSession('Session expired. Please sign in again.', opts.showToastOnFailure !== false);
      return false;
    }finally{
      _sessionRefreshPromise=null;
    }
  })();
  return _sessionRefreshPromise;
}

async function verifySessionToken(showFeedback=true){
  try{
    const sessionOk=await ensureActiveSession(showFeedback);
    if(!sessionOk)throw new Error('Session expired. Please sign in again.');
    const r=await fetch(API+'/auth/verify',{headers:headers()});
    const data=await r.json().catch(()=>({detail:'Invalid server response'}));
    if(!r.ok)throw new Error(data.detail||('Verify failed ('+r.status+')'));
    if(data && data.valid){
      AUTH_USERNAME = data.username || AUTH_USERNAME;
      AUTH_ROLES = Array.isArray(data.roles) ? data.roles : AUTH_ROLES;
      if(data.expires_at){ACCESS_TOKEN_EXPIRES_AT = parseInt(data.expires_at, 10) || ACCESS_TOKEN_EXPIRES_AT;}
      _persistSession();
      scheduleSessionTimers();
      updateAuthUi();
      if(showFeedback)toast('Token is valid','ok');
    }else{
      if(showFeedback)toast('Token is invalid','err');
    }
  }catch(e){
    clearSession(e.message || 'Session expired. Please sign in again.', !!showFeedback);
  }
}

function logout(){
  clearSession('', false);
  _setLoginStatus('Sign in with LDAP to continue.');
  toast('Logged out','ok');
}

async function runApiLink(id){
  const el=document.getElementById(id);
  const url=(el && el.dataset && el.dataset.url) || '';
  if(!url)return;
  const needsAuth=!!(el && el.dataset && el.dataset.auth === 'required');
  const statusEl=document.getElementById('api-link-status');
  const resultEl=document.getElementById('api-link-result');
  if(statusEl){
    statusEl.textContent='Loading...';
    statusEl.style.color='var(--muted)';
  }
  if(resultEl)resultEl.textContent='Fetching ' + url + ' ...';
  if(needsAuth){
    const sessionOk=await ensureActiveSession(true);
    if(!sessionOk){
      setApiLinkResult('Sign in again to call protected API endpoints.', true);
      return;
    }
  }
  try{
    const response=await fetch(url,{headers:needsAuth ? headers() : {}});
    const text=await response.text();
    let body=text;
    try{
      body=JSON.stringify(JSON.parse(text), null, 2);
    }catch(parseErr){
      body=text;
    }
    if(!response.ok){
      throw new Error((body && body.substring(0,1000)) || ('Request failed ('+response.status+')'));
    }
    setApiLinkResult(body, false);
  }catch(e){
    setApiLinkResult(e.message || 'Request failed.', true);
  }
}

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
let _detailAutoRefreshTimer = null;
function toggleDetailAutoRefresh(){
  if(_detailAutoRefreshTimer){clearInterval(_detailAutoRefreshTimer);_detailAutoRefreshTimer=null;}
  const cb=document.getElementById('detail-auto-refresh-cb');
  const detail=document.getElementById('task-detail');
  if(cb && cb.checked && _detailTaskId && detail && !detail.classList.contains('hidden')){
    const secs=parseInt(document.getElementById('detail-auto-refresh-interval').value)||10;
    refreshDetail();
    _detailAutoRefreshTimer=setInterval(function(){
      if(_detailTaskId && !detail.classList.contains('hidden'))refreshDetail();
    }, secs*1000);
  }
}

/* ------------------------------------------------------------------ */
/* Reference modal                                                     */
/* ------------------------------------------------------------------ */
let _modalObjectUrls = [];
function _trackModalObjectUrl(url){
  if(url)_modalObjectUrls.push(url);
  return url;
}
function _clearModalObjectUrls(){
  while(_modalObjectUrls.length){
    const url=_modalObjectUrls.pop();
    try{URL.revokeObjectURL(url);}catch(_e){}
  }
}
function _withBasePath(url){
  if(!url)return '';
  if(/^https?:\/\//i.test(url) || /^blob:/i.test(url) || /^data:/i.test(url))return url;
  return url.charAt(0)==='/' ? BASE_PATH + url : url;
}
function _parseRefPayload(raw){
  try{return JSON.parse(raw);}catch(_e){return null;}
}
function _normalizeReference(raw){
  const ref=raw||{};
  const doc=ref.document||{};
  const location=ref.location||{};
  const preview=ref.preview||{};
  const links=ref.links||{};
  const retrieval=ref.retrieval||{};
  const highlight=location.highlight_box||{};
  return {
    raw: ref,
    referenceType: String(ref.reference_type||'').trim(),
    documentName: String(doc.document_name||'').trim(),
    documentType: String(doc.document_type||'').trim(),
    mediaFamily: String(doc.media_family||'').trim(),
    pageNumber: location.page_number!=null ? location.page_number : null,
    locationKind: String(location.kind||'').trim(),
    locationLabel: String(location.label||'').trim(),
    textExcerpt: String(preview.text_excerpt||'').trim(),
    fullContent: String(preview.full_content||'').trim(),
    htmlContent: String(preview.html_content||'').trim(),
    tableHtml: String(preview.table_html||'').trim(),
    contentFormat: String(preview.content_format||'none').trim(),
    hasInlinePreview: !!preview.has_inline_preview,
    documentUrl: links.document_url || null,
    renderedDocumentUrl: links.rendered_document_url || null,
    imageUrl: links.image_url || null,
    sourceUrl: links.source_url || null,
    score: typeof retrieval.score === 'number' ? retrieval.score : null,
    vectorScore: typeof retrieval.vector_score === 'number' ? retrieval.vector_score : null,
    termScore: typeof retrieval.term_score === 'number' ? retrieval.term_score : null,
    highlightBox: (typeof highlight.left === 'number' && typeof highlight.right === 'number' && typeof highlight.top === 'number' && typeof highlight.bottom === 'number') ? highlight : null
  };
}
function _sanitizeHtml(html){
  const template=document.createElement('template');
  template.innerHTML=html||'';
  template.content.querySelectorAll('script,iframe,object,embed,link,meta,style').forEach(function(node){node.remove();});
  template.content.querySelectorAll('*').forEach(function(node){
    Array.from(node.attributes).forEach(function(attr){
      const name=(attr.name||'').toLowerCase();
      const value=attr.value||'';
      if(name.startsWith('on')){node.removeAttribute(attr.name);return;}
      if((name==='src' || name==='href') && /^\s*javascript:/i.test(value)){node.removeAttribute(attr.name);}
    });
  });
  return template.innerHTML;
}
function _looksLikeHtmlContent(text){
  return /<table[\s>]|<tr[\s>]|<td[\s>]|<th[\s>]|<img[\s>]|<p[\s>]|<div[\s>]|<span[\s>]/i.test(text||'');
}
function _openModal(title, bodyHtml, extraClass){
  const m=document.getElementById('img-modal');
  m.className='modal-overlay';
  m.innerHTML='<div class="modal-content '+(extraClass||'')+'"><button class="modal-close" onclick="closeImageModal()">&times;</button><div class="modal-title">'+escHtml(title||'Reference Preview')+'</div><div class="modal-body">'+bodyHtml+'</div></div>';
  m.onclick=function(e){if(e.target===m)closeImageModal();};
}
async function _fetchProtectedResource(url, init){
  if(AUTH_MODE !== 'disabled'){
    const sessionOk=await ensureActiveSession(true);
    if(!sessionOk)throw new Error('Session expired. Please sign in again.');
  }
  const merged=Object.assign({}, init||{});
  merged.headers=Object.assign({}, (init&&init.headers)||{}, headers());
  return await fetch(_withBasePath(url), merged);
}
function closeImageModal(){
  const m=document.getElementById('img-modal');
  _clearModalObjectUrls();
  m.className='hidden';
  m.innerHTML='';
}
async function showImageModal(url, title){
  _openModal(title||'Reference image', '<p class="reference-empty">Loading image…</p>', 'modal-document');
  try{
    const response=await _fetchProtectedResource(url);
    if(!response.ok){
      const text=await response.text().catch(function(){return '';});
      throw new Error(text || ('Image request failed ('+response.status+')'));
    }
    const blob=await response.blob();
    const objectUrl=_trackModalObjectUrl(URL.createObjectURL(blob));
    _openModal(title||'Reference image', '<img src="'+escAttr(objectUrl)+'" alt="Reference image"/>', 'modal-document');
  }catch(e){
    _openModal(title||'Reference image', '<p style="color:var(--danger)">'+escHtml(e.message||'Failed to load image.')+'</p>', 'modal-document');
  }
}
function openReferenceImage(rawRef){
  const parsed=_parseRefPayload(rawRef);
  const ref=_normalizeReference(parsed);
  if(!parsed || !ref.imageUrl){toast('Reference image is unavailable','err');return;}
  void showImageModal(ref.imageUrl, ref.documentName || 'Reference image');
}
function _renderCsvTable(text){
  const lines=(text||'').split(/\r?\n/).filter(Boolean);
  if(!lines.length)return '<p class="reference-empty">No table rows available.</p>';
  const rows=lines.map(function(line){return line.split(',');});
  let html='<div class="reference-html"><table>';
  rows.forEach(function(row,rowIdx){
    html+='<tr>';
    row.forEach(function(cell){
      html+='<'+(rowIdx===0?'th':'td')+'>'+escHtml(cell.trim())+'</'+(rowIdx===0?'th':'td')+'>';
    });
    html+='</tr>';
  });
  html+='</table></div>';
  return html;
}
function _isTextLikeContentType(contentType){
  return contentType.indexOf('text/')===0 ||
    contentType.indexOf('json')>=0 ||
    contentType.indexOf('xml')>=0 ||
    contentType.indexOf('csv')>=0 ||
    contentType.indexOf('html')>=0;
}
function _canInlineDocument(contentType, ref){
  if(contentType.indexOf('pdf')>=0 || contentType.indexOf('image/')===0){
    return true;
  }
  if(_isTextLikeContentType(contentType)){
    return true;
  }
  return ['txt','md','markdown','html','htm','json','xml','yaml','yml','csv'].indexOf(ref.documentType)>=0;
}
function _supportsServerRenderedDocument(ref){
  return ['docx','excel','ppt'].indexOf(ref.documentType)>=0;
}
async function openReferenceDocument(rawRef){
  const parsed=_parseRefPayload(rawRef);
  const ref=_normalizeReference(parsed);
  if(!parsed || !ref.documentUrl){toast('Reference document is unavailable','err');return;}
  _openModal(ref.documentName || 'Reference document', '<p class="reference-empty">Loading document…</p>', 'modal-document');
  try{
    const response=await _fetchProtectedResource(ref.documentUrl);
    if(!response.ok){
      const text=await response.text().catch(function(){return '';});
      throw new Error(text || ('Document request failed ('+response.status+')'));
    }
    const blob=await response.blob();
    const objectUrl=_trackModalObjectUrl(URL.createObjectURL(blob));
    const contentType=(response.headers.get('content-type')||'').toLowerCase();
    const downloadName=ref.documentName || 'reference';
    const toolbar='<div class="modal-toolbar"><a href="'+escAttr(objectUrl)+'" download="'+escAttr(downloadName)+'">Download</a><a href="'+escAttr(objectUrl)+'" target="_blank" rel="noopener">Open Raw File</a></div>';
    const renderUrl = (_supportsServerRenderedDocument(ref) && ref.renderedDocumentUrl) ? ref.renderedDocumentUrl : null;
    if(renderUrl){
      const renderResponse = await _fetchProtectedResource(renderUrl);
      if(renderResponse.ok && (renderResponse.headers.get('content-type')||'').toLowerCase().indexOf('text/html') >= 0){
        const renderedHtml = await renderResponse.text();
        const body=toolbar+'<div class="reference-html">'+renderedHtml+'</div>';
        _openModal(ref.documentName || 'Reference document', body, 'modal-document');
        return;
      }
    }
    let body=toolbar;
    if(contentType.indexOf('pdf')>=0){
      const pdfUrl=objectUrl + (ref.pageNumber!=null ? '#page='+encodeURIComponent(String(ref.pageNumber)) : '');
      body+='<iframe src="'+escAttr(pdfUrl)+'" title="'+escAttr(downloadName)+'"></iframe>';
    }else if(contentType.indexOf('image/')===0){
      body+='<img src="'+escAttr(objectUrl)+'" alt="'+escAttr(downloadName)+'"/>';
    }else{
      const isTextLike=_isTextLikeContentType(contentType);
      const text=isTextLike ? await blob.text().catch(function(){return '';}) : '';
      if(!_canInlineDocument(contentType, ref)){
        body+='<p class="reference-empty">This file type is available through the packaged proxy link, but browsers do not render it inline reliably. Use Download or Open Raw File to view the original document.</p>';
      }else if(contentType.indexOf('text/csv')>=0 || (ref.documentType==='excel' && text)){
        body+=_renderCsvTable(text);
      }else if(_looksLikeHtmlContent(text)){
        body+='<div class="reference-html">'+_sanitizeHtml(text)+'</div>';
      }else if(text){
        body+='<pre>'+escHtml(text)+'</pre>';
      }else{
        body+='<p class="reference-empty">No inline preview is available for this file type. Use Download to open the original document.</p>';
      }
    }
    _openModal(ref.documentName || 'Reference document', body, 'modal-document');
  }catch(e){
    _openModal(ref.documentName || 'Reference document', '<p style="color:var(--danger)">'+escHtml(e.message||'Failed to load document.')+'</p>', 'modal-document');
  }
}
function openReferenceContent(rawRef){
  const parsed=_parseRefPayload(rawRef);
  const ref=_normalizeReference(parsed);
  if(!parsed){toast('Reference preview is unavailable','err');return;}
  const rawContent=String(ref.tableHtml||ref.htmlContent||ref.fullContent||ref.textExcerpt||'').trim();
  if(!rawContent){toast('No inline reference content is available','err');return;}
  let body='';
  if(ref.tableHtml || ref.htmlContent || _looksLikeHtmlContent(rawContent) || ref.referenceType==='table'){
    body='<div class="reference-html">'+_sanitizeHtml(rawContent)+'</div>';
  }else{
    body='<pre>'+escHtml(rawContent)+'</pre>';
  }
  _openModal(ref.documentName || 'Reference content', body, 'modal-document');
}

// ----- TASKS -----
let _taskPage=1;
let _taskPageSize=10;

async function _deleteTaskRequest(taskId){
  const response=await fetch(API+'/assessments/'+encodeURIComponent(taskId),{
    method:'DELETE',
    headers:headers()
  });
  const text=await response.text();
  let payload={};
  if(text){
    try{payload=JSON.parse(text);}catch(_e){payload={detail:text};}
  }
  if(!response.ok){
    throw new Error(payload.detail || ('Delete failed ('+response.status+')'));
  }
  return payload;
}

async function deleteTask(taskId){
  if(!taskId)return;
  if(!confirm('Delete task '+taskId.substring(0,12)+'… and all of its results, datasets, and chat resources?'))return;
  try{
    const sessionOk=await ensureActiveSession(true);
    if(!sessionOk)return;
    await _deleteTaskRequest(taskId);
    if(_detailTaskId===taskId){
      closeDetail();
      _detailTaskId='';
    }
    toast('Task deleted','ok');
    await loadTasks();
  }catch(e){
    toast(e.message||'Delete failed','err');
  }
}

async function deleteCurrentTask(){
  if(!_detailTaskId)return;
  await deleteTask(_detailTaskId);
}

async function deleteAllTasks(){
  if(!confirm('Delete all tasks shown by the assessment API and remove each task\\'s datasets, chat, and results?'))return;
  const button=document.getElementById('btn-delete-all-tasks');
  const original=button.innerHTML;
  button.disabled=true;
  button.innerHTML='Deleting…<span class="spinner"></span>';
  try{
    const sessionOk=await ensureActiveSession(true);
    if(!sessionOk)return;
    const ids=[];
    let page=1;
    let totalPages=1;
    while(page<=totalPages){
      const response=await fetch(API+'/assessments?page='+page+'&page_size=100',{headers:headers()});
      if(!response.ok){
        const text=await response.text().catch(function(){return '';});
        throw new Error(text || ('Failed to list tasks ('+response.status+')'));
      }
      const payload=await response.json();
      const tasks=payload.tasks || [];
      totalPages=payload.total_pages || 1;
      tasks.forEach(function(task){if(task && task.task_id)ids.push(task.task_id);});
      page+=1;
    }
    if(!ids.length){
      toast('No tasks to delete','ok');
      await loadTasks();
      return;
    }
    const failures=[];
    for(const taskId of ids){
      try{
        await _deleteTaskRequest(taskId);
      }catch(e){
        failures.push(taskId.substring(0,12)+'…: '+(e.message||'Delete failed'));
      }
    }
    if(_detailTaskId && ids.indexOf(_detailTaskId)>=0){
      closeDetail();
      _detailTaskId='';
    }
    await loadTasks();
    if(failures.length){
      toast('Deleted '+(ids.length-failures.length)+' task(s); '+failures.length+' failed','err');
      console.error('Delete-all failures:', failures);
    }else{
      toast('Deleted '+ids.length+' task(s)','ok');
    }
  }catch(e){
    toast(e.message||'Delete all failed','err');
  }finally{
    button.disabled=false;
    button.innerHTML=original;
  }
}

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
      const dsIds=Array.isArray(t.dataset_ids)?t.dataset_ids:[];
      const dsId=dsIds.length?dsIds[0]:'\u2014';
      const dsLabel=dsIds.length>1?(dsId.length>12?dsId.substring(0,12)+'\u2026':dsId)+' +'+(dsIds.length-1):(dsId.length>12?dsId.substring(0,12)+'\u2026':dsId);
      const chId=t.chat_id||'\u2014';
      const taskIdEsc=escAttr(t.task_id);
      const progressLabel=t.questions_processed+'/'+t.total_questions+' ('+(t.questions_succeeded||0)+' ok, '+(t.questions_failed||0)+' failed)';
      html+='<tr><td style="font-family:monospace;font-size:.82rem">'+t.task_id.substring(0,12)+'\u2026</td>'
        +'<td><span class="'+badgeClass(t.state)+'">'+t.state+'</span></td>'
        +'<td>'+t.pipeline_stage+'</td>'
        +'<td>'+progressLabel+'</td>'
        +'<td style="font-family:monospace;font-size:.78rem">'+dsLabel+'</td>'
        +'<td style="font-family:monospace;font-size:.78rem">'+(chId.length>12?chId.substring(0,12)+'\u2026':chId)+'</td>'
        +'<td style="font-size:.82rem">'+new Date(t.created_at).toLocaleString()+'</td>'
        +'<td><div style="display:flex;gap:.4rem;flex-wrap:wrap"><button class="btn btn-outline btn-sm" onclick="viewTask(\''+taskIdEsc+'\')">View</button><button class="btn btn-danger btn-sm" onclick="deleteTask(\''+taskIdEsc+'\')">Delete</button></div></td></tr>';
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
  if(document.getElementById('detail-auto-refresh-cb').checked)toggleDetailAutoRefresh();
}
function closeDetail(){
  if(_detailAutoRefreshTimer){clearInterval(_detailAutoRefreshTimer);_detailAutoRefreshTimer=null;}
  const cb=document.getElementById('detail-auto-refresh-cb');
  if(cb)cb.checked=false;
  document.getElementById('task-detail').classList.add('hidden');
}

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
    const dsIds=Array.isArray(s.dataset_ids)?s.dataset_ids:[];
    let html='<dl class="detail-grid">';
    html+='<dt>Task ID</dt><dd style="font-family:monospace">'+s.task_id+'</dd>';
    html+='<dt>State</dt><dd><span class="'+badgeClass(s.state)+'">'+s.state+'</span></dd>';
    html+='<dt>Stage</dt><dd>'+s.pipeline_stage+'</dd>';
    html+='<dt>Progress</dt><dd>'+s.questions_processed+'/'+s.total_questions+' ('+(s.questions_succeeded||0)+' succeeded, '+(s.questions_failed||0)+' failed)</dd>';
    html+='<dt>Message</dt><dd>'+(s.progress_message||'\u2014')+'</dd>';
    if(s.error)html+='<dt>Error</dt><dd style="color:var(--danger)">'+escHtml(s.error)+'</dd>';
    html+='<dt>Created</dt><dd>'+new Date(s.created_at).toLocaleString()+'</dd>';
    html+='<dt>Updated</dt><dd>'+new Date(s.updated_at).toLocaleString()+'</dd>';
    // RAGFlow resource IDs
    if(dsIds.length)html+='<dt>Dataset IDs</dt><dd style="font-family:monospace;font-size:.85rem">'+dsIds.map(escHtml).join(', ')+'</dd>';
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
    if((s.state==='failed'||s.state==='awaiting_documents')&&dsIds.length){
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
    if(d.failed_questions&&d.failed_questions.length){
      html+='<div class="result-card"><h4>Failed Questions ('+d.failed_questions.length+')</h4><ul style="margin:.4rem 0 0 1.1rem">';
      d.failed_questions.forEach(function(fq){
        html+='<li><strong>'+escHtml(String(fq.question_serial_no))+':</strong> '+escHtml(fq.question)+'<br/><span style="color:var(--danger)">'+escHtml(fq.reason||'Question processing failed')+'</span></li>';
      });
      html+='</ul></div>';
    }
    d.results.forEach(q=>{
      html+='<div class="result-card"><h4>Q'+escHtml(String(q.question_serial_no))+': '+escHtml(q.question)+'</h4>';
      if(q.status==='failed'){
        html+='<div><strong>Status:</strong> <span class="badge badge-failed">failed</span></div>';
        html+='<div style="margin-top:.3rem;color:var(--danger)"><strong>Failure reason:</strong> '+escHtml(q.failure_reason||'Question processing failed')+'</div>';
      }else{
        html+='<div><strong>Answer:</strong> <span class="badge '+(q.ai_response==='Yes'?'badge-completed':q.ai_response==='No'?'badge-failed':'badge-pending')+'">'+escHtml(q.ai_response)+'</span></div>';
        if(q.details)html+='<div style="margin-top:.3rem"><strong>Details:</strong> '+escHtml(q.details).substring(0,500)+'</div>';
      }
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
  const view=_normalizeReference(ref);
  let html='<div class="ref-card">';
  html+='<div class="ref-card-header">';
  if(view.documentType){
    const cls=view.documentType.toLowerCase();
    html+='<span class="ref-type-badge '+escAttr(cls)+'">'+escHtml(view.documentType)+'</span>';
  }
  html+='<strong>'+escHtml(view.documentName||'Unknown document')+'</strong>';
  // Meta info
  let meta=[];
  if(view.referenceType) meta.push('Reference type: '+view.referenceType);
  if(view.locationLabel) meta.push(view.locationLabel);
  if(meta.length) html+='<span class="ref-meta">'+escHtml(meta.join(' \u2022 '))+'</span>';
  html+='</div>';
  const scores=[];
  if(typeof view.score === 'number') scores.push('Similarity '+view.score.toFixed(3));
  if(typeof view.vectorScore === 'number') scores.push('Vector '+view.vectorScore.toFixed(3));
  if(typeof view.termScore === 'number') scores.push('Term '+view.termScore.toFixed(3));
  if(scores.length){
    html+='<div class="reference-score">'+escHtml(scores.join(' \u2022 '))+'</div>';
  }
  // Snippet
  if(view.textExcerpt){
    html+='<div class="ref-snippet">'+escHtml(view.textExcerpt)+'</div>';
  }
  // Links
  let links=[];
  const refPayload=escAttr(JSON.stringify(ref));
  if(view.hasInlinePreview){
    const contentLabel = view.referenceType === 'table' ? '&#128202; Table View' : '&#128196; Excerpt';
    links.push('<a href="javascript:void(0)" data-ref="'+refPayload+'" onclick="openReferenceContent(this.dataset.ref)" title="Open excerpt in modal">'+contentLabel+'</a>');
  }
  if(view.imageUrl){
    links.push('<a href="javascript:void(0)" data-ref="'+refPayload+'" onclick="openReferenceImage(this.dataset.ref)" title="View image">&#128444; Image</a>');
  }
  if(view.documentUrl){
    const docLabel = view.pageNumber!=null ? '&#128214; Open Page '+view.pageNumber : '&#128196; Open Document';
    links.push('<a href="javascript:void(0)" data-ref="'+refPayload+'" onclick="openReferenceDocument(this.dataset.ref)" title="Open document in modal">'+docLabel+'</a>');
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
  if(ACCESS_TOKEN){
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
  const reuseExistingDs=document.getElementById('single-reuse-existing-ds').checked;
  if(ds)fd.append('dataset_name',ds);
  if(cn)fd.append('chat_name',cn);
  fd.append('reuse_exisiting_dataset',reuseExistingDs?'true':'false');
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
  const reuseExistingDs=document.getElementById('sess-reuse-existing-ds').checked;
  if(ds)fd.append('dataset_name',ds);
  fd.append('reuse_exisiting_dataset',reuseExistingDs?'true':'false');
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
  const ok=await postForm(API+'/native/documents/upload',fd,'up-result','btn-upload');
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
    const authType=(d.auth_type|| (d.auth_enabled ? 'jwt' : 'disabled'));
    AUTH_MODE = authType;
    document.getElementById('hdr-info').textContent='RAGFlow: '+d.ragflow_url+' | Auth: '+authType;
    updateAuthUi();
    scheduleSessionTimers();
    if(authType === 'ldap' && (ACCESS_TOKEN || REFRESH_TOKEN)){
      await ensureActiveSession(false);
    }
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
initApiLinks();
checkHealth();
document.addEventListener('visibilitychange', function(){
  if(document.visibilityState === 'visible'){
    void ensureActiveSession(false);
  }
});
window.addEventListener('focus', function(){
  void ensureActiveSession(false);
});
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
    const res = await fetch(`${API}/native/datasets?page=${dsPage}&page_size=${dsPageSize}`, { headers: headers() });
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
    const res = await fetch(API + '/native/datasets', {
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
    const res = await fetch(`${API}/native/datasets/${dsId}/documents?page=${docPage}&page_size=${docPageSize}`, { headers: headers() });
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
    const res = await fetch(`${API}/native/datasets/${dsId}/documents`, {
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
</html>""".replace("__FAVICON_HREF__", favicon_href)
