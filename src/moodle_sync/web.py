"""FastAPI web UI for Moodle Sync.

Run with: `uv run moodle-sync` (binds to 127.0.0.1:8765 by default).
"""

from __future__ import annotations

import asyncio
import json
import queue
import threading
import webbrowser
from dataclasses import asdict
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

from . import __version__
from .config import (
    Config,
    Course,
    clear_password,
    config_path,
    get_password,
    set_password,
)
from .core import LoginError, MoodleClient, Syncer

app = FastAPI(title="Moodle Sync", version=__version__)

# Single shared state — this is a single-user local tool, no need for sessions.
_state: dict = {
    "client": None,  # MoodleClient | None
    "username": None,  # str | None
    "available_courses": [],  # list[Course]
    "sync_queue": None,  # queue.Queue | None
    "sync_thread": None,  # threading.Thread | None
    "syncer": None,  # Syncer | None  (for cancel)
}


# ---------- API ----------


class LoginIn(BaseModel):
    moodle_url: str
    username: str
    password: str
    remember: bool = True


class CoursesIn(BaseModel):
    course_ids: list[str]
    download_dir: str = "moodle_documents"
    max_folder_depth: int = 5


@app.get("/api/state")
def api_state():
    cfg = Config.load()
    has_pw = bool(cfg.username and get_password(cfg.username))
    return {
        "version": __version__,
        "config_path": str(config_path()),
        "moodle_url": cfg.moodle_url,
        "username": cfg.username,
        "download_dir": cfg.download_dir,
        "max_folder_depth": cfg.max_folder_depth,
        "selected_courses": [asdict(c) for c in cfg.courses],
        "remembered_password": has_pw,
        "logged_in": _state["client"] is not None,
        "available_courses": [asdict(c) for c in _state["available_courses"]],
    }


@app.post("/api/login")
def api_login(body: LoginIn):
    client = MoodleClient(body.moodle_url)
    try:
        client.login(body.username, body.password)
    except LoginError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:  # network etc.
        raise HTTPException(status_code=502, detail=f"Connection error: {e}")

    _state["client"] = client
    _state["username"] = body.username

    # Persist non-sensitive parts immediately so URL+username are remembered.
    cfg = Config.load()
    cfg.moodle_url = body.moodle_url
    cfg.username = body.username
    cfg.save()

    if body.remember:
        set_password(body.username, body.password)
    else:
        clear_password(body.username)

    courses = client.fetch_available_courses()
    _state["available_courses"] = courses
    return {
        "ok": True,
        "courses": [asdict(c) for c in courses],
        "remembered": body.remember,
    }


@app.post("/api/login/cached")
def api_login_cached():
    cfg = Config.load()
    if not cfg.username:
        raise HTTPException(status_code=400, detail="No saved username.")
    pw = get_password(cfg.username)
    if not pw:
        raise HTTPException(status_code=404, detail="No password saved in keychain.")
    return api_login(
        LoginIn(
            moodle_url=cfg.moodle_url,
            username=cfg.username,
            password=pw,
            remember=True,
        )
    )


@app.post("/api/logout")
def api_logout(forget: bool = False):
    cfg = Config.load()
    if forget and cfg.username:
        clear_password(cfg.username)
    _state["client"] = None
    _state["username"] = None
    _state["available_courses"] = []
    return {"ok": True, "forgot_password": forget}


@app.post("/api/courses")
def api_save_courses(body: CoursesIn):
    available = _state["available_courses"]
    chosen = [c for c in available if c.id in body.course_ids]
    if not chosen and body.course_ids:
        raise HTTPException(status_code=400, detail="No matching courses.")
    cfg = Config.load()
    cfg.courses = chosen
    cfg.download_dir = body.download_dir or "moodle_documents"
    cfg.max_folder_depth = max(1, min(10, body.max_folder_depth))
    cfg.save()
    return {"ok": True, "selected": [asdict(c) for c in chosen]}


@app.post("/api/sync/start")
def api_sync_start():
    if _state["sync_thread"] and _state["sync_thread"].is_alive():
        raise HTTPException(status_code=409, detail="Sync already running.")
    client: Optional[MoodleClient] = _state["client"]
    if client is None:
        raise HTTPException(status_code=401, detail="Not logged in.")
    cfg = Config.load()
    if not cfg.courses:
        raise HTTPException(status_code=400, detail="No courses selected.")

    q: queue.Queue = queue.Queue()
    _state["sync_queue"] = q

    download_dir = Path(cfg.download_dir)
    if not download_dir.is_absolute():
        download_dir = Path.cwd() / download_dir

    def progress(event: dict) -> None:
        q.put(event)

    syncer = Syncer(
        client=client,
        download_dir=download_dir,
        max_folder_depth=cfg.max_folder_depth,
        progress=progress,
    )
    _state["syncer"] = syncer

    def run():
        try:
            syncer.sync_courses(cfg.courses)
        except Exception as e:  # noqa: BLE001
            q.put({"kind": "error", "msg": f"Sync crashed: {e}"})
        finally:
            q.put({"kind": "__done__"})

    t = threading.Thread(target=run, daemon=True)
    _state["sync_thread"] = t
    t.start()
    return {"ok": True}


@app.post("/api/sync/cancel")
def api_sync_cancel():
    syncer = _state.get("syncer")
    if syncer is not None:
        syncer.cancel()
    return {"ok": True}


@app.get("/api/sync/stream")
async def api_sync_stream():
    q: Optional[queue.Queue] = _state.get("sync_queue")
    if q is None:
        raise HTTPException(status_code=400, detail="No sync in progress.")

    async def gen():
        loop = asyncio.get_event_loop()
        while True:
            try:
                event = await loop.run_in_executor(None, q.get, True, 30)
            except queue.Empty:
                yield ": ping\n\n"
                continue
            if event.get("kind") == "__done__":
                yield "event: done\ndata: {}\n\n"
                return
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------- HTML ----------


_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>moodle/sync</title>
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,300;9..144,500;9..144,800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet" />
<style>
  :root {
    --ink: #111111;
    --paper: #f4f1ea;
    --paper-dim: #ebe6dc;
    --rule: #1a1a1a;
    --muted: #6f6a60;
    --accent: #ff4d1c;
    --accent-soft: rgba(255, 77, 28, 0.12);
    --ok: #2c6e3f;
    --warn: #b85a00;
    --err: #b00020;
    --display: 'Fraunces', 'Times New Roman', serif;
    --mono: 'JetBrains Mono', 'SF Mono', Menlo, monospace;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    background: var(--paper);
    color: var(--ink);
    font-family: var(--mono);
    font-size: 14px;
    line-height: 1.5;
    min-height: 100vh;
    background-image:
      repeating-linear-gradient(0deg, transparent 0 39px, rgba(0,0,0,0.025) 39px 40px),
      radial-gradient(ellipse at top right, rgba(255,77,28,0.06), transparent 50%);
  }
  ::selection { background: var(--accent); color: var(--paper); }

  .frame { max-width: 1080px; margin: 0 auto; padding: 40px 32px 96px; }

  /* masthead */
  .mast { border-top: 2px solid var(--rule); border-bottom: 1px solid var(--rule); padding: 14px 0 16px; display: flex; align-items: baseline; justify-content: space-between; gap: 24px; flex-wrap: wrap; }
  .mast h1 {
    font-family: var(--display);
    font-weight: 800;
    font-size: clamp(48px, 8vw, 96px);
    letter-spacing: -0.04em;
    line-height: 0.9;
    margin: 0;
    font-variation-settings: "opsz" 144;
  }
  .mast h1 em { font-style: italic; color: var(--accent); font-weight: 500; }
  .mast .meta {
    font-family: var(--mono);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--muted);
    text-align: right;
  }
  .mast .meta .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--muted); margin-right: 6px; vertical-align: middle; }
  .mast .meta.ok .dot { background: var(--ok); box-shadow: 0 0 0 3px rgba(44,110,63,0.15); }

  .deck { font-family: var(--display); font-style: italic; font-size: 18px; color: var(--muted); margin: 18px 0 32px; max-width: 60ch; }

  /* steps */
  .stepper { display: flex; gap: 0; border: 1px solid var(--rule); margin-bottom: 28px; }
  .step { flex: 1; padding: 12px 16px; border-right: 1px solid var(--rule); font-size: 11px; text-transform: uppercase; letter-spacing: 0.16em; color: var(--muted); position: relative; }
  .step:last-child { border-right: none; }
  .step .num { font-family: var(--display); font-weight: 800; font-size: 22px; display: block; color: var(--ink); margin-bottom: 2px; letter-spacing: -0.02em; }
  .step.active { background: var(--ink); color: var(--paper); }
  .step.active .num { color: var(--accent); }
  .step.done .num::after { content: " ✓"; color: var(--ok); }

  /* panels */
  .panel { border: 1px solid var(--rule); background: var(--paper); padding: 28px 32px; margin-bottom: 24px; position: relative; }
  .panel.hidden { display: none; }
  .panel > h2 { font-family: var(--display); font-weight: 500; font-style: italic; font-size: 32px; letter-spacing: -0.02em; margin: 0 0 4px; }
  .panel > h2 .idx { font-family: var(--mono); font-style: normal; font-weight: 700; font-size: 12px; color: var(--accent); letter-spacing: 0.1em; vertical-align: top; margin-right: 10px; padding: 4px 8px; border: 1px solid var(--accent); }
  .panel > .sub { color: var(--muted); margin: 0 0 24px; font-size: 13px; }

  /* form */
  .field { display: block; margin: 0 0 18px; }
  .field label { display: block; font-size: 11px; text-transform: uppercase; letter-spacing: 0.16em; color: var(--muted); margin-bottom: 6px; }
  .field input[type="text"], .field input[type="password"], .field input[type="url"], .field input[type="number"] {
    width: 100%;
    padding: 12px 14px;
    border: 1px solid var(--rule);
    background: var(--paper-dim);
    font: inherit;
    color: var(--ink);
    border-radius: 0;
  }
  .field input:focus { outline: 2px solid var(--accent); outline-offset: -2px; background: var(--paper); }
  .field .row { display: flex; gap: 12px; align-items: center; }
  .field .row > * { flex: 1; }

  .check { display: flex; align-items: center; gap: 10px; font-size: 13px; color: var(--muted); cursor: pointer; user-select: none; }
  .check input { accent-color: var(--accent); width: 16px; height: 16px; }

  .actions { display: flex; gap: 12px; align-items: center; margin-top: 20px; flex-wrap: wrap; }
  .btn {
    font-family: var(--mono);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.16em;
    padding: 12px 20px;
    border: 1px solid var(--rule);
    background: var(--ink);
    color: var(--paper);
    cursor: pointer;
    transition: transform 80ms ease, background 120ms ease;
  }
  .btn:hover { background: var(--accent); border-color: var(--accent); }
  .btn:active { transform: translateY(1px); }
  .btn[disabled] { opacity: 0.4; cursor: not-allowed; background: var(--ink); }
  .btn.ghost { background: transparent; color: var(--ink); }
  .btn.ghost:hover { background: var(--ink); color: var(--paper); }
  .btn.danger:hover { background: var(--err); border-color: var(--err); }

  .hint { font-size: 12px; color: var(--muted); }
  .hint code { font-family: var(--mono); background: var(--paper-dim); padding: 1px 5px; border: 1px solid rgba(0,0,0,0.08); }
  .err { color: var(--err); font-size: 13px; margin-top: 10px; min-height: 1em; }

  /* courses list */
  .courses { border-top: 1px solid var(--rule); margin-top: 8px; }
  .courses .row {
    display: grid; grid-template-columns: 36px 1fr auto; gap: 16px; align-items: center;
    padding: 14px 4px; border-bottom: 1px dashed rgba(0,0,0,0.18);
  }
  .courses .row:hover { background: var(--accent-soft); }
  .courses input[type="checkbox"] { accent-color: var(--accent); width: 18px; height: 18px; }
  .courses .name { font-family: var(--display); font-size: 18px; letter-spacing: -0.01em; }
  .courses .id { font-size: 11px; color: var(--muted); letter-spacing: 0.1em; }

  .toolbar { display: flex; justify-content: space-between; align-items: center; margin: 4px 0 16px; }
  .toolbar .left { display: flex; gap: 16px; }
  .link { background: none; border: none; color: var(--accent); font: inherit; cursor: pointer; padding: 0; text-decoration: underline; text-underline-offset: 3px; }
  .link:hover { color: var(--ink); }

  /* log */
  .log {
    background: #0f0f0f;
    color: #e8e3d6;
    padding: 18px 22px;
    font-family: var(--mono);
    font-size: 12.5px;
    line-height: 1.55;
    max-height: 480px;
    overflow-y: auto;
    border: 1px solid var(--rule);
    white-space: pre-wrap;
    word-break: break-word;
  }
  .log .l-section { color: #ff9b6a; font-weight: 700; margin-top: 10px; }
  .log .l-folder  { color: #c4b8e2; }
  .log .l-file_done { color: #8cd986; }
  .log .l-file_skip { color: #6f6a60; }
  .log .l-file_fail { color: #ff7d7d; }
  .log .l-error { color: #ff4d1c; font-weight: 700; }
  .log .l-info { color: #cfc7b3; }
  .log .l-course { color: var(--accent); font-weight: 700; font-size: 14px; margin-top: 14px; border-bottom: 1px solid #2a2a2a; padding-bottom: 4px; }
  .log .l-summary { color: #fff; background: #1a1a1a; padding: 8px 12px; margin-top: 14px; border-left: 3px solid var(--accent); }

  .stats { display: flex; gap: 28px; margin: 14px 0 4px; font-family: var(--display); }
  .stat { font-size: 13px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.14em; }
  .stat b { display: block; font-size: 38px; font-weight: 800; color: var(--ink); font-style: normal; letter-spacing: -0.03em; line-height: 1; margin-top: 4px; }
  .stat.ok b { color: var(--ok); }
  .stat.skip b { color: var(--muted); }
  .stat.fail b { color: var(--err); }

  /* footer */
  footer { margin-top: 48px; padding-top: 18px; border-top: 1px solid var(--rule); display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; font-size: 11px; text-transform: uppercase; letter-spacing: 0.16em; color: var(--muted); }
  footer code { font-family: var(--mono); background: var(--paper-dim); padding: 2px 6px; border: 1px solid rgba(0,0,0,0.1); text-transform: none; letter-spacing: 0; }

  @media (max-width: 600px) {
    .frame { padding: 24px 18px 64px; }
    .panel { padding: 22px 20px; }
    .stepper { flex-wrap: wrap; }
    .step { flex: 1 0 50%; border-bottom: 1px solid var(--rule); }
    .field .row { flex-direction: column; align-items: stretch; }
    .courses .row { grid-template-columns: 28px 1fr; }
    .courses .id { grid-column: 2; }
  }
</style>
</head>
<body>
<div class="frame">

  <header class="mast">
    <h1>moodle <em>/sync</em></h1>
    <div class="meta" id="meta">
      <span class="dot"></span><span id="meta-text">disconnected</span><br />
      <span style="opacity:0.6">v<span id="ver">·</span></span>
    </div>
  </header>

  <p class="deck">A small local console for pulling course materials off Moodle. Credentials live in your OS keychain — never on disk.</p>

  <nav class="stepper" id="stepper">
    <div class="step" data-step="1"><span class="num">01</span>credentials</div>
    <div class="step" data-step="2"><span class="num">02</span>courses</div>
    <div class="step" data-step="3"><span class="num">03</span>sync</div>
  </nav>

  <!-- STEP 1: LOGIN -->
  <section class="panel" id="panel-login">
    <h2><span class="idx">01</span>Sign in</h2>
    <p class="sub">Your password is stored in the system keychain (Keychain Access on macOS, Credential Manager on Windows, Secret Service on Linux). Uncheck "remember" to skip.</p>

    <div class="field">
      <label for="moodle_url">Moodle URL</label>
      <input id="moodle_url" type="url" autocomplete="off" />
    </div>
    <div class="field">
      <label for="username">Username</label>
      <input id="username" type="text" autocomplete="username" placeholder="e.g. s676767" />
    </div>
    <div class="field">
      <label for="password">Password</label>
      <input id="password" type="password" autocomplete="current-password" />
    </div>
    <label class="check"><input type="checkbox" id="remember" checked /> Remember password in keychain</label>

    <div class="actions">
      <button class="btn" id="btn-login">Sign in &amp; fetch courses</button>
      <button class="btn ghost" id="btn-cached" hidden>Use saved password</button>
      <button class="btn ghost danger" id="btn-forget" hidden>Forget saved password</button>
    </div>
    <div class="err" id="err-login"></div>
  </section>

  <!-- STEP 2: COURSES -->
  <section class="panel hidden" id="panel-courses">
    <h2><span class="idx">02</span>Pick what to sync</h2>
    <p class="sub">Found <b id="course-count">·</b> courses on your account. Choose any number — your selection is remembered between runs.</p>

    <div class="toolbar">
      <div class="left">
        <button class="link" id="select-all">select all</button>
        <button class="link" id="select-none">clear</button>
      </div>
      <span class="hint"><span id="selected-count">0</span> selected</span>
    </div>

    <div class="courses" id="courses-list"></div>

    <div class="field" style="margin-top: 24px">
      <div class="row">
        <div>
          <label for="download_dir">Download directory</label>
          <input id="download_dir" type="text" />
        </div>
        <div>
          <label for="max_depth">Max folder depth</label>
          <input id="max_depth" type="number" min="1" max="10" />
        </div>
      </div>
    </div>

    <div class="actions">
      <button class="btn" id="btn-save-sync">Save &amp; start sync</button>
      <button class="btn ghost" id="btn-back">Back</button>
    </div>
    <div class="err" id="err-courses"></div>
  </section>

  <!-- STEP 3: SYNC -->
  <section class="panel hidden" id="panel-sync">
    <h2><span class="idx">03</span>Syncing…</h2>
    <p class="sub">Live transfer log — each line is a section, folder, or file event from Moodle.</p>

    <div class="stats">
      <div class="stat ok">downloaded<b id="stat-down">0</b></div>
      <div class="stat skip">unchanged<b id="stat-skip">0</b></div>
      <div class="stat fail">failed<b id="stat-fail">0</b></div>
    </div>

    <div class="log" id="log"></div>

    <div class="actions">
      <button class="btn ghost danger" id="btn-cancel">Cancel</button>
      <button class="btn" id="btn-again" hidden>Sync again</button>
      <button class="btn ghost" id="btn-edit" hidden>Change selection</button>
    </div>
  </section>

  <footer>
    <span>config — <code id="cfgpath">·</code></span>
    <span>uv run moodle-sync</span>
  </footer>
</div>

<script>
const $ = (s) => document.querySelector(s);
const $$ = (s) => Array.from(document.querySelectorAll(s));

const ui = {
  setStep(n) {
    $$('.step').forEach((el) => {
      const v = +el.dataset.step;
      el.classList.toggle('active', v === n);
      el.classList.toggle('done', v < n);
    });
    $('#panel-login').classList.toggle('hidden', n !== 1);
    $('#panel-courses').classList.toggle('hidden', n !== 2);
    $('#panel-sync').classList.toggle('hidden', n !== 3);
  },
  setStatus(text, ok = false) {
    $('#meta-text').textContent = text;
    $('#meta').classList.toggle('ok', ok);
  },
};

let state = null;
let stats = { down: 0, skip: 0, fail: 0 };

async function api(path, opts = {}) {
  const r = await fetch(path, {
    method: opts.method || 'GET',
    headers: { 'content-type': 'application/json' },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.detail || ('HTTP ' + r.status));
  return data;
}

async function bootstrap() {
  state = await api('/api/state');
  $('#ver').textContent = state.version;
  $('#cfgpath').textContent = state.config_path;
  $('#moodle_url').value = state.moodle_url || 'https://moodle.dhbw-mannheim.de/';
  $('#username').value = state.username || '';
  $('#download_dir').value = state.download_dir || 'moodle_documents';
  $('#max_depth').value = state.max_folder_depth || 5;

  if (state.remembered_password && state.username) {
    $('#btn-cached').hidden = false;
    $('#btn-cached').textContent = `Use saved password for ${state.username}`;
    $('#btn-forget').hidden = false;
  }
  ui.setStep(1);
}

async function doLogin(useCached = false) {
  const errEl = $('#err-login');
  errEl.textContent = '';
  const btns = ['#btn-login', '#btn-cached'].map($);
  btns.forEach((b) => b && (b.disabled = true));
  ui.setStatus('connecting…');
  try {
    let res;
    if (useCached) {
      res = await api('/api/login/cached', { method: 'POST' });
    } else {
      res = await api('/api/login', {
        method: 'POST',
        body: {
          moodle_url: $('#moodle_url').value.trim(),
          username: $('#username').value.trim(),
          password: $('#password').value,
          remember: $('#remember').checked,
        },
      });
    }
    ui.setStatus('connected · ' + ($('#username').value || state.username), true);
    renderCourses(res.courses);
    state = await api('/api/state');
    ui.setStep(2);
  } catch (e) {
    errEl.textContent = e.message;
    ui.setStatus('disconnected');
  } finally {
    btns.forEach((b) => b && (b.disabled = false));
  }
}

function renderCourses(courses) {
  $('#course-count').textContent = courses.length;
  const list = $('#courses-list');
  const previouslySelected = new Set((state.selected_courses || []).map((c) => c.id));
  list.innerHTML = courses
    .map(
      (c) => `
      <label class="row">
        <input type="checkbox" value="${c.id}" ${previouslySelected.has(c.id) ? 'checked' : ''} />
        <div>
          <div class="name">${escapeHtml(c.name)}</div>
        </div>
        <div class="id">id ${c.id}</div>
      </label>`,
    )
    .join('');
  list.querySelectorAll('input').forEach((i) => i.addEventListener('change', updateSelectedCount));
  updateSelectedCount();
}

function updateSelectedCount() {
  $('#selected-count').textContent = $$('#courses-list input:checked').length;
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

async function saveAndSync() {
  const errEl = $('#err-courses');
  errEl.textContent = '';
  const ids = $$('#courses-list input:checked').map((i) => i.value);
  if (ids.length === 0) { errEl.textContent = 'Select at least one course.'; return; }
  try {
    await api('/api/courses', {
      method: 'POST',
      body: {
        course_ids: ids,
        download_dir: $('#download_dir').value.trim() || 'moodle_documents',
        max_folder_depth: parseInt($('#max_depth').value, 10) || 5,
      },
    });
    await startSync();
  } catch (e) {
    errEl.textContent = e.message;
  }
}

async function startSync() {
  ui.setStep(3);
  $('#log').innerHTML = '';
  stats = { down: 0, skip: 0, fail: 0 };
  renderStats();
  $('#btn-again').hidden = true;
  $('#btn-edit').hidden = true;
  $('#btn-cancel').disabled = false;
  appendLog({ kind: 'info', msg: '— sync starting —' });

  try {
    await api('/api/sync/start', { method: 'POST' });
  } catch (e) {
    appendLog({ kind: 'error', msg: e.message });
    finishSync();
    return;
  }

  const es = new EventSource('/api/sync/stream');
  es.onmessage = (ev) => {
    let event;
    try { event = JSON.parse(ev.data); } catch { return; }
    appendLog(event);
    if (event.kind === 'file_done') stats.down++;
    else if (event.kind === 'file_skip') stats.skip++;
    else if (event.kind === 'file_fail') stats.fail++;
    renderStats();
  };
  es.addEventListener('done', () => { es.close(); finishSync(); });
  es.onerror = () => { es.close(); finishSync(); };
}

function finishSync() {
  $('#btn-cancel').disabled = true;
  $('#btn-again').hidden = false;
  $('#btn-edit').hidden = false;
}

function renderStats() {
  $('#stat-down').textContent = stats.down;
  $('#stat-skip').textContent = stats.skip;
  $('#stat-fail').textContent = stats.fail;
}

function appendLog(event) {
  const log = $('#log');
  const line = document.createElement('div');
  line.className = 'l-' + (event.kind || 'info');
  let text;
  switch (event.kind) {
    case 'course':    text = '## ' + event.msg; break;
    case 'section':   text = '   §  ' + event.msg; break;
    case 'folder':    text = '      ' + '  '.repeat(event.depth || 0) + '└ ' + event.msg + '/'; break;
    case 'file_done': text = '         ↓  ' + event.msg; break;
    case 'file_skip': text = '         =  ' + event.msg; break;
    case 'file_fail': text = '         ×  ' + event.msg; break;
    case 'summary':   text = '✓ ' + event.msg + '  →  ' + event.dir; break;
    case 'error':     text = '!! ' + event.msg; break;
    default:          text = event.msg || JSON.stringify(event);
  }
  line.textContent = text;
  log.appendChild(line);
  log.scrollTop = log.scrollHeight;
}

// wire up
$('#btn-login').addEventListener('click', () => doLogin(false));
$('#btn-cached').addEventListener('click', () => doLogin(true));
$('#btn-forget').addEventListener('click', async () => {
  await api('/api/logout?forget=true', { method: 'POST' });
  $('#btn-cached').hidden = true;
  $('#btn-forget').hidden = true;
  $('#password').value = '';
});
$('#password').addEventListener('keydown', (e) => { if (e.key === 'Enter') doLogin(false); });
$('#select-all').addEventListener('click', () => { $$('#courses-list input').forEach((i) => (i.checked = true)); updateSelectedCount(); });
$('#select-none').addEventListener('click', () => { $$('#courses-list input').forEach((i) => (i.checked = false)); updateSelectedCount(); });
$('#btn-save-sync').addEventListener('click', saveAndSync);
$('#btn-back').addEventListener('click', () => ui.setStep(1));
$('#btn-cancel').addEventListener('click', async () => { await api('/api/sync/cancel', { method: 'POST' }); });
$('#btn-again').addEventListener('click', startSync);
$('#btn-edit').addEventListener('click', () => ui.setStep(2));

bootstrap().catch((e) => {
  document.body.innerHTML = '<pre style="padding:32px;font:14px monospace">Boot error: ' + e.message + '</pre>';
});
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_HTML)


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"ok": True, "version": __version__})


# ---------- entrypoint ----------


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="moodle-sync", description="Local web UI for Moodle Sync."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--no-browser", action="store_true", help="Don't auto-open the browser."
    )
    args = parser.parse_args()

    url = f"http://{args.host}:{args.port}/"
    print(f"\n  moodle-sync v{__version__}")
    print(f"  → {url}")
    print(f"  config: {config_path()}\n")

    if not args.no_browser:
        try:
            webbrowser.open(url, new=2)
        except Exception:
            pass

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
