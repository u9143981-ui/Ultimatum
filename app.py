import os
import re
import json
import time
import asyncio
import threading
from datetime import datetime

from flask import (Flask, Response, request, session, redirect,
                   url_for, render_template_string)
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetHistoryRequest

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
api_id         = int(os.environ.get("TG_API_ID", "29612794"))
api_hash       = os.environ.get("TG_API_HASH", "6edc1a58a202c9f6e62dc98466932bad")
bot_username   = os.environ.get("BOT_USERNAME", "DazaiappBot")
message_prefix = os.environ.get("MSG_PREFIX", "/au")

AUTHORIZED_KEY = os.environ.get("AUTHORIZED_KEY", "never")
ADMIN_KEY      = os.environ.get("ADMIN_KEY", "NEVERTHELESS")
SECRET_KEY     = os.environ.get("FLASK_SECRET", "change-me-in-render")

app = Flask(__name__)
app.secret_key = SECRET_KEY

# ---------------------------------------------------------
# IN-MEMORY STORE
# ---------------------------------------------------------
ACCOUNTS = []          
STATS = {
    "total_checks": 0,
    "success": 0,
    "failed": 0,
    "active_jobs": 0,
    "last_check": None,
    "server_started": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
}
LOCK = threading.Lock()
OTP_STATE = {}

# ⭐ FIX: Event Loop Error Fix for Python 3.10+
def run_async(coro):
    """Creates a fresh event loop for every call, fixing the MainThread loop error."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        try:
            loop.close()
        except Exception:
            pass

def pick_account():
    with LOCK:
        if not ACCOUNTS:
            return None
        best = min(ACCOUNTS, key=lambda a: a.get("usage", 0))
        best["usage"] = best.get("usage", 0) + 1
        return dict(best)

def _clean(s):
    return re.sub(r'[^\x00-\x7F]+', '', s).strip()

def _normalize_cc(cc):
    return re.sub(r'\s+', '', cc).strip()

def extract_cc_from_text(text):
    m = re.search(r'CC:\s*([0-9]{12,19})', text, re.IGNORECASE)
    return m.group(1) if m else None

def parse_bot_reply(text):
    cc_match       = re.search(r'CC:\s*(.*?)(?:\n|$)', text)
    status_match   = re.search(r'Status:\s*(.*?)(?:\n|$)', text)
    message_match  = re.search(r'Message:\s*(.*?)(?:\n|$)', text)
    response_match = re.search(r'Response:\s*(.*?)(?:\n|$)', text)
    gateway_match  = re.search(r'Gateway:\s*(.*?)(?:\n|$)', text)
    bank_match     = re.search(r'Bank:\s*(.*?)(?:\n|$)', text)
    country_match  = re.search(r'Country:\s*(.*?)(?:\n|$)', text)
    time_match     = re.search(r'Time Taken:\s*(.*?)(?:\n|$)', text)
    used_match     = re.search(r'Used By:\s*(.*?)(?:\n|$)', text)

    bank_raw = bank_match.group(1).strip() if bank_match else ""
    card_type = ""
    if " - " in bank_raw:
        bank_name, card_type = bank_raw.rsplit(" - ", 1)
        bank_name, card_type = bank_name.strip(), card_type.strip()
    else:
        bank_name = bank_raw

    status_raw  = status_match.group(1).strip() if status_match else ""
    message_raw = (message_match.group(1) if message_match
                   else (response_match.group(1) if response_match else "")).strip()
    country_raw = country_match.group(1).strip() if country_match else ""
    flag_match  = re.search(r'([\U0001F1E6-\U0001F1FF]{2})', country_raw)

    return {
        "cc":         cc_match.group(1).strip() if cc_match else "",
        "status":     _clean(status_raw),
        "response":   _clean(message_raw),
        "gateway":    _clean(gateway_match.group(1)) if gateway_match else "",
        "bank":       _clean(bank_name),
        "type":       _clean(card_type).lower(),
        "country":    _clean(country_raw).strip(" -"),
        "flag":       flag_match.group(1) if flag_match else "",
        "time_taken": _clean(time_match.group(1)) if time_match else "",
        "used_by":    _clean(used_match.group(1)) if used_match else "",
    }

def is_final_response(text):
    s = re.search(r'Status:\s*(.*?)(?:\n|$)', text)
    m = re.search(r'Message:\s*(.*?)(?:\n|$)', text)
    r = re.search(r'Response:\s*(.*?)(?:\n|$)', text)
    status  = s.group(1).strip().lower() if s else ""
    message = (m.group(1) if m else (r.group(1) if r else "")).strip().lower()
    if any(k in status for k in ('process','checking','pending','wait')): return False
    if any(k in message for k in ('process','checking','pending','wait')): return False
    return bool(status and message)

async def send_and_wait_for_cc(cc_data, session_str):
    target_cc = _normalize_cc(cc_data.split('|')[0])
    try:
        async with TelegramClient(StringSession(session_str), api_id, api_hash) as client:
            entity = await client.get_entity(f'@{bot_username}')
            message = f"{message_prefix} {cc_data}"
            sent_msg = await client.send_message(entity, message)
            sent_id  = sent_msg.id

            last_seen_id = sent_id
            processing_seen = False
            deadline = time.time() + 90

            while time.time() < deadline:
                await asyncio.sleep(2)
                history = await client(GetHistoryRequest(
                    peer=entity, limit=10, offset_date=None, offset_id=0,
                    max_id=0, min_id=0, add_offset=0, hash=0))

                if not history.messages:
                    continue

                for msg in reversed(history.messages):
                    if msg.id <= last_seen_id or msg.out:
                        continue
                    txt = msg.message or ""
                    last_seen_id = msg.id

                    reply_cc = extract_cc_from_text(txt)
                    if reply_cc and _normalize_cc(reply_cc) != target_cc:
                        continue

                    if is_final_response(txt):
                        return parse_bot_reply(txt)
                    else:
                        processing_seen = True
    except Exception as e:
        return {"error": str(e), "cc": cc_data}
    return {"error": "Timeout: No matching reply from bot.", "cc": cc_data}

# ---------------------------------------------------------
# APIs
# ---------------------------------------------------------
@app.route('/key=<key>/cc=<cc>', methods=['GET'])
def check_card(key, cc):
    if key != AUTHORIZED_KEY:
        return Response(json.dumps({"error":"Unauthorized key"}), status=403, mimetype='application/json')

    acc = pick_account()
    if not acc:
        return Response(json.dumps({"error":"No TG account. Login at /login/session"}), status=503, mimetype='application/json')

    with LOCK: STATS["active_jobs"] += 1
    try:
        result = run_async(send_and_wait_for_cc(cc, acc["session"]))
    finally:
        with LOCK: STATS["active_jobs"] -= 1

    result["account_used"] = acc["name"]
    result["requested_cc"] = cc

    with LOCK:
        STATS["total_checks"] += 1
        STATS["last_check"] = {
            "cc": cc, "status": result.get("status",""), "response": result.get("response",""),
            "account": acc["name"], "time": datetime.utcnow().strftime("%H:%M:%S UTC"),
        }
        STATS["failed" if result.get("error") else "success"] += 1

    return Response(json.dumps(result, ensure_ascii=False, indent=2), mimetype='application/json')

@app.route('/bulk-check', methods=['POST'])
def bulk_check():
    data = request.get_json(silent=True) or {}
    if data.get("key") != AUTHORIZED_KEY:
        return Response(json.dumps({"error":"Unauthorized key"}), status=403, mimetype='application/json')
    cards = data.get("cards", [])
    if not cards or not isinstance(cards, list):
        return Response(json.dumps({"error":"cards list required"}), status=400, mimetype='application/json')

    from concurrent.futures import ThreadPoolExecutor
    results = [None] * len(cards)

    def worker(idx, cc):
        acc = pick_account()
        if not acc:
            results[idx] = {"cc": cc, "error": "no account"}
            return
        with LOCK: STATS["active_jobs"] += 1
        try:
            r = run_async(send_and_wait_for_cc(cc, acc["session"]))
        finally:
            with LOCK: STATS["active_jobs"] -= 1
        r["account_used"] = acc["name"]
        r["requested_cc"] = cc
        results[idx] = r

    with ThreadPoolExecutor(max_workers=min(5, len(cards))) as ex:
        futs = [ex.submit(worker, i, c) for i, c in enumerate(cards)]
        for f in futs: f.result()

    return Response(json.dumps({"results": results}, ensure_ascii=False, indent=2), mimetype='application/json')

# ---------------------------------------------------------
# ADMIN ROUTES
# ---------------------------------------------------------
LOGIN_HTML = """<!doctype html><html><head><title>Admin Login</title>
<style>
body{font-family:system-ui;background:#0f172a;color:#e2e8f0;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
.card{background:#1e293b;padding:40px;border-radius:16px;width:340px;box-shadow:0 10px 40px rgba(0,0,0,.5)}
h2{margin:0 0 20px;color:#38bdf8}input{width:100%;padding:12px;border-radius:8px;border:1px solid #334155;background:#0f172a;color:#e2e8f0;box-sizing:border-box;margin-bottom:14px}
button{width:100%;padding:12px;background:#38bdf8;border:0;border-radius:8px;color:#0f172a;font-weight:bold;cursor:pointer}
button:hover{background:#0ea5e9}.err{color:#f87171;margin-bottom:12px;font-size:14px}
</style></head><body><div class="card"><h2>🔐 Admin Access</h2>
{% if error %}<div class="err">{{ error }}</div>{% endif %}
<form method="POST"><input type="password" name="key" placeholder="Enter admin key" autofocus required>
<button type="submit">Unlock</button></form></div></body></html>"""

DASH_HTML = """<!doctype html><html><head><title>Admin</title>
<style>
*{box-sizing:border-box}body{font-family:system-ui;background:#0f172a;color:#e2e8f0;margin:0;padding:24px}
h1{color:#38bdf8;margin:0}h2{color:#38bdf8;margin-top:0}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px}
.logout{color:#f87171;text-decoration:none;font-size:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin:20px 0}
.card{background:#1e293b;padding:18px;border-radius:12px}.card .num{font-size:28px;font-weight:bold;color:#38bdf8}
.card .lbl{color:#94a3b8;font-size:13px;margin-top:4px}.section{margin-top:28px;background:#1e293b;padding:20px;border-radius:12px}
table{width:100%;border-collapse:collapse}th,td{padding:10px;text-align:left;border-bottom:1px solid #334155;font-size:14px}
th{color:#38bdf8}input,textarea,select{width:100%;padding:10px;border-radius:8px;border:1px solid #334155;background:#0f172a;color:#e2e8f0;margin-bottom:10px;font-family:system-ui}
button{padding:10px 20px;background:#38bdf8;border:0;border-radius:8px;color:#0f172a;font-weight:bold;cursor:pointer}
button:hover{background:#0ea5e9}.btndel{background:#ef4444;color:#fff;padding:6px 12px;font-size:13px}
.flash{background:#065f46;padding:12px;border-radius:8px;margin-bottom:16px;font-size:14px}.flash.err{background:#7f1d1d}
.tabs{display:flex;gap:8px;margin-bottom:16px}.tab{padding:10px 18px;background:#334155;border-radius:8px;cursor:pointer;font-size:14px;font-weight:bold}
.tab.active{background:#38bdf8;color:#0f172a}.tabcontent{display:none}.tabcontent.active{display:block}
code{background:#0f172a;padding:2px 6px;border-radius:4px;font-size:12px}
.badge{background:#38bdf8;color:#0f172a;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold}
</style></head><body>
<div class="top"><h1>📊 Admin Dashboard</h1><a class="logout" href="/logout">Logout</a></div>
<p style="color:#94a3b8;font-size:13px">Server started: {{ stats.server_started }} — ⚠️ Restart pe accounts clear</p>
{% if msg %}<div class="flash {{ 'err' if msg_type=='err' else '' }}">{{ msg }}</div>{% endif %}
<div class="grid">
  <div class="card"><div class="num" id="s-total">{{ stats.total_checks }}</div><div class="lbl">Total Checks</div></div>
  <div class="card"><div class="num" style="color:#4ade80" id="s-success">{{ stats.success }}</div><div class="lbl">Success</div></div>
  <div class="card"><div class="num" style="color:#f87171" id="s-failed">{{ stats.failed }}</div><div class="lbl">Failed</div></div>
  <div class="card"><div class="num" style="color:#fbbf24" id="s-active">{{ stats.active_jobs }}</div><div class="lbl">Active Jobs</div></div>
  <div class="card"><div class="num" id="s-acc">{{ accounts|length }}</div><div class="lbl">TG Accounts</div></div>
</div>
<div class="section">
<h2>➕ Add Telegram Account</h2>
<div class="tabs">
  <div class="tab active" onclick="showTab(event,'otp')">📱 Phone + OTP</div>
  <div class="tab" onclick="showTab(event,'file')">📁 Upload .session</div>
</div>
<div class="tabcontent active" id="tab-otp">
  <form method="POST" action="/acc/send-otp">
    <input name="name" placeholder="Account name (e.g. acc1)" required>
    <input name="phone" placeholder="+919876543210" required>
    <button type="submit">Send OTP</button>
  </form>
</div>
<div class="tabcontent" id="tab-file">
  <form method="POST" action="/acc/upload-session" enctype="multipart/form-data">
    <input name="name" placeholder="Account name (e.g. acc1)" required>
    <input type="file" name="file" accept=".session,.txt" required>
    <button type="submit">Upload & Add</button>
  </form>
</div>
</div>
<div class="section">
<h2>📱 Accounts ({{ accounts|length }})</h2>
<table>
<tr><th>#</th><th>Name</th><th>Phone</th><th>Usage</th><th>Added</th><th>Action</th></tr>
{% for a in accounts %}
<tr>
<td>{{ loop.index }}</td>
<td>{{ a.name }} {% if loop.index == next_account_idx %}<span class="badge">NEXT</span>{% endif %}</td>
<td>{{ a.phone or '-' }}</td>
<td><b>{{ a.usage or 0 }}</b></td>
<td>{{ a.added_at }}</td>
<td><form method="POST" action="/acc/delete/{{ a.id }}" style="margin:0">
<button class="btndel" onclick="return confirm('Delete?')">Delete</button></form></td>
</tr>
{% else %}<tr><td colspan="6" style="text-align:center;color:#94a3b8;padding:24px">Koi account nahi.</td></tr>{% endfor %}
</table>
</div>
<div class="section">
<h2>🕒 Last Check</h2>
{% if stats.last_check %}
<table><tr><th>CC</th><th>Status</th><th>Response</th><th>Account</th><th>Time</th></tr>
<tr><td>{{ stats.last_check.cc }}</td><td>{{ stats.last_check.status }}</td><td>{{ stats.last_check.response }}</td>
<td>{{ stats.last_check.account }}</td><td>{{ stats.last_check.time }}</td></tr></table>
{% else %}<p style="color:#94a3b8">No checks yet.</p>{% endif %}
</div>
<script>
function showTab(e, t){
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.tabcontent').forEach(x=>x.classList.remove('active'));
  e.target.classList.add('active'); document.getElementById('tab-'+t).classList.add('active');
}
setInterval(async ()=>{try{const r=await fetch('/api/stats');const d=await r.json();
document.getElementById('s-total').textContent=d.total_checks;document.getElementById('s-success').textContent=d.success;
document.getElementById('s-failed').textContent=d.failed;document.getElementById('s-active').textContent=d.active_jobs;
document.getElementById('s-acc').textContent=d.accounts;}catch(e){}}, 3000);
</script></body></html>"""

OTP_HTML = """<!doctype html><html><head><title>Verify OTP</title>
<style>
body{font-family:system-ui;background:#0f172a;color:#e2e8f0;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
.card{background:#1e293b;padding:40px;border-radius:16px;width:380px}h2{margin:0 0 8px;color:#38bdf8}p{color:#94a3b8;font-size:13px;margin:0 0 16px}
input{width:100%;padding:12px;border-radius:8px;border:1px solid #334155;background:#0f172a;color:#e2e8f0;box-sizing:border-box;margin-bottom:14px}
button{width:100%;padding:12px;background:#38bdf8;border:0;border-radius:8px;color:#0f172a;font-weight:bold;cursor:pointer}
.err{color:#f87171;margin-bottom:12px;font-size:14px}a{color:#38bdf8;font-size:13px;text-decoration:none}
</style></head><body><div class="card">
<h2>📲 Enter OTP</h2><p>Phone: <b>{{ phone }}</b></p>
{% if error %}<div class="err">{{ error }}</div>{% endif %}
<form method="POST" action="/acc/verify-otp"><input type="hidden" name="phone" value="{{ phone }}">
<input name="otp" placeholder="Enter OTP" autofocus required><button type="submit">Verify</button></form>
<br><a href="/dashboard">← Cancel</a></div></body></html>"""

PWD_HTML = """<!doctype html><html><head><title>2FA Password</title>
<style>
body{font-family:system-ui;background:#0f172a;color:#e2e8f0;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
.card{background:#1e293b;padding:40px;border-radius:16px;width:380px}h2{margin:0 0 8px;color:#38bdf8}p{color:#94a3b8;font-size:13px;margin:0 0 16px}
input{width:100%;padding:12px;border-radius:8px;border:1px solid #334155;background:#0f172a;color:#e2e8f0;box-sizing:border-box;margin-bottom:14px}
button{width:100%;padding:12px;background:#38bdf8;border:0;border-radius:8px;color:#0f172a;font-weight:bold;cursor:pointer}
.err{color:#f87171;margin-bottom:12px;font-size:14px}a{color:#38bdf8;font-size:13px;text-decoration:none}
</style></head><body><div class="card">
<h2>🔐 2FA Password</h2><p>Phone: <b>{{ phone }}</b> — Two-step verification enabled</p>
{% if error %}<div class="err">{{ error }}</div>{% endif %}
<form method="POST" action="/acc/verify-pwd"><input type="hidden" name="phone" value="{{ phone }}">
<input type="password" name="pwd" placeholder="Enter 2FA password" autofocus required><button type="submit">Login</button></form>
<br><a href="/dashboard">← Cancel</a></div></body></html>"""

def logged_in():
    return session.get("admin") is True

@app.route('/login/session', methods=['GET','POST'])
def login_session():
    if logged_in(): return redirect(url_for('dashboard'))
    err = None
    if request.method == 'POST':
        if request.form.get('key','') == ADMIN_KEY:
            session['admin'] = True
            return redirect(url_for('dashboard'))
        err = "Invalid admin key"
    return render_template_string(LOGIN_HTML, error=err)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_session'))

@app.route('/')
def root():
    return redirect(url_for('login_session'))

@app.route('/dashboard')
def dashboard():
    if not logged_in(): return redirect(url_for('login_session'))
    with LOCK:
        accs = [dict(a) for a in ACCOUNTS]
        next_idx = None
        if accs:
            best = min(accs, key=lambda a: a.get("usage",0))
            next_idx = accs.index(best) + 1
    return render_template_string(DASH_HTML, stats=STATS, accounts=accs,
                                  next_account_idx=next_idx,
                                  msg=request.args.get('msg'), msg_type=request.args.get('t','ok'))

# ---------------- OTP FLOW ----------------
@app.route('/acc/send-otp', methods=['POST'])
def acc_send_otp():
    if not logged_in(): return redirect(url_for('login_session'))
    name  = request.form.get('name','').strip()
    phone = request.form.get('phone','').strip()
    if not name or not phone:
        return redirect(url_for('dashboard', msg="Name + phone required", t='err'))
    try:
        async def _send():
            c = TelegramClient(StringSession(), api_id, api_hash)
            await c.connect()
            sent = await c.send_code_request(phone)
            return c.session.save(), sent.phone_code_hash

        # ⭐ Error fix: run_async handles loop properly
        sess_str, phone_hash = run_async(_send())
        
        # Save session string so OTP verify can use it
        OTP_STATE[phone] = {"sess_str": sess_str, "hash": phone_hash, "name": name}
        return render_template_string(OTP_HTML, phone=phone, error=None)
    except Exception as e:
        return redirect(url_for('dashboard', msg=f"Send OTP failed: {e}", t='err'))

@app.route('/acc/verify-otp', methods=['POST'])
def acc_verify_otp():
    if not logged_in(): return redirect(url_for('login_session'))
    phone = request.form.get('phone','').strip()
    otp   = request.form.get('otp','').strip()
    st    = OTP_STATE.get(phone)
    if not st: return redirect(url_for('dashboard', msg="OTP state lost", t='err'))
    try:
        async def _verify():
            c = TelegramClient(StringSession(st["sess_str"]), api_id, api_hash)
            await c.connect()
            try:
                await c.sign_in(phone=phone, code=otp, phone_code_hash=st["hash"])
            except Exception as e:
                if "password" in str(e).lower() or "SessionPasswordNeeded" in str(e):
                    return "need_pwd"
                raise
            s = c.session.save()
            await c.disconnect()
            return s

        res = run_async(_verify())
        if res == "need_pwd":
            return render_template_string(PWD_HTML, phone=phone, error=None)
        
        with LOCK:
            new_id = max([a["id"] for a in ACCOUNTS], default=0) + 1
            ACCOUNTS.append({"id": new_id, "name": st["name"], "phone": phone,
                             "session": res, "usage": 0,
                             "added_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")})
        OTP_STATE.pop(phone, None)
        return redirect(url_for('dashboard', msg=f"✅ '{st['name']}' added"))
    except Exception as e:
        return render_template_string(OTP_HTML, phone=phone, error=str(e))

@app.route('/acc/verify-pwd', methods=['POST'])
def acc_verify_pwd():
    if not logged_in(): return redirect(url_for('login_session'))
    phone = request.form.get('phone','').strip()
    pwd   = request.form.get('pwd','')
    st    = OTP_STATE.get(phone)
    if not st: return redirect(url_for('dashboard', msg="State lost", t='err'))
    try:
        async def _do():
            c = TelegramClient(StringSession(st["sess_str"]), api_id, api_hash)
            await c.connect()
            await c.sign_in(password=pwd)
            s = c.session.save()
            await c.disconnect()
            return s
        
        sess = run_async(_do())
        with LOCK:
            new_id = max([a["id"] for a in ACCOUNTS], default=0) + 1
            ACCOUNTS.append({"id": new_id, "name": st["name"], "phone": phone,
                             "session": sess, "usage": 0,
                             "added_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")})
        OTP_STATE.pop(phone, None)
        return redirect(url_for('dashboard', msg=f"✅ '{st['name']}' added"))
    except Exception as e:
        return render_template_string(PWD_HTML, phone=phone, error=str(e))

# ---------------- FILE UPLOAD FLOW ----------------
@app.route('/acc/upload-session', methods=['POST'])
def acc_upload_session():
    if not logged_in(): return redirect(url_for('login_session'))
    name = request.form.get('name','').strip()
    f = request.files.get('file')
    if not name or not f:
        return redirect(url_for('dashboard', msg="Name + file required", t='err'))
    
    try:
        raw = f.read()
        # First try as text string
        try:
            txt = raw.decode('utf-8', errors='ignore').strip()
            if txt.startswith("1") and len(txt) > 100:
                sess_str = txt
                
                # ⭐ ADD TIMEOUT to prevent page hanging
                async def _chk():
                    c = TelegramClient(StringSession(sess_str), api_id, api_hash)
                    try:
                        await asyncio.wait_for(c.connect(), timeout=15.0)
                        if not await c.is_user_authorized(): 
                            await c.disconnect()
                            return None
                        me = await c.get_me()
                        await c.disconnect()
                        return me.phone if me else "?"
                    except asyncio.TimeoutError:
                        return "timeout"
                    except Exception:
                        return None
                
                phone = run_async(_chk())
                if phone == "timeout": return redirect(url_for('dashboard', msg="Timeout connecting to Telegram. File might be expired.", t='err'))
                if not phone: return redirect(url_for('dashboard', msg="Session not authorized / expired.", t='err'))
            else:
                raise ValueError("not string")
        except Exception:
            # Try as SQLite file
            import tempfile, shutil
            tmpdir = tempfile.mkdtemp()
            path = os.path.join(tmpdir, "uploaded.session")
            with open(path, "wb") as out: out.write(raw)
            
            async def _conv():
                c = TelegramClient(path, api_id, api_hash)
                try:
                    await asyncio.wait_for(c.connect(), timeout=15.0)
                    if not await c.is_user_authorized():
                        await c.disconnect()
                        return None, None
                    me = await c.get_me()
                    s = c.session.save()
                    await c.disconnect()
                    return s, (me.phone if me else "?")
                except asyncio.TimeoutError:
                    return "timeout", None
                except Exception:
                    return None, None
            
            sess_str, phone = run_async(_conv())
            shutil.rmtree(tmpdir, ignore_errors=True)
            
            if sess_str == "timeout": return redirect(url_for('dashboard', msg="Timeout connecting to Telegram (SQLite file).", t='err'))
            if not sess_str: return redirect(url_for('dashboard', msg="SQLite session invalid / expired.", t='err'))

        with LOCK:
            new_id = max([a["id"] for a in ACCOUNTS], default=0) + 1
            ACCOUNTS.append({"id": new_id, "name": name, "phone": phone, "session": sess_str, "usage": 0,
                             "added_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")})
        return redirect(url_for('dashboard', msg=f"✅ '{name}' added"))
    except Exception as e:
        return redirect(url_for('dashboard', msg=f"Upload failed: {e}", t='err'))

@app.route('/acc/delete/<int:aid>', methods=['POST'])
def acc_delete(aid):
    if not logged_in(): return redirect(url_for('login_session'))
    with LOCK:
        for i,a in enumerate(ACCOUNTS):
            if a["id"] == aid: del ACCOUNTS[i]; break
    return redirect(url_for('dashboard', msg="Account deleted"))

@app.route('/api/stats')
def api_stats():
    if not logged_in(): return Response(json.dumps({"error":"unauthorized"}), status=401, mimetype='application/json')
    with LOCK:
        return Response(json.dumps({**STATS, "accounts": len(ACCOUNTS),
            "account_usage": [{"name":a["name"],"usage":a.get("usage",0)} for a in ACCOUNTS]}), mimetype='application/json')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 3333))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
