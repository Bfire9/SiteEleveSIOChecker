#!/usr/bin/env python3
"""Portail SIO1 - serveur + interface, compatible Render.

Un seul fichier, deux rôles :

  1) Sur un PC/serveur DU RÉSEAU BTS SIO  -> il analyse les sites et
     envoie les résultats au portail en ligne (PUSH_URL + SYNC_KEY).
  2) Sur Render (hors réseau)             -> il affiche les résultats
     synchronisés à tous les visiteurs (aucune installation côté visiteur).

Local :
    python3 portail.py
    http://localhost:8000

Render :
    PORT est fourni automatiquement par Render.
    SIO1_DOMAIN peut être défini dans les variables d'environnement.
    Exemple :
        SIO1_DOMAIN=sio1.lab
"""

import json
import os
import re
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


# ============================================================
# CONFIGURATION
# ============================================================

PORT = int(os.environ.get("PORT", "8000"))

# Sur Render, tu peux définir SIO1_DOMAIN dans Environment.
# Par défaut : utilisateur.sio1.lab
SIO1_DOMAIN = os.environ.get("SIO1_DOMAIN", "sio1.lab").strip().lower()

DB_FILE = os.environ.get("DB_FILE", "sio1.db")

# Synchronisation entre l'instance du réseau BTS SIO et l'instance en ligne
SYNC_KEY = os.environ.get("SYNC_KEY", "")        # secret commun (obligatoire pour sync)
PUSH_URL = os.environ.get("PUSH_URL", "").strip()  # ex: https://mon-portail.onrender.com
PROBE = os.environ.get("SIO1_PROBE", SIO1_DOMAIN).strip().lower()  # hôte testé pour détecter le réseau

TIMEOUT = 10
MAX_BYTES = 3_000_000
MAX_BODY = 2_000_000

FETCH_SLOTS = threading.BoundedSemaphore(6)
DB_LOCK = threading.Lock()

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
PATH_RE = re.compile(r"^/(?!/)[^\s\\]*$")

SEED = []  # aucun nom en dur


# ============================================================
# BASE SQLITE
# ============================================================

def db():
    return closing(
        sqlite3.connect(
            DB_FILE,
            timeout=30,
            check_same_thread=False
        )
    )


def init_db():
    with DB_LOCK, db() as c:
        c.execute("PRAGMA journal_mode=WAL")

        fresh = not c.execute(
            "SELECT 1 FROM sqlite_master WHERE name='users'"
        ).fetchone()

        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS users(
                name TEXT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS sites(
                user TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                updated INTEGER
            );

            CREATE TABLE IF NOT EXISTS scans(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                data TEXT NOT NULL
            );
            """
        )

        if fresh:
            c.executemany(
                "INSERT OR IGNORE INTO users VALUES(?)",
                [(u,) for u in SEED]
            )

        c.commit()


# ============================================================
# UTILITAIRES
# ============================================================

def clean(v):
    return re.sub(r"[^\x20-\x7e]", "", v or "")[:100]


def site_host(user):
    return f"{user}.{SIO1_DOMAIN}"


def site_url(user, path="/"):
    return f"http://{site_host(user)}{path}"


_lan_cache = [0.0, False]


def on_lan():
    """True si CE serveur peut atteindre le réseau du BTS SIO."""
    if time.time() - _lan_cache[0] > 20:
        try:
            socket.create_connection((PROBE, 80), 2).close()
            _lan_cache[1] = True
        except OSError:
            _lan_cache[1] = False
        _lan_cache[0] = time.time()
    return _lan_cache[1]


def push_remote(act, data):
    """Envoie une écriture vers le portail en ligne (en arrière-plan)."""
    def run():
        try:
            body = json.dumps({"act": act, "data": data}).encode()
            req = urllib.request.Request(
                PUSH_URL.rstrip("/") + "/api/sync",
                data=body,
                headers={"Content-Type": "application/json",
                         "X-Sync-Key": SYNC_KEY},
            )
            urllib.request.urlopen(req, timeout=60).read()
        except Exception as e:
            print("PUSH ERROR:", e)
    threading.Thread(target=run, daemon=True).start()


def initial_push():
    with DB_LOCK, db() as c:
        users = [r[0] for r in c.execute("SELECT name FROM users ORDER BY rowid")]
        sites = [json.loads(r[0]) for r in c.execute("SELECT data FROM sites")]
    push_remote("/api/users", users)
    for st in sites:
        push_remote("/api/site", st)


# ============================================================
# REDIRECTIONS HTTP
# ============================================================

class SameHostRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        host = (urlparse(newurl).hostname or "").lower()

        if not host.endswith("." + SIO1_DOMAIN):
            raise urllib.error.URLError(
                f"redirection refusée vers {host}"
            )

        return super().redirect_request(
            req, fp, code, msg, headers, newurl
        )


OPENER = urllib.request.build_opener(SameHostRedirect)


# ============================================================
# SERVEUR HTTP
# ============================================================

class Handler(BaseHTTPRequestHandler):

    def send(self, code, body, ctype="text/plain; charset=utf-8", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")

        for k, v in (extra or {}).items():
            self.send_header(k, v)

        self.end_headers()

        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    def do_GET(self):
        url = urlparse(self.path)

        if url.path in ("/", "/index.html"):
            return self.send(
                200,
                HTML.encode("utf-8"),
                "text/html; charset=utf-8"
            )

        # ----------------------------------------------------
        # API STATE
        # ----------------------------------------------------

        if url.path == "/api/state":
            try:
                with DB_LOCK, db() as c:
                    users = [
                        r[0]
                        for r in c.execute(
                            "SELECT name FROM users ORDER BY rowid"
                        )
                    ]

                    rows = {}

                    for user, data in c.execute(
                        "SELECT user, data FROM sites"
                    ):
                        try:
                            rows[user] = json.loads(data)
                        except Exception:
                            pass

                    hist = []

                    for ts, data in c.execute(
                        "SELECT ts, data FROM scans "
                        "ORDER BY id DESC LIMIT 50"
                    ):
                        try:
                            item = json.loads(data)
                            item["ts"] = ts
                            hist.append(item)
                        except Exception:
                            pass

                return self.send(
                    200,
                    json.dumps(
                        {
                            "users": users,
                            "rows": rows,
                            "history": hist
                        },
                        ensure_ascii=False
                    ).encode("utf-8"),
                    "application/json; charset=utf-8"
                )

            except Exception as e:
                return self.send(
                    500,
                    json.dumps(
                        {"error": str(e)}
                    ).encode(),
                    "application/json"
                )

        # ----------------------------------------------------
        # FETCH
        # ----------------------------------------------------

        if url.path == "/api/lan":
            return self.send(
                200,
                json.dumps({"lan": on_lan()}).encode(),
                "application/json"
            )

        if url.path == "/fetch":
            q = parse_qs(url.query)

            name = q.get("user", [""])[0].strip().lower()
            path = q.get("path", ["/"])[0] or "/"

            if (
                not NAME_RE.match(name)
                or not PATH_RE.match(path)
                or ".." in path
            ):
                return self.send(
                    400,
                    "Requête invalide".encode("utf-8")
                )

            host = site_host(name)
            target = f"http://{host}{path}"

            if not FETCH_SLOTS.acquire(timeout=15):
                return self.send(
                    503,
                    "Serveur occupé, réessaie".encode("utf-8")
                )

            try:
                req = urllib.request.Request(
                    target,
                    headers={
                        "User-Agent": "SIO1-Portail/4.0"
                    }
                )

                with OPENER.open(
                    req,
                    timeout=TIMEOUT
                ) as r:

                    content_type = r.headers.get_content_type()

                    if content_type not in (
                        "text/html",
                        "application/xhtml+xml"
                    ):
                        return self.send(
                            415,
                            b"Pas une page HTML"
                        )

                    data = r.read(MAX_BYTES)

                    extra = {
                        "X-Up-Server": clean(
                            r.headers.get("Server")
                        ),
                        "X-Up-Powered": clean(
                            r.headers.get("X-Powered-By")
                        )
                    }

                return self.send(
                    200,
                    data,
                    "text/html; charset=utf-8",
                    extra
                )

            except urllib.error.HTTPError as e:
                return self.send(
                    e.code,
                    f"HTTP {e.code} sur {target}".encode()
                )

            except Exception as e:
                print("!", target, e)

                return self.send(
                    502,
                    f"{target} : {e}".encode()
                )

            finally:
                FETCH_SLOTS.release()

        return self.send(404, b"Not found")

    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------

    def do_POST(self):
        try:
            n = int(
                self.headers.get("Content-Length") or 0
            )
        except ValueError:
            n = 0

        if n > MAX_BODY:
            return self.send(
                413,
                b"Trop gros"
            )

        try:
            raw = self.rfile.read(n)
            data = json.loads(raw or b"null")

        except (ValueError, json.JSONDecodeError):
            return self.send(
                400,
                b"JSON invalide"
            )

        act = urlparse(self.path).path
        synced = False

        if act == "/api/sync":
            if not SYNC_KEY or self.headers.get("X-Sync-Key") != SYNC_KEY:
                return self.send(403, b"Cle invalide")
            if not isinstance(data, dict):
                return self.send(400, b"JSON invalide")
            act, data = data.get("act", ""), data.get("data")
            synced = True

        try:
            with DB_LOCK, db() as c:

                if (
                    act == "/api/users"
                    and isinstance(data, list)
                ):
                    c.execute("DELETE FROM users")

                    values = []

                    for u in data:
                        if (
                            isinstance(u, str)
                            and NAME_RE.match(u)
                        ):
                            values.append((u,))

                    c.executemany(
                        "INSERT OR IGNORE INTO users VALUES(?)",
                        values
                    )

                elif (
                    act == "/api/site"
                    and isinstance(data, dict)
                    and NAME_RE.match(
                        str(data.get("user", ""))
                    )
                ):
                    c.execute(
                        "INSERT OR REPLACE INTO sites "
                        "(user, data, updated) "
                        "VALUES(?,?,?)",
                        (
                            data["user"],
                            json.dumps(
                                data,
                                ensure_ascii=False
                            ),
                            int(time.time())
                        )
                    )

                elif act == "/api/clear":
                    c.execute("DELETE FROM sites")

                elif (
                    act == "/api/scan"
                    and isinstance(data, dict)
                ):
                    c.execute(
                        "INSERT INTO scans(ts, data) "
                        "VALUES(?,?)",
                        (
                            int(time.time()),
                            json.dumps(
                                data,
                                ensure_ascii=False
                            )
                        )
                    )

                else:
                    return self.send(
                        404,
                        b"Not found"
                    )

                c.commit()

                if PUSH_URL and not synced:
                    push_remote(act, data)

            return self.send(
                200,
                b'{"ok":true}',
                "application/json"
            )

        except Exception as e:
            print("DB ERROR:", e)

            return self.send(
                500,
                json.dumps(
                    {"error": str(e)}
                ).encode(),
                "application/json"
            )

    def log_message(self, fmt, *args):
        print("•", fmt % args)


# ============================================================
# INTERFACE WEB
# ============================================================

HTML = r"""<!DOCTYPE html>
<html lang="fr">

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>SIO1 • Portail & Extraction</title>

<style>

*{
    box-sizing:border-box;
    margin:0;
    padding:0
}

:root{
    --bg:#080b12;
    --panel:#101621;
    --panel2:#151d2b;
    --border:#263247;
    --text:#f8fafc;
    --muted:#94a3b8;
    --primary:#6366f1;
    --primary2:#8b5cf6;
    --green:#22c55e;
    --red:#ef4444;
    --orange:#f59e0b;
    --blue:#38bdf8
}

body{
    min-height:100vh;
    font-family:Inter,ui-sans-serif,system-ui,-apple-system,
    "Segoe UI",sans-serif;
    background:
        radial-gradient(
            circle at 10% 0%,
            rgba(99,102,241,.18),
            transparent 30%
        ),
        radial-gradient(
            circle at 90% 100%,
            rgba(139,92,246,.12),
            transparent 30%
        ),
        var(--bg);
    color:var(--text)
}

header{
    height:70px;
    display:flex;
    align-items:center;
    justify-content:space-between;
    padding:0 28px;
    background:rgba(8,11,18,.9);
    border-bottom:1px solid var(--border);
    backdrop-filter:blur(15px);
    position:sticky;
    top:0;
    z-index:100
}

.logo{
    display:flex;
    align-items:center;
    gap:12px;
    font-size:19px;
    font-weight:700
}

.logo-icon{
    width:38px;
    height:38px;
    display:flex;
    align-items:center;
    justify-content:center;
    border-radius:11px;
    background:linear-gradient(
        135deg,
        var(--primary),
        var(--primary2)
    );
    box-shadow:0 8px 25px rgba(99,102,241,.3)
}

.header-right{
    color:var(--muted);
    font-size:13px;
    display:flex;
    align-items:center;
    gap:8px
}

.online{
    width:8px;
    height:8px;
    border-radius:50%;
    background:var(--green);
    box-shadow:0 0 10px var(--green)
}

.layout{
    display:grid;
    grid-template-columns:300px 1fr;
    min-height:calc(100vh - 70px)
}

.sidebar{
    padding:22px;
    border-right:1px solid var(--border);
    background:rgba(16,22,33,.75);
    overflow-y:auto
}

.section-title{
    font-size:11px;
    text-transform:uppercase;
    letter-spacing:.1em;
    color:var(--muted);
    font-weight:700;
    margin-bottom:12px
}

textarea{
    width:100%;
    min-height:140px;
    resize:vertical;
    background:var(--bg);
    color:var(--text);
    border:1px solid var(--border);
    border-radius:11px;
    padding:13px;
    outline:none;
    font-family:inherit;
    font-size:13px;
    line-height:1.5;
    transition:.2s
}

textarea:focus{
    border-color:var(--primary);
    box-shadow:0 0 0 3px rgba(99,102,241,.12)
}

button{
    border:none;
    cursor:pointer;
    font-family:inherit
}

.btn{
    border-radius:9px;
    padding:10px 13px;
    font-weight:600;
    transition:.2s
}

.btn:hover{
    transform:translateY(-1px)
}

.btn-primary{
    width:100%;
    margin-top:10px;
    color:#fff;
    background:linear-gradient(
        135deg,
        var(--primary),
        var(--primary2)
    )
}

.btn-secondary{
    background:var(--panel2);
    border:1px solid var(--border);
    color:var(--text)
}

.btn-secondary:hover{
    background:#1c2738
}

.btn-danger{
    background:rgba(239,68,68,.1);
    color:#fca5a5;
    border:1px solid rgba(239,68,68,.2)
}

.btn-danger:hover{
    background:rgba(239,68,68,.18)
}

.help{
    color:var(--muted);
    font-size:11px;
    line-height:1.5;
    margin-top:8px
}

.search{
    position:relative;
    margin-top:22px
}

.search input{
    width:100%;
    height:38px;
    padding:0 12px;
    background:var(--bg);
    border:1px solid var(--border);
    border-radius:9px;
    color:var(--text);
    outline:none
}

.search input:focus{
    border-color:var(--primary)
}

.users{
    display:flex;
    flex-direction:column;
    gap:5px;
    margin-top:12px
}

.user{
    display:flex;
    align-items:center;
    justify-content:space-between;
    padding:9px;
    border:1px solid transparent;
    border-radius:9px;
    transition:.2s
}

.user:hover{
    background:var(--panel2);
    border-color:var(--border)
}

.user-left{
    display:flex;
    align-items:center;
    gap:9px;
    cursor:pointer;
    min-width:0
}

.avatar{
    width:31px;
    height:31px;
    flex-shrink:0;
    display:flex;
    align-items:center;
    justify-content:center;
    border-radius:8px;
    background:linear-gradient(
        135deg,
        rgba(99,102,241,.25),
        rgba(139,92,246,.2)
    );
    color:#a5b4fc;
    font-size:11px;
    font-weight:700
}

.user-name{
    font-size:13px;
    overflow:hidden;
    white-space:nowrap;
    text-overflow:ellipsis
}

.delete{
    width:27px;
    height:27px;
    border-radius:7px;
    color:#64748b;
    background:transparent
}

.delete:hover{
    color:#f87171;
    background:rgba(239,68,68,.1)
}

.main{
    padding:25px;
    min-width:0
}

.title{
    margin-bottom:20px
}

.title h1{
    font-size:27px;
    margin-bottom:5px
}

.title p{
    color:var(--muted);
    font-size:13px
}

.browser{
    height:420px;
    margin-top:22px;
    border:1px solid var(--border);
    border-radius:15px;
    overflow:hidden;
    background:#05070b;
    box-shadow:0 20px 50px rgba(0,0,0,.25)
}

.browser-header{
    height:48px;
    display:flex;
    align-items:center;
    gap:10px;
    padding:0 12px;
    background:#101621;
    border-bottom:1px solid var(--border)
}

.dots{
    display:flex;
    gap:5px
}

.dot{
    width:9px;
    height:9px;
    border-radius:50%
}

.red{
    background:#ef4444
}

.yellow{
    background:#eab308
}

.green{
    background:#22c55e
}

.address{
    flex:1;
    height:30px;
    display:flex;
    align-items:center;
    padding:0 10px;
    border-radius:7px;
    background:#080b12;
    border:1px solid #1e293b;
    color:#94a3b8;
    font-size:11px;
    overflow:hidden;
    text-overflow:ellipsis;
    white-space:nowrap
}

iframe{
    width:100%;
    height:calc(100% - 48px);
    display:block;
    border:none;
    background:#fff
}

.placeholder{
    height:calc(100% - 48px);
    display:flex;
    align-items:center;
    justify-content:center;
    text-align:center
}

.placeholder-inner{
    max-width:450px
}

.placeholder-icon{
    width:65px;
    height:65px;
    display:flex;
    align-items:center;
    justify-content:center;
    margin:0 auto 17px;
    border-radius:17px;
    background:rgba(99,102,241,.1);
    color:#818cf8;
    font-size:27px
}

.placeholder h2{
    margin-bottom:8px
}

.placeholder p{
    color:var(--muted);
    font-size:13px;
    line-height:1.6
}

.database{
    margin-top:22px;
    border:1px solid var(--border);
    border-radius:15px;
    background:rgba(16,22,33,.75);
    overflow:hidden
}

.database-header{
    padding:15px 17px;
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:15px;
    border-bottom:1px solid var(--border)
}

.database-header h2{
    font-size:16px
}

.database-actions{
    display:flex;
    gap:7px;
    flex-wrap:wrap;
    align-items:center
}

.db-search{
    width:220px;
    height:35px;
    padding:0 10px;
    border-radius:8px;
    border:1px solid var(--border);
    background:var(--bg);
    color:var(--text);
    outline:none
}

.table-wrapper{
    overflow-x:auto
}

table{
    width:100%;
    border-collapse:collapse;
    min-width:1200px
}

th{
    text-align:left;
    padding:11px 15px;
    color:var(--muted);
    font-size:10px;
    text-transform:uppercase;
    letter-spacing:.06em;
    background:#0c111b;
    border-bottom:1px solid var(--border)
}

td{
    padding:12px 15px;
    border-bottom:1px solid rgba(38,50,71,.55);
    font-size:12px;
    vertical-align:top
}

tr:hover td{
    background:rgba(255,255,255,.015)
}

.tag{
    display:inline-block;
    padding:4px 7px;
    margin:2px;
    border-radius:6px;
    background:rgba(99,102,241,.1);
    border:1px solid rgba(99,102,241,.15);
    color:#a5b4fc;
    font-size:10px
}

.muted{
    color:var(--muted)
}

.empty-db{
    padding:35px;
    text-align:center;
    color:var(--muted);
    font-size:13px
}

.loading{
    opacity:.65;
    pointer-events:none
}

.progress{
    flex:1;
    min-width:160px;
    height:8px;
    background:var(--panel2);
    border-radius:99px;
    overflow:hidden
}

#progressFill{
    height:100%;
    width:0;
    background:linear-gradient(
        90deg,
        var(--primary),
        var(--primary2)
    );
    transition:width .3s
}

.extract-status{
    margin:0 0 14px;
    padding:10px 12px;
    border-radius:8px;
    background:rgba(99,102,241,.08);
    border:1px solid rgba(99,102,241,.15);
    color:#a5b4fc;
    font-size:11px;
    display:none
}

.extract-status.visible{
    display:block
}

.extract-status.error{
    background:rgba(239,68,68,.08);
    border-color:rgba(239,68,68,.2);
    color:#fca5a5
}

.extract-status.success{
    background:rgba(34,197,94,.08);
    border-color:rgba(34,197,94,.2);
    color:#86efac
}

.results{
    display:grid;
    grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
    gap:10px;
    padding:0;
    margin-bottom:18px
}

.stat{
    padding:13px;
    border-radius:10px;
    background:var(--bg);
    border:1px solid var(--border)
}

.stat-label{
    color:var(--muted);
    font-size:10px;
    text-transform:uppercase;
    letter-spacing:.07em
}

.stat-number{
    font-size:23px;
    font-weight:700;
    margin-top:4px
}

.green-stat .stat-number{
    color:var(--green)
}

.blue-stat .stat-number{
    color:var(--blue)
}

.orange-stat .stat-number{
    color:var(--orange)
}

.bar{
    display:flex;
    gap:10px;
    align-items:center;
    flex-wrap:wrap;
    margin-bottom:12px
}

.bar .btn{
    width:auto;
    margin:0
}

.opts{
    display:flex;
    gap:14px;
    align-items:center;
    flex-wrap:wrap;
    margin-bottom:12px;
    color:var(--muted);
    font-size:12px
}

.opts input[type=number]{
    width:60px;
    height:28px;
    background:var(--bg);
    color:var(--text);
    border:1px solid var(--border);
    border-radius:6px;
    padding:0 6px
}

.tabs{
    display:flex;
    gap:6px;
    margin-bottom:16px;
    flex-wrap:wrap
}

.tab{
    padding:8px 14px;
    border-radius:9px;
    background:var(--panel2);
    border:1px solid var(--border);
    color:var(--muted);
    font-weight:600
}

.tab.active{
    color:#fff;
    border-color:var(--primary);
    background:rgba(99,102,241,.2)
}

.panel{
    padding:15px;
    border:1px solid var(--border);
    border-radius:15px;
    background:rgba(16,22,33,.75);
    overflow-x:auto
}

.panel table{
    min-width:0
}

.modal{
    position:fixed;
    inset:0;
    background:rgba(0,0,0,.65);
    display:none;
    align-items:center;
    justify-content:center;
    z-index:200;
    padding:20px
}

.modal.open{
    display:flex
}

.fiche{
    background:var(--panel);
    border:1px solid var(--border);
    border-radius:15px;
    padding:22px;
    max-width:640px;
    width:100%;
    max-height:85vh;
    overflow-y:auto;
    font-size:13px;
    line-height:1.6
}

.fiche h2{
    margin-bottom:4px
}

.fiche h4{
    margin:14px 0 5px;
    color:var(--muted);
    font-size:11px;
    text-transform:uppercase;
    letter-spacing:.08em
}

.ok{
    color:var(--green)
}

.warn{
    color:var(--orange)
}

td div{
    margin-bottom:4px
}

td.err{
    color:#fca5a5
}

td a{
    color:#a5b4fc;
    text-decoration:none
}

td a.name{
    color:var(--text)
}

@media(max-width:1000px){

    .layout{
        grid-template-columns:1fr
    }

    .sidebar{
        border-right:none;
        border-bottom:1px solid var(--border)
    }

    .results{
        grid-template-columns:repeat(2,1fr)
    }
}

@media(max-width:600px){

    header{
        padding:0 16px
    }

    .header-right{
        display:none
    }

    .main,
    .sidebar{
        padding:16px
    }

    .results{
        grid-template-columns:1fr
    }

    .database-header{
        align-items:stretch;
        flex-direction:column
    }

    .database-actions{
        flex-direction:column;
        align-items:stretch
    }

    .db-search{
        width:100%
    }

    .browser{
        height:400px
    }
}

</style>
</head>

<body>

<header>

<div class="logo">
    <div class="logo-icon">◈</div>

    <span>
        SIO1 • Portail

        <small style="
            color:#22c55e;
            font-size:11px;
            border:1px solid #22c55e;
            border-radius:6px;
            padding:1px 6px;
            margin-left:6px
        ">
            Render
        </small>
    </span>
</div>

<div class="header-right">
    <span class="online"></span>
    <span id="netState">Vérification…</span>
</div>

</header>

<div class="layout">

<aside class="sidebar">

<div class="section-title">
    Utilisateurs
</div>

<textarea
    id="namesInput"
    placeholder="alice&#10;bob"
></textarea>

<div class="help">
    Un nom par ligne →
    <strong>http://nom.sio1.lab</strong>
</div>

<button
    class="btn btn-primary"
    onclick="addNames()"
>
    + Ajouter les noms
</button>

<div class="search">

<input
    id="userSearch"
    placeholder="Rechercher un utilisateur..."
    oninput="renderUsers()"
>

</div>

<div
    id="users"
    class="users"
></div>

</aside>


<main class="main">

<div class="title">

<h1>
    Tableau de bord
</h1>

<p>
    Lit les pages de chaque site et résume les informations trouvées.
</p>

</div>


<div class="tabs">

<button
    class="tab active"
    data-v="Dash"
    onclick="showView('Dash')"
>
    📊 Dashboard
</button>

<button
    class="tab"
    data-v="Cmp"
    onclick="showView('Cmp')"
>
    ⚖ Comparer
</button>

<button
    class="tab"
    data-v="Hist"
    onclick="showView('Hist')"
>
    🕐 Historique
</button>

</div>


<div class="bar">

<button
    class="btn btn-primary"
    id="allBtn"
    onclick="scanAll()"
>
    ⚡ Tout chercher automatiquement
</button>

<button
    class="btn btn-danger"
    onclick="stopScan()"
>
    ■ Arrêter
</button>

<div class="progress">
    <div id="progressFill"></div>
</div>

</div>


<div class="opts">

<span>
    Pages max
    <input
        type="number"
        id="optMax"
        value="15"
        min="1"
        max="100"
    >
</span>

<span>
    Profondeur
    <input
        type="number"
        id="optDepth"
        value="3"
        min="0"
        max="10"
    >
</span>

<label>
    <input
        type="checkbox"
        id="optFollow"
        checked
    >
    Suivre les liens internes
</label>

<label>
    <input
        type="checkbox"
        id="optSkip"
        checked
    >
    Ignorer les fichiers
</label>

</div>


<div
    id="extractStatus"
    class="extract-status"
></div>


<div id="viewDash">


<div
    id="stats"
    class="results"
></div>


<section class="database">

<div class="database-header">

<h2>
    Résultats
</h2>

<div class="database-actions">

<input
    id="dbSearch"
    class="db-search"
    placeholder="Rechercher..."
    oninput="renderTable()"
>

<button
    class="btn btn-secondary"
    onclick="exportCSV()"
>
    ↓ CSV
</button>

<button
    class="btn btn-danger"
    onclick="clearTable()"
>
    Tout supprimer
</button>

</div>

</div>


<div class="table-wrapper">

<table>

<thead>

<tr>

<th>Site</th>
<th>Nom</th>
<th>Téléphone</th>
<th>E-mail</th>
<th>Adresse</th>
<th>Formation</th>
<th>Technologies</th>
<th>Réseaux</th>
<th>Diagnostic</th>

</tr>

</thead>

<tbody id="databaseBody"></tbody>

</table>

</div>

</section>


<div class="browser">

<div class="browser-header">

<div class="dots">

<span class="dot red"></span>
<span class="dot yellow"></span>
<span class="dot green"></span>

</div>


<div
    id="address"
    class="address"
>
    Clique sur un utilisateur pour prévisualiser son site
</div>


<button
    id="openButton"
    class="btn btn-secondary"
    style="display:none"
    onclick="window.open(currentUrl,'_blank','noopener')"
>
    ↗ Ouvrir
</button>

</div>


<div
    id="placeholder"
    class="placeholder"
>

<div class="placeholder-inner">

<div class="placeholder-icon">
    ◎
</div>

<p>
    Aperçu du site sélectionné
</p>

</div>

</div>


<iframe
    id="frame"
    sandbox=""
    style="display:none"
    title="Site SIO1"
></iframe>

</div>

</div>


<div
    id="viewCmp"
    class="panel"
    style="display:none"
></div>


<div
    id="viewHist"
    class="panel"
    style="display:none"
></div>

</main>


<div
    id="modal"
    class="modal"
    onclick="if(event.target===this)closeFiche()"
>

<div
    class="fiche"
    id="fiche"
></div>

</div>

</div>


<script>

const POOL = 3;

const DOMAIN = "__DOMAIN_PLACEHOLDER__";

const $ = id =>
    document.getElementById(id);


async function api(path, body) {

    try {

        const r = await fetch(
            path,
            body === undefined
                ? {}
                : {
                    method:"POST",
                    headers:{
                        "Content-Type":"application/json"
                    },
                    body:JSON.stringify(body)
                }
        );

        if (!r.ok) {
            return null;
        }

        return await r.json();

    } catch {
        return null;
    }
}


const saveUsers = () =>
    api("/api/users", users);


const esc = v =>
    String(v ?? "")
        .replace(/&/g,"&amp;")
        .replace(/</g,"&lt;")
        .replace(/>/g,"&gt;")
        .replace(/"/g,"&quot;")
        .replace(/'/g,"&#039;");


const strip = s =>
    String(s || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g,"")
        .toLowerCase();


const siteHost = u => `${u}.${DOMAIN}`;

let lanOK = false;

async function detectLan() {
    const j = await api("/api/lan");
    lanOK = !!(j && j.lan);
    $("netState").textContent = lanOK
        ? "Serveur sur le réseau BTS SIO — analyse active"
        : "Mode consultation — données synchronisées";
    $("allBtn").style.display = lanOK ? "" : "none";
    document.querySelectorAll(".opts, .progress").forEach(
        e => e.style.display = lanOK ? "" : "none"
    );
}

detectLan();
setInterval(detectLan, 20000);

let users = [];
let rows = {};
let history = [];
let selected = new Set();

let running = false;
let stopFlag = false;
let currentUrl = "";


/* ============================================================
   STATUS
============================================================ */

function setStatus(msg,type="") {

    const s = $("extractStatus");

    s.textContent = msg;

    s.className =
        "extract-status visible " +
        type;
}


function setProgress(done,total) {

    $("progressFill").style.width =
        (total ? 100 * done / total : 0) +
        "%";
}


/* ============================================================
   UTILISATEURS
============================================================ */

function addNames() {

    const names =
        $("namesInput")
            .value
            .split("\n")
            .map(x => x.trim().toLowerCase())
            .filter(x =>
                /^[a-z0-9][a-z0-9-]*$/.test(x)
            );

    users = [
        ...new Set([
            ...users,
            ...names
        ])
    ];

    saveUsers();

    $("namesInput").value = "";

    renderUsers();
}


function renderUsers() {

    const q =
        $("userSearch")
            .value
            .toLowerCase();

    const list =
        users.filter(u =>
            u.includes(q)
        );

    $("users").innerHTML =
        list.length
            ? ""
            : `<div class="empty-db">
                Aucun utilisateur.
               </div>`;

    list.forEach(u => {

        const el =
            document.createElement("div");

        el.className = "user";

        el.innerHTML = `
            <div class="user-left">

                <div class="avatar">
                    ${esc(
                        u.slice(0,2)
                        .toUpperCase()
                    )}
                </div>

                <div class="user-name">
                    ${esc(u)}
                    ${
                        rows[u]
                            ? (
                                rows[u].error
                                    ? " ✗"
                                    : " ✓"
                            )
                            : ""
                    }
                </div>

            </div>
        `;

        el.querySelector(
            ".user-left"
        ).onclick = () => {

            stopFlag = false;

            lanOK ? scanUser(u,true) : showFiche(u);

        };


        const del =
            document.createElement("button");

        del.className = "delete";

        del.textContent = "×";

        del.onclick = e => {

            e.stopPropagation();

            users =
                users.filter(
                    x => x !== u
                );

            saveUsers();

            renderUsers();

        };

        el.appendChild(del);

        $("users").appendChild(el);

    });

}


/* ============================================================
   FETCH + CRAWL
============================================================ */

async function fetchPage(name,path="/") {

    const r =
        await fetch(
            `/fetch?user=${
                encodeURIComponent(name)
            }&path=${
                encodeURIComponent(path)
            }`,
            {
                cache:"no-store"
            }
        );

    if (!r.ok) {

        throw Object.assign(
            new Error(
                (await r.text()) ||
                "HTTP " + r.status
            ),
            {
                status:r.status
            }
        );

    }

    return {
        html:await r.text(),

        headers:
            (
                r.headers.get(
                    "X-Up-Server"
                ) || ""
            ) +
            " " +
            (
                r.headers.get(
                    "X-Up-Powered"
                ) || ""
            )
    };
}


const SKIP =
    /\.(pdf|png|jpe?g|gif|svg|webp|ico|zip|rar|docx?|xlsx?|pptx?|mp[34]|css|js|json|xml|txt)$/i;


const canon = p =>
    p.replace(
        /index\.(html?|php)$/i,
        ""
    );


function getOpts() {

    const d =
        parseInt(
            $("optDepth").value
        );

    return {
        max:Math.max(
            1,
            parseInt(
                $("optMax").value
            ) || 15
        ),

        depth:isNaN(d)
            ? 3
            : Math.max(0,d),

        follow:$("optFollow").checked,

        skip:$("optSkip").checked
    };
}


function findLinks(
    html,
    name,
    path,
    skip
) {

    const doc =
        new DOMParser()
            .parseFromString(
                html,
                "text/html"
            );

    const out = [];

    doc.querySelectorAll(
        "a[href]"
    ).forEach(a => {

        try {

            const u =
                new URL(
                    a.getAttribute("href"),
                    `http://${siteHost(name)}${path}`
                );

            if (
                (
                    u.protocol === "http:"
                    ||
                    u.protocol === "https:"
                )
                &&
                u.hostname === siteHost(name)
                &&
                !(skip && SKIP.test(u.pathname))
            ) {

                out.push(
                    u.pathname +
                    u.search
                );

            }

        } catch {}

    });

    return out;
}


async function crawlSite(
    name,
    progress
) {

    const o = getOpts();

    const queue = [
        {
            path:"/",
            depth:0,
            from:""
        }
    ];

    const seen = new Set();
    const pages = [];
    const broken = [];


    while (
        queue.length &&
        pages.length < o.max &&
        !stopFlag
    ) {

        const item =
            queue.shift();

        const path = item.path;
        const depth = item.depth;
        const from = item.from;

        const key =
            canon(path);

        if (seen.has(key)) {
            continue;
        }

        seen.add(key);


        try {

            const result =
                await fetchPage(
                    name,
                    path
                );

            pages.push({
                path,
                ...result
            });

        } catch(e) {

            if (!pages.length) {
                throw e;
            }

            if (e.status !== 415) {

                broken.push({
                    path,
                    code:e.status || "ERR",
                    from
                });

            }

            continue;
        }


        progress(
            pages.length,
            path
        );


        if (
            o.follow &&
            depth < o.depth
        ) {

            const links =
                findLinks(
                    pages.at(-1).html,
                    name,
                    path,
                    o.skip
                );

            links.forEach(p => {

                if (
                    !seen.has(
                        canon(p)
                    )
                ) {

                    queue.push({
                        path:p,
                        depth:depth + 1,
                        from:path
                    });

                }

            });

        }

    }

    return {
        pages,
        broken
    };
}


/* ============================================================
   HTML → TEXTE
============================================================ */

function parsePage(html) {

    const doc =
        new DOMParser()
            .parseFromString(
                html,
                "text/html"
            );

    const title =
        doc.title || "";

    const doc0 =
        doc.cloneNode(true);

    const heads =
        [...doc.querySelectorAll(
            "h1,h2,h3"
        )]
        .map(h =>
            h.textContent
        );


    const links =
        [...doc.querySelectorAll(
            'a[href^="mailto:"],a[href^="tel:"]'
        )]
        .map(a => {

            try {

                return decodeURIComponent(
                    a.getAttribute("href")
                        .replace(
                            /^(mailto|tel):/i,
                            ""
                        )
                        .split("?")[0]
                );

            } catch {

                return "";

            }

        });


    doc.querySelectorAll(
        "script,style,noscript,template"
    ).forEach(e =>
        e.remove()
    );


    doc.querySelectorAll("br")
        .forEach(e =>
            e.replaceWith("\n")
        );


    doc.querySelectorAll(
        "p,div,li,tr,td,th,dt,dd,h1,h2,h3,h4,h5,h6,section,article,header,footer,address"
    )
    .forEach(e =>
        e.append("\n")
    );


    const text =
        (
            doc.body
                ? doc.body.textContent
                : ""
        )
        .replace(
            /[ \t\u00a0]+/g,
            " "
        )
        .replace(
            /\s*\n\s*/g,
            "\n"
        );


    const hrefs =
        [...doc0.querySelectorAll(
            "a[href]"
        )]
        .map(a =>
            a.getAttribute("href") || ""
        );


    return {
        title,
        heads,
        links,
        text,
        hrefs
    };
}


/* ============================================================
   EXTRACTION
============================================================ */

function findPhones(text) {

    const out = new Map();

    for (
        const m of text.matchAll(
            /(?<!\d)(?:\+33\s?[1-9]|0[1-9])(?:[\s.-]?\d{2}){4}(?!\d)/g
        )
    ) {

        let d =
            m[0].replace(
                /\D/g,
                ""
            );

        if (
            d.startsWith("33")
        ) {

            d =
                "0" +
                d.slice(2);

        }

        if (
            d.length === 10
        ) {

            out.set(
                d,
                d.replace(
                    /(\d{2})(?=\d)/g,
                    "$1 "
                )
            );

        }

    }

    return [
        ...out.values()
    ];
}


function findEmails(text) {

    const re =
        /[a-z0-9._%+-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)*?\.(?:com|fr|net|org|eu|io|dev|be|ch|ca|info|edu|xyz|me)/gi;

    return [
        ...new Set(
            (
                text.match(re) || []
            )
            .map(e =>
                e.toLowerCase()
            )
        )
    ];
}


const STREET =
    /\b\d{1,4}\s*(?:bis|ter)?\s*,?\s*(?:rue|avenue|av\.|boulevard|bd|chemin|route|allée|allee|impasse|place|quai|square|cours|résidence|residence|lotissement|hameau)\b/i;


function findAddress(text) {

    const lines =
        text
            .split("\n")
            .map(l => l.trim())
            .filter(Boolean);

    let best = "";
    let bestScore = -1;


    lines.forEach(
        (line,i) => {

            if (
                line.length > 200
            ) {
                return;
            }

            let s;
            let score;

            const m =
                STREET.exec(line);


            if (m) {

                s =
                    line.slice(
                        m.index
                    );

                score = 2;


                if (
                    !/\b\d{5}\b/.test(s) &&
                    /^\d{5}\b/.test(
                        lines[i + 1] || ""
                    )
                ) {

                    s +=
                        ", " +
                        lines[i + 1];

                }

            } else {

                const c =
                    /\b\d{5}\s+[A-ZÀ-Ý][\p{L}' -]{2,30}/u
                        .exec(line);

                if (!c) {
                    return;
                }

                s = c[0];
                score = 1;

            }


            if (
                /adresse|domicile/i.test(
                    line +
                    " " +
                    (lines[i - 1] || "")
                )
            ) {
                score += 3;
            }


            if (
                score > bestScore
            ) {

                best =
                    s.slice(
                        0,
                        120
                    ).trim();

                bestScore =
                    score;

            }

        }
    );


    return best;
}


const FORM =
    /\b(?:BTS|BUT|CAP|DNB|BAC)\b|\b(?:[Bb]ac|[Bb]accalauréat|[Ll]icence|[Bb]revet|[Dd]iplômes?)\b/;


function findFormations(text) {

    const seen = new Set();
    const out = [];

    for (
        const l of text.split("\n")
    ) {

        const line = l.trim();
        const k = line.toLowerCase();

        if (
            line.length < 4 ||
            line.length > 90 ||
            line.includes("©") ||
            !FORM.test(line) ||
            seen.has(k)
        ) {
            continue;
        }

        seen.add(k);
        out.push(line);
    }

    return out.slice(0,4);
}


const NAME_LINE =
    /^[A-ZÀ-ÝÇ][\p{L}'’-]+(?:\s+[A-ZÀ-ÝÇ][\p{L}'’-]+){1,3}$/u;


const BAD =
    /(?<!\p{L})(?:bts|sio|sisr|slam|bac|cv|profil|compétences?|competences?|formations?|expériences?|experiences?|contact|accueil|bienvenue|bonjour|portfolio|projets?|mon|ma|mes|site|page|lycée|lycee|stages?|diplômes?|langues?|propos|présentation|presentation|informatique|technicien|étudiant|etudiant|développeur|réseaux|reseaux|centre|école|ecole|université|services?|professionnelle?|personnels?|certification|anglais|français|francais)(?!\p{L})/iu;


function guessName(
    user,
    parsed
) {

    const u = strip(user);
    const score = new Map();


    const add = (
        raw,
        w
    ) => {

        const s =
            raw
                .replace(/\s+/g," ")
                .trim();

        if (
            s.length > 40 ||
            !NAME_LINE.test(s) ||
            BAD.test(s)
        ) {
            return;
        }

        score.set(
            s,
            (
                score.get(s) || 0
            ) +
            w +
            (
                strip(s).includes(u)
                    ? 5
                    : 0
            )
        );

    };


    parsed.forEach(
        (p,i) => {

            const w =
                i === 0
                    ? 2
                    : 1;

            p.title
                .split(/[|•\-–—:]/)
                .forEach(t =>
                    add(t,w)
                );

            p.heads.forEach(h =>
                add(h,w)
            );

            p.text
                .split("\n")
                .slice(0,12)
                .forEach(l =>
                    add(l,1)
                );

        }
    );


    const best =
        [...score]
            .sort(
                (a,b) =>
                    b[1] - a[1]
            )[0];


    return best
        ? best[0]
        : user.charAt(0).toUpperCase() +
          user.slice(1);
}


const SKILLS = [
    "Python",
    "Java",
    "JavaScript",
    "PHP",
    "SQL",
    "MySQL",
    "PostgreSQL",
    "HTML",
    "CSS",
    "C#",
    "C++",
    "Linux",
    "Windows Server",
    "Active Directory",
    "Docker",
    "Git",
    "GitHub",
    "Cisco",
    "VMware",
    "Proxmox",
    "Symfony",
    "Laravel",
    "React",
    "Node.js",
    "GLPI",
    "Zabbix",
    "pfSense",
    "VLAN",
    "TCP/IP",
    "Bootstrap"
];


const rxEsc = s =>
    s.replace(
        /[.*+?^${}()|[\]\\]/g,
        "\\$&"
    );


const TECH = [
    ["JavaScript",/javascript|\.js\b/i],
    ["CSS",/\.css\b|inline\.css/i],
    ["Python",/werkzeug|gunicorn|uvicorn|python/i],
    ["PHP",/\.php\b|php/i],
    ["WordPress",/wp-content|wp-includes/i],
    ["Bootstrap",/bootstrap/i],
    ["Tailwind",/tailwind/i],
    ["jQuery",/jquery/i],
    ["React",/react(?:\.|-dom)|data-reactroot|__next/i],
    ["Vue",/vue(?:\.|@|\/)/i],
    ["Angular",/ng-version|angular/i],
    ["GitHub Pages",/github\.io/i],
    ["Apache",/apache/i],
    ["Nginx",/nginx/i]
];


const SOCIALS = {

    GitHub:
        /^https?:\/\/(?:www\.)?github\.com\/[^/?#]+/i,

    LinkedIn:
        /^https?:\/\/(?:[a-z]+\.)?linkedin\.com/i,

    Instagram:
        /^https?:\/\/(?:www\.)?instagram\.com/i,

    Facebook:
        /^https?:\/\/(?:www\.)?facebook\.com/i,

    X:
        /^https?:\/\/(?:www\.)?(?:twitter|x)\.com/i
};


const SECT = {

    CV:
        /(^|[^a-z])cv([^a-z]|$)|curriculum/,

    Formation:
        /formation|parcours|diplom/,

    Projets:
        /projet|realisation|portfolio/,

    Contact:
        /contact/,

    Stage:
        /stage|experience/,

    "Compétences":
        /competence|skills/
};


function detectTech(pages) {

    const bag = [];


    pages.forEach(p => {

        const d =
            new DOMParser()
                .parseFromString(
                    p.html,
                    "text/html"
                );


        d.querySelectorAll(
            "[src],[href],meta[name=generator]"
        )
        .forEach(e =>
            bag.push(
                e.getAttribute("src") ||
                e.getAttribute("href") ||
                e.getAttribute("content") ||
                ""
            )
        );


        bag.push(
            p.headers || "",
            (
                p.html.match(
                    /data-reactroot|ng-version|__NEXT_DATA__/g
                ) || []
            ).join(" ")
        );


        if (
            /<script/i.test(
                p.html
            )
        ) {
            bag.push("inline.js");
        }


        if (
            /<style/i.test(
                p.html
            )
        ) {
            bag.push("inline.css");
        }

    });


    const src =
        bag.join("\n");


    return [
        "HTML",
        ...TECH
            .filter(
                ([,re]) =>
                    re.test(src)
            )
            .map(t =>
                t[0]
            )
    ];
}


function diagnose(
    pages,
    broken
) {

    const d =
        new DOMParser()
            .parseFromString(
                pages[0].html,
                "text/html"
            );

    const out = [];


    const t = (
        ok,
        yes,
        no
    ) =>
        out.push({
            ok,
            text:
                ok
                    ? yes
                    : no
        });


    t(
        !!(
            d.title || ""
        ).trim(),

        "Titre HTML présent",

        "Titre HTML absent"
    );


    t(
        !!d.querySelector(
            "meta[name=description]"
        )?.getAttribute(
            "content"
        ),

        "Meta description présente",

        "Meta description absente"
    );


    t(
        !!d.querySelector("h1"),

        "H1 présent",

        "H1 absent"
    );


    t(
        !!d.querySelector(
            "meta[name=viewport]"
        ),

        "Responsive : viewport détecté",

        "Responsive : viewport absent"
    );


    const noAlt =
        pages.reduce(
            (n,p) =>
                n +
                [
                    ...new DOMParser()
                        .parseFromString(
                            p.html,
                            "text/html"
                        )
                        .querySelectorAll(
                            "img"
                        )
                ]
                .filter(
                    i =>
                        !(i.alt || "").trim()
                )
                .length,

            0
        );


    t(
        !noAlt,

        "Toutes les images ont un attribut alt",

        `${noAlt} image(s) sans attribut alt`
    );


    t(
        !broken.length,

        "Aucun lien cassé",

        `${broken.length} lien(s) cassé(s)`
    );


    t(
        pages[0].html.length < 1e6,

        "Page d'accueil < 1 Mo",

        "Page d'accueil > 1 Mo"
    );


    return out;
}


function extractRow(
    user,
    pages,
    broken=[]
) {

    const parsed =
        pages.map(
            p =>
                parsePage(
                    p.html
                )
        );


    const text =
        parsed
            .map(
                p =>
                    p.text +
                    "\n" +
                    p.links.join("\n")
            )
            .join("\n");


    const lines =
        text
            .split("\n")
            .map(
                l =>
                    l.trim()
            )
            .filter(
                l =>
                    l &&
                    l.length <= 90
            );


    const hrefs =
        parsed.flatMap(
            p =>
                p.hrefs
        );


    const socials = {};


    for (
        const [k,re]
        of Object.entries(
            SOCIALS
        )
    ) {

        const h =
            hrefs.find(
                x =>
                    re.test(x)
            );

        if (h) {
            socials[k] = h;
        }

    }


    const sections = {};


    const projets =
        pages.filter(
            (p,i) =>
                SECT.Projets.test(
                    strip(
                        p.path +
                        " " +
                        parsed[i].title
                    )
                )
        ).length;


    for (
        const [k,re]
        of Object.entries(
            SECT
        )
    ) {

        sections[k] =
            pages.some(
                (p,i) =>
                    re.test(
                        strip(
                            p.path +
                            " " +
                            parsed[i].title
                        )
                    )
            );

    }


    const age =
        [
            ...text.matchAll(
                /\b(\d{2})\s*ans\b/g
            )
        ]
        .map(
            m =>
                +m[1]
        )
        .find(
            a =>
                a >= 15 &&
                a <= 60
        );


    return {

        user,

        url:
            `http://${siteHost(user)}`,

        pages:
            pages.length,

        name:
            guessName(
                user,
                parsed
            ),

        phones:
            findPhones(text),

        emails:
            findEmails(text),

        address:
            findAddress(text),

        formations:
            findFormations(text),

        tech:
            detectTech(pages),

        socials,

        sections,

        projets,

        stage:
            sections.Stage ||
            /\bstages?\b/i.test(
                text
            ),

        skills:
            SKILLS.filter(
                k =>
                    new RegExp(
                        `(?<![\\p{L}\\d])${rxEsc(k)}(?![\\p{L}\\d])`,
                        "iu"
                    ).test(text)
            ),

        langues:
            [
                ...new Set(
                    (
                        text.match(
                            /\b(?:anglais|espagnol|allemand|italien|arabe|portugais|chinois)\b/gi
                        ) || []
                    )
                    .map(
                        x =>
                            x.toLowerCase()
                    )
                )
            ],

        certifs:
            [
                ...new Set(
                    lines.filter(
                        l =>
                            /certification|toeic|ccna|\bpix\b|cnil/i.test(l)
                    )
                )
            ].slice(0,3),

        etab:
            lines.find(
                l =>
                    /lyc[ée]e|universit[ée]|[ée]cole|\biut\b|campus/i.test(l)
            ) || "",

        age:
            age || "",

        paths:
            pages.map(
                p =>
                    p.path
            ),

        broken,

        diag:
            diagnose(
                pages,
                broken
            ),

        size:
            pages.reduce(
                (n,p) =>
                    n +
                    p.html.length,
                0
            ),

        content:
            strip(
                [
                    ...new Set(
                        lines
                    )
                ].join(" ")
            ).slice(
                0,
                8000
            )
    };
}


/* ============================================================
   SCAN
============================================================ */

function showPreview(
    name,
    html
) {

    currentUrl =
        `http://${siteHost(name)}`;

    $("address").textContent =
        currentUrl;

    $("openButton").style.display =
        "block";

    $("placeholder").style.display =
        "none";

    const base =
        `<base href="${currentUrl}/">`;

    $("frame").style.display =
        "block";

    $("frame").srcdoc =
        /<head[^>]*>/i.test(html)
            ? html.replace(
                /<head([^>]*)>/i,
                `<head$1>${base}`
            )
            : base + html;
}


async function scanUser(
    name,
    preview=false
) {

    const t0 =
        performance.now();

    const prev =
        rows[name] &&
        !rows[name].error
            ? rows[name]
            : null;


    try {

        const {
            pages,
            broken
        } =
            await crawlSite(
                name,
                (n,p) => {

                    if (
                        running
                    ) {
                        setStatus(
                            `${name} : ${n} page(s) lue(s) — ${p}`
                        );
                    }

                }
            );


        if (!pages.length) {
            throw new Error(
                "aucune page lue"
            );
        }


        if (preview) {
            showPreview(
                name,
                pages[0].html
            );
        }


        rows[name] =
            extractRow(
                name,
                pages,
                broken
            );


        Object.assign(
            rows[name],
            {
                ms:
                    Math.round(
                        performance.now() -
                        t0
                    ),

                at:
                    Date.now()
            }
        );


        if (prev) {

            rows[name].prev = {
                pages:prev.pages,
                projets:prev.projets,
                at:prev.at
            };

        }


        if (!running) {

            setStatus(
                `${name} : ${pages.length} page(s) analysée(s).`,
                "success"
            );

        }

    } catch(e) {

        rows[name] = {

            user:name,

            url:
                `http://${siteHost(name)}`,

            error:
                "Injoignable : " +
                e.message,

            at:
                Date.now()

        };


        if (!running) {

            setStatus(
                rows[name].error,
                "error"
            );

        }

    }


    await api(
        "/api/site",
        rows[name]
    );


    renderTable();
    renderUsers();
}


async function scanAll() {

    if (running) {
        return;
    }


    running = true;
    stopFlag = false;


    const queue =
        [...users];

    const total =
        queue.length;

    let done = 0;


    $("allBtn")
        .classList
        .add("loading");


    setProgress(
        0,
        total
    );


    const worker =
        async () => {

            while (
                queue.length &&
                !stopFlag
            ) {

                const name =
                    queue.shift();

                await scanUser(
                    name
                );

                setProgress(
                    ++done,
                    total
                );

                setStatus(
                    `${done} / ${total} sites analysés...`
                );

            }

        };


    await Promise.all(
        Array.from(
            {
                length:
                    Math.min(
                        POOL,
                        Math.max(
                            total,
                            1
                        )
                    )
            },
            worker
        )
    );


    running = false;

    $("allBtn")
        .classList
        .remove("loading");


    setStatus(
        stopFlag
            ? `Arrêté : ${done} / ${total} sites.`
            : `Terminé : ${done} site(s) analysé(s).`,

        stopFlag
            ? ""
            : "success"
    );


    if (
        !stopFlag &&
        total
    ) {

        const l =
            users
                .map(
                    u =>
                        rows[u]
                )
                .filter(Boolean);


        const ok =
            l.filter(
                r =>
                    !r.error
            );


        const sum =
            f =>
                ok.reduce(
                    (n,r) =>
                        n + f(r),
                    0
                );


        const h = {

            sites:
                l.length,

            ok:
                ok.length,

            ko:
                l.length -
                ok.length,

            pages:
                sum(
                    r =>
                        r.pages
                ),

            emails:
                sum(
                    r =>
                        r.emails.length
                ),

            phones:
                sum(
                    r =>
                        r.phones.length
                )

        };


        await api(
            "/api/scan",
            h
        );


        history.unshift({
            ...h,
            ts:
                Date.now() /
                1000
        });

    }

}


function stopScan() {

    stopFlag = true;

}


/* ============================================================
   TABLEAU
============================================================ */

const cell = a => {

    a =
        (a || [])
            .filter(Boolean);


    return a.length
        ? a
            .map(
                x =>
                    `<div>${esc(x)}</div>`
            )
            .join("")
        : `<span class="muted">—</span>`;
};


const tags = a =>
    (a || []).length
        ? a
            .map(
                x =>
                    `<span class="tag">${esc(x)}</span>`
            )
            .join("")
        : `<span class="muted">—</span>`;


function toggleSel(
    u,
    on
) {

    if (on) {
        selected.add(u);
    } else {
        selected.delete(u);
    }

}


function renderStats() {

    const l =
        Object.values(rows);

    const ok =
        l.filter(
            r =>
                !r.error
        );


    const sum =
        f =>
            ok.reduce(
                (n,r) =>
                    n + f(r),
                0
            );


    const t =
        ok.filter(
            r =>
                r.ms
        );


    const cards = [

        [
            "Sites accessibles",
            ok.length,
            "green-stat"
        ],

        [
            "Inaccessibles",
            l.length -
            ok.length,
            ""
        ],

        [
            "Pages analysées",
            sum(
                r =>
                    r.pages
            ),
            "blue-stat"
        ],

        [
            "E-mails",
            sum(
                r =>
                    r.emails.length
            ),
            ""
        ],

        [
            "Téléphones",
            sum(
                r =>
                    r.phones.length
            ),
            ""
        ],

        [
            "Formations",
            sum(
                r =>
                    r.formations.length
            ),
            ""
        ],

        [
            "Temps moyen",

            t.length
                ? (
                    t.reduce(
                        (n,r) =>
                            n + r.ms,
                        0
                    )
                    /
                    t.length
                    /
                    1000
                ).toFixed(1) +
                " s"
                : "—",

            "orange-stat"
        ]

    ];


    $("stats").innerHTML =
        cards
            .map(
                ([a,b,c]) =>
                    `<div class="stat ${c}">
                        <div class="stat-label">
                            ${a}
                        </div>
                        <div class="stat-number">
                            ${b}
                        </div>
                    </div>`
            )
            .join("");
}


function renderTable() {

    renderStats();


    const q =
        strip(
            $("dbSearch")
                .value
                .trim()
        );


    const list =
        Object.values(rows)
            .sort(
                (a,b) =>
                    a.user.localeCompare(
                        b.user
                    )
            )
            .filter(
                r =>
                    strip(
                        JSON.stringify(r)
                    ).includes(q)
            );


    $("databaseBody").innerHTML =
        !list.length

            ? `
                <tr>
                    <td
                        colspan="9"
                        class="empty-db"
                    >
                        Aucune donnée.
                        Clique sur
                        « Tout chercher automatiquement ».
                    </td>
                </tr>
              `

            : list
                .map(
                    r => {

                        const site =
                            `<a
                                href="${esc(r.url)}"
                                target="_blank"
                                rel="noopener"
                            >
                                ${esc(r.user)}
                            </a>`;


                        if (r.error) {

                            return `
                                <tr>
                                    <td>
                                        ${site}
                                    </td>

                                    <td
                                        colspan="8"
                                        class="err"
                                    >
                                        ${esc(r.error)}
                                    </td>
                                </tr>
                            `;

                        }


                        const nok =
                            r.diag.filter(
                                d =>
                                    d.ok
                            ).length;


                        const nko =
                            r.diag.length -
                            nok;


                        const soc =
                            Object.entries(
                                r.socials
                            )
                            .map(
                                ([k,v]) =>
                                    `<div>
                                        <a
                                            href="${esc(v)}"
                                            target="_blank"
                                            rel="noopener"
                                        >
                                            ${esc(k)}
                                        </a>
                                    </div>`
                            )
                            .join("")
                            ||
                            `<span class="muted">—</span>`;


                        return `
                            <tr>

                                <td>

                                    <input
                                        type="checkbox"
                                        ${
                                            selected.has(
                                                r.user
                                            )
                                            ? "checked"
                                            : ""
                                        }
                                        onchange="
                                            toggleSel(
                                                '${esc(r.user)}',
                                                this.checked
                                            )
                                        "
                                    >

                                    ${site}

                                    <div class="muted">
                                        ${r.pages}
                                        page(s)
                                    </div>

                                </td>

                                <td>

                                    <a
                                        href="#"
                                        class="name"
                                        onclick="
                                            showFiche(
                                                '${esc(r.user)}'
                                            );
                                            return false
                                        "
                                    >
                                        <strong>
                                            ${esc(r.name)}
                                        </strong>
                                    </a>

                                </td>

                                <td>
                                    ${cell(r.phones)}
                                </td>

                                <td>
                                    ${cell(r.emails)}
                                </td>

                                <td>
                                    ${cell([r.address])}
                                </td>

                                <td>
                                    ${cell(r.formations)}
                                </td>

                                <td>
                                    ${tags(r.tech)}
                                </td>

                                <td>
                                    ${soc}
                                </td>

                                <td>
                                    <span class="ok">
                                        ✓ ${nok}
                                    </span>

                                    <span class="warn">
                                        ⚠ ${nko}
                                    </span>
                                </td>

                            </tr>
                        `;

                    }
                )
                .join("");
}


function clearTable() {

    if (
        !confirm(
            "Supprimer tous les résultats ?"
        )
    ) {
        return;
    }


    rows = {};

    api(
        "/api/clear",
        {}
    );

    renderTable();
    renderUsers();

    setStatus(
        "Résultats supprimés."
    );

    setProgress(
        0,
        1
    );
}


function exportCSV() {

    const list =
        Object.values(rows)
            .filter(
                r =>
                    !r.error
            )
            .sort(
                (a,b) =>
                    a.user.localeCompare(
                        b.user
                    )
            );


    if (!list.length) {

        alert(
            "Rien à exporter."
        );

        return;
    }


    const data = [

        [
            "Site",
            "Nom",
            "Téléphone",
            "E-mail",
            "Adresse",
            "Formation",
            "Technologies",
            "GitHub",
            "LinkedIn"
        ],

        ...list.map(
            r => [

                r.url,

                r.name,

                r.phones.join(
                    " | "
                ),

                r.emails.join(
                    " | "
                ),

                r.address,

                r.formations.join(
                    " | "
                ),

                r.tech.join(
                    " | "
                ),

                r.socials.GitHub ||
                "",

                r.socials.LinkedIn ||
                ""

            ]
        )

    ];


    const csv =
        data
            .map(
                r =>
                    r
                        .map(
                            v =>
                                '"' +
                                String(v)
                                    .replace(
                                        /"/g,
                                        '""'
                                    ) +
                                '"'
                        )
                        .join(";")
            )
            .join("\n");


    const a =
        document.createElement(
            "a"
        );


    a.href =
        URL.createObjectURL(
            new Blob(
                [
                    "\ufeff" +
                    csv
                ],
                {
                    type:
                        "text/csv;charset=utf-8;"
                }
            )
        );


    a.download =
        "base-sio1.csv";


    document.body.appendChild(a);

    a.click();

    a.remove();
}


/* ============================================================
   VUES
============================================================ */

function showView(v) {

    [
        "Dash",
        "Cmp",
        "Hist"
    ].forEach(
        n =>
            $("view" + n)
                .style.display =
                    n === v
                        ? ""
                        : "none"
    );


    document
        .querySelectorAll(
            ".tab"
        )
        .forEach(
            t =>
                t.classList.toggle(
                    "active",
                    t.dataset.v === v
                )
        );


    if (v === "Cmp") {
        renderCompare();
    }

    if (v === "Hist") {
        renderHistory();
    }

}


const fmtDate =
    t =>
        new Date(t)
            .toLocaleString(
                "fr-FR",
                {
                    dateStyle:"short",
                    timeStyle:"short"
                }
            );


function showFiche(u) {

    const r = rows[u];

    if (
        !r ||
        r.error
    ) {
        return;
    }


    const evo =
        (a,b) =>
            r.prev &&
            r.prev[b] !== undefined &&
            r.prev[b] !== a
                ? `${r.prev[b]} → ${a}`
                : a;


    const sec =
        (t,h) =>
            `<h4>${t}</h4><div>${h}</div>`;


    const nok =
        r.diag.filter(
            d =>
                d.ok
        ).length;


    $("fiche").innerHTML =

        `<h2>
            ${esc(r.name)}
         </h2>

         <div class="muted">
            ${
                r.age
                    ? esc(r.age) +
                      " ans · "
                    : ""
            }

            ${
                r.at
                    ? "Dernier scan : " +
                      fmtDate(r.at)
                    : ""
            }
         </div>`

        +

        sec(
            "🌐 Site",

            `<a
                href="${esc(r.url)}"
                target="_blank"
                rel="noopener"
                style="color:#a5b4fc"
            >
                ${esc(r.url)}
            </a>

            <br>

            📄
            ${evo(r.pages,"pages")}
            page(s)

            ·

            ${evo(r.projets,"projets")}
            page(s) projet`
        )

        +

        sec(
            "📞 Contact",

            cell(r.phones) +
            cell(r.emails) +
            cell([r.address])
        )

        +

        sec(
            "🎓 Formation",

            cell(r.formations) +
            (
                r.etab
                    ? `<div>
                        ${esc(r.etab)}
                       </div>`
                    : ""
            )
        )

        +

        sec(
            "💻 Technologies du site",
            tags(r.tech)
        )

        +

        sec(
            "🧠 Compétences citées",
            tags(r.skills)
        )

        +

        sec(
            "🌍 Langues / certifications",

            tags(r.langues) +
            cell(r.certifs)
        )

        +

        sec(
            "🔗 Liens",

            Object.keys(
                r.socials
            ).length

                ? Object.entries(
                    r.socials
                )
                .map(
                    ([k,v]) =>
                        `<div>
                            <a
                                href="${esc(v)}"
                                target="_blank"
                                rel="noopener"
                                style="color:#a5b4fc"
                            >
                                ${esc(k)}
                            </a>
                         </div>`
                )
                .join("")

                : "—"
        )

        +

        sec(
            "🗂 Sections",

            Object.entries(
                r.sections
            )
            .map(
                ([k,v]) =>
                    `<span
                        class="${v ? "ok" : "muted"}"
                    >
                        ${v ? "✓" : "✗"}
                        ${esc(k)}
                     </span>`
            )
            .join(" · ")
        )

        +

        sec(
            "📁 Pages découvertes",
            cell(r.paths)
        )

        +

        sec(
            `🔍 Diagnostic (${nok}/${r.diag.length} OK)`,

            r.diag
                .map(
                    d =>
                        `<div
                            class="${d.ok ? "ok" : "warn"}"
                        >
                            ${d.ok ? "✓" : "⚠"}
                            ${esc(d.text)}
                         </div>`
                )
                .join("")

            +

            (
                r.broken.length
                    ? "<br>" +
                      r.broken
                        .map(
                            b =>
                                `<div class="warn">
                                    ${esc(b.path)}
                                    →
                                    ${esc(b.code)}

                                    <span class="muted">
                                        (depuis
                                        ${esc(b.from)})
                                    </span>
                                 </div>`
                        )
                        .join("")
                    : ""
            )
        )

        +

        `<div
            style="
                margin-top:16px;
                text-align:right
            "
        >

            <button
                class="btn btn-secondary"
                onclick="closeFiche()"
            >
                Fermer
            </button>

        </div>`;


    $("modal")
        .classList
        .add("open");
}


function closeFiche() {

    $("modal")
        .classList
        .remove("open");

}


function renderCompare() {

    const list =
        Object.values(rows)
            .filter(
                r =>
                    !r.error &&
                    (
                        !selected.size ||
                        selected.has(r.user)
                    )
            )
            .sort(
                (a,b) =>
                    a.user.localeCompare(
                        b.user
                    )
            );


    if (!list.length) {

        $("viewCmp").innerHTML =
            `<div class="empty-db">
                Aucun site à comparer
                (coche des sites dans le tableau,
                ou lance une analyse).
             </div>`;

        return;
    }


    const y =
        v =>
            v
                ? `<span class="ok">✓</span>`
                : `<span class="warn">✗</span>`;


    const L = [

        [
            "Pages",
            r => r.pages
        ],

        [
            "Téléphone",
            r => y(
                r.phones.length
            )
        ],

        [
            "E-mail",
            r => y(
                r.emails.length
            )
        ],

        [
            "Adresse",
            r => y(
                r.address
            )
        ],

        [
            "Formation",
            r => y(
                r.formations.length
            )
        ],

        [
            "CV",
            r => y(
                r.sections.CV
            )
        ],

        [
            "Projets",
            r => r.projets
        ],

        [
            "Stage",
            r => y(
                r.stage
            )
        ],

        [
            "GitHub",
            r => y(
                r.socials.GitHub
            )
        ],

        [
            "LinkedIn",
            r => y(
                r.socials.LinkedIn
            )
        ],

        [
            "Technologies",
            r => r.tech.length
        ],

        [
            "Liens cassés",
            r => r.broken.length
        ]

    ];


    $("viewCmp").innerHTML =

        `<div
            class="muted"
            style="
                margin-bottom:10px;
                font-size:12px
            "
        >
            ${
                selected.size
                    ? "Sites cochés"
                    : "Tous les sites accessibles"
            }

            :
            ${list.length}
        </div>

        <table>

        <thead>

        <tr>

            <th></th>

            ${
                list
                    .map(
                        r =>
                            `<th>
                                ${esc(r.user)}
                             </th>`
                    )
                    .join("")
            }

        </tr>

        </thead>

        <tbody>

        ${
            L
                .map(
                    ([n,f]) =>
                        `<tr>

                            <td class="muted">
                                ${n}
                            </td>

                            ${
                                list
                                    .map(
                                        r =>
                                            `<td>
                                                ${f(r)}
                                             </td>`
                                    )
                                    .join("")
                            }

                         </tr>`
                )
                .join("")
        }

        </tbody>

        </table>`;
}


function renderHistory() {

    $("viewHist").innerHTML =
        history.length

            ? history
                .map(
                    h =>
                        `<div
                            style="
                                padding:10px 0;
                                border-bottom:
                                    1px solid
                                    var(--border)
                            "
                        >

                            <strong>
                                ${fmtDate(
                                    h.ts * 1000
                                )}
                            </strong>

                            <div class="muted">

                                ${h.sites}
                                sites ·

                                ${h.pages}
                                pages ·

                                <span class="ok">
                                    ${h.ok}
                                    accessibles
                                </span>

                                ·

                                ${h.ko}
                                erreur(s)

                                ·

                                ${h.emails}
                                e-mails

                                ·

                                ${h.phones}
                                téléphones

                            </div>

                         </div>`
                )
                .join("")

            :

            `<div class="empty-db">
                Aucun scan complet enregistré.
                Lance « Tout chercher automatiquement ».
             </div>`;
}


/* ============================================================
   INITIALISATION
============================================================ */

(async () => {

    const st =
        await api(
            "/api/state"
        );


    if (st) {

        users =
            st.users || [];

        rows =
            st.rows || {};

        history =
            st.history || [];

    } else {

        setStatus(
            "Base SQLite injoignable.",
            "error"
        );

    }


    renderUsers();
    renderTable();

})();

</script>

</body>
</html>
"""


# Remplace le placeholder dans le HTML par le domaine configuré.
HTML = HTML.replace(
    "__DOMAIN_PLACEHOLDER__",
    SIO1_DOMAIN
)


# ============================================================
# LANCEMENT
# ============================================================

if __name__ == "__main__":

    init_db()

    if PUSH_URL:
        initial_push()

    try:

        srv = ThreadingHTTPServer(
            ("0.0.0.0", PORT),
            Handler
        )

    except OSError as e:

        raise SystemExit(
            f"Impossible de démarrer le serveur "
            f"sur le port {PORT}: {e}"
        )


    print(
        f"Portail SIO1 : "
        f"http://0.0.0.0:{PORT}"
    )

    print(
        f"Domaine cible : "
        f"*.{SIO1_DOMAIN}"
    )

    srv.serve_forever()
