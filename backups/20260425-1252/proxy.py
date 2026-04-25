#!/usr/bin/env python3
"""Options Pro Ultra v6 Proxy — Port 5010"""
import csv, http.server, json, os, ssl, time, urllib.request, urllib.parse, urllib.error
from pathlib import Path
from datetime import datetime

PORT = 5010
BASE = Path("/Applications/OptionsPro.app/Contents/Resources")
HTML = BASE / "options_pro_ultra.html"
KEY_FILE = BASE / "api_key.txt"
DATA_DIR = Path.home() / "options-pro" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def data_file(name):
    path = DATA_DIR / name
    old_path = Path.home() / "Desktop" / name
    if not path.exists() and old_path.exists():
        try:
            path.write_bytes(old_path.read_bytes())
        except Exception:
            pass
    return path

JOURNAL = data_file("autotrade_journal.csv")
LIVE_POSITIONS_FILE = data_file("live_positions.json")
SIGNALS_FILE = data_file("trade_signals.json")
SIGNAL_ONLY_FILE = data_file("optionspro_signal_only")
SIGNAL_MODE_RATE_FILE = Path("/tmp/optionspro_signal_mode_last_change")
SIGNAL_MODE_RATE_SECONDS = 60
SIGNALS_STALE_SECONDS = 4 * 60 * 60
REAL_RULES_FILE = data_file("real_account_rules.json")
MIRROR_KILL_FILE = data_file("optionspro_real_mirror_kill")
SIGNAL_AUDIT_FILE = data_file("signal_audit.json")
SIGNAL_AUDIT_EVENTS_FILE = data_file("signal_audit_events.jsonl")
PAPER_CLOSE_REQUESTS_FILE = data_file("paper_close_requests.jsonl")
PAPER_CLOSE_RESULTS_FILE = data_file("paper_close_results.jsonl")
MIRROR_COOLDOWN_SECONDS = 24 * 60 * 60
FILLED_STATUSES = {"filled", "closed", "manualclose"}

def now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")

DEFAULT_REAL_RULES = {
    "enabled": False,
    "capital": 0,
    "max_risk_per_trade_pct": 1.0,
    "max_risk_per_trade_dollars": 0,
    "max_open_positions": 3,
    "max_total_risk_pct": 10.0,
    "allowed_tickers": ["SPY", "QQQ", "IWM", "GLD", "SMH", "XLE", "TLT", "GDX", "XSP", "QQQM"],
    "allowed_strategies": ["BPS", "CSP"],
    "manual_submit_required": True,
    "notes": [
        "Copy-to-real is a manual workflow only. Verify live bid/ask in TWS.",
        "Do not submit around earnings, major news, or stale signal files.",
        "Use real account sizing rules, not paper account size."
    ],
}

def get_key():
    try: return KEY_FILE.read_text().strip()
    except: return ""

def load_real_rules():
    rules = dict(DEFAULT_REAL_RULES)
    try:
        if REAL_RULES_FILE.exists():
            saved = json.loads(REAL_RULES_FILE.read_text())
            if isinstance(saved, dict):
                rules.update(saved)
    except Exception:
        pass
    try:
        rules["capital"] = float(rules.get("capital") or 0)
    except Exception:
        rules["capital"] = 0
    try:
        rules["max_risk_per_trade_pct"] = float(rules.get("max_risk_per_trade_pct") or 0)
    except Exception:
        rules["max_risk_per_trade_pct"] = 0
    try:
        rules["max_risk_per_trade_dollars"] = float(rules.get("max_risk_per_trade_dollars") or 0)
    except Exception:
        rules["max_risk_per_trade_dollars"] = 0
    return rules

def save_real_rules(rules):
    merged = dict(DEFAULT_REAL_RULES)
    if isinstance(rules, dict):
        merged.update(rules)
    REAL_RULES_FILE.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n")
    try:
        os.chmod(REAL_RULES_FILE, 0o600)
    except Exception:
        pass
    return load_real_rules()

def _parse_ts(value):
    if not value:
        return None
    text = str(value).strip().replace("T", " ")
    for fmt, width in (
        ("%Y-%m-%d %H:%M:%S", 19),
        ("%Y-%m-%d %H:%M", 16),
        ("%m-%d %H:%M", 11),
        ("%Y/%m/%d %H:%M:%S", 19),
    ):
        try:
            dt = datetime.strptime(text[:width], fmt)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt
        except Exception:
            pass
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def closed_trade_pnls():
    closes = []
    if not JOURNAL.exists():
        return closes
    try:
        with open(JOURNAL, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                action = (row.get("action") or "").upper()
                status = (row.get("status") or "").lower()
                if not action.startswith("CLOSE") or action == "CLOSE_CANCEL":
                    continue
                if status not in FILLED_STATUSES:
                    continue
                try:
                    pnl = float(row.get("pnl") or 0)
                except Exception:
                    continue
                closes.append({
                    "timestamp": row.get("timestamp", ""),
                    "ticker": row.get("ticker", ""),
                    "pnl": pnl,
                    "dt": _parse_ts(row.get("timestamp", "")),
                })
    except Exception:
        return []
    return sorted(closes, key=lambda x: x.get("dt") or datetime.min)

def mirror_cooldown_state():
    closes = closed_trade_pnls()
    if len(closes) < 2:
        return {"active": False, "until": None, "last_two": closes[-2:]}
    last_two = closes[-2:]
    if not all(float(t.get("pnl") or 0) < 0 for t in last_two):
        return {"active": False, "until": None, "last_two": last_two}
    last_dt = last_two[-1].get("dt")
    if not last_dt:
        return {"active": True, "until": None, "last_two": last_two}
    until_ts = last_dt.timestamp() + MIRROR_COOLDOWN_SECONDS
    active = time.time() < until_ts
    return {
        "active": active,
        "until": datetime.fromtimestamp(until_ts).astimezone().isoformat(timespec="seconds") if active else None,
        "last_two": last_two,
    }

def signals_stale_state():
    if not SIGNALS_FILE.exists():
        return {"exists": False, "age_secs": None, "stale": False}
    age = int(time.time() - SIGNALS_FILE.stat().st_mtime)
    return {"exists": True, "age_secs": age, "stale": age > SIGNALS_STALE_SECONDS}

def mirror_state_payload():
    rules = load_real_rules()
    cooldown = mirror_cooldown_state()
    sig_state = signals_stale_state()
    killed = MIRROR_KILL_FILE.exists()
    rules_configured = bool(rules.get("enabled")) and float(rules.get("capital") or 0) > 0
    reasons = []
    if killed:
        reasons.append("Manual mirror kill switch is on.")
    if cooldown.get("active"):
        reasons.append("Two consecutive losing closes triggered a 24-hour cooldown.")
    if sig_state.get("stale"):
        reasons.append("Latest signal file is stale. Wait for a fresh market-open scan.")
    if not rules_configured:
        reasons.append("Real-account rules are not enabled/configured yet.")
    return {
        "enabled": not killed and not cooldown.get("active") and not sig_state.get("stale") and rules_configured,
        "kill_switch": killed,
        "kill_switch_file": str(MIRROR_KILL_FILE),
        "cooldown_active": bool(cooldown.get("active")),
        "cooldown_until": cooldown.get("until"),
        "signals_stale": bool(sig_state.get("stale")),
        "signals_age_secs": sig_state.get("age_secs"),
        "signals_stale_after_secs": SIGNALS_STALE_SECONDS,
        "rules_configured": rules_configured,
        "rules_file": str(REAL_RULES_FILE),
        "rules": rules,
        "reasons": reasons,
    }

def signal_audit_summary(closes=None):
    empty = {
        "events": 0,
        "unique_signals": 0,
        "copyable_events": 0,
        "copyable_event_rate": 0,
        "copyable_unique_signals": 0,
        "closed_signal_trades": 0,
        "copyable_closed_signal_trades": 0,
        "copyable_closed_signal_wins": 0,
        "copyable_closed_signal_win_rate": None,
        "last_generated": None,
    }
    if not SIGNAL_AUDIT_FILE.exists():
        audit = {"events": [], "signals": {}}
    else:
        try:
            audit = json.loads(SIGNAL_AUDIT_FILE.read_text())
        except Exception:
            audit = {"events": [], "signals": {}}
    if not isinstance(audit, dict):
        audit = {"events": [], "signals": {}}
    if not audit.get("events") and SIGNAL_AUDIT_EVENTS_FILE.exists():
        recovered_events = []
        recovered_signals = {}
        for line in SIGNAL_AUDIT_EVENTS_FILE.read_text().splitlines():
            try:
                event = json.loads(line)
            except Exception:
                continue
            sid = event.get("id")
            if not sid:
                continue
            recovered_events.append(event)
            recovered_signals.setdefault(sid, event)
        audit = {
            "events": recovered_events,
            "signals": recovered_signals,
            "updated": recovered_events[-1].get("generated") if recovered_events else None,
        }
    events = audit.get("events") if isinstance(audit, dict) else []
    signals = audit.get("signals") if isinstance(audit, dict) else {}
    if not isinstance(events, list):
        events = []
    if not isinstance(signals, dict):
        signals = {}
    copyable_events = [e for e in events if e.get("copyable")]
    copyable_ids = {e.get("id") for e in copyable_events if e.get("id")}
    out = {
        "events": len(events),
        "unique_signals": len(signals),
        "copyable_events": len(copyable_events),
        "copyable_event_rate": round(len(copyable_events) / len(events) * 100, 1) if events else 0,
        "copyable_unique_signals": len(copyable_ids),
        "closed_signal_trades": 0,
        "copyable_closed_signal_trades": 0,
        "copyable_closed_signal_wins": 0,
        "copyable_closed_signal_win_rate": None,
        "last_generated": audit.get("updated"),
    }
    if closes:
        copyable_closed = []
        for trade in closes:
            notes = trade.get("notes", "")
            marker = "SignalID "
            if marker not in notes:
                continue
            sid = notes.split(marker, 1)[1].split("|", 1)[0].strip()
            out["closed_signal_trades"] += 1
            if sid in copyable_ids:
                copyable_closed.append(trade)
        wins = [t for t in copyable_closed if float(t.get("pnl") or 0) > 0]
        out["copyable_closed_signal_trades"] = len(copyable_closed)
        out["copyable_closed_signal_wins"] = len(wins)
        out["copyable_closed_signal_win_rate"] = round(len(wins) / len(copyable_closed) * 100, 1) if copyable_closed else None
    return out

def load_live_snapshot():
    if not LIVE_POSITIONS_FILE.exists():
        return {}
    try:
        return json.loads(LIVE_POSITIONS_FILE.read_text())
    except Exception:
        return {}

def append_paper_close_request(req):
    with open(PAPER_CLOSE_REQUESTS_FILE, "a") as f:
        f.write(json.dumps(req) + "\n")
    try:
        os.chmod(PAPER_CLOSE_REQUESTS_FILE, 0o600)
    except Exception:
        pass

def fetch(url, method="GET", headers=None, body=None, timeout=25, verify=True):
    req = urllib.request.Request(url, method=method)
    if headers:
        for k,v in headers.items(): req.add_header(k,v)
    if body: req.data = json.dumps(body).encode() if isinstance(body,dict) else body
    ctx = None
    if not verify:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            d = r.read()
            try: return json.loads(d)
            except: return {"raw": d.decode("utf-8","replace")}
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "detail": e.read().decode("utf-8","replace")[:500]}
    except Exception as e:
        return {"error": str(e)}

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self,f,*a): print(f"[{datetime.now():%H:%M:%S}] {a[0] if a else f}")
    def _json(self,d,s=200):
        r=json.dumps(d).encode()
        self.send_response(s);self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*");self.send_header("Content-Length",len(r))
        self.end_headers();self.wfile.write(r)
    def _body(self):
        n=int(self.headers.get("Content-Length",0))
        return json.loads(self.rfile.read(n)) if n else {}
    def _resolve_trades(self, trades):
        # Find closed ticker+strike combos and remove their matching open entries
        closed_keys=set()
        for t in trades:
            if t['status']=='closed':
                closed_keys.add(f"{t['ticker']}-{t['strike']}")
        # Keep: open trades not closed, plus closed trades (for history)
        result=[]
        for t in trades:
            if t['status']=='open' and f"{t['ticker']}-{t['strike']}" in closed_keys:
                continue  # This open was later closed
            result.append(t)
        return result
    def do_OPTIONS(self):
        self.send_response(204)
        for h in["Access-Control-Allow-Origin","Access-Control-Allow-Methods","Access-Control-Allow-Headers"]:
            self.send_header(h,"*")
        self.end_headers()
    def do_GET(self):
        p=self.path.split("?")[0];q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if p in("/","/index.html"):
            try:
                h=HTML.read_bytes();self.send_response(200)
                self.send_header("Content-Type","text/html; charset=utf-8")
                self.send_header("Content-Length",len(h));self.end_headers();self.wfile.write(h)
            except Exception as e: self._json({"error":str(e)},500)
            return
        if p=="/api-key": self._json({"key":get_key()});return
        if p=="/api/journal":
            try:
                if not JOURNAL.exists(): self._json({"trades":[]});return
                lines=JOURNAL.read_text().strip().split('\n')
                if len(lines)<2: self._json({"trades":[]});return
                # Auto-detect: if first line looks like a header (contains letters in most fields)
                first_cols=lines[0].split(',')
                has_header=sum(1 for c in first_cols[:6] if any(ch.isalpha() for ch in c.strip()))>=4
                start=1 if has_header else 0
                # Build header map if present
                hmap={}
                if has_header:
                    for i,h in enumerate(first_cols):
                        hmap[h.strip().lower().replace(' ','_')]=i
                trades=[]
                for line in lines[start:]:
                    cols=[c.strip() for c in line.split(',')]
                    if len(cols)<10: continue
                    # Try header-based mapping first, then known autotrade positional format:
                    # pos 0:datetime, 1:status, 2:ticker, 3:type, 4:strike, 5:expiry, 6:qty, 7:premium, 8:delta, 9:iv, 10:dte, 11:order_status, 12:pnl, 13+:notes
                    def g(names,pos):
                        if hmap:
                            for n in names:
                                if n in hmap and hmap[n]<len(cols): return cols[hmap[n]]
                        return cols[pos] if pos<len(cols) else ''
                    action_val=g(['action'],1).lower().strip()
                    order_status=g(['status','order_status'],11).lower().strip()
                    status_val=action_val
                    # Map action values: OPEN -> open, CLOSE_PROFIT/CLOSE -> closed
                    if action_val in ('close_profit','close','closed','close_loss','close_roll','close_dte') and order_status in FILLED_STATUSES:
                        status_val='closed'
                    elif action_val=='open':
                        status_val='open'
                    elif action_val in ('close_cancel','close_manual'):
                        status_val='closed'
                    ticker_val=g(['ticker','symbol'],2).upper().strip()
                    if not ticker_val: continue
                    try: strike_f=float(g(['strike'],4))
                    except: strike_f=0
                    try: premium_f=float(g(['premium','credit'],7))
                    except: premium_f=0
                    try: delta_f=float(g(['delta'],8))
                    except: delta_f=0
                    iv_raw=g(['iv'],9).replace('%','')
                    try: iv_f=float(iv_raw)
                    except: iv_f=0
                    try: dte_i=int(g(['dte'],10))
                    except: dte_i=0
                    try: pnl_f=float(g(['pnl'],12))
                    except: pnl_f=0
                    trades.append({
                        "date":g(['date','datetime','timestamp'],0),
                        "status":status_val,
                        "ticker":ticker_val,
                        "strategy":g(['type','strategy'],3),
                        "strike":strike_f,
                        "expiry":g(['expiry','exp'],5),
                        "contracts":int(g(['qty','contracts'],6) or 1),
                        "premium":premium_f,
                        "delta":delta_f,
                        "iv":iv_f,
                        "dte":dte_i,
                        "pnl":pnl_f,
                        "notes":','.join(cols[13:]) if len(cols)>13 else ''
                    })
                self._json({"trades":self._resolve_trades(trades)})
            except Exception as e: self._json({"error":str(e)},500)
            return
        if p=="/api/yahoo":
            t=q.get("ticker",["SPY"])[0];r=q.get("range",["1y"])[0]
            url=f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range={r}&interval=1d&includePrePost=false"
            self._json(fetch(url,headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}))
            return
        if p.startswith("/api/ibkr/"):
            route=p.replace("/api/ibkr/","")
            try:
                if route=="status":
                    r=fetch("https://localhost:5000/v1/api/iserver/auth/status",method="POST",verify=False)
                    try:
                        a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                        if isinstance(a,dict) and "accounts" in a: r["accounts"]=a["accounts"]
                    except: pass
                    self._json(r);return
                aid=q.get("accountId",[None])[0]
                if not aid:
                    try:
                        a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                        aid=a.get("accounts",[""])[0] if isinstance(a,dict) else ""
                    except: pass
                if route=="positions": self._json(fetch(f"https://localhost:5000/v1/api/portfolio/{aid}/positions/0",verify=False));return
                if route=="account": self._json(fetch(f"https://localhost:5000/v1/api/portfolio/{aid}/summary",verify=False));return
                if route=="orders":
                    r=fetch("https://localhost:5000/v1/api/iserver/account/orders",verify=False)
                    self._json(r.get("orders",[]) if isinstance(r,dict) else r if isinstance(r,list) else []);return
                if route=="trades": self._json(fetch("https://localhost:5000/v1/api/iserver/account/trades",verify=False));return
            except Exception as e: self._json({"error":str(e)},500);return
        if p=="/api/live":
            import json as _json, time as _time, subprocess as _sp
            fp=LIVE_POSITIONS_FILE
            if fp.exists():
                try:
                    with open(fp) as f: data=_json.load(f)
                    age=int(_time.time()-fp.stat().st_mtime)
                    try:
                        alive=bool(_sp.run(["pgrep","-f","autotrade_engine.py"],capture_output=True,text=True).stdout.strip())
                    except Exception:
                        alive=age < 1800
                    data["snapshot_age_secs"]=age
                    data["snapshot_stale"]=age > 1800
                    data["engine_running"]=bool(alive and age <= 1800)
                    data["signal_only"]=SIGNAL_ONLY_FILE.exists()
                    data["signal_only_file"]=str(SIGNAL_ONLY_FILE)
                    data["signals_file"]=str(SIGNALS_FILE)
                    if data["snapshot_stale"] or not data["engine_running"]:
                        data["connected"]=False
                    self._json(data); return
                except: pass
            self._json({"error":"snapshot not found","engine_running":False},200); return
        if p=="/api/signals":
            payload = {
                "generated": None,
                "mode": "signal_only" if SIGNAL_ONLY_FILE.exists() else "paper_auto",
                "signal_only": SIGNAL_ONLY_FILE.exists(),
                "signal_only_file": str(SIGNAL_ONLY_FILE),
                "signals_file": str(SIGNALS_FILE),
                "signals": [],
                "message": "",
                "signals_age_secs": None,
                "signals_stale": False,
                "signals_stale_after_secs": SIGNALS_STALE_SECONDS,
                "warnings": [
                    "Paper/delayed data signal. Verify live bid/ask in the real account before copying.",
                    "Do not copy if portfolio risk, correlation, or news/event risk is elevated.",
                ],
            }
            try:
                live_status = "unknown"
                live_fp = LIVE_POSITIONS_FILE
                if live_fp.exists():
                    try:
                        live_status = json.loads(live_fp.read_text()).get("market_status", "unknown")
                    except:
                        pass
                payload["market_status"] = live_status
                if SIGNALS_FILE.exists():
                    age = int(time.time() - SIGNALS_FILE.stat().st_mtime)
                    payload["signals_age_secs"] = age
                    payload["signals_stale"] = age > SIGNALS_STALE_SECONDS
                    with open(SIGNALS_FILE) as f:
                        saved = json.load(f)
                    if isinstance(saved, dict):
                        payload.update(saved)
                        payload["signals"] = saved.get("signals", []) if isinstance(saved.get("signals", []), list) else []
                    payload["signals_age_secs"] = age
                    payload["signals_stale"] = age > SIGNALS_STALE_SECONDS
                    payload["signals_stale_after_secs"] = SIGNALS_STALE_SECONDS
                payload["signal_only"] = SIGNAL_ONLY_FILE.exists()
                payload["mode"] = "signal_only" if SIGNAL_ONLY_FILE.exists() else payload.get("mode", "paper_auto")
                payload["market_status"] = live_status
                if not SIGNALS_FILE.exists():
                    payload["message"] = (
                        "Signal-only mode is enabled, but no scan file exists yet. "
                        "Signals are generated by the next market-open scan."
                    ) if SIGNAL_ONLY_FILE.exists() else (
                        "No scan file exists yet. Signals are generated by the next market-open scan."
                    )
                elif not payload["signals"]:
                    payload["message"] = "The latest scan completed, but no candidates passed the safety filters."
                elif payload.get("signals_stale"):
                    payload["message"] = "Stale signal file. Do not copy these recommendations without a fresh scan."
                self._json(payload); return
            except Exception as e:
                payload["error"] = str(e)
                self._json(payload, 500); return
        if p=="/api/performance":
            # Compute analytics from journal CSV.
            # Returns: win rate, total P/L, avg winner/loser, counts,
            # per-ticker breakdown, and cumulative P/L time series.
            try:
                if not JOURNAL.exists():
                    self._json({"trades": 0, "wins": 0, "losses": 0,
                                "win_rate": 0, "total_pnl": 0,
                                "avg_win": 0, "avg_loss": 0,
                                "largest_win": 0, "largest_loss": 0,
                                "by_ticker": {}, "cumulative": []})
                    return

                closes = []  # list of dicts: {date, ticker, pnl}
                with open(JOURNAL, newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        action = (row.get("action") or "").upper()
                        status = (row.get("status") or "").lower()
                        if not action.startswith("CLOSE"):
                            continue
                        if action in {"CLOSE_CANCEL"}:
                            continue
                        if status not in FILLED_STATUSES:
                            continue
                        try:
                            pnl = float(row.get("pnl") or 0)
                        except:
                            continue
                        if action == "CLOSE_MANUAL" and pnl == 0:
                            continue
                        closes.append({
                            "date":   row.get("timestamp", ""),
                            "ticker": row.get("ticker", ""),
                            "pnl":    pnl,
                            "notes":  row.get("notes", ""),
                        })

                if not closes:
                    self._json({"trades": 0, "wins": 0, "losses": 0,
                                "win_rate": 0, "total_pnl": 0,
                                "avg_win": 0, "avg_loss": 0,
                                "by_ticker": {}, "cumulative": []})
                    return

                wins   = [t for t in closes if t["pnl"] >  0]
                losses = [t for t in closes if t["pnl"] <= 0]
                total  = sum(t["pnl"] for t in closes)

                by_ticker = {}
                for t in closes:
                    tk = t["ticker"]
                    if tk not in by_ticker:
                        by_ticker[tk] = {"count": 0, "pnl": 0.0, "wins": 0, "losses": 0}
                    by_ticker[tk]["count"] += 1
                    by_ticker[tk]["pnl"]   += t["pnl"]
                    if t["pnl"] > 0: by_ticker[tk]["wins"]   += 1
                    else:            by_ticker[tk]["losses"] += 1
                for tk in by_ticker:
                    by_ticker[tk]["pnl"] = round(by_ticker[tk]["pnl"], 2)

                # Cumulative P/L time series
                closes_sorted = sorted(closes, key=lambda x: x["date"])
                cumulative = []
                running = 0.0
                for t in closes_sorted:
                    running += t["pnl"]
                    cumulative.append({
                        "date":   t["date"],
                        "ticker": t["ticker"],
                        "pnl":    round(t["pnl"], 2),
                        "cum":    round(running, 2),
                    })

                # Max drawdown on cumulative series
                peak = 0.0
                max_dd = 0.0
                for c in cumulative:
                    if c["cum"] > peak: peak = c["cum"]
                    dd = peak - c["cum"]
                    if dd > max_dd: max_dd = dd

                self._json({
                    "trades":       len(closes),
                    "wins":         len(wins),
                    "losses":       len(losses),
                    "win_rate":     round(len(wins) / len(closes) * 100, 1) if closes else 0,
                    "total_pnl":    round(total, 2),
                    "avg_win":      round(sum(t["pnl"] for t in wins)   / len(wins),   2) if wins   else 0,
                    "avg_loss":     round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0,
                    "largest_win":  round(max((t["pnl"] for t in wins),   default=0), 2),
                    "largest_loss": round(min((t["pnl"] for t in losses), default=0), 2),
                    "max_drawdown": round(max_dd, 2),
                    "by_ticker":    by_ticker,
                    "cumulative":   cumulative,
                    "signal_audit": signal_audit_summary(closes),
                })
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return
        if p=="/api/engine-status":
            import subprocess as _sp
            try:
                r=_sp.run(["pgrep","-f","autotrade_engine"],capture_output=True,text=True)
                alive=bool(r.stdout.strip())
                age=None
                if LIVE_POSITIONS_FILE.exists():
                    import time as _t; age=int(_t.time()-LIVE_POSITIONS_FILE.stat().st_mtime)
                self._json({"running":alive,"snapshot_age_secs":age}); return
            except Exception as e: self._json({"running":False,"error":str(e)}); return
        if p=="/api/real-rules":
            self._json(load_real_rules()); return
        if p=="/api/mirror-state":
            self._json(mirror_state_payload()); return
        if p=="/api/paper-close-results":
            results = []
            try:
                if PAPER_CLOSE_RESULTS_FILE.exists():
                    for line in PAPER_CLOSE_RESULTS_FILE.read_text().splitlines()[-50:]:
                        if line.strip():
                            results.append(json.loads(line))
            except Exception as e:
                self._json({"error": str(e)}, 500); return
            self._json({"results": results}); return
        self._json({"error":"Not found"},404)
    def do_POST(self):
        p=self.path.split("?")[0];b=self._body()
        if p=="/api/claude":
            k=get_key()
            if not k: self._json({"error":{"message":"No API key in api_key.txt"}},401);return
            req=urllib.request.Request("https://api.anthropic.com/v1/messages",method="POST")
            for h,v in{"Content-Type":"application/json","x-api-key":k,"anthropic-version":"2023-06-01"}.items():
                req.add_header(h,v)
            req.data=json.dumps(b).encode()
            try:
                with urllib.request.urlopen(req,timeout=60) as r:
                    d=r.read();self.send_response(200)
                    self.send_header("Content-Type","application/json")
                    self.send_header("Access-Control-Allow-Origin","*")
                    self.send_header("Content-Length",len(d));self.end_headers();self.wfile.write(d)
            except urllib.error.HTTPError as e:
                self._json({"error":{"message":f"API {e.code}: {e.read().decode()[:300]}"}},e.code)
            except Exception as e: self._json({"error":{"message":str(e)}},500)
            return
        if p=="/api/notify":
            os.system(f'''osascript -e 'display notification "{b.get("body","")}" with title "{b.get("title","Options Pro")}"' ''')
            self._json({"ok":True});return
        if p=="/api/signal-mode":
            try:
                enabled = bool(b.get("enabled"))
                current = SIGNAL_ONLY_FILE.exists()
                now = time.time()
                last = 0.0
                if SIGNAL_MODE_RATE_FILE.exists():
                    try:
                        last = float(SIGNAL_MODE_RATE_FILE.read_text().strip() or "0")
                    except:
                        last = 0.0
                remaining = int(SIGNAL_MODE_RATE_SECONDS - (now - last))
                if enabled != current and remaining > 0:
                    self._json({
                        "ok": False,
                        "error": f"Signal mode can only be changed once every {SIGNAL_MODE_RATE_SECONDS} seconds.",
                        "retry_after_secs": remaining,
                        "signal_only": current,
                        "signal_only_file": str(SIGNAL_ONLY_FILE),
                    }, 429);return
                if enabled:
                    SIGNAL_ONLY_FILE.write_text(f"enabled {now_iso()}\n")
                elif SIGNAL_ONLY_FILE.exists():
                    SIGNAL_ONLY_FILE.unlink()
                if enabled != current:
                    SIGNAL_MODE_RATE_FILE.write_text(str(now))
                self._json({
                    "ok": True,
                    "signal_only": SIGNAL_ONLY_FILE.exists(),
                    "signal_only_file": str(SIGNAL_ONLY_FILE),
                });return
            except Exception as e: self._json({"ok":False,"error":str(e)},500);return
        if p=="/api/real-rules":
            try:
                rules = save_real_rules(b)
                self._json({"ok": True, "rules": rules}); return
            except Exception as e: self._json({"ok": False, "error": str(e)}, 500); return
        if p=="/api/mirror-kill":
            try:
                killed = bool(b.get("killed"))
                if killed:
                    MIRROR_KILL_FILE.write_text(f"killed {now_iso()}\n")
                elif MIRROR_KILL_FILE.exists():
                    MIRROR_KILL_FILE.unlink()
                self._json({"ok": True, **mirror_state_payload()}); return
            except Exception as e: self._json({"ok": False, "error": str(e)}, 500); return
        if p=="/api/paper-close":
            try:
                live = load_live_snapshot()
                if not live.get("engine_running") or not live.get("connected"):
                    self._json({"ok": False, "error": "Engine/TWS is not connected."}, 409); return
                if live.get("market_status") == "tws_restart":
                    self._json({"ok": False, "error": "TWS restart window is active."}, 409); return
                ticker = str(b.get("ticker", "")).upper().strip()
                try:
                    strike = round(float(b.get("strike")), 2)
                    qty = int(b.get("qty") or 0)
                except Exception:
                    self._json({"ok": False, "error": "Invalid strike/quantity."}, 400); return
                expiry = str(b.get("expiry", "")).strip()
                strategy = str(b.get("strategy", "CSP")).upper().strip()
                phrase = str(b.get("confirm", "")).strip().upper()
                expected = f"CLOSE PAPER {ticker} {strike:g}"
                if phrase != expected:
                    self._json({"ok": False, "error": f"Confirmation phrase must be: {expected}"}, 400); return
                match = None
                for ppos in live.get("positions", []):
                    if str(ppos.get("ticker", "")).upper() != ticker:
                        continue
                    if round(float(ppos.get("strike", 0) or 0), 2) != strike:
                        continue
                    if str(ppos.get("expiry", "")) != expiry:
                        continue
                    match = ppos
                    break
                if not match:
                    self._json({"ok": False, "error": "Position is not present in the live paper snapshot."}, 404); return
                qty = min(qty, int(match.get("qty") or 0))
                if qty <= 0:
                    self._json({"ok": False, "error": "Quantity is zero."}, 400); return
                req = {
                    "id": f"pc_{int(time.time() * 1000)}",
                    "created_at": now_iso(),
                    "ticker": ticker,
                    "strike": strike,
                    "expiry": expiry,
                    "qty": qty,
                    "strategy": strategy,
                    "source": "dashboard",
                    "account_scope": "paper",
                }
                append_paper_close_request(req)
                self._json({"ok": True, "queued": req}); return
            except Exception as e: self._json({"ok": False, "error": str(e)}, 500); return
        if p=="/api/email":
            try:
                import smtplib;from email.mime.text import MIMEText;from email.mime.multipart import MIMEMultipart
                msg=MIMEMultipart("alternative");msg["Subject"]=b.get("subject","Options Pro Test")
                msg["From"]=b.get("smtp_user","");msg["To"]=b.get("to","")
                html="<h2>✅ Test OK</h2>" if b.get("type")=="test" else f"<h2>🌅 Briefing</h2><p>{b.get('regime','')}</p><p>{b.get('opps','')}</p>"
                msg.attach(MIMEText(html,"html"))
                with smtplib.SMTP(b.get("smtp_host","smtp.gmail.com"),int(b.get("smtp_port",587))) as s:
                    s.ehlo();s.starttls();s.login(b["smtp_user"],b["smtp_pass"]);s.sendmail(b["smtp_user"],b["to"],msg.as_string())
                self._json({"ok":True})
            except Exception as e: self._json({"error":str(e)},500)
            return
        if p.startswith("/api/ibkr/"):
            route=p.replace("/api/ibkr/","")
            if route=="order":
                aid=b.get("accountId","")
                if not aid:
                    try:
                        a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                        aid=a.get("accounts",[""])[0] if isinstance(a,dict) else ""
                    except: pass
                r=fetch(f"https://localhost:5000/v1/api/iserver/account/{aid}/orders",method="POST",
                    headers={"Content-Type":"application/json"},body={"orders":[{
                        "acctId":aid,"orderType":"LMT","side":"SELL",
                        "quantity":b.get("qty",1),"price":b.get("limit",0),"tif":"DAY"
                    }]},verify=False)
                self._json(r);return
        self._json({"error":"Not found"},404)
    def do_DELETE(self):
        p=self.path.split("?")[0]
        if "/api/ibkr/order/" in p:
            oid=p.split("/")[-1]
            try:
                a=fetch("https://localhost:5000/v1/api/iserver/accounts",verify=False)
                aid=a.get("accounts",[""])[0] if isinstance(a,dict) else ""
                self._json(fetch(f"https://localhost:5000/v1/api/iserver/account/{aid}/order/{oid}",method="DELETE",verify=False))
            except Exception as e: self._json({"error":str(e)},500)
            return
        self._json({"error":"Not found"},404)

class ReusableHTTPServer(http.server.HTTPServer):
    allow_reuse_address = True

if __name__=="__main__":
    print(f"\n🚀 Options Pro Ultra v6 Proxy\n   http://localhost:{PORT}\n   HTML: {HTML}\n   Key: {'✓' if get_key() else '⚠ missing'}\n")
    ReusableHTTPServer(("",PORT),H).serve_forever()
