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
    return DATA_DIR / name

JOURNAL = data_file("autotrade_journal.csv")
LIVE_POSITIONS_FILE = data_file("live_positions.json")
REAL_POSITIONS_FILE = data_file("real_positions.json")
EARNINGS_CALENDAR_FILE = data_file("earnings_calendar.json")
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
SCAN_NOW_FILE = data_file("scan_now_requested")
SCAN_NOW_RATE_FILE = Path("/tmp/optionspro_scan_now_last")
SCAN_NOW_RATE_SECONDS = 5 * 60  # one manual scan request per 5 minutes
MIRROR_COOLDOWN_SECONDS = 24 * 60 * 60
FILLED_STATUSES = {"filled", "closed", "manualclose"}
JOURNAL_HEADER = ["timestamp", "action", "ticker", "strategy", "strike", "expiry", "qty", "credit", "delta", "iv", "dte", "status", "pnl", "notes"]

def now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")

def journal_key(ticker, strike, expiry):
    try:
        strike_f = round(float(strike), 2)
    except Exception:
        strike_f = 0.0
    return f"{str(ticker).upper()}-{strike_f}-{str(expiry)}"

def ensure_journal():
    if not JOURNAL.exists():
        JOURNAL.write_text(",".join(JOURNAL_HEADER) + "\n")

def append_journal_row(action, ticker, strategy, strike, expiry, qty, credit, delta, iv, dte, status, pnl, notes):
    ensure_journal()
    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        action,
        ticker,
        strategy,
        strike,
        expiry,
        qty,
        credit,
        delta,
        iv,
        dte,
        status,
        pnl,
        notes,
    ]
    with open(JOURNAL, "a", newline="") as f:
        csv.writer(f).writerow(row)

def read_journal_open_positions():
    positions = {}
    if not JOURNAL.exists():
        return positions
    with open(JOURNAL, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            action = (row.get("action") or "").strip().upper()
            ticker = (row.get("ticker") or "").strip().upper()
            expiry = (row.get("expiry") or "").strip()
            try:
                strike = round(float(row.get("strike") or 0), 2)
            except Exception:
                strike = 0.0
            key = journal_key(ticker, strike, expiry)
            if action == "OPEN":
                try:
                    qty = int(float(row.get("qty") or 0))
                except Exception:
                    qty = 0
                positions[key] = {
                    "ticker": ticker,
                    "strike": strike,
                    "expiry": expiry,
                    "qty": qty,
                    "strategy": (row.get("strategy") or "CSP").strip().upper(),
                    "credit": row.get("credit") or 0,
                    "dte": row.get("dte") or 0,
                }
            elif action.startswith("CLOSE"):
                status = (row.get("status") or "").strip().lower()
                if status in FILLED_STATUSES or action in {"CLOSE_CANCEL", "CLOSE_MANUAL"}:
                    positions.pop(key, None)
    return positions

def _f(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default

def _i(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default

def load_real_positions():
    if not REAL_POSITIONS_FILE.exists():
        return []
    try:
        data = json.loads(REAL_POSITIONS_FILE.read_text())
        positions = data.get("positions", data) if isinstance(data, dict) else data
        return [normalize_real_position(p) for p in positions if isinstance(p, dict)]
    except Exception:
        return []

def save_real_positions(positions):
    payload = {
        "updated": now_iso(),
        "positions": [normalize_real_position(p) for p in positions if isinstance(p, dict)],
    }
    REAL_POSITIONS_FILE.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    try:
        os.chmod(REAL_POSITIONS_FILE, 0o600)
    except Exception:
        pass
    return real_positions_payload(payload["positions"])

def normalize_real_position(pos):
    ticker = str(pos.get("ticker", "")).upper().strip()
    strategy = str(pos.get("strategy", "CSP")).upper().strip() or "CSP"
    expiry = str(pos.get("expiry", "")).strip()
    strike = round(_f(pos.get("strike")), 2)
    long_strike_raw = pos.get("long_strike")
    long_strike = round(_f(long_strike_raw), 2) if str(long_strike_raw or "").strip() else None
    qty = max(0, _i(pos.get("qty"), 0))
    entry_credit = _f(pos.get("entry_credit", pos.get("credit", 0)))
    current_price = _f(pos.get("current_price", pos.get("mark", 0)))
    exit_debit = _f(pos.get("exit_debit", 0))
    status = str(pos.get("status", "open")).lower().strip() or "open"
    opened_at = str(pos.get("opened_at") or now_iso())
    pid = str(pos.get("id") or "").strip()
    if not pid:
        pid = f"{ticker}-{strategy}-{strike:g}-{long_strike or ''}-{expiry}-{opened_at}".replace(":", "").replace(" ", "T")
    price_valid = current_price > 0
    pnl = None
    pnl_pct = None
    if status == "open" and price_valid and qty > 0:
        pnl = round((entry_credit - current_price) * qty * 100, 2)
        denom = entry_credit * qty * 100
        pnl_pct = round((pnl / denom * 100), 1) if denom else None
    elif status != "open" and exit_debit > 0 and qty > 0:
        pnl = round((entry_credit - exit_debit) * qty * 100, 2)
        denom = entry_credit * qty * 100
        pnl_pct = round((pnl / denom * 100), 1) if denom else None
    return {
        "id": pid,
        "ticker": ticker,
        "strategy": strategy,
        "strike": strike,
        "long_strike": long_strike,
        "expiry": expiry,
        "qty": qty,
        "entry_credit": entry_credit,
        "current_price": current_price,
        "exit_debit": exit_debit,
        "price_valid": price_valid,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "status": status,
        "opened_at": opened_at,
        "closed_at": str(pos.get("closed_at") or ""),
        "notes": str(pos.get("notes") or ""),
    }

def real_positions_payload(positions=None):
    positions = [normalize_real_position(p) for p in (positions if positions is not None else load_real_positions())]
    open_positions = [p for p in positions if p.get("status") == "open"]
    priced = [p for p in open_positions if p.get("price_valid") and p.get("pnl") is not None]
    total_pnl = round(sum(_f(p.get("pnl")) for p in priced), 2)
    return {
        "updated": now_iso(),
        "file": str(REAL_POSITIONS_FILE),
        "positions": positions,
        "open_count": len(open_positions),
        "closed_count": len([p for p in positions if p.get("status") != "open"]),
        "priced_count": len(priced),
        "total_unrealized_pnl": total_pnl,
    }

def load_earnings_calendar():
    try:
        if EARNINGS_CALENDAR_FILE.exists():
            data = json.loads(EARNINGS_CALENDAR_FILE.read_text())
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

def save_earnings_calendar(data):
    if not isinstance(data, dict):
        raise ValueError("Earnings calendar must be an object keyed by ticker.")
    clean = {}
    for ticker, value in data.items():
        tk = str(ticker).upper().strip()
        if not tk:
            continue
        clean[tk] = value
    EARNINGS_CALENDAR_FILE.write_text(json.dumps(clean, indent=2, sort_keys=True) + "\n")
    try:
        os.chmod(EARNINGS_CALENDAR_FILE, 0o600)
    except Exception:
        pass
    return {"file": str(EARNINGS_CALENDAR_FILE), "calendar": clean}

DEFAULT_REAL_RULES = {
    "enabled": False,
    "capital": 0,
    "max_risk_per_trade_pct": 1.0,
    "max_risk_per_trade_dollars": 0,
    "csp_max_collateral_pct": 5.0,
    "csp_max_collateral_dollars": 0,
    "bps_max_loss_pct": 1.0,
    "bps_max_loss_dollars": 0,
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
    for key in ("csp_max_collateral_pct", "csp_max_collateral_dollars", "bps_max_loss_pct", "bps_max_loss_dollars"):
        try:
            rules[key] = float(rules.get(key) or 0)
        except Exception:
            rules[key] = 0
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
    scan_pending = scan_now_state()
    if not SIGNALS_FILE.exists():
        return {"exists": False, "age_secs": None, "stale": False, "scan_pending": scan_pending}
    age = int(time.time() - SIGNALS_FILE.stat().st_mtime)
    return {"exists": True, "age_secs": age, "stale": age > SIGNALS_STALE_SECONDS, "scan_pending": scan_pending}

def scan_now_state():
    if not SCAN_NOW_FILE.exists():
        return {"pending": False, "age_secs": None, "requested": None}
    try:
        requested = SCAN_NOW_FILE.read_text().strip()
    except Exception:
        requested = None
    try:
        age = int(time.time() - SCAN_NOW_FILE.stat().st_mtime)
    except Exception:
        age = None
    return {"pending": True, "age_secs": age, "requested": requested, "marker_file": str(SCAN_NOW_FILE)}

def current_market_status():
    try:
        if LIVE_POSITIONS_FILE.exists():
            data = json.loads(LIVE_POSITIONS_FILE.read_text())
            return str(data.get("market_status") or "unknown")
    except Exception:
        pass
    return "unknown"

def mirror_state_payload():
    rules = load_real_rules()
    cooldown = mirror_cooldown_state()
    sig_state = signals_stale_state()
    market_status = current_market_status()
    market_open = market_status == "open"
    killed = MIRROR_KILL_FILE.exists()
    rules_configured = bool(rules.get("enabled")) and float(rules.get("capital") or 0) > 0
    reasons = []
    if not market_open:
        if market_status == "unknown":
            reasons.append("Market status is unknown; wait for a fresh engine snapshot.")
        else:
            reasons.append(f"Market is {market_status.replace('_', ' ')}; real-account copy is disabled until market open.")
    if killed:
        reasons.append("Manual mirror kill switch is on.")
    if cooldown.get("active"):
        reasons.append("Two consecutive losing closes triggered a 24-hour cooldown.")
    if sig_state.get("stale"):
        reasons.append("Latest signal file is stale. Wait for a fresh market-open scan.")
    if sig_state.get("scan_pending", {}).get("pending"):
        reasons.append("Manual Scan Now request is queued; wait for a new generated timestamp.")
    if not rules_configured:
        reasons.append("Real-account rules are not enabled/configured yet.")
    return {
        "enabled": market_open and not killed and not cooldown.get("active") and not sig_state.get("stale") and not sig_state.get("scan_pending", {}).get("pending") and rules_configured,
        "market_status": market_status,
        "kill_switch": killed,
        "kill_switch_file": str(MIRROR_KILL_FILE),
        "cooldown_active": bool(cooldown.get("active")),
        "cooldown_until": cooldown.get("until"),
        "signals_stale": bool(sig_state.get("stale")),
        "signals_age_secs": sig_state.get("age_secs"),
        "signals_stale_after_secs": SIGNALS_STALE_SECONDS,
        "scan_pending": sig_state.get("scan_pending"),
        "rules_configured": rules_configured,
        "rules_file": str(REAL_RULES_FILE),
        "rules": rules,
        "reasons": reasons,
    }

def real_qty_for_signal(signal, rules):
    try:
        capital = float(rules.get("capital") or 0)
        strategy = str(signal.get("strategy") or "CSP").upper()
        if strategy == "CSP":
            risk_pct = float(rules.get("csp_max_collateral_pct") or rules.get("max_risk_per_trade_pct") or 0)
            hard_cap = float(rules.get("csp_max_collateral_dollars") or rules.get("max_risk_per_trade_dollars") or 0)
        else:
            risk_pct = float(rules.get("bps_max_loss_pct") or rules.get("max_risk_per_trade_pct") or 0)
            hard_cap = float(rules.get("bps_max_loss_dollars") or rules.get("max_risk_per_trade_dollars") or 0)
        paper_qty = max(1, int(float(signal.get("qty") or 1)))
        total_risk = float(signal.get("estimated_risk") or 0)
    except Exception:
        return 0
    if capital <= 0 or risk_pct <= 0 or total_risk <= 0:
        return 0
    pct_cap = capital * (risk_pct / 100)
    max_risk = min(hard_cap, pct_cap) if hard_cap > 0 else pct_cap
    unit_risk = total_risk / paper_qty
    return max(0, int(max_risk // unit_risk)) if unit_risk > 0 and max_risk > 0 else 0

def signal_gate_payload(signal, signals_stale=False, scan_pending=None, mirror=None):
    mirror = mirror or mirror_state_payload()
    scan_pending = scan_pending or {"pending": False}
    rules = mirror.get("rules") or {}
    reasons = []
    notices = []
    if signals_stale:
        reasons.append("Signal file is stale.")
    if scan_pending.get("pending"):
        reasons.append("Manual Scan Now is queued; wait for the refreshed scan result.")
    for reason in mirror.get("reasons") or []:
        if reason not in reasons:
            reasons.append(reason)
    for reason in signal.get("reason_labels") or []:
        if reason not in reasons:
            reasons.append(reason)
    if signal.get("ticker") not in set(rules.get("allowed_tickers") or []):
        reasons.append("Ticker is not allowed by current real-account rules.")
    if signal.get("strategy") not in set(rules.get("allowed_strategies") or []):
        reasons.append("Strategy is not allowed by current real-account rules.")
    if SIGNAL_ONLY_FILE.exists():
        notices.append("Signal Only mode is on: paper auto will not submit opening orders.")
    real_qty = real_qty_for_signal(signal, rules)
    if real_qty < 1 and "Real-account risk rules allow 0 contracts for this setup." not in reasons:
        reasons.append("Real-account risk rules allow 0 contracts for this setup.")
    signal["real_qty"] = real_qty
    return {
        "enabled": not reasons,
        "reasons": reasons,
        "notices": notices,
        "real_qty": max(0, real_qty),
        "paper_auto_enabled": not SIGNAL_ONLY_FILE.exists(),
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

def reconciliation_from_live_snapshot(live):
    """Compare the current live snapshot against the current journal state."""
    live_positions = {}
    for pos in (live or {}).get("positions") or []:
        key = journal_key(pos.get("ticker", ""), pos.get("strike", 0), pos.get("expiry", ""))
        try:
            qty = int(float(pos.get("qty") or 0))
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        live_positions[key] = {
            "ticker": str(pos.get("ticker", "")).upper(),
            "strike": pos.get("strike", 0),
            "expiry": pos.get("expiry", ""),
            "qty": qty,
            "strategy": str(pos.get("strategy", "CSP")).upper(),
        }

    journal_positions = read_journal_open_positions()
    missing_in_ibkr = []
    missing_in_journal = []
    qty_mismatch = []

    for key, jpos in journal_positions.items():
        if key not in live_positions:
            missing_in_ibkr.append({
                "ticker": jpos.get("ticker", ""),
                "strike": jpos.get("strike", 0),
                "expiry": jpos.get("expiry", ""),
                "qty": jpos.get("qty", 0),
                "strategy": jpos.get("strategy", "CSP"),
                "status": "Open",
            })
            continue
        live_qty = int(live_positions[key].get("qty") or 0)
        journal_qty = int(jpos.get("qty") or 0)
        if live_qty != journal_qty:
            qty_mismatch.append({
                "ticker": jpos.get("ticker", ""),
                "strike": jpos.get("strike", 0),
                "expiry": jpos.get("expiry", ""),
                "journal_qty": journal_qty,
                "ibkr_qty": live_qty,
                "strategy": jpos.get("strategy", "CSP"),
            })

    for key, pos in live_positions.items():
        if key not in journal_positions:
            missing_in_journal.append(pos)

    ok = not missing_in_ibkr and not missing_in_journal and not qty_mismatch
    return {
        "checked_at": now_iso(),
        "ok": ok,
        "missing_in_ibkr": missing_in_ibkr,
        "missing_in_journal": missing_in_journal,
        "qty_mismatch": qty_mismatch,
    }

def quote_summary_from_live_snapshot(live):
    positions = (live or {}).get("positions") or []
    total = len(positions)
    valid = sum(1 for p in positions if p.get("price_valid"))
    missing = max(0, total - valid)
    if total <= 0:
        status = "none"
        message = "No open positions."
    elif valid <= 0:
        status = "unavailable"
        message = "No valid option quotes; live P/L is unavailable."
    elif missing > 0:
        status = "partial"
        message = f"Live P/L uses {valid} of {total} positions; {missing} quote(s) missing."
    else:
        status = "complete"
        message = "All open positions have valid option quotes."
    return {
        "status": status,
        "positions": total,
        "valid": valid,
        "missing": missing,
        "message": message,
    }

def enrich_live_snapshot(data):
    if not isinstance(data, dict):
        return data
    quote_summary = quote_summary_from_live_snapshot(data)
    data["quote_summary"] = quote_summary
    data["pnl_status"] = quote_summary["status"]
    data["valid_price_count"] = quote_summary["valid"]
    data["missing_quote_count"] = quote_summary["missing"]
    if not data.get("snapshot_stale") and data.get("positions") is not None:
        data["reconciliation"] = reconciliation_from_live_snapshot(data)
    return data

def format_age(secs):
    try:
        n = int(float(secs))
    except Exception:
        return "-"
    if n < 60:
        return f"{n}s"
    if n < 3600:
        return f"{n // 60}m"
    return f"{n // 3600}h {(n % 3600) // 60}m"

def append_paper_close_request(req):
    with open(PAPER_CLOSE_REQUESTS_FILE, "a") as f:
        f.write(json.dumps(req) + "\n")
    try:
        os.chmod(PAPER_CLOSE_REQUESTS_FILE, 0o600)
    except Exception:
        pass

def reconcile_journal_to_live_snapshot(live):
    """Append journal rows so the current IBKR/live snapshot becomes truth."""
    if not isinstance(live, dict):
        raise ValueError("Live snapshot is unavailable")
    live_positions = {}
    for pos in live.get("positions") or []:
        key = journal_key(pos.get("ticker", ""), pos.get("strike", 0), pos.get("expiry", ""))
        live_positions[key] = pos
    journal_positions = read_journal_open_positions()
    actions = []

    for key, jpos in journal_positions.items():
        if key in live_positions:
            live_qty = int(live_positions[key].get("qty") or 0)
            journal_qty = int(jpos.get("qty") or 0)
            if live_qty == journal_qty:
                continue
            append_journal_row(
                "CLOSE_MANUAL",
                jpos.get("ticker", ""),
                "CLOSE",
                jpos.get("strike", 0),
                jpos.get("expiry", ""),
                journal_qty,
                0,
                0,
                "",
                0,
                "ManualClose",
                0,
                f"Manual reconcile accepted IBKR qty {live_qty}; closing stale journal qty {journal_qty}",
            )
            actions.append({"type": "qty_mismatch_closed", "key": key, "journal_qty": journal_qty, "ibkr_qty": live_qty})
            continue
        append_journal_row(
            "CLOSE_MANUAL",
            jpos.get("ticker", ""),
            "CLOSE",
            jpos.get("strike", 0),
            jpos.get("expiry", ""),
            jpos.get("qty", 0),
            0,
            0,
            "",
            0,
            "ManualClose",
            0,
            "Manual reconcile accepted IBKR view; position absent from live snapshot",
        )
        actions.append({"type": "closed_missing_in_ibkr", "key": key})

    for key, pos in live_positions.items():
        journal_qty = int(journal_positions.get(key, {}).get("qty") or 0)
        live_qty = int(pos.get("qty") or 0)
        if key in journal_positions and journal_qty == live_qty:
            continue
        append_journal_row(
            "OPEN",
            str(pos.get("ticker", "")).upper(),
            str(pos.get("strategy", "CSP")).upper(),
            pos.get("strike", 0),
            pos.get("expiry", ""),
            live_qty,
            pos.get("entry_credit", 0),
            0,
            "",
            pos.get("dte", 0),
            "Reconciled",
            0,
            "Manual reconcile reconstructed from IBKR/live snapshot",
        )
        actions.append({"type": "opened_from_ibkr", "key": key, "qty": live_qty})

    return actions

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
                closed_keys.add(journal_key(t.get("ticker", ""), t.get("strike", 0), t.get("expiry", "")))
        # Keep: open trades not closed, plus closed trades (for history)
        result=[]
        for t in trades:
            if t['status']=='open' and journal_key(t.get("ticker", ""), t.get("strike", 0), t.get("expiry", "")) in closed_keys:
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
                    enrich_live_snapshot(data)
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
                pending_scan = scan_now_state()
                payload["scan_pending"] = pending_scan
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
                mirror = mirror_state_payload()
                payload["mirror_state"] = mirror
                payload["scan_pending"] = pending_scan
                for sig in payload.get("signals", []):
                    if isinstance(sig, dict):
                        sig["copy_gate"] = signal_gate_payload(
                            sig,
                            signals_stale=payload.get("signals_stale", False),
                            scan_pending=pending_scan,
                            mirror=mirror,
                        )
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
        if p=="/api/real-positions":
            self._json(real_positions_payload()); return
        if p=="/api/earnings-calendar":
            self._json({"file": str(EARNINGS_CALENDAR_FILE), "calendar": load_earnings_calendar()}); return
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
        if p=="/api/trigger-scan":
            try:
                now = time.time()
                last = 0.0
                if SCAN_NOW_RATE_FILE.exists():
                    try:
                        last = float(SCAN_NOW_RATE_FILE.read_text().strip() or "0")
                    except:
                        last = 0.0
                remaining = int(SCAN_NOW_RATE_SECONDS - (now - last))
                if remaining > 0:
                    self._json({
                        "ok": False,
                        "error": f"Scan can only be triggered once every {SCAN_NOW_RATE_SECONDS // 60} minutes.",
                        "retry_after_secs": remaining,
                    }, 429); return
                queued_at = now_iso()
                SCAN_NOW_FILE.write_text(f"requested {queued_at}\n")
                SCAN_NOW_RATE_FILE.write_text(str(now))
                self._json({
                    "ok": True,
                    "queued_at": queued_at,
                    "marker_file": str(SCAN_NOW_FILE),
                    "next_allowed_in_secs": SCAN_NOW_RATE_SECONDS,
                }); return
            except Exception as e: self._json({"ok": False, "error": str(e)}, 500); return
        if p=="/api/real-positions":
            try:
                if isinstance(b.get("positions"), list):
                    self._json({"ok": True, **save_real_positions(b.get("positions"))}); return
                action = str(b.get("action", "upsert")).lower().strip()
                positions = load_real_positions()
                if action in {"upsert", "add", "update"}:
                    pos = normalize_real_position(b.get("position", b))
                    if not pos.get("ticker") or not pos.get("expiry") or pos.get("qty", 0) <= 0:
                        self._json({"ok": False, "error": "Real position requires ticker, expiry, and quantity."}, 400); return
                    replaced = False
                    for idx, existing in enumerate(positions):
                        if existing.get("id") == pos.get("id"):
                            positions[idx] = pos
                            replaced = True
                            break
                    if not replaced:
                        positions.append(pos)
                    self._json({"ok": True, **save_real_positions(positions)}); return
                if action == "delete":
                    pid = str(b.get("id", "")).strip()
                    positions = [p for p in positions if p.get("id") != pid]
                    self._json({"ok": True, **save_real_positions(positions)}); return
                if action == "close":
                    pid = str(b.get("id", "")).strip()
                    found = False
                    for pos in positions:
                        if pos.get("id") == pid:
                            pos["status"] = "closed"
                            pos["closed_at"] = b.get("closed_at") or now_iso()
                            if b.get("exit_debit") not in (None, ""):
                                pos["exit_debit"] = b.get("exit_debit")
                            found = True
                            break
                    if not found:
                        self._json({"ok": False, "error": "Real position not found."}, 404); return
                    self._json({"ok": True, **save_real_positions(positions)}); return
                self._json({"ok": False, "error": "Unsupported real-position action."}, 400); return
            except Exception as e: self._json({"ok": False, "error": str(e)}, 500); return
        if p=="/api/earnings-calendar":
            try:
                data = b.get("calendar", b)
                self._json({"ok": True, **save_earnings_calendar(data)}); return
            except Exception as e: self._json({"ok": False, "error": str(e)}, 500); return
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
        if p=="/api/reconcile-journal":
            try:
                confirm = str(b.get("confirm", "")).strip().upper()
                if confirm != "ACCEPT IBKR":
                    self._json({"ok": False, "error": "Confirmation phrase must be: ACCEPT IBKR"}, 400); return
                live = load_live_snapshot()
                if not live.get("engine_running") or not live.get("connected"):
                    self._json({"ok": False, "error": "Engine/TWS is not connected; refusing to rewrite journal from a stale broker view."}, 409); return
                snapshot_age = int(time.time() - LIVE_POSITIONS_FILE.stat().st_mtime) if LIVE_POSITIONS_FILE.exists() else 999999
                if live.get("snapshot_stale") or snapshot_age > 1800:
                    self._json({"ok": False, "error": "Live snapshot is stale; refresh engine data before reconciling."}, 409); return
                actions = reconcile_journal_to_live_snapshot(live)
                self._json({
                    "ok": True,
                    "actions": actions,
                    "count": len(actions),
                    "journal": str(JOURNAL),
                    "reconciliation": reconciliation_from_live_snapshot(live),
                }); return
            except Exception as e: self._json({"ok": False, "error": str(e)}, 500); return
        if p=="/api/paper-close":
            try:
                live = load_live_snapshot()
                if not live.get("engine_running") or not live.get("connected"):
                    self._json({"ok": False, "error": "Engine/TWS is not connected."}, 409); return
                snapshot_age = int(time.time() - LIVE_POSITIONS_FILE.stat().st_mtime) if LIVE_POSITIONS_FILE.exists() else 999999
                if live.get("snapshot_stale") or snapshot_age > 1800:
                    self._json({"ok": False, "error": "Live snapshot is stale; refresh engine data before closing."}, 409); return
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
                pending_key = journal_key(ticker, strike, expiry)
                pending_meta = None
                for item in live.get("pending_closes") or []:
                    if item.get("key") == pending_key:
                        pending_meta = item
                        break
                if match.get("pending_close") or pending_meta:
                    age = match.get("pending_close_age_secs")
                    if pending_meta and age is None:
                        age = pending_meta.get("age_secs")
                    remaining = match.get("pending_close_remaining_secs")
                    if pending_meta and remaining is None:
                        remaining = pending_meta.get("remaining_secs")
                    suffix = f" Pending age {format_age(age)}"
                    if remaining is not None:
                        suffix += f"; expires in {format_age(remaining)}."
                    self._json({"ok": False, "error": f"Close already pending for {ticker} {strike:g}P.{suffix}"}, 409); return
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
