#!/usr/bin/env python3
"""
Options Pro Ultra v6 — Autotrade Engine
Scans ETFs, scores opportunities, places trades, monitors positions via TWS API.
Usage: python3 autotrade_engine.py [scan_passes]
"""

import sys
import os
import csv
import time
import json
import math
import smtplib
import threading
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from zoneinfo import ZoneInfo

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ComboLeg
from ibapi.order import Order
from ibapi.common import TickerId

# ═══════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════
TWS_HOST = "127.0.0.1"
TWS_PORT = 7497
CLIENT_ID = 2
ACCOUNT_ID = "DU4735568"
ACCOUNT_SIZE = 250_000

# File paths
BASE = Path("/Applications/OptionsPro.app/Contents/Resources")
DATA_DIR = Path.home() / "options-pro" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def _data_file(name):
    path = DATA_DIR / name
    old_path = Path.home() / "Desktop" / name
    if not path.exists() and old_path.exists():
        try:
            path.write_bytes(old_path.read_bytes())
        except Exception:
            pass
    return path

JOURNAL = _data_file("autotrade_journal.csv")
LOG_FILE = _data_file("autotrade_log.txt")
RECONCILE_LOG = _data_file("reconcile_log.txt")
API_KEY_FILE = BASE / "api_key.txt"
KILL_SWITCH_FILE = _data_file("optionspro_kill_switch")
SIGNAL_ONLY_FILE = _data_file("optionspro_signal_only")
SIGNALS_FILE = _data_file("trade_signals.json")
LIVE_POSITIONS_FILE = _data_file("live_positions.json")
REAL_RULES_FILE = _data_file("real_account_rules.json")
MIRROR_KILL_FILE = _data_file("optionspro_real_mirror_kill")
SIGNAL_AUDIT_FILE = _data_file("signal_audit.json")

# Email config
EMAIL_FROM = "islamalbaz90@gmail.com"
EMAIL_TO = "islamalbaz90@gmail.com"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

def _load_email_pass():
    """Load Gmail app password from ~/options-pro/credentials.env or environment.
    Never commit this file to git. Format:
        GMAIL_APP_PASS=xxxxxxxxxxxxxxxx
    """
    # 1) Environment variable takes priority
    p = os.environ.get("GMAIL_APP_PASS", "").strip()
    if p:
        return p
    # 2) ~/options-pro/credentials.env
    try:
        env_file = Path.home() / "options-pro" / "credentials.env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line.startswith("GMAIL_APP_PASS="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return ""

EMAIL_PASS = _load_email_pass()

# ═══════════════════════════════════════════════
# TRADING RULES
# ═══════════════════════════════════════════════
MAX_POSITIONS = 5
MAX_RISK_PCT = 0.05  # 5% per trade
MAX_RISK = ACCOUNT_SIZE * MAX_RISK_PCT  # $12,500

# Watchlist & Tiers
TIER1 = ["SPY", "QQQ", "SMH", "GDX"]       # Spreads ONLY
TIER2 = ["IWM", "GLD", "XLE", "TLT"]       # Naked CSP OK
TIER3 = ["XSP", "QQQM"]                     # CSP mini
WATCHLIST = TIER1 + TIER2 + TIER3

# Spread widths per ticker (wider = more credit but more capital)
_TRADING_CLASS = {
    "SPY": "SPY", "QQQ": "QQQ", "IWM": "IWM", "SMH": "SMH",
    "GLD": "GLD", "GDX": "GDX", "XLE": "XLE", "TLT": "TLT",
    "XSP": "XSP", "QQQM": "QQQM", "DIA": "DIA", "EEM": "EEM",
}

SPREAD_WIDTHS = {
    "SPY": 10, "QQQ": 10, "SMH": 5, "GDX": 2,
}

# Delta / DTE targets
DELTA_MIN = 0.15
DELTA_MAX = 0.25
DTE_MIN = 21
DTE_MAX = 45
DTE_TARGET = 35

# Exit rules
PROFIT_TARGET_PCT = 0.50   # Close at 50% profit
STOP_LOSS_MULT = 2.0       # Stop at 2× credit received
DTE_EXIT = 21              # Close at 21 DTE if not profitable

# Signal/risk guardrails
# CSP sizing uses assignment exposure, not theoretical loss to zero after credit.
MAX_CSP_ASSIGNMENT_RISK = MAX_RISK
MAX_OPTION_BID_ASK_PCT = 0.35
MAX_OPTION_BID_ASK_ABS = 0.75
MIN_SPREAD_CREDIT_PCT = 0.15  # Credit should be at least 15% of spread width.

# ═══════════════════════════════════════════════
# MARKET REGIME — VIX-based adjustments
# ═══════════════════════════════════════════════
# Delta and sizing are dynamically adjusted based on VIX level.
# Low VIX = more conservative (wider strikes, smaller size)
# High VIX = more aggressive (richer premiums, closer strikes)
REGIMES = {
    "calm":      {"vix_max": 15, "delta_min": 0.10, "delta_max": 0.18, "risk_mult": 0.70, "label": "Calm"},
    "normal":    {"vix_max": 20, "delta_min": 0.15, "delta_max": 0.25, "risk_mult": 1.00, "label": "Normal"},
    "elevated":  {"vix_max": 30, "delta_min": 0.15, "delta_max": 0.22, "risk_mult": 0.85, "label": "Elevated"},
    "high":      {"vix_max": 40, "delta_min": 0.12, "delta_max": 0.18, "risk_mult": 0.60, "label": "High"},
    "extreme":   {"vix_max": 999, "delta_min": 0.08, "delta_max": 0.15, "risk_mult": 0.30, "label": "Extreme"},
}

# Correlation matrix (approximate)
CORRELATIONS = {
    ("SPY", "QQQ"): 0.92, ("SPY", "SMH"): 0.82, ("SPY", "IWM"): 0.88,
    ("SPY", "XLE"): 0.65, ("SPY", "GLD"): -0.15, ("SPY", "TLT"): -0.35,
    ("SPY", "GDX"): -0.10, ("SPY", "XSP"): 0.99, ("SPY", "QQQM"): 0.92,
    ("QQQ", "SMH"): 0.90, ("QQQ", "IWM"): 0.78, ("QQQ", "XLE"): 0.55,
    ("QQQ", "GLD"): -0.20, ("QQQ", "TLT"): -0.40, ("QQQ", "GDX"): -0.15,
    ("QQQ", "XSP"): 0.92, ("QQQ", "QQQM"): 0.99,
    ("SMH", "IWM"): 0.72, ("SMH", "XLE"): 0.50, ("SMH", "GLD"): -0.15,
    ("SMH", "TLT"): -0.30, ("SMH", "GDX"): -0.10, ("SMH", "XSP"): 0.82,
    ("SMH", "QQQM"): 0.90,
    ("IWM", "XLE"): 0.70, ("IWM", "GLD"): -0.10, ("IWM", "TLT"): -0.25,
    ("IWM", "GDX"): 0.00, ("IWM", "XSP"): 0.88, ("IWM", "QQQM"): 0.78,
    ("XLE", "GLD"): 0.10, ("XLE", "TLT"): -0.15, ("XLE", "GDX"): 0.30,
    ("XLE", "XSP"): 0.65, ("XLE", "QQQM"): 0.55,
    ("GLD", "TLT"): 0.35, ("GLD", "GDX"): 0.85, ("GLD", "XSP"): -0.15,
    ("GLD", "QQQM"): -0.20,
    ("TLT", "GDX"): 0.25, ("TLT", "XSP"): -0.35, ("TLT", "QQQM"): -0.40,
    ("GDX", "XSP"): -0.10, ("GDX", "QQQM"): -0.15,
    ("XSP", "QQQM"): 0.92,
}

# Scoring weights
SCORE_DELTA_WEIGHT = 20
SCORE_IV_WEIGHT = 20
SCORE_BUFFER_WEIGHT = 25
SCORE_DTE_WEIGHT = 15
SCORE_CORR_PENALTY = -25
SCORE_CORR_BONUS = 15

# Monitoring interval (seconds). This also drives the journal-vs-IBKR
# reconciliation cadence outside the nightly TWS restart window.
MONITOR_INTERVAL = 900  # 15 minutes
AUTO_CLOSE_UNMATCHED_LONGS = False  # Safety first: alert on unexpected longs, do not market-sell by default.
RECONCILE_ALERT_INTERVAL = 7200  # seconds between journal-vs-IBKR drift email alerts

FILLED_STATUSES = {"FILLED", "CLOSED", "MANUALCLOSE"}
OPEN_LIKE_STATUSES = {"SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT", "PENDING", "WORKING"}


# ═══════════════════════════════════════════════
# LOGGING & UTILITIES
# ═══════════════════════════════════════════════
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except:
        pass


def log_reconciliation(result):
    try:
        line = (
            f"{datetime.now().isoformat(timespec='seconds')},"
            f"status={result.get('status', 'checked')},"
            f"ok={result.get('ok')},"
            f"missing_in_ibkr={len(result.get('missing_in_ibkr', []))},"
            f"missing_in_journal={len(result.get('missing_in_journal', []))},"
            f"qty_mismatch={len(result.get('qty_mismatch', []))},"
            f"reason={result.get('reason', '')}"
        )
        with open(RECONCILE_LOG, "a") as f:
            f.write(line + "\n")
    except:
        pass


def send_email(subject, body_html):
    if not EMAIL_PASS:
        log(f"  ⚠ Email skipped (no GMAIL_APP_PASS in env or ~/options-pro/credentials.env): {subject}")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.ehlo()
            s.starttls()
            s.login(EMAIL_FROM, EMAIL_PASS)
            s.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        log(f"  📧 Email sent: {subject}")
    except Exception as e:
        log(f"  ⚠ Email failed: {e}")


def get_correlation(a, b):
    if a == b:
        return 1.0
    key = (a, b) if (a, b) in CORRELATIONS else (b, a)
    return CORRELATIONS.get(key, 0.0)


def get_tier(ticker):
    if ticker in TIER1:
        return 1
    if ticker in TIER2:
        return 2
    if ticker in TIER3:
        return 3
    return 0


def get_strategy(ticker):
    tier = get_tier(ticker)
    if tier == 1:
        return "BPS"  # Bull Put Spread
    return "CSP"       # Cash Secured Put


def kill_switch_active():
    """True means monitor existing positions but do not open new trades."""
    return KILL_SWITCH_FILE.exists()


def signal_only_active():
    """True means generate signals but do not submit opening orders."""
    return SIGNAL_ONLY_FILE.exists()


def _signal_id(signal):
    parts = [
        signal.get("ticker", ""),
        signal.get("strategy", ""),
        str(signal.get("strike", "")),
        str(signal.get("long_strike") or ""),
        str(signal.get("expiry", "")),
    ]
    return "-".join(str(p).replace(" ", "") for p in parts).upper()


def _load_real_rules():
    defaults = {
        "enabled": False,
        "capital": 0,
        "max_risk_per_trade_pct": 1.0,
        "max_risk_per_trade_dollars": 0,
        "allowed_tickers": WATCHLIST,
        "allowed_strategies": ["BPS", "CSP"],
    }
    try:
        if REAL_RULES_FILE.exists():
            saved = json.loads(REAL_RULES_FILE.read_text())
            if isinstance(saved, dict):
                defaults.update(saved)
    except Exception:
        pass
    return defaults


def _real_copyability(signal):
    rules = _load_real_rules()
    reasons = []
    if MIRROR_KILL_FILE.exists():
        reasons.append("mirror_kill")
    if not rules.get("enabled") or float(rules.get("capital") or 0) <= 0:
        reasons.append("rules_not_configured")
    if signal.get("ticker") not in set(rules.get("allowed_tickers") or []):
        reasons.append("ticker_not_allowed")
    if signal.get("strategy") not in set(rules.get("allowed_strategies") or []):
        reasons.append("strategy_not_allowed")

    paper_qty = max(1, int(signal.get("qty") or 1))
    total_risk = float(signal.get("estimated_risk") or 0)
    unit_risk = total_risk / paper_qty if total_risk > 0 else 0
    capital = float(rules.get("capital") or 0)
    pct_cap = capital * (float(rules.get("max_risk_per_trade_pct") or 0) / 100)
    hard_cap = float(rules.get("max_risk_per_trade_dollars") or 0)
    max_risk = min(hard_cap, pct_cap) if hard_cap > 0 else pct_cap
    real_qty = int(max_risk // unit_risk) if unit_risk > 0 and max_risk > 0 else 0
    if real_qty < 1:
        reasons.append("real_qty_zero")

    return {
        "copyable": not reasons,
        "real_qty": max(0, real_qty),
        "reasons": reasons,
        "max_real_risk": round(max_risk, 2),
    }


def _update_signal_audit(payload):
    try:
        audit = {"signals": {}, "events": []}
        if SIGNAL_AUDIT_FILE.exists():
            loaded = json.loads(SIGNAL_AUDIT_FILE.read_text())
            if isinstance(loaded, dict):
                audit.update(loaded)
        now = payload.get("generated") or datetime.now().isoformat(timespec="seconds")
        for sig in payload.get("signals", []):
            sid = sig.get("id") or _signal_id(sig)
            sig["id"] = sid
            rec = audit["signals"].setdefault(sid, {
                "id": sid,
                "ticker": sig.get("ticker"),
                "strategy": sig.get("strategy"),
                "strike": sig.get("strike"),
                "long_strike": sig.get("long_strike"),
                "expiry": sig.get("expiry"),
                "first_seen": now,
                "seen_count": 0,
                "copyable_seen_count": 0,
            })
            rec["last_seen"] = now
            rec["seen_count"] = int(rec.get("seen_count") or 0) + 1
            rec["last_score"] = sig.get("score")
            rec["last_copyable"] = bool(sig.get("copyable"))
            rec["last_real_qty"] = sig.get("real_qty")
            if sig.get("copyable"):
                rec["copyable_seen_count"] = int(rec.get("copyable_seen_count") or 0) + 1
            audit["events"].append({
                "generated": now,
                "id": sid,
                "ticker": sig.get("ticker"),
                "strategy": sig.get("strategy"),
                "strike": sig.get("strike"),
                "expiry": sig.get("expiry"),
                "score": sig.get("score"),
                "copyable": bool(sig.get("copyable")),
                "real_qty": sig.get("real_qty"),
            })
        audit["events"] = audit.get("events", [])[-1000:]
        audit["updated"] = now
        SIGNAL_AUDIT_FILE.write_text(json.dumps(audit, indent=2))
    except Exception as e:
        log(f"  ⚠ Signal audit update failed: {e}")


# ═══════════════════════════════════════════════
# JOURNAL MANAGEMENT
# ═══════════════════════════════════════════════
JOURNAL_HEADER = "timestamp,action,ticker,strategy,strike,expiry,qty,credit,delta,iv,dte,status,pnl,notes"


def ensure_journal():
    if not JOURNAL.exists():
        with open(JOURNAL, "w") as f:
            f.write(JOURNAL_HEADER + "\n")


def write_journal(action, ticker, strategy, strike, expiry, qty, credit, delta, iv, dte, status, pnl, notes):
    ensure_journal()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    iv_str = f"{iv:.2f}%" if isinstance(iv, float) else str(iv)
    row = [ts, action, ticker, strategy, strike, expiry, qty, credit, delta, iv_str, dte, status, pnl, notes]
    with open(JOURNAL, "a", newline="") as f:
        csv.writer(f).writerow(row)
    log(f"  📝 Journal: {action} {ticker} ${strike} x{qty}")


def write_trade_signals(opportunities, mode="paper_auto"):
    """Write vetted scan results for manual review/copying to a real account."""
    signals = []
    for opp in opportunities:
        risk = 0
        if opp.get("strategy") == "BPS" and opp.get("long_strike"):
            width = float(opp["strike"]) - float(opp["long_strike"])
            risk = max(0, (width - float(opp.get("net_credit", 0))) * 100 * int(opp.get("qty", 0)))
        else:
            risk = float(opp.get("strike", 0)) * 100 * int(opp.get("qty", 0))
        signal = {
            "ticker": opp.get("ticker"),
            "strategy": opp.get("strategy"),
            "strike": opp.get("strike"),
            "long_strike": opp.get("long_strike"),
            "expiry": opp.get("expiry"),
            "qty": opp.get("qty"),
            "credit": round(float(opp.get("net_credit", opp.get("premium", 0))), 2),
            "delta": round(float(opp.get("delta", 0)), 3),
            "iv": round(float(opp.get("iv", 0)) * 100, 1),
            "dte": opp.get("dte"),
            "score": opp.get("score"),
            "estimated_risk": round(risk, 2),
            "buffer_pct": round(float(opp.get("buffer", 0)), 1),
            "warnings": [
                "Paper/delayed data signal. Verify live bid/ask in real account before copying.",
                "Do not copy if portfolio risk, correlation, or news/event risk is elevated.",
            ],
        }
        signal["id"] = _signal_id(signal)
        copy_state = _real_copyability(signal)
        signal.update(copy_state)
        opp["signal_id"] = signal["id"]
        opp["real_qty"] = signal["real_qty"]
        opp["copyable"] = signal["copyable"]
        signals.append(signal)
    payload = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "mode": mode,
        "account": ACCOUNT_ID,
        "max_positions": MAX_POSITIONS,
        "signals": signals,
        "audit": {
            "file": str(SIGNAL_AUDIT_FILE),
            "total": len(signals),
            "copyable": sum(1 for s in signals if s.get("copyable")),
        },
    }
    _update_signal_audit(payload)
    with open(SIGNALS_FILE, "w") as f:
        json.dump(payload, f, indent=2)
    log(f"  📡 Signals written: {len(signals)} candidate(s) -> {SIGNALS_FILE}")


def read_open_positions():
    """Read open positions from journal, accounting for closed trades."""
    ensure_journal()
    positions = {}
    with open(JOURNAL, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("ticker", "").strip()
            action = row.get("action", "").strip().upper()
            strike_raw = row.get("strike", "0").strip()
            try:
                strike_f = float(strike_raw)
            except:
                strike_f = 0
            # Normalize key: ticker + float strike (handles "390" vs "390.0")
            key = f"{ticker}-{strike_f}"

            if action == "OPEN":
                positions[key] = {
                    "ticker": ticker,
                    "strategy": row.get("strategy", "CSP").strip(),
                    "strike": strike_f,
                    "expiry": row.get("expiry", "").strip(),
                    "qty": int(row.get("qty", "1").strip()),
                    "credit": float(row.get("credit", "0").strip()),
                    "delta": float(row.get("delta", "0").strip()),
                    "iv": row.get("iv", "0").strip().replace("%", ""),
                    "dte": int(row.get("dte", "0").strip()),
                    "status": row.get("status", "").strip(),
                    "notes": row.get("notes", "").strip(),
                }
            elif action.startswith("CLOSE"):
                status = row.get("status", "").strip().upper()
                # A submitted close order is only a working order, not a closed
                # trade. Remove the OPEN only after a fill/manual reconciliation,
                # or when explicitly cancelling a never-filled OPEN ghost.
                if key in positions and (status in FILLED_STATUSES or action in {"CLOSE_CANCEL", "CLOSE_MANUAL"}):
                    del positions[key]
    return positions


# ═══════════════════════════════════════════════
# TWS API CONNECTION
# ═══════════════════════════════════════════════
class TWSApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.next_order_id = None
        self.positions = {}
        self.market_data = {}
        self.option_chains = {}
        self.contract_details = {}
        self.order_statuses = {}
        self._data_events = {}
        self._price_events = {}
        self._chain_events = {}
        self._detail_events = {}
        self._connected = False
        self._connection_lost_at = None
        self._last_farm_error = None
        self._account_values = {}

    # ── Connection ──
    def nextValidId(self, orderId):
        self.next_order_id = orderId
        self._connected = True
        log(f"✅ TWS connected — next order ID: {orderId}")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        # Filter informational messages
        if errorCode in (2104, 2106, 2158, 2107):
            return  # Market data connection messages (benign)
        if errorCode == 10091:
            return  # Partial data access warning

        # ── CRITICAL DISCONNECTS — flag for reconnect ──
        if errorCode == 1100:
            # "Connectivity between IBKR and TWS has been lost"
            if self._connected:
                log(f"  🔴 TWS error 1100: IBKR connectivity LOST — marking disconnected")
                self._connected = False
                self._connection_lost_at = datetime.now()
            return
        if errorCode == 1101:
            # "Connectivity restored — data lost"
            log(f"  🟡 TWS error 1101: Connection restored (data lost) — reconnecting")
            self._connected = False
            return
        if errorCode == 1102:
            # "Connectivity restored — data maintained"
            log(f"  🟢 TWS error 1102: Connection fully restored")
            self._connected = True
            self._connection_lost_at = None
            return
        if errorCode == 1300:
            # "Socket port has been reset and this connection is being dropped"
            log(f"  🔴 TWS error 1300: Socket reset — reconnect needed")
            self._connected = False
            return
        if errorCode == 2110:
            # "Connectivity between TWS and server is broken"
            log(f"  🔴 TWS error 2110: TWS-server connection broken")
            self._connected = False
            return

        # Farm broken messages are informational — log once not spam
        if errorCode in (2103, 2105, 2157):
            # Only log if not seen recently
            now = datetime.now()
            last = getattr(self, '_last_farm_error', None)
            if not last or (now - last).total_seconds() > 60:
                log(f"  ⚠ TWS error {errorCode}: data farm issue")
                self._last_farm_error = now
            return

        if errorCode == 2119:
            log(f"  ⚠ TWS market data delayed")
            return
        if errorCode in (200, 162, 321, 354):
            log(f"  ⚠ TWS error {errorCode}: {errorString}")
        else:
            log(f"  TWS error {errorCode}: {errorString}")

    def connectionClosed(self):
        self._connected = False
        self._connection_lost_at = datetime.now()
        log("❌ TWS connection lost (connectionClosed callback)")

    # ── Account & Positions ──
    def position(self, account, contract, pos, avgCost):
        if pos != 0 and contract.secType == "OPT":
            # IBKR reports option avgCost as total per contract, not per share
            cost_per_share = avgCost / 100
            key = f"{contract.symbol}-{contract.strike}"
            self.positions[key] = {
                "account": account,
                "symbol": contract.symbol,
                "secType": contract.secType,
                "strike": contract.strike,
                "right": contract.right,
                "expiry": contract.lastTradeDateOrContractMonth,
                "position": pos,
                "avgCost": cost_per_share,
                "exchange": contract.exchange,
                "conId": contract.conId,
            }

    def positionEnd(self):
        log(f"  📊 {len(self.positions)} option position(s) loaded from IBKR")
        for k, v in self.positions.items():
            log(f"     IBKR: {k} = {v['symbol']} ${v['strike']} {v['right']} x{v['position']} avg=${v['avgCost']:.2f}")

    def accountSummary(self, reqId, account, tag, value, currency):
        self._account_values[tag] = value

    def accountSummaryEnd(self, reqId):
        pass

    # ── Market Data ──
    def tickPrice(self, reqId, tickType, price, attrib):
        if price <= 0:
            return
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        # tickType: 1=bid, 2=ask, 4=last, 6=high, 7=low, 9=close, 66=delayed_bid, 67=delayed_ask, 68=delayed_last
        type_map = {1: "bid", 2: "ask", 4: "last", 6: "high", 7: "low", 9: "close",
                    66: "bid", 67: "ask", 68: "last", 73: "high", 74: "low", 75: "close"}
        name = type_map.get(tickType)
        if name:
            self.market_data[reqId][name] = price
            # Signal data ready when we have bid+ask or last
            if reqId in self._price_events:
                md = self.market_data[reqId]
                if ("bid" in md and "ask" in md) or "last" in md:
                    self._price_events[reqId].set()

    def tickSize(self, reqId, tickType, size):
        pass

    def tickGeneric(self, reqId, tickType, value):
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        # tickType 24 = impliedVolatility (delayed: 58)
        if tickType in (24, 58) and value > 0:
            self.market_data[reqId]["iv"] = value

    def tickOptionComputation(self, reqId, tickType, tickAttrib, impliedVol,
                                delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        if impliedVol and impliedVol > 0:
            self.market_data[reqId]["iv"] = impliedVol
        if delta is not None:
            self.market_data[reqId]["delta"] = delta
        if optPrice and optPrice > 0:
            self.market_data[reqId]["optPrice"] = optPrice
        if undPrice and undPrice > 0:
            self.market_data[reqId]["undPrice"] = undPrice
        if gamma is not None:
            self.market_data[reqId]["gamma"] = gamma
        if theta is not None:
            self.market_data[reqId]["theta"] = theta
        if vega is not None:
            self.market_data[reqId]["vega"] = vega
        # Fire event when we have usable data (delta + price).
        # Don't gate on tickType==13 — after-hours/delayed data only sends
        # tickType 10/11 (bid/ask). We fire as soon as we have real values.
        md = self.market_data.get(reqId, {})
        has_delta = md.get("delta") is not None and md.get("delta") not in (-1, -2)
        has_price = (md.get("optPrice", 0) or 0) > 0
        if (has_delta or has_price) and reqId in self._price_events:
            self._price_events[reqId].set()

    # ── Contract Details (for resolving conId) ──
    def contractDetails(self, reqId, contractDetails):
        self.contract_details[reqId] = contractDetails

    def contractDetailsEnd(self, reqId):
        if reqId in self._detail_events:
            self._detail_events[reqId].set()

    # ── Security Definition Option Parameters (chain discovery) ──
    def securityDefinitionOptionParameter(self, reqId, exchange, underlyingConId,
                                            tradingClass, multiplier, expirations, strikes):
        if reqId not in self.option_chains:
            self.option_chains[reqId] = {"expirations": set(), "strikes": set()}
        self.option_chains[reqId]["expirations"].update(expirations)
        self.option_chains[reqId]["strikes"].update(strikes)

    def securityDefinitionOptionParameterEnd(self, reqId):
        if reqId in self._chain_events:
            self._chain_events[reqId].set()

    # ── Order Status ──
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        self.order_statuses[orderId] = {
            "status": status, "filled": filled, "remaining": remaining,
            "avgFillPrice": avgFillPrice,
        }
        if status == "Filled":
            log(f"  ✅ Order {orderId} FILLED at ${avgFillPrice:.2f}")
        elif status == "Cancelled":
            log(f"  ❌ Order {orderId} CANCELLED")

    def openOrder(self, orderId, contract, order, orderState):
        pass

    def execDetails(self, reqId, contract, execution):
        log(f"  📋 Exec: {execution.side} {execution.shares}x {contract.symbol} "
            f"@ ${execution.price:.2f} (OrderID {execution.orderId})")


# ═══════════════════════════════════════════════
# ENGINE CLASS
# ═══════════════════════════════════════════════
class AutoTradeEngine:
    def __init__(self, scan_passes=1):
        self.app = TWSApp()
        self.scan_passes = scan_passes
        self._req_id = 1000
        self._running = True
        self._pending_orders = {}  # key: "TICKER-STRIKE" -> timestamp of order placement
        self._pending_closes = {}  # key: "TICKER-STRIKE" -> timestamp of close order placement
        self._last_reconcile_alert = None

    def next_req_id(self):
        self._req_id += 1
        return self._req_id

    # ── Connect ──
    def connect(self):
        log("🔌 Connecting to TWS...")
        self.app.connect(TWS_HOST, TWS_PORT, CLIENT_ID)

        # Start API thread
        api_thread = threading.Thread(target=self.app.run, daemon=True)
        api_thread.start()

        # Wait for connection
        for i in range(30):
            if self.app._connected:
                break
            time.sleep(1)

        if not self.app._connected:
            log("❌ Failed to connect to TWS after 30s")
            return False

        # Request delayed market data (type 3)
        self.app.reqMarketDataType(3)
        time.sleep(1)

        # Load existing positions from IBKR
        self.app.reqPositions()
        time.sleep(3)

        return True

    def reconnect(self):
        """Try to reconnect to TWS after disconnection."""
        log("🔄 Attempting reconnect...")
        try:
            self.app.disconnect()
        except:
            pass
        time.sleep(5)

        self.app = TWSApp()
        return self.connect()

    # ── Market Data Helpers ──
    def get_stock_price(self, ticker, timeout=10):
        """Get current stock (or index) price."""
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = ticker
        contract.currency = "USD"
        if ticker in self.INDEX_TICKERS:
            # XSP is an index — IND on CBOE, not STK on SMART
            contract.secType = "IND"
            contract.exchange = self.PRIMARY_EXCHANGES.get(ticker, "CBOE")
        else:
            contract.secType = "STK"
            contract.exchange = "SMART"

        event = threading.Event()
        self.app._price_events[req_id] = event
        self.app.market_data[req_id] = {}

        self.app.reqMktData(req_id, contract, "", False, False, [])
        event.wait(timeout=timeout)
        self.app.cancelMktData(req_id)

        md = self.app.market_data.get(req_id, {})
        price = md.get("last") or md.get("close")
        if not price and "bid" in md and "ask" in md:
            price = (md["bid"] + md["ask"]) / 2
        return price

    def get_option_data(self, ticker, strike, expiry, right="P", timeout=10):
        """Get option price, IV, delta, greeks."""
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "OPT"
        # XSP options are CBOE-listed, not SMART-routed
        contract.exchange = "CBOE" if ticker in self.INDEX_TICKERS else "SMART"
        contract.currency = "USD"
        contract.strike = strike
        contract.lastTradeDateOrContractMonth = expiry
        contract.right = right
        contract.multiplier = "100"

        event = threading.Event()
        self.app._price_events[req_id] = event
        self.app.market_data[req_id] = {}

        # tradingClass required — without it IBKR returns error 200 for ETF options
        contract.tradingClass = _TRADING_CLASS.get(ticker, ticker)
        
        self.app.reqMktData(req_id, contract, "106", False, False, [])
        event.wait(timeout=timeout)
        self.app.cancelMktData(req_id)

        md = self.app.market_data.get(req_id, {})
        bid = md.get("bid", 0) or 0
        ask = md.get("ask", 0) or 0

        # Price fallback: use bid/ask mid if optPrice not populated by model tick
        if not md.get("optPrice"):
            if bid > 0 and ask > 0:
                md["optPrice"] = (bid + ask) / 2
        price = md.get("optPrice") or md.get("last")
        if not price and "bid" in md and "ask" in md:
            price = (md["bid"] + md["ask"]) / 2
        spread = (ask - bid) if bid > 0 and ask > 0 else 0
        spread_pct = (spread / price) if price and spread > 0 else 0

        return {
            "price": price or 0,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "spread_pct": spread_pct,
            "quote_valid": bool((price or 0) > 0 and (not spread or spread > 0)),
            "iv": md.get("iv", 0),
            "delta": md.get("delta", 0),
            "gamma": md.get("gamma", 0),
            "theta": md.get("theta", 0),
            "vega": md.get("vega", 0),
            "undPrice": md.get("undPrice", 0),
        }

    # Primary exchange mapping for ETFs (needed for unambiguous conId resolution)
    PRIMARY_EXCHANGES = {
        "SPY": "ARCA", "QQQ": "NASDAQ", "SMH": "NASDAQ", "GDX": "ARCA",
        "IWM": "ARCA", "GLD": "ARCA", "XLE": "ARCA", "TLT": "NASDAQ",
        "XSP": "CBOE", "QQQM": "NASDAQ",
    }

    # Tickers that are INDICES (not stocks/ETFs).
    # XSP is Mini-SPX — cash-settled index, not an ETF.
    INDEX_TICKERS = {"XSP"}

    def _underlying_sec_type(self, ticker):
        return "IND" if ticker in self.INDEX_TICKERS else "STK"

    def get_con_id(self, ticker, sec_type=None, timeout=10):
        """Resolve a ticker's conId via reqContractDetails.
        Pass sec_type='IND' for index tickers like XSP (defaults auto-detected)."""
        if sec_type is None:
            sec_type = self._underlying_sec_type(ticker)
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = ticker
        contract.secType = sec_type
        contract.currency = "USD"
        if sec_type == "IND":
            # Indices trade on their native exchange (CBOE for XSP)
            contract.exchange = self.PRIMARY_EXCHANGES.get(ticker, "CBOE")
        else:
            contract.exchange = "SMART"
            # Set primaryExchange to avoid ambiguity errors
            pex = self.PRIMARY_EXCHANGES.get(ticker)
            if pex:
                contract.primaryExchange = pex

        event = threading.Event()
        self.app._detail_events[req_id] = event
        self.app.contract_details[req_id] = None

        self.app.reqContractDetails(req_id, contract)
        event.wait(timeout=timeout)

        details = self.app.contract_details.get(req_id)
        if details:
            con_id = details.contract.conId
            log(f"  {ticker}: conId={con_id} ({sec_type})")
            return con_id
        log(f"  {ticker}: could not resolve conId (sec_type={sec_type})")
        return None

    def get_option_con_id(self, ticker, strike, expiry, right="P", timeout=10):
        """Resolve an option contract's conId — needed for combo legs."""
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "OPT"
        contract.exchange = "CBOE" if ticker in self.INDEX_TICKERS else "SMART"
        contract.currency = "USD"
        contract.strike = strike
        contract.lastTradeDateOrContractMonth = str(expiry)
        contract.right = right
        contract.multiplier = "100"

        event = threading.Event()
        self.app._detail_events[req_id] = event
        self.app.contract_details[req_id] = None

        self.app.reqContractDetails(req_id, contract)
        event.wait(timeout=timeout)

        details = self.app.contract_details.get(req_id)
        if details:
            return details.contract.conId
        return None

    def get_vix(self, timeout=10):
        """Fetch current VIX level from TWS."""
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = "VIX"
        contract.secType = "IND"
        contract.exchange = "CBOE"
        contract.currency = "USD"

        event = threading.Event()
        self.app._price_events[req_id] = event
        self.app.market_data[req_id] = {}

        self.app.reqMktData(req_id, contract, "", False, False, [])
        event.wait(timeout=timeout)
        self.app.cancelMktData(req_id)

        md = self.app.market_data.get(req_id, {})
        vix = md.get("last") or md.get("close")
        if not vix and md.get("bid") and md.get("ask"):
            vix = (md["bid"] + md["ask"]) / 2
        return vix

    def detect_regime(self, vix):
        """Return regime dict based on VIX level."""
        if vix is None or vix <= 0:
            return REGIMES["normal"]  # Default if VIX unavailable
        for name in ["calm", "normal", "elevated", "high", "extreme"]:
            r = REGIMES[name]
            if vix <= r["vix_max"]:
                return {**r, "name": name, "vix": vix}
        return REGIMES["extreme"]

    def get_option_chain(self, ticker, timeout=10):
        """Get available expirations and strikes for a ticker."""
        sec_type = self._underlying_sec_type(ticker)  # "IND" for XSP, "STK" otherwise

        # Resolve the conId with correct underlying secType
        con_id = self.get_con_id(ticker, sec_type=sec_type, timeout=timeout)
        if not con_id:
            log(f"  {ticker}: could not resolve conId")
            return {"expirations": [], "strikes": []}

        req_id = self.next_req_id()

        event = threading.Event()
        self.app._chain_events[req_id] = event
        self.app.option_chains[req_id] = {"expirations": set(), "strikes": set()}

        # futFopExchange ("") is ONLY for futures options. For STK/IND underlyings
        # it must be empty — passing "SMART" here causes error 200.
        # Underlying secType must match what the conId resolves to.
        self.app.reqSecDefOptParams(req_id, ticker, "", sec_type, con_id)
        event.wait(timeout=timeout)

        chain = self.app.option_chains.get(req_id, {})
        return {
            "expirations": sorted(chain.get("expirations", set())),
            "strikes": sorted(chain.get("strikes", set())),
        }

    # ── Scoring ──
    def score_opportunity(self, ticker, strike, stock_price, opt_data, dte, open_tickers):
        """Score an opportunity 0-100."""
        score = 0
        delta = abs(opt_data.get("delta", 0))
        iv = opt_data.get("iv", 0)
        premium = opt_data.get("price", 0)
        buffer = (stock_price - strike) / stock_price * 100 if stock_price > 0 else 0

        # Delta score (sweet spot 0.15–0.25, ideal ~0.20)
        if DELTA_MIN <= delta <= DELTA_MAX:
            # Closer to 0.20 is better
            delta_score = SCORE_DELTA_WEIGHT * (1 - abs(delta - 0.20) / 0.05)
            score += max(0, delta_score)

        # IV score (higher IV = more premium = better)
        if iv > 0:
            iv_pct = iv * 100
            if iv_pct >= 30:
                score += SCORE_IV_WEIGHT
            elif iv_pct >= 20:
                score += SCORE_IV_WEIGHT * 0.7
            elif iv_pct >= 15:
                score += SCORE_IV_WEIGHT * 0.4

        # Buffer score (distance from stock to strike)
        if buffer >= 10:
            score += SCORE_BUFFER_WEIGHT
        elif buffer >= 7:
            score += SCORE_BUFFER_WEIGHT * 0.8
        elif buffer >= 5:
            score += SCORE_BUFFER_WEIGHT * 0.6
        elif buffer >= 3:
            score += SCORE_BUFFER_WEIGHT * 0.3

        # DTE score (closer to target DTE is better)
        if DTE_MIN <= dte <= DTE_MAX:
            dte_score = SCORE_DTE_WEIGHT * (1 - abs(dte - DTE_TARGET) / (DTE_MAX - DTE_MIN))
            score += max(0, dte_score)

        # Correlation penalty/bonus
        for open_ticker in open_tickers:
            corr = abs(get_correlation(ticker, open_ticker))
            if corr > 0.80:
                score += SCORE_CORR_PENALTY
            elif corr < 0.30:
                score += SCORE_CORR_BONUS

        return max(0, min(100, round(score)))

    def quote_is_acceptable(self, ticker, opt_data, context=""):
        """Reject missing or excessively wide option quotes before signaling/trading."""
        price = opt_data.get("price", 0) or opt_data.get("optPrice", 0) or 0
        bid = opt_data.get("bid", 0) or 0
        ask = opt_data.get("ask", 0) or 0
        spread = opt_data.get("spread", 0) or ((ask - bid) if bid > 0 and ask > 0 else 0)
        spread_pct = opt_data.get("spread_pct", 0) or ((spread / price) if price > 0 and spread > 0 else 0)
        label = f"{ticker} {context}".strip()
        if price <= 0:
            return False, f"{label}: missing option price"
        if bid > 0 and ask > 0:
            if spread > MAX_OPTION_BID_ASK_ABS and spread_pct > MAX_OPTION_BID_ASK_PCT:
                return False, (
                    f"{label}: quote too wide bid=${bid:.2f} ask=${ask:.2f} "
                    f"spread={spread_pct*100:.0f}%"
                )
        return True, ""

    # ── Position Sizing ──
    def calc_position_size(self, ticker, strike, premium, stock_price):
        """Calculate number of contracts based on tier, risk rules, and regime."""
        # Regime-adjusted max risk
        regime = getattr(self, '_current_regime', None) or REGIMES["normal"]
        risk_mult = regime.get("risk_mult", 1.0)
        adjusted_max_risk = MAX_RISK * risk_mult

        # CSP risk for sizing is assignment exposure, not loss-to-zero after
        # credit. If one contract exceeds the budget, return 0 and skip.
        max_loss_per_contract = strike * 100

        if max_loss_per_contract <= 0:
            return 0

        max_contracts = int(adjusted_max_risk / max_loss_per_contract)
        if max_contracts <= 0:
            return 0

        # Additional caps based on stock price tier
        if stock_price > 500:
            cap = 2   # Large ETFs (SPY, QQQ): max 2 contracts
        elif stock_price > 100:
            cap = 3   # Mid-price (SMH, GDX, IWM): max 3
        elif stock_price > 50:
            cap = 4   # Lower price (XLE, TLT): max 4
        else:
            cap = 5   # Mini (QQQM-sized): max 5

        return min(max_contracts, cap)

    # ── Find Best Expiry ──
    def find_target_expiry(self, expirations):
        """Find the expiry closest to DTE_TARGET days out."""
        today = datetime.now()
        best = None
        best_diff = float("inf")
        for exp in expirations:
            try:
                exp_date = datetime.strptime(exp, "%Y%m%d")
                dte = (exp_date - today).days
                if DTE_MIN <= dte <= DTE_MAX:
                    diff = abs(dte - DTE_TARGET)
                    if diff < best_diff:
                        best_diff = diff
                        best = exp
                        best_dte = dte
            except:
                continue
        return best

    # ── Find Best Strike ──
    def find_target_strike(self, ticker, stock_price, strikes, expiry):
        """Find the put strike with delta closest to target range.
        Uses regime-adjusted delta bounds when available.
        Falls back to buffer-based selection if delta data unavailable."""
        # Get regime-adjusted delta bounds (fall back to config defaults)
        regime = getattr(self, '_current_regime', None) or REGIMES["normal"]
        d_min = regime.get("delta_min", DELTA_MIN)
        d_max = regime.get("delta_max", DELTA_MAX)
        d_target = (d_min + d_max) / 2

        # Filter strikes below current price (OTM puts)
        otm_strikes = [s for s in strikes if s < stock_price * 0.98 and s > stock_price * 0.80]
        if not otm_strikes:
            return None, None

        otm_strikes.sort(reverse=True)

        best_strike = None
        best_data = None
        best_delta_fit = float("inf")

        buffer_target = stock_price * 0.93
        best_buffer_strike = None
        best_buffer_data = None
        best_buffer_dist = float("inf")

        for strike in otm_strikes[:10]:
            opt = self.get_option_data(ticker, strike, expiry, "P")
            delta = abs(opt.get("delta", 0))
            # get_option_data returns "optPrice" — check both keys for safety
            price = opt.get("optPrice", 0) or opt.get("price", 0)

            if price <= 0:
                continue
            ok, reason = self.quote_is_acceptable(ticker, {**opt, "price": price}, f"${strike}P")
            if not ok:
                log(f"  {reason}")
                continue

            # Use regime-adjusted delta range
            if d_min <= delta <= d_max:
                fit = abs(delta - d_target)
                if fit < best_delta_fit:
                    best_delta_fit = fit
                    best_strike = strike
                    best_data = {**opt, "price": price}  # normalize key

            dist = abs(strike - buffer_target)
            if dist < best_buffer_dist:
                best_buffer_dist = dist
                best_buffer_strike = strike
                best_buffer_data = {**opt, "price": price}

        if best_strike:
            return best_strike, best_data

        if best_buffer_strike and best_buffer_data and best_buffer_data.get("price", 0) > 0.50:
            log(f"  {ticker}: using buffer-based strike (delta unavailable)")
            return best_buffer_strike, best_buffer_data

        return None, None

    # ── Place Order ──
    def place_bull_put_spread(self, ticker, short_strike, long_strike, expiry, qty, net_credit):
        """Place a bull put spread as a combo order.
        Sells a higher-strike put and buys a lower-strike put.
        Net credit = short premium - long premium."""
        if self.app.next_order_id is None:
            log("  ❌ No valid order ID — cannot place spread")
            return None

        # Get conIds for both legs
        short_conid = self.get_option_con_id(ticker, short_strike, expiry, "P")
        long_conid = self.get_option_con_id(ticker, long_strike, expiry, "P")

        if not short_conid or not long_conid:
            log(f"  ❌ Could not resolve conIds for spread ({short_conid}, {long_conid})")
            return None

        order_id = self.app.next_order_id
        self.app.next_order_id += 1

        # Build combo contract (BAG)
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "BAG"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # Short leg (sell higher strike)
        short_leg = ComboLeg()
        short_leg.conId = short_conid
        short_leg.ratio = 1
        short_leg.action = "SELL"
        short_leg.exchange = "SMART"

        # Long leg (buy lower strike)
        long_leg = ComboLeg()
        long_leg.conId = long_conid
        long_leg.ratio = 1
        long_leg.action = "BUY"
        long_leg.exchange = "SMART"

        contract.comboLegs = [short_leg, long_leg]

        # Order: SELL combo for net credit
        order = Order()
        order.action = "SELL"  # Sell the spread = receive credit
        order.totalQuantity = qty
        order.orderType = "LMT"
        order.lmtPrice = round(net_credit, 2)
        order.tif = "DAY"
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        order.account = ACCOUNT_ID

        log(f"  📤 SPREAD: SELL {qty}x {ticker} ${short_strike}/{long_strike}P @ ${net_credit:.2f} credit (ID: {order_id})")

        try:
            self.app.placeOrder(order_id, contract, order)
            return order_id
        except Exception as e:
            log(f"  ❌ Spread placement failed: {e}")
            return None

    def place_sell_put(self, ticker, strike, expiry, qty, limit_price, strategy="CSP"):
        """Place a sell put order (or spread for Tier 1)."""
        if self.app.next_order_id is None:
            log("  ❌ No valid order ID — cannot place order")
            return None

        order_id = self.app.next_order_id
        self.app.next_order_id += 1

        contract = Contract()
        contract.symbol = ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.strike = strike
        contract.lastTradeDateOrContractMonth = expiry
        contract.right = "P"
        contract.multiplier = "100"

        order = Order()
        order.action = "SELL"
        order.totalQuantity = qty
        order.orderType = "LMT"
        order.lmtPrice = round(limit_price, 2)
        order.tif = "DAY"
        order.eTradeOnly = False  # Capital T required for ibapi 9.81.1
        order.firmQuoteOnly = False
        order.account = ACCOUNT_ID

        log(f"  📤 Placing order: SELL {qty}x {ticker} ${strike}P @ ${limit_price:.2f} (ID: {order_id})")

        try:
            self.app.placeOrder(order_id, contract, order)
            return order_id
        except Exception as e:
            log(f"  ❌ Order placement failed: {e}")
            return None

    # ═══════════════════════════════════════════════
    # SCANNER — Find & Score Opportunities
    # ═══════════════════════════════════════════════
    def scan(self):
        """Scan watchlist for trading opportunities."""
        log("\n" + "=" * 60)
        log("🌅 MORNING SCAN STARTED")
        log("=" * 60)

        if kill_switch_active():
            log(f"  🛑 Kill switch active ({KILL_SWITCH_FILE}) — no new opening trades")
            return []

        # Detect market regime from VIX
        vix = self.get_vix()
        regime = self.detect_regime(vix)
        self._current_regime = regime  # Store for use by find_target_strike and sizing
        log(f"  📊 VIX: {vix:.2f} — Regime: {regime['label']} "
            f"(Δ {regime['delta_min']}-{regime['delta_max']}, risk × {regime['risk_mult']})")

        # Use IBKR positions as the gating source of truth. The journal is useful
        # metadata, but submitted/cancelled orders can make it drift.
        ibkr_shorts = [p for p in (self.app.positions or {}).values() if p.get("position", 0) < 0]
        open_tickers = [p["symbol"] for p in ibkr_shorts]
        num_open = len(ibkr_shorts)

        log(f"  Open positions: {num_open}/{MAX_POSITIONS}")
        if num_open >= MAX_POSITIONS:
            log("  ⚠ Maximum positions reached — no new trades")
            return []

        slots = MAX_POSITIONS - num_open
        opportunities = []

        for ticker in WATCHLIST:
            # Skip if already have a position in this ticker
            if ticker in open_tickers:
                log(f"  {ticker}: skipped (already have position)")
                continue

            log(f"\n  Scanning {ticker}...")

            # Get stock price
            stock_price = self.get_stock_price(ticker)
            if not stock_price or stock_price <= 0:
                log(f"  {ticker}: no price data")
                continue

            log(f"  {ticker}: ${stock_price:.2f}")

            # Get option chain
            chain = self.get_option_chain(ticker)
            if not chain["expirations"]:
                log(f"  {ticker}: no option chain data")
                continue

            # Find target expiry
            expiry = self.find_target_expiry(chain["expirations"])
            if not expiry:
                log(f"  {ticker}: no suitable expiry in {DTE_MIN}-{DTE_MAX} DTE range")
                continue

            exp_date = datetime.strptime(expiry, "%Y%m%d")
            dte = (exp_date - datetime.now()).days

            # Find best strike
            strike, opt_data = self.find_target_strike(ticker, stock_price, chain["strikes"], expiry)
            if not strike or not opt_data:
                log(f"  {ticker}: no suitable strike found")
                continue

            premium = opt_data["price"]
            delta = abs(opt_data.get("delta", 0))
            iv = opt_data.get("iv", 0)
            buffer = (stock_price - strike) / stock_price * 100

            # Score it
            score = self.score_opportunity(ticker, strike, stock_price, opt_data, dte, open_tickers)

            strategy = get_strategy(ticker)
            qty = self.calc_position_size(ticker, strike, premium, stock_price)
            if strategy == "CSP" and qty <= 0:
                exposure = strike * 100
                regime_mult = getattr(self, '_current_regime', REGIMES["normal"]).get("risk_mult", 1.0)
                risk_budget = MAX_CSP_ASSIGNMENT_RISK * regime_mult
                log(f"  {ticker}: skipped CSP — assignment exposure ${exposure:,.0f} "
                    f"exceeds risk budget ${risk_budget:,.0f}")
                continue

            # For Tier 1: find the long leg (protective put) for the spread
            long_strike = None
            long_premium = 0
            net_credit = premium
            if strategy == "BPS":
                width = SPREAD_WIDTHS.get(ticker, 5)
                target_long = strike - width
                # Find closest available strike below short strike
                candidate_longs = [s for s in chain["strikes"] if s <= target_long and s >= strike - width * 2]
                if candidate_longs:
                    long_strike = max(candidate_longs)  # Closest to target
                    long_opt = self.get_option_data(ticker, long_strike, expiry, "P")
                    long_premium = long_opt.get("price", 0)
                    if long_premium > 0:
                        net_credit = premium - long_premium
                        # Recompute qty based on spread risk with regime adjustment
                        regime = getattr(self, '_current_regime', None) or REGIMES["normal"]
                        adjusted_risk = MAX_RISK * regime.get("risk_mult", 1.0)
                        spread_max_loss = (width - net_credit) * 100
                        if spread_max_loss > 0:
                            max_spreads = int(adjusted_risk / spread_max_loss)
                            if net_credit < width * MIN_SPREAD_CREDIT_PCT:
                                log(f"  {ticker}: skipped spread — credit ${net_credit:.2f} "
                                    f"is below {MIN_SPREAD_CREDIT_PCT*100:.0f}% of ${width} width")
                                long_strike = None
                                qty = 0
                                continue
                            # Apply same per-price caps as CSP (stock >$500 = 2 contracts)
                            if stock_price > 500:
                                price_cap = 2
                            elif stock_price > 100:
                                price_cap = 3
                            elif stock_price > 50:
                                price_cap = 4
                            else:
                                price_cap = 5
                            qty = min(max_spreads, price_cap)
                            if qty <= 0:
                                log(f"  {ticker}: skipped spread — max risk budget allows 0 contracts")
                                long_strike = None
                                continue
                    else:
                        # Can't get long leg price — fall back to naked
                        long_strike = None
            if qty <= 0:
                continue

            log(f"  {ticker}: ${strike}P @ ${premium:.2f} | Δ={delta:.3f} IV={iv*100:.1f}% "
                f"Buffer={buffer:.1f}% DTE={dte} | Score={score}"
                + (f" | Spread ${strike}/{long_strike} credit=${net_credit:.2f}" if long_strike else ""))

            opportunities.append({
                "ticker": ticker,
                "strike": strike,
                "long_strike": long_strike,
                "expiry": expiry,
                "premium": premium,
                "net_credit": net_credit,
                "delta": delta,
                "iv": iv,
                "buffer": buffer,
                "dte": dte,
                "score": score,
                "strategy": strategy,
                "qty": qty,
                "stock_price": stock_price,
            })

        # Sort by score descending after scanning the full watchlist.
        opportunities.sort(key=lambda x: x["score"], reverse=True)
        selected = opportunities[:slots]
        write_trade_signals(selected, mode="signal_only" if signal_only_active() else "paper_auto")

        log(f"\n  📊 Found {len(opportunities)} opportunities, {slots} slot(s) available")
        for opp in selected:
            log(f"    #{opp['score']}: {opp['ticker']} ${opp['strike']}P @ ${opp['premium']:.2f} "
                f"({opp['strategy']}) DTE={opp['dte']}")

        return selected

    # ═══════════════════════════════════════════════
    # TRADE EXECUTION
    # ═══════════════════════════════════════════════
    def execute_trades(self, opportunities):
        """Place orders for top opportunities."""
        if not opportunities:
            log("  No trades to execute")
            return

        for opp in opportunities:
            ticker = opp["ticker"]
            strike = opp["strike"]
            long_strike = opp.get("long_strike")
            expiry = opp["expiry"]
            premium = opp["premium"]
            net_credit = opp.get("net_credit", premium)
            qty = int(opp.get("qty", 0) or 0)
            strategy = opp["strategy"]
            delta = opp["delta"]
            iv = opp["iv"]
            dte = opp["dte"]
            buffer = opp["buffer"]
            score = opp["score"]

            if qty <= 0:
                log(f"  {ticker}: skipped execution — quantity is 0 after risk checks")
                continue
            if signal_only_active():
                log(f"  📡 SIGNAL ONLY: {ticker} ${strike}P x{qty} — no paper order submitted")
                continue

            # Route to spread or naked put
            is_spread = (strategy == "BPS" and long_strike is not None)

            if is_spread:
                log(f"\n  🎯 EXECUTING SPREAD: {qty}x {ticker} ${strike}/{long_strike}P @ ${net_credit:.2f} credit")
                order_id = self.place_bull_put_spread(ticker, strike, long_strike, expiry, qty, net_credit)
                order_desc = f"${strike}/{long_strike}P"
                journal_credit = net_credit
                journal_strategy = "BPS"
            else:
                log(f"\n  🎯 EXECUTING: SELL {qty}x {ticker} ${strike}P @ ${premium:.2f}")
                order_id = self.place_sell_put(ticker, strike, expiry, qty, premium, strategy)
                order_desc = f"${strike}P"
                journal_credit = premium
                journal_strategy = strategy

            if order_id is not None:
                # Track as pending to avoid monitoring conflict
                pending_key = f"{ticker}-{float(strike)}"
                self._pending_orders[pending_key] = datetime.now()

                # Write to journal
                notes = f"Score {score} | Buffer {buffer:.1f}% | OrderID {order_id}"
                if opp.get("signal_id"):
                    notes += f" | SignalID {opp.get('signal_id')}"
                if is_spread:
                    notes = f"Spread ${strike}/{long_strike} | {notes}"
                write_journal(
                    "OPEN", ticker, journal_strategy, strike, expiry, qty, journal_credit,
                    delta, iv * 100, dte, "Submitted", 0, notes
                )

                # Send email notification
                send_email(
                    f"🎯 Trade Placed: {ticker} {order_desc}",
                    f"<h2>Trade Placed</h2>"
                    f"<p><b>{ticker}</b> {order_desc} × {qty} @ ${journal_credit:.2f}</p>"
                    f"<p>Strategy: {journal_strategy} | Score: {score} | DTE: {dte}</p>"
                    f"<p>Delta: {delta:.3f} | IV: {iv*100:.1f}% | Buffer: {buffer:.1f}%</p>"
                )

            time.sleep(2)

    # ═══════════════════════════════════════════════
    # POSITION MONITOR — IBKR-First
    # ═══════════════════════════════════════════════
    def monitor_positions(self):
        """Monitor IBKR actual positions directly. Uses journal only for entry prices."""
        ibkr_pos = self.app.positions
        if not ibkr_pos:
            log("  No IBKR positions to monitor")
            return

        # Build journal lookup for entry prices.
        # Key by (ticker, rounded strike) so multiple positions in the same
        # ticker at different strikes don't collide — and so that a later
        # CLOSE row for one strike doesn't overwrite the OPEN for another.
        journal_pos = read_open_positions()
        journal_by_pos = {}
        for k, v in journal_pos.items():
            journal_by_pos[(v["ticker"], round(float(v["strike"]), 2))] = v

        # Separate short (ours) from accidental longs
        shorts = {k: v for k, v in ibkr_pos.items() if v["position"] < 0}
        longs = {k: v for k, v in ibkr_pos.items() if v["position"] > 0}

        def is_protective_bps_long(long_pos):
            """Return True when a long put is the hedge leg of a journaled BPS."""
            if long_pos.get("right") != "P":
                return False
            ticker = long_pos["symbol"]
            expiry = str(long_pos.get("expiry", ""))
            long_strike = float(long_pos["strike"])
            long_qty = int(abs(long_pos["position"]))
            width = SPREAD_WIDTHS.get(ticker, 5)
            for short_pos in shorts.values():
                if short_pos.get("symbol") != ticker:
                    continue
                if str(short_pos.get("expiry", "")) != expiry:
                    continue
                if short_pos.get("right") != "P":
                    continue
                if int(abs(short_pos.get("position", 0))) != long_qty:
                    continue
                short_strike = float(short_pos["strike"])
                lookup_key = (ticker, round(short_strike, 2))
                j = journal_by_pos.get(lookup_key)
                if not j or j.get("strategy") != "BPS":
                    continue
                expected_long = short_strike - width
                if abs(long_strike - expected_long) < 0.01:
                    return True
            return False

        # Handle unmatched long positions. Protective BPS long puts are expected
        # and must not be sold independently, or the spread becomes naked.
        for key, pos in longs.items():
            if is_protective_bps_long(pos):
                log(f"  🛡 Protective BPS long kept: {pos['symbol']} ${pos['strike']}P x{int(abs(pos['position']))}")
                continue
            ticker = pos["symbol"]
            strike = pos["strike"]
            qty = int(abs(pos["position"]))
            expiry = pos.get("expiry", "")
            log(f"  ⚠ UNMATCHED LONG: {ticker} ${strike}{pos.get('right','')} x{qty} — manual review required")
            send_email(
                f"⚠ Options Pro: Unmatched long {ticker} ${strike}{pos.get('right','')}",
                f"<h2>Unmatched Long Option Detected</h2>"
                f"<p>{ticker} ${strike}{pos.get('right','')} x{qty}, expiry {expiry}</p>"
                f"<p>The engine did not sell it automatically. Review TWS manually.</p>"
            )
            if AUTO_CLOSE_UNMATCHED_LONGS:
                self._close_long_position(ticker, strike, expiry, qty)

        log(f"\n{'='*60}")
        log(f"📡 MONITORING {len(shorts)} SHORT POSITION(S)")
        log(f"{'='*60}")

        if not shorts:
            return

        for key, ibkr in shorts.items():
            ticker = ibkr["symbol"]
            strike = ibkr["strike"]
            qty = int(abs(ibkr["position"]))
            expiry = ibkr.get("expiry", "")
            avg_cost = ibkr.get("avgCost", 0)

            # Skip pending orders
            pending_key = f"{ticker}-{float(strike)}"
            if pending_key in self._pending_orders:
                age = (datetime.now() - self._pending_orders[pending_key]).total_seconds()
                if age < 600:
                    log(f"  {ticker} ${strike}P: pending order ({int(age)}s ago) — skipping")
                    continue
                else:
                    del self._pending_orders[pending_key]
            if pending_key in self._pending_closes:
                age = (datetime.now() - self._pending_closes[pending_key]).total_seconds()
                if age < MONITOR_INTERVAL:
                    log(f"  {ticker} ${strike}P: close order pending ({int(age)}s ago) — skipping")
                    continue
                else:
                    del self._pending_closes[pending_key]

            lookup_key = (ticker, round(float(strike), 2))
            j = journal_by_pos.get(lookup_key)
            strategy = j.get("strategy", "CSP") if j else "CSP"

            # Entry credit: for BPS, the journal stores the NET spread credit.
            # IBKR avgCost on a short option leg is only that leg's price and
            # will overstate credit/profit targets if used for spreads.
            if strategy == "BPS" and j:
                entry_credit = j["credit"]
            else:
                entry_credit = avg_cost
                if entry_credit <= 0 and j:
                    entry_credit = j["credit"]
            if entry_credit <= 0:
                log(f"  {ticker} ${strike}P x{qty}: no entry price — skipping")
                continue

            # DTE
            dte = 0
            if expiry and len(str(expiry)) >= 8:
                try:
                    exp_date = datetime.strptime(str(expiry)[:8], "%Y%m%d")
                    dte = (exp_date - datetime.now()).days
                except:
                    pass

            # Current price of short leg (optPrice key from patched get_option_data)
            opt_data = self.get_option_data(ticker, strike, str(expiry), "P")
            current_price = opt_data.get("optPrice", 0) or opt_data.get("price", 0)

            # For BPS: also fetch long leg price to compute net spread value
            net_close_debit = current_price  # default: just short leg cost
            if strategy == "BPS":
                width = SPREAD_WIDTHS.get(ticker, 5)
                long_strike = strike - width
                long_data = self.get_option_data(ticker, long_strike, str(expiry), "P")
                long_price = long_data.get("optPrice", 0) or long_data.get("price", 0)
                if long_price > 0:
                    # Net debit to close spread = buy short - sell long
                    net_close_debit = current_price - long_price
                    log(f"  {ticker} spread: short=${current_price:.2f} long=${long_price:.2f} "
                        f"net debit=${net_close_debit:.2f}")

            if net_close_debit <= 0:
                log(f"  {ticker} ${strike}P x{qty}: no current price — skipping")
                continue

            # Cache NET debit (not just short leg) for live snapshot, but only
            # after confirming it is a real positive market value. A missing
            # quote of 0 must not become a fake 100% profit in the dashboard.
            if not hasattr(self, '_live_prices'): self._live_prices = {}
            self._live_prices[f"{ticker}-{strike}"] = net_close_debit

            # P/L based on net spread credit received vs net debit to close
            entry_total = entry_credit * qty * 100
            current_total = net_close_debit * qty * 100
            pnl = entry_total - current_total

            # Thresholds
            profit_target = entry_total * PROFIT_TARGET_PCT
            stop_loss = -(entry_total * STOP_LOSS_MULT)

            log(f"\n  {ticker:5s} ${strike}P x{qty} [{strategy}]: entry=${entry_total:.0f} "
                f"now=${net_close_debit:.2f} P/L=${pnl:.0f} DTE={dte}")

            # Exit check
            action = None
            if pnl >= profit_target:
                action = "CLOSE_PROFIT"
                reason = f"50% profit (${pnl:.0f} >= ${profit_target:.0f})"
            elif pnl <= stop_loss:
                action = "CLOSE_LOSS"
                reason = f"2× stop (${pnl:.0f} <= ${stop_loss:.0f})"
            elif dte <= DTE_EXIT and pnl < 0:
                action = "CLOSE_DTE"
                reason = f"DTE={dte} losing — exit"

            if action:
                log(f"   >> {action}: {reason}")
                self.close_position(ticker, strike, expiry, qty, net_close_debit,
                                    action, reason, pnl, strategy=strategy)
            else:
                log(f"   >> HOLD (target=${profit_target:.0f} stop=${stop_loss:.0f})")

    def _close_long_position(self, ticker, strike, expiry, qty):
        """Close an accidental long position by selling it."""
        if self.app.next_order_id is None:
            return
        order_id = self.app.next_order_id
        self.app.next_order_id += 1

        contract = Contract()
        contract.symbol = ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.strike = strike
        contract.lastTradeDateOrContractMonth = str(expiry)
        contract.right = "P"
        contract.multiplier = "100"

        order = Order()
        order.action = "SELL"
        order.totalQuantity = qty
        order.orderType = "MKT"
        order.tif = "DAY"
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        order.account = ACCOUNT_ID

        log(f"  📤 Selling LONG: {qty}x {ticker} ${strike}P (MKT) ID:{order_id}")
        try:
            self.app.placeOrder(order_id, contract, order)
            send_email(
                f"⚠ Long Cleanup: {ticker} ${strike}P",
                f"<h2>Accidental Long Closed</h2>"
                f"<p>Sold {qty}x {ticker} ${strike}P at market</p>"
            )
        except Exception as e:
            log(f"  ❌ Long cleanup failed: {e}")

    def close_position(self, ticker, strike, expiry, qty, current_price,
                        action, reason, pnl, strategy="CSP"):
        """Close a position.
        
        For CSP: single buy-to-close on the short put.
        For BPS: BAG combo order that closes both legs simultaneously —
                 BUY the short put + SELL the long put.
                 This prevents orphaned long puts which waste margin.
        """
        if self.app.next_order_id is None:
            log("  ❌ No valid order ID — cannot close")
            return

        try:
            if strategy == "BPS":
                self._close_spread_position(ticker, strike, expiry, qty,
                                            current_price, action, reason, pnl)
            else:
                self._close_single_position(ticker, strike, expiry, qty,
                                            current_price, action, reason, pnl)
        except Exception as e:
            log(f"  ❌ Close failed: {e}")
            import traceback
            log(traceback.format_exc())

    def _close_single_position(self, ticker, strike, expiry, qty, current_price,
                                action, reason, pnl):
        """Close a single-leg CSP position (buy to close)."""
        order_id = self.app.next_order_id
        self.app.next_order_id += 1

        contract = Contract()
        contract.symbol = ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.strike = float(strike)
        contract.lastTradeDateOrContractMonth = str(expiry)
        contract.right = "P"
        contract.multiplier = "100"
        contract.tradingClass = _TRADING_CLASS.get(ticker, ticker)

        order = Order()
        order.action = "BUY"
        order.totalQuantity = qty
        order.orderType = "LMT"
        order.lmtPrice = round(current_price, 2)
        order.tif = "DAY"
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        order.account = ACCOUNT_ID

        log(f"  📤 Close CSP: BUY {qty}x {ticker} ${strike}P @ ${current_price:.2f} (ID: {order_id})")
        self.app.placeOrder(order_id, contract, order)
        self._journal_and_email(action, ticker, strike, expiry, qty,
                                current_price, pnl, reason, order_id, "CSP")

    def _close_spread_position(self, ticker, strike, expiry, qty, net_debit,
                                action, reason, pnl):
        """Close a BPS position as a BAG combo order.
        
        Simultaneously:
          BUY  short_strike put  (close our short leg)
          SELL long_strike put   (close our long leg)
        
        Net debit = short premium - long premium.
        Using a combo order avoids leg risk and gets better fills.
        """
        width = SPREAD_WIDTHS.get(ticker, 5)
        long_strike = float(strike) - width
        short_strike = float(strike)

        log(f"  📤 Close BPS: {ticker} ${short_strike}P/${long_strike}P x{qty} "
            f"net debit=${net_debit:.2f}")

        # Resolve conIds for both legs
        short_conid = self.get_option_con_id(ticker, short_strike, expiry, "P")
        long_conid  = self.get_option_con_id(ticker, long_strike,  expiry, "P")

        if not short_conid or not long_conid:
            log(f"  ❌ Cannot resolve spread conIds ({short_conid}, {long_conid}) "
                f"— falling back to single-leg close")
            self._close_single_position(ticker, strike, expiry, qty,
                                        net_debit, action, reason, pnl)
            return

        order_id = self.app.next_order_id
        self.app.next_order_id += 1

        # BAG combo contract
        from ibapi.contract import ComboLeg
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "BAG"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # To CLOSE a spread we originally opened as:
        #   SELL short_strike (short leg) + BUY long_strike (long leg)
        # We now reverse:
        #   BUY  short_strike (ratio 1, action BUY)
        #   SELL long_strike  (ratio 1, action SELL)
        short_leg = ComboLeg()
        short_leg.conId = short_conid
        short_leg.ratio = 1
        short_leg.action = "BUY"    # buy back our short
        short_leg.exchange = "SMART"

        long_leg = ComboLeg()
        long_leg.conId = long_conid
        long_leg.ratio = 1
        long_leg.action = "SELL"    # sell back our long
        long_leg.exchange = "SMART"

        contract.comboLegs = [short_leg, long_leg]

        order = Order()
        order.action = "BUY"        # direction of the combo
        order.totalQuantity = qty
        order.orderType = "LMT"
        order.lmtPrice = round(net_debit, 2)
        order.tif = "DAY"
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        order.account = ACCOUNT_ID

        self.app.placeOrder(order_id, contract, order)
        log(f"  ✅ BPS close order placed (ID: {order_id})")
        self._journal_and_email(action, ticker, strike, expiry, qty,
                                net_debit, pnl, reason, order_id, "BPS")

    def _journal_and_email(self, action, ticker, strike, expiry, qty,
                            price, pnl, reason, order_id, strategy):
        """Shared journal write + email for close_position variants."""
        try:
            self._pending_closes[f"{ticker}-{float(strike)}"] = datetime.now()
            write_journal(
                action, ticker, "CLOSE", strike, expiry, qty, price,
                0, 0, 0, "Submitted", round(pnl, 2),
                f"{reason} | {strategy} | OrderID {order_id}"
            )
            log(f"  📓 Journal written: {action} {ticker} ${strike}P P/L=${pnl:.0f}")
        except Exception as e:
            log(f"  ❌ Journal write failed: {e}")

        try:
            emoji = "✅" if pnl >= 0 else "❌"
            sign = "+" if pnl >= 0 else ""
            subject = f"{emoji} Closed: {ticker} ${strike}P {sign}${pnl:.0f} [{strategy}]"
            body = (
                f"<h2>{action.replace('_', ' ')}</h2>"
                f"<p><b>{ticker}</b> ${strike}P × {qty} [{strategy}]</p>"
                f"<p>Close price: <b>${price:.2f}</b></p>"
                f"<p>P/L: <b>${pnl:.2f}</b> ({sign}{pnl/max(qty*price*100,1)*100:.1f}%)</p>"
                f"<p>Reason: {reason}</p>"
                f"<p>Order ID: {order_id}</p>"
                f"<p><small>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small></p>"
            )
            send_email(subject, body)
            log(f"  📧 Email sent: {subject}")
        except Exception as e:
            log(f"  ❌ Email failed for {ticker} close: {e}")

    # ═══════════════════════════════════════════════
    # MAIN LOOP
    # ═══════════════════════════════════════════════
    def reconcile_positions(self, ibkr_pos=None, journal_pos=None, alert=True):
        """Compare IBKR short option positions with journal OPEN rows."""
        ibkr_pos = ibkr_pos if ibkr_pos is not None else (self.app.positions or {})
        journal_pos = journal_pos if journal_pos is not None else read_open_positions()

        def key_for(ticker, strike):
            try:
                strike_f = round(float(strike), 2)
            except:
                strike_f = 0.0
            return f"{str(ticker).upper()}-{strike_f}"

        ibkr_shorts = {}
        for pos in ibkr_pos.values():
            if pos.get("position", 0) >= 0:
                continue
            key = key_for(pos.get("symbol", ""), pos.get("strike", 0))
            ibkr_shorts[key] = {
                "ticker": pos.get("symbol", ""),
                "strike": round(float(pos.get("strike", 0) or 0), 2),
                "qty": int(abs(pos.get("position", 0) or 0)),
            }

        journal_open = {}
        for pos in journal_pos.values():
            key = key_for(pos.get("ticker", ""), pos.get("strike", 0))
            journal_open[key] = {
                "ticker": pos.get("ticker", ""),
                "strike": round(float(pos.get("strike", 0) or 0), 2),
                "qty": int(abs(pos.get("qty", 0) or 0)),
                "strategy": pos.get("strategy", "CSP"),
                "status": pos.get("status", ""),
            }

        missing_in_ibkr = [v for k, v in journal_open.items() if k not in ibkr_shorts]
        missing_in_journal = [v for k, v in ibkr_shorts.items() if k not in journal_open]
        qty_mismatch = []
        for key, broker in ibkr_shorts.items():
            journal = journal_open.get(key)
            if journal and broker["qty"] != journal["qty"]:
                qty_mismatch.append({"key": key, "ibkr_qty": broker["qty"], "journal_qty": journal["qty"]})

        result = {
            "checked_at": datetime.now().isoformat(timespec="seconds"),
            "ok": not missing_in_ibkr and not missing_in_journal and not qty_mismatch,
            "missing_in_ibkr": missing_in_ibkr,
            "missing_in_journal": missing_in_journal,
            "qty_mismatch": qty_mismatch,
        }

        if alert and not result["ok"]:
            now = datetime.now()
            last = getattr(self, "_last_reconcile_alert", None)
            if not last or (now - last).total_seconds() > RECONCILE_ALERT_INTERVAL:
                self._last_reconcile_alert = now
                log("  ⚠ Reconciliation drift detected between IBKR and journal")
                send_email(
                    "⚠ Options Pro: Position Reconciliation Drift",
                    "<h2>IBKR and journal positions do not match</h2>"
                    f"<pre>{json.dumps(result, indent=2)}</pre>"
                )

        return result

    def write_live_snapshot(self):
        """Write current position state to ~/options-pro/data/live_positions.json.
        Proxy serves this as /api/live — dashboard polls it every 30s."""
        try:
            positions = []
            ibkr_pos = self.app.positions or {}
            journal_pos = read_open_positions()
            # Key by (ticker, strike) so per-strike positions never collide
            journal_by_pos = {(v["ticker"], round(float(v["strike"]), 2)): v
                              for v in journal_pos.values()}

            # Count legs so dashboard can show WHY shorts < total
            shorts_count = sum(1 for v in ibkr_pos.values() if v.get("position", 0) < 0)
            longs_count  = sum(1 for v in ibkr_pos.values() if v.get("position", 0) > 0)
            total_ibkr   = shorts_count + longs_count
            if total_ibkr != shorts_count:
                log(f"  📊 IBKR {total_ibkr} legs = {shorts_count} short + {longs_count} long "
                    f"(longs are BPS protective puts, excluded from snapshot)")

            total_pnl = 0.0
            for key, ibkr in ibkr_pos.items():
                if ibkr.get("position", 0) >= 0:
                    continue  # skip longs
                ticker  = ibkr["symbol"]
                strike  = ibkr["strike"]
                qty     = int(abs(ibkr["position"]))
                expiry  = ibkr.get("expiry", "")
                avg_cost = ibkr.get("avgCost", 0)

                lookup_key = (ticker, round(float(strike), 2))
                j = journal_by_pos.get(lookup_key)
                strategy = j.get("strategy", "CSP") if j else "CSP"
                if strategy == "BPS" and j:
                    entry_credit = j.get("credit", 0)
                else:
                    entry_credit = avg_cost
                    if entry_credit <= 0 and j:
                        entry_credit = j.get("credit", 0)

                # Use price cached during last monitor_positions run (per-position).
                # For BPS this is the NET close debit (short - long), not just short leg.
                cache_key = f"{ticker}-{strike}"
                current_price = getattr(self, '_live_prices', {}).get(cache_key, 0.0)
                has_live_price = current_price > 0

                dte = 0
                if expiry and len(str(expiry)) >= 8:
                    try:
                        exp_date = datetime.strptime(str(expiry)[:8], "%Y%m%d")
                        dte = max(0, (exp_date - datetime.now()).days)
                    except: pass

                entry_total   = entry_credit * qty * 100
                current_total = current_price * qty * 100
                pnl           = round(entry_total - current_total, 2) if has_live_price else 0.0
                pnl_pct       = round((pnl / entry_total * 100) if entry_total and has_live_price else 0, 1)
                if has_live_price:
                    total_pnl += pnl

                positions.append({
                    "ticker":        ticker,
                    "strike":        strike,
                    "expiry":        str(expiry),
                    "qty":           qty,
                    "strategy":      strategy,
                    "entry_credit":  round(entry_credit, 2),
                    "current_price": round(current_price, 2),
                    "price_valid":   has_live_price,
                    "pnl":           pnl,
                    "pnl_pct":       pnl_pct,
                    "dte":           dte,
                })

            regime = getattr(self, "_current_regime", None) or {}
            vix    = regime.get("vix", 0)
            ms     = self.market_status()
            connected = bool(self.app._connected)
            if ms == "tws_restart" or not connected:
                reason = "tws_restart" if ms == "tws_restart" else "tws_disconnected"
                reconciliation = {
                    "checked_at": datetime.now().isoformat(timespec="seconds"),
                    "ok": None,
                    "status": "skipped",
                    "reason": reason,
                    "missing_in_ibkr": [],
                    "missing_in_journal": [],
                    "qty_mismatch": [],
                }
            else:
                reconciliation = self.reconcile_positions(ibkr_pos, journal_pos)
            log_reconciliation(reconciliation)

            snapshot = {
                "updated":          datetime.now().isoformat(timespec="seconds"),
                "engine_running":   True,
                "connected":        connected,
                "market_status":    ms,
                "kill_switch":      kill_switch_active(),
                "kill_switch_file": str(KILL_SWITCH_FILE),
                "signal_only":      signal_only_active(),
                "signal_only_file": str(SIGNAL_ONLY_FILE),
                "signals_file":     str(SIGNALS_FILE),
                "vix":              round(vix, 2) if vix else None,
                "regime":           regime.get("label", "Unknown"),
                "positions":        positions,
                "positions_count":  len(positions),
                "max_positions":    MAX_POSITIONS,
                "total_pnl":        round(total_pnl, 2),
                "account_size":     ACCOUNT_SIZE,
                "ibkr_legs_total":  total_ibkr,
                "ibkr_legs_short":  shorts_count,
                "ibkr_legs_long":   longs_count,
                "reconciliation":   reconciliation,
            }

            with open(LIVE_POSITIONS_FILE, "w") as f:
                json.dump(snapshot, f, indent=2)

        except Exception as e:
            log(f"  ⚠ write_live_snapshot failed: {e}")

    def market_status(self):
        """Return current market session based on US Eastern time.

        Returns:
            "tws_restart"  — 11:45 PM to 6:30 AM ET (TWS daily restart window)
                             Engine sleeps entirely, no TWS interaction.
            "closed"       — 6:30 AM to 9:25 AM ET and 4:05 PM to 11:45 PM ET
                             Monitor existing positions only, no new scans/trades.
                             Stale data alerts suppressed (no market data expected).
            "open"         — 9:25 AM to 4:05 PM ET (includes 5-min buffer each side)
                             Full scan, trade, and monitor cycle.
        """
        now_et = datetime.now(ZoneInfo("America/New_York"))
        hhmm = now_et.hour * 60 + now_et.minute  # minutes since midnight ET

        TWS_RESTART_START = 23 * 60 + 45   # 11:45 PM
        TWS_RESTART_END   =  6 * 60 + 30   #  6:30 AM
        MARKET_OPEN       =  9 * 60 + 25   #  9:25 AM (5-min early buffer)
        MARKET_CLOSE      = 16 * 60 +  5   #  4:05 PM (5-min late buffer)

        if hhmm >= TWS_RESTART_START or hhmm < TWS_RESTART_END:
            return "tws_restart"
        if hhmm < MARKET_OPEN or hhmm >= MARKET_CLOSE:
            return "closed"
        return "open"

    def run(self):
        """Main engine loop."""
        log("\n" + "🚀" * 20)
        log("Options Pro Ultra v6 — Autotrade Engine")
        log(f"Account: {ACCOUNT_ID} | Size: ${ACCOUNT_SIZE:,}")
        log(f"Max positions: {MAX_POSITIONS} | Max risk: ${MAX_RISK:,.0f}")
        log(f"Delta: {DELTA_MIN}-{DELTA_MAX} | DTE: {DTE_MIN}-{DTE_MAX}")
        log(f"Close: {PROFIT_TARGET_PCT*100:.0f}% profit | Stop: {STOP_LOSS_MULT}× credit")
        log(f"Scan passes: {self.scan_passes}")
        log("🚀" * 20 + "\n")

        # Backup engine to GitHub on every restart (captures latest patches)
        try:
            import subprocess as _sp
            repo = os.path.expanduser("~/options-pro")
            if os.path.exists(repo):
                eng_src = "/Applications/OptionsPro.app/Contents/Resources/autotrade_engine.py"
                eng_dst = os.path.join(repo, "app", "autotrade_engine.py")
                import shutil as _sh
                _sh.copy2(eng_src, eng_dst)
                ts = datetime.now().strftime("%Y-%m-%d %H:%M")
                _sp.run(["git", "-C", repo, "add", "-A"], capture_output=True)
                result = _sp.run(
                    ["git", "-C", repo, "commit", "-m", f"auto-backup on restart {ts}"],
                    capture_output=True, text=True
                )
                if "nothing to commit" not in result.stdout:
                    _sp.run(["git", "-C", repo, "push"], capture_output=True)
                    log("  ✅ GitHub backup on restart complete")
        except Exception as _e:
            log(f"  ⚠ Startup backup failed (non-critical): {_e}")

        ensure_journal()

        if not self.connect():
            log("❌ Cannot start — TWS connection failed")
            return

        # Initial scan & trade — only during market hours
        ms = self.market_status()
        if ms == "open":
            for pass_num in range(self.scan_passes):
                log(f"\n━━━ Scan Pass {pass_num + 1}/{self.scan_passes} ━━━")
                opportunities = self.scan()
                if opportunities:
                    self.execute_trades(opportunities)
                time.sleep(5)
        elif ms == "tws_restart":
            log("😴 Startup during TWS restart window — skipping initial scan, entering monitor mode")
        else:
            log("🌙 Market closed — skipping initial scan, entering monitor mode")

        # Write initial snapshot immediately so dashboard shows data
        self.write_live_snapshot()

        # Enter monitoring loop
        log(f"\n✅ Entering monitor mode (checking every {MONITOR_INTERVAL // 60} min)")

        consecutive_stale = 0  # cycles with no position prices
        last_alert_sent = None
        last_successful_monitor = datetime.now()

        while self._running:
            try:
                # ── Market hours gate ──
                ms = self.market_status()

                if ms == "tws_restart":
                    # TWS is doing its nightly restart (11:45 PM – 6:30 AM ET).
                    # Don't touch TWS at all — sleep and check again in 5 min.
                    log("😴 TWS restart window — sleeping (next check in 5 min)")
                    time.sleep(300)
                    continue

                # ── Connection health check ──
                if not self.app._connected:
                    lost_age = 0
                    if self.app._connection_lost_at:
                        lost_age = (datetime.now() - self.app._connection_lost_at).total_seconds()
                    log(f"⚠ Connection lost ({int(lost_age)}s ago) — attempting reconnect...")

                    # Send alert if disconnected > 15 min and haven't alerted recently.
                    # Rate limit: one disconnect alert per 2 hours (was 30 min — too spammy).
                    if lost_age > 900 and (
                        not last_alert_sent
                        or (datetime.now() - last_alert_sent).total_seconds() > 7200
                    ):
                        send_email(
                            "🚨 Options Pro: TWS Disconnected > 15 min",
                            f"<h2>Engine has been disconnected from TWS for {int(lost_age/60)} minutes.</h2>"
                            f"<p>Check TWS is open and logged in. Engine will keep trying to reconnect.</p>"
                            f"<p><small>This alert will not repeat for 2 hours.</small></p>"
                        )
                        last_alert_sent = datetime.now()
                        log(f"  📧 Disconnect alert sent (next alert in 2h)")

                    if not self.reconnect():
                        log("❌ Reconnect failed — waiting 60s before retry")
                        time.sleep(60)
                        continue
                    else:
                        # Reconnect succeeded — reset counters
                        consecutive_stale = 0
                        if last_alert_sent:
                            send_email(
                                "✅ Options Pro: Reconnected",
                                f"<h2>Engine reconnected to TWS successfully</h2>"
                            )
                            last_alert_sent = None

                # ── Watchdog: force reconnect if no successful monitor in 1 hour ──
                stale_age = (datetime.now() - last_successful_monitor).total_seconds()
                if stale_age > 3600:
                    log(f"⚠ WATCHDOG: No successful monitor in {int(stale_age/60)} min — forcing reconnect")
                    try:
                        self.app.disconnect()
                    except:
                        pass
                    time.sleep(5)
                    self.app = TWSApp()
                    self.connect()
                    last_successful_monitor = datetime.now()
                    continue

                # ── Refresh IBKR positions ──
                self.app.positions = {}
                self.app.reqPositions()
                time.sleep(3)

                # ── Run monitor ──
                price_count_before = sum(1 for _ in self.app.market_data.values())
                self.monitor_positions()

                # ── Write live snapshot for dashboard ──
                self.write_live_snapshot()

                # ── Check for stale data (market hours only) ──
                # Outside market hours, no prices are expected — skip stale detection
                # to avoid spurious reconnect storms overnight.
                if self.app.positions and ms == "open":
                    got_prices = False
                    for key in list(self.app.market_data.keys())[-20:]:
                        md = self.app.market_data.get(key, {})
                        if md.get("last") or md.get("optPrice") or (md.get("bid") and md.get("ask")):
                            got_prices = True
                            break

                    if not got_prices:
                        consecutive_stale += 1
                        log(f"⚠ Stale data detected (cycle {consecutive_stale}/3)")
                        if consecutive_stale >= 3:
                            log("🔴 3 consecutive stale cycles — forcing reconnect")
                            consecutive_stale = 0
                            try:
                                self.app.disconnect()
                            except:
                                pass
                            time.sleep(5)
                            self.app = TWSApp()
                            self.connect()
                            continue
                    else:
                        consecutive_stale = 0
                        last_successful_monitor = datetime.now()
                elif ms != "open":
                    # Outside market hours: reset stale counter and keep last_successful_monitor
                    # fresh so the 1-hour watchdog doesn't fire unnecessarily.
                    consecutive_stale = 0
                    last_successful_monitor = datetime.now()

                # Sleep until next check — longer interval outside market hours
                if ms == "open":
                    sleep_secs = MONITOR_INTERVAL
                    log(f"\n  ⏳ Next check in {sleep_secs // 60} min...")
                else:
                    sleep_secs = 900  # 15 min when closed (saves CPU, avoids log spam)
                    log(f"\n  🌙 Market closed — next check in 15 min...")
                time.sleep(sleep_secs)

            except KeyboardInterrupt:
                log("\n🛑 Engine stopped by user")
                break
            except Exception as e:
                log(f"  ❌ Monitor error: {e}")
                import traceback
                log(traceback.format_exc())
                time.sleep(60)

        try:
            self.app.disconnect()
        except:
            pass
        log("Engine shutdown complete.")


# ═══════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════
if __name__ == "__main__":
    passes = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    engine = AutoTradeEngine(scan_passes=passes)
    engine.run()
