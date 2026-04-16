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
JOURNAL = Path.home() / "Desktop" / "autotrade_journal.csv"
LOG_FILE = Path.home() / "Desktop" / "autotrade_log.txt"
API_KEY_FILE = BASE / "api_key.txt"

# Email config
EMAIL_FROM = "islamalbaz90@gmail.com"
EMAIL_PASS = "fwnpftcqwlskrpjn"
EMAIL_TO = "islamalbaz90@gmail.com"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

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

# Monitoring interval (seconds)
MONITOR_INTERVAL = 1800  # 30 minutes


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


def send_email(subject, body_html):
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
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
    row = f"{ts},{action},{ticker},{strategy},{strike},{expiry},{qty},{credit},{delta},{iv_str},{dte},{status},{pnl},{notes}"
    with open(JOURNAL, "a") as f:
        f.write(row + "\n")
    log(f"  📝 Journal: {action} {ticker} ${strike} x{qty}")


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
                if key in positions:
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
        if reqId in self._price_events:
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
        """Get current stock price."""
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

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
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.strike = strike
        contract.lastTradeDateOrContractMonth = expiry
        contract.right = right
        contract.multiplier = "100"

        event = threading.Event()
        self.app._price_events[req_id] = event
        self.app.market_data[req_id] = {}

        self.app.reqMktData(req_id, contract, "", False, False, [])
        event.wait(timeout=timeout)
        self.app.cancelMktData(req_id)

        md = self.app.market_data.get(req_id, {})
        price = md.get("optPrice") or md.get("last")
        if not price and "bid" in md and "ask" in md:
            price = (md["bid"] + md["ask"]) / 2

        return {
            "price": price or 0,
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

    def get_con_id(self, ticker, timeout=10):
        """Resolve a ticker's conId via reqContractDetails."""
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
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
            log(f"  {ticker}: conId={con_id}")
            return con_id
        log(f"  {ticker}: could not resolve conId")
        return None

    def get_option_con_id(self, ticker, strike, expiry, right="P", timeout=10):
        """Resolve an option contract's conId — needed for combo legs."""
        req_id = self.next_req_id()
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
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

    def get_option_chain(self, ticker, timeout=10):
        """Get available expirations and strikes for a ticker."""
        # First resolve the conId
        con_id = self.get_con_id(ticker, timeout=timeout)
        if not con_id:
            log(f"  {ticker}: could not resolve conId")
            return {"expirations": [], "strikes": []}

        req_id = self.next_req_id()

        event = threading.Event()
        self.app._chain_events[req_id] = event
        self.app.option_chains[req_id] = {"expirations": set(), "strikes": set()}

        self.app.reqSecDefOptParams(req_id, ticker, "", "STK", con_id)
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

    # ── Position Sizing ──
    def calc_position_size(self, ticker, strike, premium, stock_price):
        """Calculate number of contracts based on tier and risk rules.
        Since engine places single-leg puts (not actual spreads yet),
        always size based on CSP risk = (strike - premium) × 100."""
        # CSP risk: max loss = (strike - premium) × 100 per contract
        max_loss_per_contract = (strike - premium) * 100

        if max_loss_per_contract <= 0:
            return 1

        max_contracts = int(MAX_RISK / max_loss_per_contract)

        # Additional caps based on stock price tier
        if stock_price > 500:
            cap = 2   # Large ETFs (SPY, QQQ): max 2 contracts
        elif stock_price > 100:
            cap = 3   # Mid-price (SMH, GDX, IWM): max 3
        elif stock_price > 50:
            cap = 4   # Lower price (XLE, TLT): max 4
        else:
            cap = 5   # Mini (QQQM-sized): max 5

        return max(1, min(max_contracts, cap))

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
        Falls back to buffer-based selection if delta data unavailable."""
        # Filter strikes below current price (OTM puts)
        otm_strikes = [s for s in strikes if s < stock_price * 0.98 and s > stock_price * 0.80]
        if not otm_strikes:
            return None, None

        # Sort descending (closest to ATM first)
        otm_strikes.sort(reverse=True)

        best_strike = None
        best_data = None
        best_delta_fit = float("inf")

        # Buffer-based fallback: track best by distance to ~7% buffer
        buffer_target = stock_price * 0.93  # ~7% OTM
        best_buffer_strike = None
        best_buffer_data = None
        best_buffer_dist = float("inf")

        # Check top candidates
        for strike in otm_strikes[:10]:
            opt = self.get_option_data(ticker, strike, expiry, "P")
            delta = abs(opt.get("delta", 0))
            price = opt.get("price", 0)

            if price <= 0:
                continue

            # Try delta-based selection first
            if DELTA_MIN <= delta <= DELTA_MAX:
                fit = abs(delta - 0.20)
                if fit < best_delta_fit:
                    best_delta_fit = fit
                    best_strike = strike
                    best_data = opt

            # Track buffer-based fallback (any strike with a price)
            dist = abs(strike - buffer_target)
            if dist < best_buffer_dist:
                best_buffer_dist = dist
                best_buffer_strike = strike
                best_buffer_data = opt

        # Use delta-based if found, otherwise buffer-based fallback
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

        open_positions = read_open_positions()
        open_tickers = [p["ticker"] for p in open_positions.values()]
        num_open = len(open_positions)

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
                        # Recompute qty based on spread risk: max_loss = width - net_credit
                        spread_max_loss = (width - net_credit) * 100
                        if spread_max_loss > 0:
                            max_spreads = int(MAX_RISK / spread_max_loss)
                            qty = max(1, min(max_spreads, 20))
                    else:
                        # Can't get long leg price — fall back to naked
                        long_strike = None

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

        # Sort by score descending
        opportunities.sort(key=lambda x: x["score"], reverse=True)

        log(f"\n  📊 Found {len(opportunities)} opportunities, {slots} slot(s) available")
        for opp in opportunities[:slots]:
            log(f"    #{opp['score']}: {opp['ticker']} ${opp['strike']}P @ ${opp['premium']:.2f} "
                f"({opp['strategy']}) DTE={opp['dte']}")

        return opportunities[:slots]

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
            qty = opp["qty"]
            strategy = opp["strategy"]
            delta = opp["delta"]
            iv = opp["iv"]
            dte = opp["dte"]
            buffer = opp["buffer"]
            score = opp["score"]

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

        # Build journal lookup for entry prices
        journal_pos = read_open_positions()
        journal_by_ticker = {}
        for k, v in journal_pos.items():
            journal_by_ticker[v["ticker"]] = v

        # Separate short (ours) from accidental longs
        shorts = {k: v for k, v in ibkr_pos.items() if v["position"] < 0}
        longs = {k: v for k, v in ibkr_pos.items() if v["position"] > 0}

        # Auto-close accidental long positions
        for key, pos in longs.items():
            ticker = pos["symbol"]
            strike = pos["strike"]
            qty = int(abs(pos["position"]))
            expiry = pos.get("expiry", "")
            log(f"  ⚠ LONG CLEANUP: {ticker} ${strike}P x{qty} — selling at market")
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

            # Entry credit: IBKR avgCost first, then journal fallback
            entry_credit = avg_cost
            if entry_credit <= 0:
                j = journal_by_ticker.get(ticker)
                if j and abs(j["strike"] - strike) < 1.0:
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

            # Current price
            opt_data = self.get_option_data(ticker, strike, str(expiry), "P")
            current_price = opt_data.get("price", 0)
            if current_price <= 0:
                log(f"  {ticker} ${strike}P x{qty}: no current price — skipping")
                continue

            # P/L
            entry_total = entry_credit * qty * 100
            current_total = current_price * qty * 100
            pnl = entry_total - current_total

            # Thresholds
            profit_target = entry_total * PROFIT_TARGET_PCT
            stop_loss = -(entry_total * STOP_LOSS_MULT)

            log(f"\n  {ticker:5s} ${strike}P x{qty}: entry=${entry_total:.0f} now=${current_price:.2f} "
                f"P/L=${pnl:.0f} DTE={dte}")

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
                self.close_position(ticker, strike, expiry, qty, current_price, action, reason, pnl)
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

    def close_position(self, ticker, strike, expiry, qty, current_price, action, reason, pnl):
        """Close a position by buying back the option."""
        if self.app.next_order_id is None:
            log("  ❌ No valid order ID — cannot close")
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
        order.action = "BUY"
        order.totalQuantity = qty
        order.orderType = "LMT"
        order.lmtPrice = round(current_price, 2)
        order.tif = "DAY"
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        order.account = ACCOUNT_ID

        log(f"  📤 Closing: BUY {qty}x {ticker} ${strike}P @ ${current_price:.2f} (ID: {order_id})")

        try:
            self.app.placeOrder(order_id, contract, order)

            # Write to journal
            write_journal(
                action, ticker, "CLOSE", strike, expiry, qty, current_price,
                0, 0, 0, "Submitted", round(pnl, 2), f"{reason} | OrderID {order_id}"
            )

            # Send email
            emoji = "✅" if pnl >= 0 else "❌"
            send_email(
                f"{emoji} Position Closed: {ticker} ${strike}P (${pnl:.0f})",
                f"<h2>{action.replace('_', ' ')}</h2>"
                f"<p><b>{ticker}</b> ${strike}P × {qty} closed @ ${current_price:.2f}</p>"
                f"<p>P/L: <b>${pnl:.2f}</b></p>"
                f"<p>Reason: {reason}</p>"
            )
        except Exception as e:
            log(f"  ❌ Close failed: {e}")

    # ═══════════════════════════════════════════════
    # MAIN LOOP
    # ═══════════════════════════════════════════════
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

        ensure_journal()

        if not self.connect():
            log("❌ Cannot start — TWS connection failed")
            return

        # Initial scan & trade
        for pass_num in range(self.scan_passes):
            log(f"\n━━━ Scan Pass {pass_num + 1}/{self.scan_passes} ━━━")
            opportunities = self.scan()
            if opportunities:
                self.execute_trades(opportunities)
            time.sleep(5)

        # Enter monitoring loop
        log(f"\n✅ Entering monitor mode (checking every {MONITOR_INTERVAL // 60} min)")

        consecutive_stale = 0  # cycles with no position prices
        last_alert_sent = None
        last_successful_monitor = datetime.now()

        while self._running:
            try:
                # ── Connection health check ──
                if not self.app._connected:
                    lost_age = 0
                    if self.app._connection_lost_at:
                        lost_age = (datetime.now() - self.app._connection_lost_at).total_seconds()
                    log(f"⚠ Connection lost ({int(lost_age)}s ago) — attempting reconnect...")

                    # Send alert if disconnected > 15 min and haven't alerted recently
                    if lost_age > 900:
                        if not last_alert_sent or (datetime.now() - last_alert_sent).total_seconds() > 1800:
                            send_email(
                                "🚨 Options Pro: TWS Disconnected > 15 min",
                                f"<h2>Engine has been disconnected from TWS for {int(lost_age/60)} minutes.</h2>"
                                f"<p>Check TWS is open and logged in. Engine will keep trying to reconnect.</p>"
                            )
                            last_alert_sent = datetime.now()

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

                # ── Check for stale data ──
                # If we have positions but got no prices, count as stale
                if self.app.positions:
                    got_prices = False
                    for key in list(self.app.market_data.keys())[-20:]:  # recent requests
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

                # Sleep until next check
                log(f"\n  ⏳ Next check in {MONITOR_INTERVAL // 60} min...")
                time.sleep(MONITOR_INTERVAL)

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
