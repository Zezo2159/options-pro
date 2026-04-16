#!/bin/bash
# Options Pro Ultra v6 — Master Startup Script
# Manages proxy, engine, health checks, and daily backups

PYTHON="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3"
BASE="/Applications/OptionsPro.app/Contents/Resources"
LOG="/tmp/optionspro_launch.log"
JOURNAL="$HOME/Desktop/autotrade_journal.csv"
BACKUP_DIR="$HOME/options-pro/app"

PROXY_PID=""
ENGINE_PID=""

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"
}

# ── Cleanup on exit ──
cleanup() {
    log "🛑 Shutting down..."
    [ -n "$ENGINE_PID" ] && kill "$ENGINE_PID" 2>/dev/null
    [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null
    exit 0
}
trap cleanup SIGTERM SIGINT

# ── Start proxy ──
start_proxy() {
    # Check if something is already listening on 5010
    EXISTING_PID=$(lsof -ti:5010 2>/dev/null | head -1)
    if [ -n "$EXISTING_PID" ]; then
        # Verify it's actually our proxy.py
        if ps -p "$EXISTING_PID" -o command= 2>/dev/null | grep -q "proxy.py"; then
            PROXY_PID="$EXISTING_PID"
            log "✅ Proxy already running (PID: $PROXY_PID) — adopting"
            return 0
        else
            log "⚠ Port 5010 used by non-proxy process — killing"
            kill -9 "$EXISTING_PID" 2>/dev/null
            sleep 1
        fi
    fi

    log "🌐 Starting proxy..."
    "$PYTHON" "$BASE/proxy.py" >> "$LOG" 2>&1 &
    PROXY_PID=$!
    sleep 2

    if kill -0 "$PROXY_PID" 2>/dev/null; then
        log "✅ Proxy running (PID: $PROXY_PID)"
        return 0
    else
        log "❌ Proxy failed to start"
        return 1
    fi
}

# ── Start engine ──
start_engine() {
    # Check if engine is already running
    EXISTING_PID=$(pgrep -f "autotrade_engine.py" | head -1)
    if [ -n "$EXISTING_PID" ]; then
        ENGINE_PID="$EXISTING_PID"
        log "✅ Engine already running (PID: $ENGINE_PID) — adopting"
        return 0
    fi

    log "🚀 Starting engine..."
    "$PYTHON" -u "$BASE/autotrade_engine.py" 2 >> "$LOG" 2>&1 &
    ENGINE_PID=$!
    sleep 5

    if kill -0 "$ENGINE_PID" 2>/dev/null; then
        log "✅ Engine running (PID: $ENGINE_PID)"
        return 0
    else
        log "❌ Engine failed to start"
        return 1
    fi
}

# ── Health check ──
check_health() {
    local healthy=true

    # Check proxy — rediscover PID if needed
    if ! kill -0 "$PROXY_PID" 2>/dev/null; then
        # Try to find existing proxy
        FOUND_PID=$(lsof -ti:5010 2>/dev/null | head -1)
        if [ -n "$FOUND_PID" ] && ps -p "$FOUND_PID" -o command= 2>/dev/null | grep -q "proxy.py"; then
            PROXY_PID="$FOUND_PID"
            log "ℹ Proxy rediscovered (PID: $PROXY_PID)"
        else
            log "⚠ Proxy died — restarting..."
            start_proxy
            healthy=false
        fi
    fi

    # Check engine — rediscover PID if needed
    if ! kill -0 "$ENGINE_PID" 2>/dev/null; then
        FOUND_PID=$(pgrep -f "autotrade_engine.py" | head -1)
        if [ -n "$FOUND_PID" ]; then
            ENGINE_PID="$FOUND_PID"
            log "ℹ Engine rediscovered (PID: $ENGINE_PID)"
        else
            log "⚠ Engine died — restarting..."
            start_engine
            healthy=false
        fi
    fi

    # Check proxy is responding
    if ! curl -s --max-time 5 "http://localhost:5010/api/journal" > /dev/null 2>&1; then
        log "⚠ Proxy not responding on port 5010"
        healthy=false
    fi

    if [ "$healthy" = false ]; then
        return 1
    fi
    return 0
}

# ── Daily backup ──
daily_backup() {
    if [ -d "$BACKUP_DIR" ]; then
        cp "$JOURNAL" "$BACKUP_DIR/autotrade_journal.csv" 2>/dev/null
        cp "$HOME/Desktop/autotrade_log.txt" "$BACKUP_DIR/autotrade_log.txt" 2>/dev/null
        cd "$HOME/options-pro" 2>/dev/null
        git add -A 2>/dev/null
        git commit -m "Auto-backup: $(date '+%Y-%m-%d %H:%M')" 2>/dev/null
        git push 2>/dev/null
        log "💾 Daily backup pushed to GitHub"
    fi
}

# ── Send health alert email ──
send_alert() {
    "$PYTHON" -c "
import smtplib
from email.mime.text import MIMEText
msg = MIMEText('$1')
msg['Subject'] = '⚠ Options Pro Alert: $2'
msg['From'] = 'islamalbaz90@gmail.com'
msg['To'] = 'islamalbaz90@gmail.com'
with smtplib.SMTP('smtp.gmail.com', 587) as s:
    s.starttls()
    s.login('islamalbaz90@gmail.com', 'fwnpftcqwlskrpjn')
    s.sendmail(msg['From'], msg['To'], msg.as_string())
print('Alert sent')
" 2>/dev/null
}

# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════
log ""
log "═══════════════════════════════════════════════"
log "Options Pro Ultra v6 — Master Controller"
log "═══════════════════════════════════════════════"

# Wait for network (important on boot)
for i in $(seq 1 30); do
    # Use curl — more reliable than ping which is often blocked
    if curl -s --max-time 3 --head https://www.google.com > /dev/null 2>&1; then
        log "✅ Network ready"
        break
    fi
    # Also accept if we can resolve any hostname
    if host google.com > /dev/null 2>&1; then
        log "✅ Network ready (DNS works)"
        break
    fi
    log "⏳ Waiting for network... ($i/30)"
    sleep 5
done

# Start services
start_proxy
start_engine

if [ -z "$ENGINE_PID" ] || ! kill -0 "$ENGINE_PID" 2>/dev/null; then
    log "❌ Engine didn't start — will retry in health loop"
    send_alert "Engine failed to start on $(hostname). Check TWS." "Engine Start Failed"
fi

# Track last backup date
LAST_BACKUP_DATE=""

# ── Health monitoring loop ──
log "✅ Entering health monitor loop (every 5 min)"

while true; do
    sleep 300  # Check every 5 minutes

    # Health check
    if ! check_health; then
        FAIL_COUNT=$((${FAIL_COUNT:-0} + 1))
        if [ "$FAIL_COUNT" -ge 3 ]; then
            send_alert "Engine or proxy has failed $FAIL_COUNT times. Last check: $(date)" "Multiple Failures"
            FAIL_COUNT=0
        fi
    else
        FAIL_COUNT=0
    fi

    # Daily backup at ~6 AM (before market open)
    CURRENT_HOUR=$(date '+%H')
    CURRENT_DATE=$(date '+%Y-%m-%d')
    if [ "$CURRENT_HOUR" = "06" ] && [ "$LAST_BACKUP_DATE" != "$CURRENT_DATE" ]; then
        daily_backup
        LAST_BACKUP_DATE="$CURRENT_DATE"
    fi
done
