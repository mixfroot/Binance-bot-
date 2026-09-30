#!/usr/bin/env python3
"""
24/7 Binance Perpetual Futures Mover Bot
---------------------------------------
- Selects coins with 24h volume >= $60 Million
- Keeps Top 16 by volume
- Every 15 minutes:
    • Updates Top 16
    • Checks just-closed 15m candle for ±1% move
    • Sends ranked alerts (highest volume first)
    • Sends Heartbeat
    • Reports any errors that occurred in the cycle
- Fully self-healing with retries
"""

import time
import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

import requests

# ===================== CONFIG =====================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"

MIN_VOLUME_USDT = 60_000_000          # $60 Million
TOP_N = 16
MOVE_THRESHOLD = 1.0                  # ±1%
TIMEFRAME = "15m"
CYCLE_SECONDS = 15 * 60               # 15 minutes

# API endpoints (Futures preferred, fallback to vision)
FAPI_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
]
VISION = "https://data-api.binance.vision"

MAX_RETRIES = 4
RETRY_DELAY = 3


# ===================== TELEGRAM =====================
def send_telegram(text: str, silent: bool = False) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_notification": silent,
    }
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code == 200:
                return True
        except Exception:
            time.sleep(2)
    return False


# ===================== HTTP HELPERS =====================
def safe_get(url: str, params: dict = None, timeout: int = 12) -> Optional[dict | list]:
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):  # rate limit / ban
                time.sleep(RETRY_DELAY * (attempt + 2))
            else:
                time.sleep(RETRY_DELAY)
        except Exception:
            time.sleep(RETRY_DELAY)
    return None


def get_24h_tickers() -> List[dict]:
    """Return all USDT perpetual tickers with volume."""
    for base in FAPI_BASES:
        data = safe_get(f"{base}/fapi/v1/ticker/24hr")
        if data and isinstance(data, list):
            return data
    # fallback (spot style – less ideal but better than nothing)
    data = safe_get(f"{VISION}/api/v3/ticker/24hr")
    if data and isinstance(data, list):
        return data
    return []


def get_closed_15m_candle(symbol: str) -> Optional[dict]:
    """
    Fetch the most recently closed 15m candle.
    We request last 3 candles and pick the one that is fully closed.
    """
    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": 3,
    }
    for base in FAPI_BASES:
        data = safe_get(f"{base}/fapi/v1/klines", params)
        if data and len(data) >= 2:
            # data[-1] is current forming, data[-2] is last closed
            c = data[-2]
            return {
                "open_time": int(c[0]),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
                "close_time": int(c[6]),
            }
    # vision fallback
    data = safe_get(f"{VISION}/api/v3/klines", params)
    if data and len(data) >= 2:
        c = data[-2]
        return {
            "open_time": int(c[0]),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5]),
            "close_time": int(c[6]),
        }
    return None


# ===================== CORE LOGIC =====================
def select_top_coins() -> List[dict]:
    """
    Returns list of dicts sorted by quoteVolume desc:
    [{"symbol": "BTCUSDT", "volume": 123456789.0}, ...]
    Only volume >= MIN_VOLUME_USDT, max TOP_N.
    """
    tickers = get_24h_tickers()
    if not tickers:
        return []

    candidates = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        # skip some leveraged tokens if needed
        if any(x in sym for x in ["UP", "DOWN", "BULL", "BEAR"]):
            continue
        try:
            vol = float(t.get("quoteVolume", 0))
        except Exception:
            continue
        if vol >= MIN_VOLUME_USDT:
            candidates.append({"symbol": sym, "volume": vol})

    candidates.sort(key=lambda x: x["volume"], reverse=True)
    return candidates[:TOP_N]


def check_moves(coins: List[dict]) -> List[dict]:
    """
    For each coin check last closed 15m candle.
    Return list of movers with % change, sorted by volume desc.
    """
    movers = []
    for coin in coins:
        sym = coin["symbol"]
        candle = get_closed_15m_candle(sym)
        if not candle:
            continue
        o = candle["open"]
        c = candle["close"]
        if o <= 0:
            continue
        pct = ((c - o) / o) * 100.0
        if abs(pct) >= MOVE_THRESHOLD:
            movers.append({
                "symbol": sym,
                "volume": coin["volume"],
                "pct": pct,
                "open": o,
                "close": c,
                "direction": "🟢 LONG" if pct > 0 else "🔴 SHORT",
            })
        time.sleep(0.08)  # gentle on API

    movers.sort(key=lambda x: x["volume"], reverse=True)
    return movers


def format_volume(v: float) -> str:
    if v >= 1_000_000_000:
        return f"${v/1_000_000_000:.2f}B"
    return f"${v/1_000_000:.1f}M"


def build_alert(movers: List[dict], top_coins: List[dict], errors: List[str]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"<b>⚡ 15m Mover Alert</b>  |  {now}", ""]

    if movers:
        lines.append(f"<b>Moved ≥ {MOVE_THRESHOLD}%  ({len(movers)} coins)</b>")
        for i, m in enumerate(movers, 1):
            lines.append(
                f"{i}. <b>{m['symbol']}</b>  {m['direction']}  "
                f"<b>{m['pct']:+.2f}%</b>  |  Vol {format_volume(m['volume'])}"
            )
        lines.append("")
    else:
        lines.append(f"No coin moved ≥ {MOVE_THRESHOLD}% this candle.")
        lines.append("")

    # Top 16 snapshot (short)
    lines.append(f"<b>Top {len(top_coins)} by Volume (≥$60M)</b>")
    for i, c in enumerate(top_coins[:8], 1):  # show first 8 to keep message short
        lines.append(f"{i}. {c['symbol']}  {format_volume(c['volume'])}")
    if len(top_coins) > 8:
        lines.append(f"... +{len(top_coins)-8} more")

    if errors:
        lines.append("")
        lines.append("<b>⚠️ Errors in this cycle:</b>")
        for e in errors[:5]:
            lines.append(f"• {e}")

    return "\n".join(lines)


def build_heartbeat(top_coins: List[dict], errors: List[str]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status = "✅ OK" if not errors else "⚠️ HAD ERRORS"
    text = (
        f"<b>Heartbeat</b>  |  {now}\n"
        f"Status: {status}\n"
        f"Watching: {len(top_coins)} coins\n"
        f"Next check in \~15 min"
    )
    if errors:
        text += "\n\nErrors:\n" + "\n".join(f"• {e}" for e in errors[:3])
    return text


# ===================== MAIN LOOP =====================
def wait_until_next_cycle():
    """
    Sleep until the next clean 15-minute boundary + 8 seconds buffer
    (so the candle is fully closed on Binance side).
    """
    now = datetime.now(timezone.utc)
    # next 15-min mark
    minute = (now.minute // 15 + 1) * 15
    if minute >= 60:
        next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_time = now.replace(minute=minute, second=0, microsecond=0)

    # small buffer so candle is closed
    next_time += timedelta(seconds=8)

    sleep_sec = (next_time - datetime.now(timezone.utc)).total_seconds()
    if sleep_sec < 5:
        sleep_sec += CYCLE_SECONDS
    print(f"Sleeping {sleep_sec:.0f}s until next cycle ({next_time}) ...")
    time.sleep(max(5, sleep_sec))


def run_cycle() -> None:
    errors = []
    top_coins = []
    movers = []

    try:
        print(f"\n[{datetime.now(timezone.utc)}] Starting cycle ...")
        top_coins = select_top_coins()
        if not top_coins:
            errors.append("Could not fetch any high-volume coins")
        else:
            print(f"Top {len(top_coins)} coins selected")
            movers = check_moves(top_coins)
            print(f"Movers found: {len(movers)}")
    except Exception as e:
        err = f"Cycle error: {type(e).__name__}: {e}"
        errors.append(err)
        print(err)
        traceback.print_exc()

    # Alerts
    try:
        if movers or errors:
            alert = build_alert(movers, top_coins, errors)
            send_telegram(alert)
        # Always send heartbeat
        hb = build_heartbeat(top_coins, errors)
        send_telegram(hb, silent=True)
    except Exception as e:
        print("Telegram send failed:", e)


def main():
    print("=" * 60)
    print("Volume Mover Bot started")
    print(f"Min Volume : ${MIN_VOLUME_USDT/1e6:.0f}M")
    print(f"Top N      : {TOP_N}")
    print(f"Move       : ±{MOVE_THRESHOLD}% on {TIMEFRAME}")
    print("=" * 60)

    send_telegram(
        f"<b>Bot Started</b>\n"
        f"Watching Perpetual Futures\n"
        f"Volume ≥ ${MIN_VOLUME_USDT/1e6:.0f}M → Top {TOP_N}\n"
        f"Alert on ±{MOVE_THRESHOLD}% 15m close\n"
        f"Heartbeat every 15 min"
    )

    # First run after short delay
    time.sleep(5)

    while True:
        try:
            run_cycle()
        except Exception as e:
            msg = f"Critical loop error: {e}"
            print(msg)
            send_telegram(f"🚨 <b>Critical Error</b>\n{msg}")
            time.sleep(30)

        wait_until_next_cycle()


if __name__ == "__main__":
    main()