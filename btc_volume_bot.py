#!/usr/bin/env python3
"""
Structure State Bot
- Auto-selects up to 10 high-volume USDT perpetuals
- Filters: 24h quote volume ≥ 50M + at least one 15m candle ≥ ±1% move in last 21 candles
- Alerts only when 1m structure state changes
"""

import asyncio
import json
import time
from collections import defaultdict

import aiohttp
import websockets

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"   # rotate this
CHAT_ID = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

MIN_24H_VOLUME = 50_000_000          # 50 million USDT
MAX_SYMBOLS = 10
VOLATILITY_LOOKBACK = 21             # last 21 closed 15m candles
MIN_CANDLE_MOVE_PCT = 0.01           # 1%
REFRESH_INTERVAL = 3600              # re-select symbols every 1 hour

# --------------------------------------------------------------------------
# STATE
# --------------------------------------------------------------------------
structure = defaultdict(lambda: defaultdict(lambda: {
    "sup": None, "res": None, "state": "neutral",
    "prev_green": None, "prev_red": None,
    "last_high": None, "last_low": None
}))

active_symbols = set()
listener_tasks = {}                  # symbol → asyncio.Task
_http_session = None


def now():
    return time.time()


# --------------------------------------------------------------------------
# TELEGRAM
# --------------------------------------------------------------------------
async def send_telegram(text: str):
    global _http_session
    try:
        if _http_session is None or _http_session.closed:
            _http_session = aiohttp.ClientSession()
        payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
        async with _http_session.post(TELEGRAM_API_URL, json=payload, timeout=10) as resp:
            if resp.status != 200:
                print(f"[TG] Error {resp.status}: {await resp.text()}")
    except Exception as e:
        print(f"[TG] Exception: {e}")


# --------------------------------------------------------------------------
# Structure Logic
# --------------------------------------------------------------------------
def update_structure(symbol: str, tf: str, o: float, h: float, l: float, c: float, is_closed: bool):
    s = structure[symbol][tf]

    green = c > o
    red = c < o

    prev_green = s.get("prev_green")
    prev_red = s.get("prev_red")

    green_to_red = prev_green and red
    red_to_green = prev_red and green

    if green_to_red:
        s["res"] = max(h, s.get("last_high") or h)
    if red_to_green:
        s["sup"] = min(l, s.get("last_low") or l)

    if red and not green_to_red and s["res"] is not None and h > s["res"]:
        s["res"] = h
    if green and not red_to_green and s["sup"] is not None and l < s["sup"]:
        s["sup"] = l

    if s["sup"] is not None and l < s["sup"] and c > s["sup"]:
        s["sup"] = l

    if is_closed:
        old_state = s["state"]

        if s["res"] is not None and c > s["res"]:
            s["state"] = "bullish"
            s["res"] = None
        elif s["sup"] is not None and c < s["sup"]:
            s["state"] = "bearish"
            s["sup"] = None

        s["prev_green"] = green
        s["prev_red"] = red
        s["last_high"] = h
        s["last_low"] = l

        return old_state != s["state"]

    s["prev_green"] = green
    s["prev_red"] = red
    s["last_high"] = h
    s["last_low"] = l
    return False


# --------------------------------------------------------------------------
# Higher TF states (REST)
# --------------------------------------------------------------------------
async def get_higher_tf_states(symbol: str):
    results = {}
    async with aiohttp.ClientSession() as session:
        for tf_name in ["5m", "15m", "1h", "4h"]:
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={tf_name}&limit=10"
            try:
                async with session.get(url, timeout=8) as resp:
                    data = await resp.json()
                    if not isinstance(data, list) or not data:
                        results[tf_name] = "neutral"
                        continue

                    for candle in data[:-1]:  # only closed candles
                        o, h, l, c = float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4])
                        update_structure(symbol, tf_name, o, h, l, c, is_closed=True)

                    results[tf_name] = structure[symbol][tf_name]["state"]
            except Exception as e:
                print(f"[REST] {symbol} {tf_name} error: {e}")
                results[tf_name] = "neutral"
    return results


def make_alert(symbol: str, new_state: str, higher: dict):
    emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "⚪"}
    msg = f"<b>{symbol}</b>\n"
    msg += f"1m → <b>{new_state.upper()}</b> {emoji.get(new_state, '')}\n"
    msg += "────────────────\n"
    msg += f"1m  : {new_state.capitalize()} {emoji.get(new_state, '')}\n"
    for tf in ["5m", "15m", "1h", "4h"]:
        st = higher.get(tf, "neutral")
        msg += f"{tf.upper():<4}: {st.capitalize()} {emoji.get(st, '')}\n"
    return msg.strip()


# --------------------------------------------------------------------------
# Symbol selection (volume + volatility filter)
# --------------------------------------------------------------------------
async def has_volatile_15m_candle(session: aiohttp.ClientSession, symbol: str) -> bool:
    """True if any of the last 21 closed 15m candles moved ≥ 1%."""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=15m&limit={VOLATILITY_LOOKBACK + 1}"
    try:
        async with session.get(url, timeout=8) as resp:
            data = await resp.json()
            if not isinstance(data, list) or len(data) < 2:
                return False

            # exclude the currently forming candle
            for candle in data[:-1]:
                o = float(candle[1])
                c = float(candle[4])
                if o == 0:
                    continue
                move = abs(c - o) / o
                if move >= MIN_CANDLE_MOVE_PCT:
                    return True
            return False
    except Exception as e:
        print(f"[FILTER] {symbol} 15m check failed: {e}")
        return False


async def select_symbols() -> list[str]:
    """
    1. All USDT perpetuals with 24h quote volume ≥ 50M
    2. Sorted highest → lowest volume
    3. Keep only those that had ≥1% move on at least one of the last 21 15m candles
    4. Return max 10 symbols
    """
    print("[SELECT] Fetching 24h tickers...")
    async with aiohttp.ClientSession() as session:
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15) as resp:
            tickers = await resp.json()

        if not isinstance(tickers, list):
            print(f"[SELECT] Unexpected response: {tickers}")
            return []

        # Filter USDT + volume
        candidates = []
        for t in tickers:
            symbol = t.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            try:
                vol = float(t.get("quoteVolume", 0))
            except (TypeError, ValueError):
                continue
            if vol >= MIN_24H_VOLUME:
                candidates.append((symbol, vol))

        # Highest volume first
        candidates.sort(key=lambda x: x[1], reverse=True)
        print(f"[SELECT] {len(candidates)} symbols with ≥ {MIN_24H_VOLUME/1e6:.0f}M volume")

        # Volatility filter (still ordered by volume)
        selected = []
        for symbol, vol in candidates:
            if len(selected) >= MAX_SYMBOLS:
                break
            if await has_volatile_15m_candle(session, symbol):
                selected.append(symbol)
                print(f"[SELECT] ✅ {symbol}  vol={vol/1e6:.1f}M  (volatile 15m)")
            else:
                print(f"[SELECT] ❌ {symbol}  vol={vol/1e6:.1f}M  (no 1% 15m move)")

        return selected


# --------------------------------------------------------------------------
# WebSocket listener (one per symbol)
# --------------------------------------------------------------------------
async def kline_listener(symbol: str):
    stream = f"{symbol.lower()}@kline_1m"
    url = f"wss://fstream.binance.com/market/ws/{stream}"

    backoff = 2
    while symbol in active_symbols:          # auto-stop when removed from list
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                print(f"[WS] Connected {symbol} 1m")
                backoff = 2

                async for raw in ws:
                    if symbol not in active_symbols:
                        break
                    try:
                        data = json.loads(raw)
                        k = data["k"]
                        is_closed = k["x"]

                        o = float(k["o"])
                        h = float(k["h"])
                        l = float(k["l"])
                        c = float(k["c"])

                        changed = update_structure(symbol, "1m", o, h, l, c, is_closed)

                        if changed and is_closed:
                            new_state = structure[symbol]["1m"]["state"]
                            print(f"[CHANGE] {symbol} 1m → {new_state}")

                            higher = await get_higher_tf_states(symbol)
                            alert = make_alert(symbol, new_state, higher)
                            await send_telegram(alert)

                    except Exception as e:
                        print(f"[WS] Parse error {symbol}: {e}")

        except Exception as e:
            if symbol not in active_symbols:
                break
            print(f"[WS] Disconnected {symbol}: {e} → reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    print(f"[WS] Stopped listener for {symbol}")


# --------------------------------------------------------------------------
# Dynamic symbol management
# --------------------------------------------------------------------------
async def refresh_symbols():
    global active_symbols, listener_tasks

    new_list = await select_symbols()
    new_set = set(new_list)

    # Stop listeners that are no longer wanted
    to_remove = active_symbols - new_set
    for sym in to_remove:
        active_symbols.discard(sym)
        task = listener_tasks.pop(sym, None)
        if task and not task.done():
            task.cancel()
        print(f"[MGR] Removed {sym}")

    # Start listeners for new symbols
    to_add = new_set - active_symbols
    for sym in to_add:
        active_symbols.add(sym)
        listener_tasks[sym] = asyncio.create_task(kline_listener(sym))
        print(f"[MGR] Started {sym}")

    if new_list:
        msg = "🔄 <b>Active symbols updated</b>\n" + "\n".join(f"• {s}" for s in new_list)
        await send_telegram(msg)
    else:
        await send_telegram("⚠️ No symbols passed the filters right now")


async def symbol_refresher():
    while True:
        try:
            await refresh_symbols()
        except Exception as e:
            print(f"[REFRESH] Error: {e}")
            await send_telegram(f"⚠️ Symbol refresh failed: {e}")
        await asyncio.sleep(REFRESH_INTERVAL)


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
async def main():
    print("Structure Bot starting...")
    await send_telegram("Structure Bot started ✅\nSelecting high-volume volatile coins...")

    # Initial selection + start listeners
    await refresh_symbols()

    # Keep refreshing every hour
    await symbol_refresher()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")