#!/usr/bin/env python3
"""
Volume Z-Score Bot + HTF Structure Filter
- Simple alerts: only "SYMBOL  COLOR"
- Each color has independent cooldown (set only when alert is sent)
- Green candle → needs any HTF Bearish
- Red candle → needs any HTF Bullish
"""

import asyncio
import json
import statistics
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Set, Tuple

import aiohttp
import websockets

# ==========================================================================
# CONFIG
# ==========================================================================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"  # rotate this
CHAT_ID   = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Volume Z-Score settings
TIMEFRAME   = "5m"          # change to "1m" if you want
Z_LENGTH    = 1000
ALERT_Z_MIN = 1.5

LEVELS = [
    (6.0, "red",    "🔴 "),
    (4.5, "orange", "🟠 "),
    (3.0, "yellow", "🟡 "),
    (1.5, "green",  "🟢 "),
]

# Coin selection
MIN_24H_VOLUME      = 50_000_000
MAX_SYMBOLS         = 10
VOLATILITY_LOOKBACK = 21
MIN_CANDLE_MOVE_PCT = 0.01
REFRESH_INTERVAL    = 3600          # 1 hour

HTF_LIST = ["5m", "15m", "1h", "4h"]

# ==========================================================================
# STATE
# ==========================================================================
volumes: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=Z_LENGTH))

cooldown: Dict[str, Dict[str, bool]] = defaultdict(lambda: {
    "red": False, "orange": False, "yellow": False, "green": False
})

structure = defaultdict(lambda: defaultdict(lambda: {
    "sup": None, "res": None, "state": "neutral",
    "prev_green": None, "prev_red": None,
    "last_high": None, "last_low": None
}))

active_symbols: Set[str] = set()
listener_tasks: Dict[str, asyncio.Task] = {}
_http_session: Optional[aiohttp.ClientSession] = None


# ==========================================================================
# TELEGRAM
# ==========================================================================
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


# ==========================================================================
# SUPPORT / RESISTANCE STRUCTURE
# ==========================================================================
def update_structure(symbol: str, tf: str, o: float, h: float, l: float, c: float, is_closed: bool) -> bool:
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


async def get_htf_states(symbol: str) -> Dict[str, str]:
    results = {}
    async with aiohttp.ClientSession() as session:
        for tf in HTF_LIST:
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={tf}&limit=12"
            try:
                async with session.get(url, timeout=8) as resp:
                    data = await resp.json()
                    if not isinstance(data, list) or not data:
                        results[tf] = "neutral"
                        continue

                    for candle in data[:-1]:
                        o = float(candle[1])
                        h = float(candle[2])
                        l = float(candle[3])
                        c = float(candle[4])
                        update_structure(symbol, tf, o, h, l, c, is_closed=True)

                    results[tf] = structure[symbol][tf]["state"]
            except Exception as e:
                print(f"[HTF] {symbol} {tf} error: {e}")
                results[tf] = "neutral"
    return results


# ==========================================================================
# Z-SCORE
# ==========================================================================
def calc_zscore(vols: Deque[float]) -> Optional[float]:
    if len(vols) < 2:
        return None
    try:
        avg = statistics.mean(vols)
        std = statistics.stdev(vols)
        if std == 0:
            return 0.0
        return (vols[-1] - avg) / std
    except statistics.StatisticsError:
        return None


def get_level(z: float) -> Optional[Tuple[float, str, str]]:
    for thresh, color, label in LEVELS:
        if z >= thresh:
            return thresh, color, label
    return None


# ==========================================================================
# HISTORICAL LOAD
# ==========================================================================
async def load_historical(session: aiohttp.ClientSession, symbol: str) -> bool:
    url = (
        f"https://fapi.binance.com/fapi/v1/klines"
        f"?symbol={symbol}&interval={TIMEFRAME}&limit={Z_LENGTH + 1}"
    )
    try:
        async with session.get(url, timeout=12) as resp:
            data = await resp.json()
            if not isinstance(data, list) or len(data) < 10:
                return False

            closed = data[:-1]
            vols = [float(c[5]) for c in closed]
            volumes[symbol].clear()
            volumes[symbol].extend(vols[-Z_LENGTH:])
            print(f"[HIST] {symbol} loaded {len(volumes[symbol])} bars")
            return True
    except Exception as e:
        print(f"[HIST] {symbol} failed: {e}")
        return False


# ==========================================================================
# COIN SELECTION
# ==========================================================================
async def has_volatile_15m(session: aiohttp.ClientSession, symbol: str) -> bool:
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=15m&limit={VOLATILITY_LOOKBACK + 1}"
    try:
        async with session.get(url, timeout=8) as resp:
            data = await resp.json()
            if not isinstance(data, list) or len(data) < 2:
                return False
            for candle in data[:-1]:
                o = float(candle[1])
                c = float(candle[4])
                if o > 0 and abs(c - o) / o >= MIN_CANDLE_MOVE_PCT:
                    return True
            return False
    except Exception:
        return False


async def select_symbols() -> List[str]:
    print("[SELECT] Fetching 24h tickers...")
    async with aiohttp.ClientSession() as session:
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15) as resp:
            tickers = await resp.json()

        if not isinstance(tickers, list):
            return []

        candidates = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            try:
                vol = float(t.get("quoteVolume", 0))
            except (TypeError, ValueError):
                continue
            if vol >= MIN_24H_VOLUME:
                candidates.append((sym, vol))

        candidates.sort(key=lambda x: x[1], reverse=True)
        print(f"[SELECT] {len(candidates)} coins ≥ {MIN_24H_VOLUME/1e6:.0f}M")

        selected = []
        for sym, vol in candidates:
            if len(selected) >= MAX_SYMBOLS:
                break
            if await has_volatile_15m(session, sym):
                selected.append(sym)
                print(f"[SELECT] ✅ {sym}  {vol/1e6:.1f}M")
            else:
                print(f"[SELECT] ❌ {sym}")

        return selected


# ==========================================================================
# WEBSOCKET LISTENER
# ==========================================================================
async def kline_listener(symbol: str):
    stream = f"{symbol.lower()}@kline_{TIMEFRAME}"
    url = f"wss://fstream.binance.com/market/ws/{stream}"

    backoff = 2
    while symbol in active_symbols:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=12) as ws:
                print(f"[WS] Connected {symbol} {TIMEFRAME}")
                backoff = 2

                async for raw in ws:
                    if symbol not in active_symbols:
                        break
                    try:
                        msg = json.loads(raw)
                        k = msg["k"]
                        if not k["x"]:
                            continue

                        o = float(k["o"])
                        c = float(k["c"])
                        vol = float(k["v"])

                        volumes[symbol].append(vol)
                        z = calc_zscore(volumes[symbol])
                        if z is None:
                            continue

                        # Reset all cooldowns on Gray / Navy
                        if z < ALERT_Z_MIN:
                            for color in list(cooldown[symbol].keys()):
                                if cooldown[symbol][color]:
                                    print(f"[RESET] {symbol} {color} cleared (Z={z:.2f})")
                                cooldown[symbol][color] = False
                            continue

                        level = get_level(z)
                        if level is None:
                            continue

                        _, color, label = level

                        # Already on cooldown for this specific color?
                        if cooldown[symbol][color]:
                            continue

                        # ========== HTF FILTER ==========
                        is_green = c > o
                        is_red = c < o

                        htf_states = await get_htf_states(symbol)

                        any_bearish = any(st == "bearish" for st in htf_states.values())
                        any_bullish = any(st == "bullish" for st in htf_states.values())

                        allowed = False
                        if is_green and any_bearish:
                            allowed = True
                        elif is_red and any_bullish:
                            allowed = True

                        if not allowed:
                            print(f"[FILTER] {symbol} {label} blocked by HTF")
                            continue

                        # ===== SEND ALERT (only here) =====
                        cooldown[symbol][color] = True
                        alert_msg = f"<b>{symbol}</b>  {label}"
                        print(f"[ALERT] {symbol} {label}")
                        await send_telegram(alert_msg)

                    except Exception as e:
                        print(f"[WS] Parse error {symbol}: {e}")

        except Exception as e:
            if symbol not in active_symbols:
                break
            print(f"[WS] {symbol} disconnected: {e} → retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    print(f"[WS] Stopped {symbol}")


# ==========================================================================
# SYMBOL MANAGER
# ==========================================================================
async def refresh_symbols():
    global active_symbols, listener_tasks

    new_list = await select_symbols()
    new_set = set(new_list)

    # Remove old
    for sym in list(active_symbols - new_set):
        active_symbols.discard(sym)
        task = listener_tasks.pop(sym, None)
        if task and not task.done():
            task.cancel()
        volumes.pop(sym, None)
        cooldown.pop(sym, None)
        structure.pop(sym, None)
        print(f"[MGR] Removed {sym}")

    # Add new
    async with aiohttp.ClientSession() as session:
        for sym in new_set - active_symbols:
            ok = await load_historical(session, sym)
            if not ok:
                print(f"[MGR] Skipping {sym}")
                continue
            active_symbols.add(sym)
            listener_tasks[sym] = asyncio.create_task(kline_listener(sym))
            print(f"[MGR] Started {sym}")

    if new_list:
        await send_telegram(
            "🔄 <b>Active coins updated</b>\n" +
            "\n".join(f"• {s}" for s in new_list)
        )
    else:
        await send_telegram("⚠️ No coins passed the filters")


async def symbol_refresher():
    while True:
        try:
            await refresh_symbols()
        except Exception as e:
            print(f"[REFRESH] {e}")
            await send_telegram(f"⚠️ Refresh failed: {e}")
        await asyncio.sleep(REFRESH_INTERVAL)


# ==========================================================================
# MAIN
# ==========================================================================
async def main():
    print("Volume Z-Score + HTF Filter Bot starting...")
    await send_telegram(
        f"Bot started ✅\n"
        f"TF: {TIMEFRAME} | Max coins: {MAX_SYMBOLS}\n"
        f"Simple alerts: SYMBOL + COLOR only"
    )
    await refresh_symbols()
    await symbol_refresher()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")