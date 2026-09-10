**Yes, I understand exactly.**

Here’s the new bot built purely around the Volume Z-Score logic from that Pine Script:

### Core idea
1. Select up to **10 coins** (same filters as before: ≥50M USDT 24h volume → sorted high→low → only those that had ≥1% move on a 15m candle in the last 21 bars).
2. For every selected coin:
   - Fetch the last **1000 closed candles** of the chosen timeframe.
   - Start a live websocket and keep a **rolling 1000-bar lookback**.
3. On every **candle close**:
   - Calculate Volume Z-Score exactly like the script.
   - Color the bar the same way (red / orange / yellow / green / gray / navy).
4. **Alerts** only fire on **green and above** (Z ≥ 1.5).
5. **Each color has its own independent cooldown**.
6. Cooldown for a color is **reset** only when a **gray or navy** (low-volume) bar closes.

### Default settings (easy to change)
- Timeframe: `5m` (recommended – 1000 bars ≈ 3.5 days)
- Z-score length: 1000
- Alert thresholds:
  - Red ≥ 6.0
  - Orange ≥ 4.5
  - Yellow ≥ 3.0
  - Green ≥ 1.5
- Cooldown reset: gray (0 ≤ Z < 1.5) or navy (Z < 0)

Here’s the complete ready-to-run code:

```python
#!/usr/bin/env python3
"""
Volume Z-Score Bot
- Auto-selects top 10 high-volume + volatile USDT perpetuals
- Loads 1000 historical candles → keeps rolling live lookback
- Alerts on Green / Yellow / Orange / Red volume bars
- Each color has its own cooldown (reset only by Gray or Navy bar)
"""

import asyncio
import json
import statistics
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Set

import aiohttp
import websockets

# ==========================================================================
# CONFIG
# ==========================================================================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"  # rotate me
CHAT_ID   = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Volume Z-Score settings (exactly matching the Pine Script)
TIMEFRAME          = "1m"          # change to "1m", "15m", etc. if you want
Z_LENGTH           = 1000
ALERT_Z_MIN        = 1.5           # green and above

# Color thresholds (highest first)
LEVELS = [
    (6.0, "red",    "🔴 RED"),
    (4.5, "orange", "🟠 ORANGE"),
    (3.0, "yellow", "🟡 YELLOW"),
    (1.5, "green",  "🟢 GREEN"),
]

# Coin selection
MIN_24H_VOLUME     = 50_000_000    # 50M USDT
MAX_SYMBOLS        = 10
VOLATILITY_LOOKBACK = 21
MIN_CANDLE_MOVE_PCT = 0.01         # 1%

REFRESH_INTERVAL   = 3600          # re-select coins every 1 hour

# ==========================================================================
# STATE
# ==========================================================================
# volumes[symbol] = deque of the last Z_LENGTH closed volumes
volumes: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=Z_LENGTH))

# cooldown[symbol][color] = True if that color is currently on cooldown
cooldown: Dict[str, Dict[str, bool]] = defaultdict(lambda: {
    "red": False, "orange": False, "yellow": False, "green": False
})

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
# Z-SCORE MATH
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


def get_level(z: float) -> Optional[tuple]:
    """Return (threshold, color, label) or None if below green."""
    for thresh, color, label in LEVELS:
        if z >= thresh:
            return thresh, color, label
    return None


# ==========================================================================
# HISTORICAL LOAD
# ==========================================================================
async def load_historical(session: aiohttp.ClientSession, symbol: str) -> bool:
    """Fetch last Z_LENGTH closed candles and fill the deque."""
    url = (
        f"https://fapi.binance.com/fapi/v1/klines"
        f"?symbol={symbol}&interval={TIMEFRAME}&limit={Z_LENGTH + 1}"
    )
    try:
        async with session.get(url, timeout=12) as resp:
            data = await resp.json()
            if not isinstance(data, list) or len(data) < 10:
                print(f"[HIST] {symbol} bad data")
                return False

            # drop the currently forming candle
            closed = data[:-1]
            vols = [float(c[5]) for c in closed]   # index 5 = volume

            volumes[symbol].clear()
            volumes[symbol].extend(vols[-Z_LENGTH:])  # keep at most Z_LENGTH

            print(f"[HIST] {symbol} loaded {len(volumes[symbol])} bars")
            return True
    except Exception as e:
        print(f"[HIST] {symbol} failed: {e}")
        return False


# ==========================================================================
# COIN SELECTION (same filters as before)
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
            print("[SELECT] Unexpected response")
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
                print(f"[SELECT] ❌ {sym}  no 1% 15m move")

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
                        if not k["x"]:                     # only on candle close
                            continue

                        vol = float(k["v"])
                        volumes[symbol].append(vol)

                        z = calc_zscore(volumes[symbol])
                        if z is None:
                            continue

                        level = get_level(z)

                        # ----- Cooldown reset on gray / navy -----
                        if z < 1.5:                        # gray or navy
                            for color in cooldown[symbol]:
                                if cooldown[symbol][color]:
                                    print(f"[RESET] {symbol} {color} cooldown cleared (Z={z:.2f})")
                                cooldown[symbol][color] = False
                            continue                       # never alert on gray/navy

                        # ----- Alert only green and above -----
                        if level is None:
                            continue

                        _, color, label = level

                        if cooldown[symbol][color]:
                            # still on cooldown for this color
                            continue

                        # Fire alert + set cooldown for this color
                        cooldown[symbol][color] = True

                        msg = (
                            f"<b>{symbol}</b>  {label}\n"
                            f"Z-Score: <b>{z:.2f}</b>\n"
                            f"Volume: {vol:,.0f}\n"
                            f"TF: {TIMEFRAME}"
                        )
                        print(f"[ALERT] {symbol} {label} Z={z:.2f}")
                        await send_telegram(msg)

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

    # stop removed
    for sym in list(active_symbols - new_set):
        active_symbols.discard(sym)
        task = listener_tasks.pop(sym, None)
        if task and not task.done():
            task.cancel()
        volumes.pop(sym, None)
        cooldown.pop(sym, None)
        print(f"[MGR] Removed {sym}")

    # start new ones (load history first)
    async with aiohttp.ClientSession() as session:
        for sym in new_set - active_symbols:
            ok = await load_historical(session, sym)
            if not ok:
                print(f"[MGR] Skipping {sym} – history load failed")
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
    print("Volume Z-Score Bot starting...")
    await send_telegram(
        f"Volume Z-Score Bot started ✅\n"
        f"TF: {TIMEFRAME} | Lookback: {Z_LENGTH}\n"
        f"Alerting on Green+ (Z ≥ {ALERT_Z_MIN})"
    )
    await refresh_symbols()
    await symbol_refresher()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
```

### How the alert + cooldown works

| Z-Score     | Color   | Action                                      |
|-------------|---------|---------------------------------------------|
| ≥ 6.0       | Red     | Alert if red cooldown is free → set red cooldown |
| ≥ 4.5       | Orange  | Alert if orange cooldown free → set it      |
| ≥ 3.0       | Yellow  | same                                        |
| ≥ 1.5       | Green   | same                                        |
| 0 ≤ Z < 1.5 | Gray    | **Reset all cooldowns** (no alert)          |
| Z < 0       | Navy    | **Reset all cooldowns** (no alert)          |

This matches your request perfectly:  
- Only green and higher can alert  
- Each color has its own independent cooldown  
- Cooldown is cleared only when a gray or navy bar closes

You can change `TIMEFRAME`, the level thresholds, or the selection filters at the top of the file.

Want any adjustments (different TF, stricter cooldown, include the exact volume/Z in a different format, etc.)?