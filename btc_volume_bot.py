import asyncio
import logging
import os
import time
import traceback
from datetime import datetime, timezone

import requests

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "6263967739")

VOLUME_THRESHOLD = 50_000_000          # $50 million
MOVE_THRESHOLD = 1.0                   # 1%
INTERVAL = "15m"
INTERVAL_MS = 15 * 60 * 1000

# How many seconds after the official candle close we start scanning
# (gives Binance a moment to finalize the candle)
SCAN_DELAY_AFTER_CLOSE = 4

REST_BASE = "https://fapi.binance.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("15m-mover-bot")


# ==================== TELEGRAM ====================
def _send_telegram_sync(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": text}, timeout=15)
        if r.status_code != 200:
            log.error(f"Telegram send failed: {r.status_code} {r.text}")
    except Exception:
        log.error(f"Telegram send exception:\n{traceback.format_exc()}")


async def send_telegram(text: str):
    await asyncio.to_thread(_send_telegram_sync, text)


# ==================== TIME HELPERS ====================
def get_next_close_timestamp() -> float:
    """Return the Unix timestamp (seconds) of the next 15m candle close."""
    now_ms = int(time.time() * 1000)
    current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
    next_close = current_open + INTERVAL_MS
    return next_close / 1000


def ms_to_readable(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%H:%M UTC")


# ==================== DATA FETCHING ====================
def fetch_24hr_tickers() -> list:
    """Get 24hr ticker for all symbols. Weight = 40."""
    r = requests.get(f"{REST_BASE}/fapi/v1/ticker/24hr", timeout=10)
    r.raise_for_status()
    return r.json()


def fetch_last_closed_kline(symbol: str) -> dict | None:
    """
    Returns the most recently closed 15m kline as a dict with open/close, or None.
    We request limit=2 and take the older one (guaranteed closed).
    """
    try:
        r = requests.get(
            f"{REST_BASE}/fapi/v1/klines",
            params={"symbol": symbol, "interval": INTERVAL, "limit": 2},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if len(data) < 2:
            return None

        # data[0] is older (closed), data[1] is the current forming candle
        k = data[0]
        return {
            "open_time": int(k[0]),
            "open": float(k[1]),
            "close": float(k[4]),
            "high": float(k[2]),
            "low": float(k[3]),
        }
    except Exception as e:
        log.warning(f"Failed to fetch kline for {symbol}: {e}")
        return None


# ==================== SCAN LOGIC ====================
async def run_scan():
    log.info("Starting 15m close scan...")

    try:
        tickers = await asyncio.to_thread(fetch_24hr_tickers)
    except Exception:
        err = traceback.format_exc()
        log.error(err)
        await send_telegram(f"⚠️ Failed to fetch 24hr tickers:\n{err[-400:]}")
        return

    # First filter: volume + USDT perpetual-looking symbols
    candidates = []
    for t in tickers:
        symbol = t["symbol"]
        if not symbol.endswith("USDT"):
            continue
        # Skip delivery contracts (they contain digits in a certain way, simple heuristic)
        if any(c.isdigit() for c in symbol.replace("USDT", "")):
            continue

        quote_vol = float(t.get("quoteVolume", 0))
        if quote_vol < VOLUME_THRESHOLD:
            continue

        candidates.append({
            "symbol": symbol,
            "quote_volume": quote_vol,
        })

    log.info(f"Volume filter passed: {len(candidates)} symbols")

    # Now check the actual 15m move for each candidate
    movers = []
    for c in candidates:
        kline = await asyncio.to_thread(fetch_last_closed_kline, c["symbol"])
        if not kline:
            continue

        open_p = kline["open"]
        close_p = kline["close"]
        if open_p <= 0:
            continue

        pct = (close_p - open_p) / open_p * 100

        if abs(pct) >= MOVE_THRESHOLD:
            movers.append({
                "symbol": c["symbol"],
                "pct": pct,
                "volume": c["quote_volume"],
                "open_time": kline["open_time"],
            })

        # Tiny polite delay so we don't burst the API
        await asyncio.sleep(0.05)

    # Sort by absolute move (strongest first)
    movers.sort(key=lambda x: abs(x["pct"]), reverse=True)

    # Build message
    if not movers:
        msg = (
            f"15m Close Scan ({ms_to_readable(int(time.time()*1000))})\n"
            f"No coins met the criteria (≥{MOVE_THRESHOLD}% move + ≥${VOLUME_THRESHOLD//1_000_000}M volume)"
        )
    else:
        lines = [f"15m Close Scan ({ms_to_readable(movers[0]['open_time'] + INTERVAL_MS)})\n"]
        for m in movers:
            arrow = "▲" if m["pct"] > 0 else "▼"
            vol_m = m["volume"] / 1_000_000
            lines.append(
                f"{arrow} {m['symbol']:<12} {m['pct']:+.2f}%   Vol: ${vol_m:.0f}M"
            )
        msg = "\n".join(lines)

    await send_telegram(msg)
    log.info(f"Scan complete. {len(movers)} movers found.")


# ==================== MAIN LOOP ====================
async def main_loop():
    await send_telegram(
        "🚀 15m Mover Bot started\n"
        f"Looking for coins with ≥{MOVE_THRESHOLD}% move on the closed 15m candle\n"
        f"and ≥${VOLUME_THRESHOLD//1_000_000}M 24h futures volume.\n"
        "Will alert after every 15m close (including when none qualify)."
    )

    while True:
        try:
            next_close = get_next_close_timestamp()
            now = time.time()
            sleep_seconds = next_close - now + SCAN_DELAY_AFTER_CLOSE

            if sleep_seconds > 0:
                log.info(f"Sleeping {sleep_seconds:.1f}s until next 15m close + {SCAN_DELAY_AFTER_CLOSE}s")
                await asyncio.sleep(sleep_seconds)
            else:
                # We somehow missed it, run immediately and continue
                log.warning("Missed the close window, scanning now")

            await run_scan()

        except Exception:
            err = traceback.format_exc()
            log.error(err)
            await send_telegram(f"⚠️ Unexpected error in main loop:\n{err[-500:]}")
            # Wait a bit before retrying so we don't spam
            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main_loop())