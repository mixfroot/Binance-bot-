import asyncio
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone

import requests
import websockets

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "6263967739")

SYMBOL = "btcusdt"          # lowercase, no .p
INTERVAL = "15m"
INTERVAL_MS = 15 * 60 * 1000

RECONNECT_DELAY = 8         # seconds (as you requested)
HEARTBEAT_SECONDS = 360     # 6 minutes
VERIFY_CANDLES = 3

WICK_SHARE_THRESHOLD = 0.50
WICK_RATIO_THRESHOLD = 0.50

# Small delay between paginated REST calls (protects rate limit even for 1 symbol)
REST_PAGE_DELAY = 0.15

STREAM_URL = f"wss://fstream.binance.com/market/stream?streams={SYMBOL}@kline_{INTERVAL}"
REST_BASE = "https://fapi.binance.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("absorption-bot")

verify_remaining = 0


# ==================== TELEGRAM ====================
def _send_telegram_sync(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": text}, timeout=10)
        if r.status_code != 200:
            log.error(f"Telegram send failed: {r.status_code} {r.text}")
    except Exception:
        log.error(f"Telegram send exception:\n{traceback.format_exc()}")


async def send_telegram(text: str):
    await asyncio.to_thread(_send_telegram_sync, text)


# ==================== REGION / RATIO MATH (unchanged from your logic) ====================
def bucket_trades_by_region(trades, open_p, close_p, high_p, low_p):
    body_top = max(open_p, close_p)
    body_bottom = min(open_p, close_p)

    regions = {
        "upper_wick": {"buy": 0.0, "sell": 0.0},
        "body": {"buy": 0.0, "sell": 0.0},
        "lower_wick": {"buy": 0.0, "sell": 0.0},
    }

    for price, qty, is_buyer_maker in trades:
        if price > body_top:
            region = "upper_wick"
        elif price < body_bottom:
            region = "lower_wick"
        else:
            region = "body"

        if is_buyer_maker:
            regions[region]["sell"] += qty
        else:
            regions[region]["buy"] += qty

    return regions


def region_ratio(region: dict) -> float:
    b, s = region["buy"], region["sell"]
    total = b + s
    if total == 0:
        return 0.0
    return (b - s) / total


def evaluate_absorption(trades, open_p, close_p, high_p, low_p):
    if close_p == open_p:
        return False, None

    candle_positive = close_p > open_p
    candle_negative = close_p < open_p

    regions = bucket_trades_by_region(trades, open_p, close_p, high_p, low_p)

    total_buy = sum(r["buy"] for r in regions.values())
    total_sell = sum(r["sell"] for r in regions.values())
    delta = total_buy - total_sell

    def buy_share(name):
        return regions[name]["buy"] / total_buy if total_buy > 0 else 0.0

    def sell_share(name):
        return regions[name]["sell"] / total_sell if total_sell > 0 else 0.0

    upper_ratio = region_ratio(regions["upper_wick"])
    lower_ratio = region_ratio(regions["lower_wick"])

    buy_absorption = (
        (delta > 0 and candle_positive and buy_share("upper_wick") >= WICK_SHARE_THRESHOLD)
        or (delta > 0 and candle_negative and (
            buy_share("upper_wick") >= WICK_SHARE_THRESHOLD or buy_share("body") >= WICK_SHARE_THRESHOLD
        ))
        or (delta < 0 and candle_negative and upper_ratio >= WICK_RATIO_THRESHOLD)
    )

    sell_absorption = (
        (delta < 0 and candle_negative and sell_share("lower_wick") >= WICK_SHARE_THRESHOLD)
        or (delta < 0 and candle_positive and (
            sell_share("lower_wick") >= WICK_SHARE_THRESHOLD or sell_share("body") >= WICK_SHARE_THRESHOLD
        ))
        or (delta > 0 and candle_positive and lower_ratio <= -WICK_RATIO_THRESHOLD)
    )

    if buy_absorption:
        return True, "BUY"
    if sell_absorption:
        return True, "SELL"
    return False, None


def absorption_label(direction: str) -> str:
    return "BUY ABSORPTION" if direction == "BUY" else "SHORT ABSORPTION"


# ==================== FETCH ALL TRADES FOR ONE CANDLE ====================
def fetch_agg_trades_sync(open_time: int, close_time: int) -> list:
    """
    Pulls every aggTrade that belongs to [open_time, close_time].
    Paginates with fromId when needed. Returns list of (price, qty, is_buyer_maker).
    """
    trades = []
    from_id = None
    max_pages = 40          # safety: 40 * 1000 = 40k trades is more than enough for 15m

    for _ in range(max_pages):
        params = {
            "symbol": SYMBOL.upper(),
            "startTime": open_time,
            "endTime": close_time,
            "limit": 1000,
        }
        if from_id is not None:
            # When using fromId we drop startTime/endTime (Binance prefers one or the other)
            params = {
                "symbol": SYMBOL.upper(),
                "fromId": from_id,
                "limit": 1000,
            }

        r = requests.get(f"{REST_BASE}/fapi/v1/aggTrades", params=params, timeout=10)
        if r.status_code != 200:
            raise RuntimeError(f"aggTrades HTTP {r.status_code}: {r.text[:300]}")

        batch = r.json()
        if not batch:
            break

        for t in batch:
            # Only keep trades that actually fall inside this candle
            t_time = int(t["T"])
            if t_time < open_time:
                continue
            if t_time > close_time:
                return trades          # past the candle, done

            price = float(t["p"])
            qty = float(t["q"])
            is_buyer_maker = t["m"]
            trades.append((price, qty, is_buyer_maker))

        if len(batch) < 1000:
            break

        # Next page starts after the last trade id we received
        from_id = int(batch[-1]["a"]) + 1
        time.sleep(REST_PAGE_DELAY)

    return trades


async def fetch_agg_trades(open_time: int, close_time: int) -> list:
    return await asyncio.to_thread(fetch_agg_trades_sync, open_time, close_time)


# ==================== CORE: PROCESS A CLOSED CANDLE ====================
async def process_closed_candle(k: dict):
    global verify_remaining

    open_time = int(k["t"])
    close_time = open_time + INTERVAL_MS - 1   # inclusive end

    open_price = float(k["o"])
    close_price = float(k["c"])
    high_price = float(k["h"])
    low_price = float(k["l"])

    try:
        trades = await fetch_agg_trades(open_time, close_time)
        log.info(f"Fetched {len(trades)} trades for candle open={open_time}")

        is_absorption, direction = evaluate_absorption(
            trades, open_price, close_price, high_price, low_price
        )

        if is_absorption:
            await send_telegram(f"{SYMBOL.upper()} — {absorption_label(direction)}")
        else:
            log.info(f"Candle closed. No absorption. O:{open_price} C:{close_price}")

        # Verification candles after (re)connect
        if verify_remaining > 0:
            if close_price == open_price:
                await send_telegram(
                    f"🔍 Verification candle ({VERIFY_CANDLES - verify_remaining + 1}/{VERIFY_CANDLES})\n"
                    f"{SYMBOL.upper()} {INTERVAL}: DOJI (O==C=={open_price}) — data is flowing."
                )
            elif not is_absorption:
                direction_label = "GREEN" if close_price > open_price else "RED"
                await send_telegram(
                    f"🔍 Verification candle ({VERIFY_CANDLES - verify_remaining + 1}/{VERIFY_CANDLES})\n"
                    f"{SYMBOL.upper()} {INTERVAL} closed {direction_label} — "
                    f"no absorption, confirming data is live."
                )
            verify_remaining -= 1

    except Exception:
        err = traceback.format_exc()
        log.error(err)
        await send_telegram(f"⚠️ Error while evaluating candle close:\n{err[-500:]}")


# ==================== HEARTBEAT ====================
async def heartbeat_task():
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        await send_telegram(f"❤️ Heartbeat — bot is alive ({now})")


# ==================== WEBSOCKET LOOP ====================
async def process_message(raw: str):
    try:
        msg = json.loads(raw)
        data = msg.get("data", {})
        k = data.get("k")
        if not k:
            return

        if k.get("x"):  # candle closed
            asyncio.create_task(process_closed_candle(k))

    except Exception:
        err = traceback.format_exc()
        log.error(err)
        await send_telegram(f"⚠️ Error processing message:\n{err[-500:]}")


async def run_forever():
    global verify_remaining

    await send_telegram(
        f"🚀 Bot started — watching {SYMBOL.upper()} {INTERVAL} (close-only mode).\n"
        f"Will confirm the first {VERIFY_CANDLES} closed candles."
    )
    first_connect = True

    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=20, ping_timeout=20) as ws:
                if not first_connect:
                    await send_telegram("✅ Reconnected successfully.")
                first_connect = False
                verify_remaining = VERIFY_CANDLES
                log.info("Connected to Binance websocket.")

                async for raw in ws:
                    await process_message(raw)

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.error(f"Connection lost: {e}")
            await send_telegram(
                f"❌ Websocket disconnected: {e}\nReconnecting in {RECONNECT_DELAY}s..."
            )

        except Exception:
            err = traceback.format_exc()
            log.error(err)
            await send_telegram(
                f"⚠️ Unexpected error:\n{err[-500:]}\nReconnecting in {RECONNECT_DELAY}s..."
            )

        await asyncio.sleep(RECONNECT_DELAY)


async def main():
    await asyncio.gather(run_forever(), heartbeat_task())


if __name__ == "__main__":
    asyncio.run(main())