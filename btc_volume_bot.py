import asyncio
import json
import logging
import os
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone

import requests
import websockets

# ==================== CONFIG ====================
# Put these in environment variables instead of hardcoding them.
# (This token has been visible in shared chat — rotate it via @BotFather if that ever matters.)
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "6263967739")

SYMBOL_RAW = "btcusdt.p"    # ".p" (TradingView perpetual notation) is stripped automatically
SYMBOL = SYMBOL_RAW.lower().replace(".p", "")
INTERVAL = "1m"
INTERVAL_MS = 60_000        # 1m in milliseconds; update if you ever change INTERVAL

# Binance USDS-M Futures websocket (as of the 2026 base-URL migration).
# Kline + aggTrade both fall under the "/market" routed path, using stream (query) mode.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
STREAM_URL = (
    f"wss://fstream.binance.com/market/stream"
    f"?streams={SYMBOL}@kline_{INTERVAL}/{SYMBOL}@aggTrade"
)

RECONNECT_DELAY = 6        # seconds
HEARTBEAT_SECONDS = 360    # 6 minutes
VERIFY_CANDLES = 3         # send a status alert for this many candles after each (re)connect,
                            # win or no-win, so you can confirm data is actually flowing

WICK_SHARE_THRESHOLD = 0.50   # >= this share of total buy/sell volume in a region = absorption
WICK_RATIO_THRESHOLD = 0.50   # region's own (buy-sell)/(buy+sell) ratio threshold

GRACE_PERIOD_SECONDS = 2.0     # wait this long after a kline closes before finalizing,
                                # to let slightly-late aggTrades for that minute arrive
BUCKET_MAX_AGE_SECONDS = 300   # safety net: purge any trade bucket older than this that
                                # never got a matching kline close (missed message, etc.)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("absorption-bot")


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
    # requests is blocking, so push it to a thread and keep the websocket loop free
    await asyncio.to_thread(_send_telegram_sync, text)


# ==================== TRADE BUCKETS (timestamp-based, not stream-order-based) ====================
# Keyed by candle open_time (ms). Each trade is filed into the bucket that matches
# ITS OWN trade timestamp, not "whatever candle we currently think is open." This
# is what makes attribution correct regardless of message arrival order between
# the two independent websocket streams.
candle_buckets = defaultdict(list)          # open_time -> list of (price, qty, is_buyer_maker)
bucket_created_at = {}                      # open_time -> wall-clock time.time() when first seen

verify_remaining = 0   # counts down on each closed candle after a (re)connect


def trade_open_time(trade_time_ms: int) -> int:
    return (trade_time_ms // INTERVAL_MS) * INTERVAL_MS


# ==================== REGION / RATIO MATH ====================
def bucket_trades_by_region(trades, open_p, close_p, high_p, low_p):
    """
    Classify every trade into upper_wick / body / lower_wick based on final O/H/L/C,
    and sum buy vs sell volume (taker side) within each region.
    """
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

        # isBuyerMaker True  -> taker was a SELLER -> sell volume
        # isBuyerMaker False -> taker was a BUYER  -> buy volume
        if is_buyer_maker:
            regions[region]["sell"] += qty
        else:
            regions[region]["buy"] += qty

    return regions


def region_ratio(region: dict) -> float:
    """(buy - sell) / (buy + sell) for one region. Range -1 (all sell) to +1 (all buy)."""
    b, s = region["buy"], region["sell"]
    total = b + s
    if total == 0:
        return 0.0
    return (b - s) / total


def fmt_regions(regions: dict) -> str:
    lines = []
    for name in ("upper_wick", "body", "lower_wick"):
        r = regions[name]
        lines.append(
            f"  {name}: buy={r['buy']:.4f} sell={r['sell']:.4f} ratio={region_ratio(r):+.2f}"
        )
    return "\n".join(lines)


# ==================== HEARTBEAT ====================
async def heartbeat_task():
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        await send_telegram(f"\U0001F493 Heartbeat — bot is alive ({now})")


# ==================== STALE BUCKET CLEANUP ====================
async def bucket_cleanup_task():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        stale = [ot for ot, created in bucket_created_at.items() if now - created > BUCKET_MAX_AGE_SECONDS]
        for ot in stale:
            n_trades = len(candle_buckets.get(ot, []))
            candle_buckets.pop(ot, None)
            bucket_created_at.pop(ot, None)
            log.warning(f"Purged stale trade bucket open_time={ot} ({n_trades} trades, never closed).")
            if n_trades > 0:
                await send_telegram(
                    f"\u26A0\uFE0F Purged a stale candle bucket (open_time={ot}, {n_trades} trades) — "
                    f"never received a matching kline close for it."
                )


# ==================== CORE LOGIC ====================
async def finalize_candle(open_time: int, k: dict):
    """Runs after a short grace period so late/out-of-order aggTrades can still land."""
    global verify_remaining

    await asyncio.sleep(GRACE_PERIOD_SECONDS)

    trades = candle_buckets.pop(open_time, [])
    bucket_created_at.pop(open_time, None)

    try:
        open_price = float(k["o"])
        close_price = float(k["c"])
        high_price = float(k["h"])
        low_price = float(k["l"])

        # ---- DOJI: skip entirely, no color = no absorption logic applies ----
        if close_price == open_price:
            log.info(f"Doji candle skipped (open==close=={open_price}), open_time={open_time}.")
            if verify_remaining > 0:
                await send_telegram(
                    f"\U0001F50D Verification candle ({VERIFY_CANDLES - verify_remaining + 1}/{VERIFY_CANDLES})\n"
                    f"{SYMBOL.upper()} {INTERVAL}: DOJI (O==C=={open_price}) — skipped, data is flowing."
                )
                verify_remaining -= 1
            return

        candle_positive = close_price > open_price   # green
        candle_negative = close_price < open_price   # red

        regions = bucket_trades_by_region(trades, open_price, close_price, high_price, low_price)

        total_buy = sum(r["buy"] for r in regions.values())
        total_sell = sum(r["sell"] for r in regions.values())
        delta = total_buy - total_sell

        def buy_share(name):
            return regions[name]["buy"] / total_buy if total_buy > 0 else 0.0

        def sell_share(name):
            return regions[name]["sell"] / total_sell if total_sell > 0 else 0.0

        upper_ratio = region_ratio(regions["upper_wick"])
        lower_ratio = region_ratio(regions["lower_wick"])

        # ---- BUY ABSORPTION (as specified) ----
        buy_abs_1 = delta > 0 and candle_positive and buy_share("upper_wick") >= WICK_SHARE_THRESHOLD
        buy_abs_2 = delta > 0 and candle_negative and (
            buy_share("upper_wick") >= WICK_SHARE_THRESHOLD or buy_share("body") >= WICK_SHARE_THRESHOLD
        )
        buy_abs_3 = delta < 0 and candle_negative and upper_ratio >= WICK_RATIO_THRESHOLD

        # ---- SELL ABSORPTION (mirrored: upper_wick<->lower_wick, buy<->sell, green<->red) ----
        sell_abs_1 = delta < 0 and candle_negative and sell_share("lower_wick") >= WICK_SHARE_THRESHOLD
        sell_abs_2 = delta < 0 and candle_positive and (
            sell_share("lower_wick") >= WICK_SHARE_THRESHOLD or sell_share("body") >= WICK_SHARE_THRESHOLD
        )
        sell_abs_3 = delta > 0 and candle_positive and lower_ratio <= -WICK_RATIO_THRESHOLD

        triggered = []
        if buy_abs_1: triggered.append("BUY absorption #1 (green candle, buying stalled in upper wick)")
        if buy_abs_2: triggered.append("BUY absorption #2 (red candle, buying absorbed in upper wick/body)")
        if buy_abs_3: triggered.append("BUY absorption #3 (red candle, upper wick net-buy dominant)")
        if sell_abs_1: triggered.append("SELL absorption #1 (red candle, selling stalled in lower wick)")
        if sell_abs_2: triggered.append("SELL absorption #2 (green candle, selling absorbed in lower wick/body)")
        if sell_abs_3: triggered.append("SELL absorption #3 (green candle, lower wick net-sell dominant)")

        is_absorption = len(triggered) > 0

        if is_absorption:
            emoji = "\U0001F7E2\U0001F53B" if any(t.startswith("BUY") for t in triggered) else "\U0001F534\U0001F53A"
            await send_telegram(
                f"{emoji} ABSORPTION DETECTED\n"
                f"{SYMBOL.upper()} {INTERVAL}\n"
                f"Triggered: {', '.join(triggered)}\n"
                f"O:{open_price} H:{high_price} L:{low_price} C:{close_price}\n"
                f"Delta: {delta:+.4f}\n"
                f"{fmt_regions(regions)}"
            )
        else:
            log.info(
                f"Candle closed. No absorption. delta={delta:+.4f} "
                f"O:{open_price} C:{close_price} upper_ratio={upper_ratio:+.2f} lower_ratio={lower_ratio:+.2f}"
            )

        # First few candles after every (re)connect: report either way, so you
        # can confirm klines + trades are actually flowing without waiting for
        # a real absorption signal.
        if verify_remaining > 0:
            if not is_absorption:
                direction = "GREEN" if candle_positive else "RED"
                await send_telegram(
                    f"\U0001F50D Verification candle ({VERIFY_CANDLES - verify_remaining + 1}/{VERIFY_CANDLES})\n"
                    f"{SYMBOL.upper()} {INTERVAL} closed {direction}, delta={delta:+.4f}\n"
                    f"O:{open_price} C:{close_price}\n"
                    f"{fmt_regions(regions)}\n"
                    f"No absorption — this is just confirming data is live."
                )
            verify_remaining -= 1

    except Exception:
        err = traceback.format_exc()
        log.error(err)
        await send_telegram(f"\u26A0\uFE0F Logic error while evaluating candle close:\n{err[-500:]}")


async def process_message(raw: str):
    try:
        msg = json.loads(raw)
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if stream.endswith("@kline_" + INTERVAL):
            k = data["k"]
            if k["x"]:  # candle closed -> schedule finalize after grace period, don't block the loop
                asyncio.create_task(finalize_candle(k["t"], k))

        elif stream.endswith("@aggTrade"):
            price = float(data["p"])
            qty = float(data["q"])
            is_buyer_maker = data["m"]
            trade_time_ms = int(data["T"])
            ot = trade_open_time(trade_time_ms)

            if ot not in bucket_created_at:
                bucket_created_at[ot] = time.time()
            candle_buckets[ot].append((price, qty, is_buyer_maker))

    except Exception:
        err = traceback.format_exc()
        log.error(err)
        await send_telegram(f"\u26A0\uFE0F Error processing message:\n{err[-500:]}")


# ==================== WEBSOCKET LOOP ====================
async def run_forever():
    global verify_remaining

    await send_telegram(
        f"\U0001F680 Bot started — watching {SYMBOL.upper()} {INTERVAL} candles.\n"
        f"Will confirm the first {VERIFY_CANDLES} closed candles so you can verify it's working."
    )
    first_connect = True

    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=20, ping_timeout=20) as ws:
                if not first_connect:
                    await send_telegram("\u2705 Reconnected successfully.")
                first_connect = False
                verify_remaining = VERIFY_CANDLES
                log.info("Connected to Binance websocket.")

                async for raw in ws:
                    await process_message(raw)

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.error(f"Connection lost: {e}")
            await send_telegram(f"\u274C Websocket disconnected: {e}\nReconnecting in {RECONNECT_DELAY}s...")

        except Exception:
            err = traceback.format_exc()
            log.error(err)
            await send_telegram(f"\u26A0\uFE0F Unexpected error:\n{err[-500:]}\nReconnecting in {RECONNECT_DELAY}s...")

        await asyncio.sleep(RECONNECT_DELAY)


async def main():
    await asyncio.gather(run_forever(), heartbeat_task(), bucket_cleanup_task())


if __name__ == "__main__":
    asyncio.run(main())
