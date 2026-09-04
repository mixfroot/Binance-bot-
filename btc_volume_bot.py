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

# Comma-separated list, TradingView-style suffixes (".p") stripped automatically.
SYMBOLS_RAW = os.environ.get("SYMBOLS", "btcusdt.p,seiusdt.p,suiusdt.p,pixelusdt.p")
SYMBOLS = [s.strip().lower().replace(".p", "") for s in SYMBOLS_RAW.split(",") if s.strip()]


def parse_timeframe_to_ms(tf: str) -> int:
    """Turns '1m' / '3m' / '15m' / '1h' / '4h' / '1d' into milliseconds."""
    tf = tf.strip().lower()
    unit = tf[-1]
    multipliers = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
    if unit not in multipliers or not tf[:-1].isdigit():
        raise ValueError(f"Unsupported timeframe '{tf}'. Use formats like '1m', '3m', '15m', '1h', '1d'.")
    return int(tf[:-1]) * multipliers[unit]


# ---- Timeframe setting: change this one value to switch candle size ----
TIMEFRAME = os.environ.get("TIMEFRAME", "3m")   # e.g. "1m", "3m", "5m", "15m", "1h"
INTERVAL = TIMEFRAME                            # Binance kline stream name uses the same format
INTERVAL_MS = parse_timeframe_to_ms(TIMEFRAME)  # derived automatically, no manual math needed

# How long before candle close to send an early "possible absorption" warning.
PRECLOSE_LEAD_SECONDS = int(os.environ.get("PRECLOSE_LEAD_SECONDS", "60"))

# Binance USDS-M Futures websocket (as of the 2026 base-URL migration).
# Kline + aggTrade both fall under the "/market" routed path, using stream (query) mode.
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
_stream_parts = []
for _sym in SYMBOLS:
    _stream_parts.append(f"{_sym}@kline_{INTERVAL}")
    _stream_parts.append(f"{_sym}@aggTrade")

STREAM_URL = f"wss://fstream.binance.com/market/stream?streams=" + "/".join(_stream_parts)

RECONNECT_DELAY = 6        # seconds
HEARTBEAT_SECONDS = 360    # 6 minutes
VERIFY_CANDLES = 3         # send a status alert for this many candles (per symbol) after each
                            # (re)connect, win or no-win, so you can confirm data is actually flowing

WICK_SHARE_THRESHOLD = 0.50   # >= this share of total buy/sell volume in a region = absorption
WICK_RATIO_THRESHOLD = 0.50   # region's own (buy-sell)/(buy+sell) ratio threshold

GRACE_PERIOD_SECONDS = 2.0     # wait this long after a kline closes before finalizing,
                                # to let slightly-late aggTrades for that candle arrive
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
# Keyed by (symbol, candle open_time (ms)). Each trade is filed into the bucket that matches
# ITS OWN trade timestamp, not "whatever candle we currently think is open." This
# is what makes attribution correct regardless of message arrival order between
# the streams, and keeping symbol in the key keeps coins from mixing.
candle_buckets = defaultdict(list)          # (symbol, open_time) -> list of (price, qty, is_buyer_maker)
bucket_created_at = {}                      # (symbol, open_time) -> wall-clock time.time() when first seen
latest_kline = {}                           # (symbol, open_time) -> most recent kline dict seen (updates live, even before close)
scheduled_preclose = set()                  # (symbol, open_time) values we've already scheduled a pre-close check for
pre_alert_state = {}                        # (symbol, open_time) -> "BUY" / "SELL" if a pre-close warning was sent for it

verify_remaining = defaultdict(lambda: 0)   # symbol -> countdown of closed candles after a (re)connect


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


def evaluate_absorption(trades, open_p, close_p, high_p, low_p):
    """
    Runs the absorption rules against whatever trades/O-H-L-C are passed in.
    Returns (is_absorption: bool, direction: "BUY" | "SELL" | None).
    Used both for the early pre-close check and the final close check.
    """
    if close_p == open_p:
        return False, None  # doji, no color, no absorption logic applies

    candle_positive = close_p > open_p   # green
    candle_negative = close_p < open_p   # red

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


def direction_label(direction: str) -> str:
    """BUY absorption -> go long, SELL absorption -> go short."""
    return "long" if direction == "BUY" else "short"


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
        stale = [key for key, created in bucket_created_at.items() if now - created > BUCKET_MAX_AGE_SECONDS]
        for key in stale:
            symbol, open_time = key
            n_trades = len(candle_buckets.get(key, []))
            candle_buckets.pop(key, None)
            bucket_created_at.pop(key, None)
            latest_kline.pop(key, None)
            scheduled_preclose.discard(key)
            pre_alert_state.pop(key, None)
            log.warning(f"Purged stale trade bucket {symbol} open_time={open_time} ({n_trades} trades, never closed).")
            if n_trades > 0:
                await send_telegram(
                    f"\u26A0\uFE0F Purged a stale candle bucket ({symbol.upper()}, open_time={open_time}, "
                    f"{n_trades} trades) — never received a matching kline close for it."
                )


# ==================== PRE-CLOSE CHECK ====================
async def pre_close_check_task(symbol: str, open_time: int):
    """
    Fires PRECLOSE_LEAD_SECONDS before this candle is due to close. If the
    absorption logic is already true against the live (not-yet-final) O/H/L/C
    and trades so far, sends an early "possible absorption" warning.
    """
    if INTERVAL_MS <= PRECLOSE_LEAD_SECONDS * 1000:
        return  # timeframe too short for a meaningful early warning

    key = (symbol, open_time)
    close_time_sec = (open_time + INTERVAL_MS) / 1000
    fire_at = close_time_sec - PRECLOSE_LEAD_SECONDS
    delay = fire_at - time.time()
    if delay > 0:
        await asyncio.sleep(delay)

    k = latest_kline.get(key)
    if not k or k.get("x"):
        return  # never saw a live update, or it already closed — finalize_candle covers it

    trades = list(candle_buckets.get(key, []))

    try:
        is_absorption, direction = evaluate_absorption(
            trades, float(k["o"]), float(k["c"]), float(k["h"]), float(k["l"])
        )
    except Exception:
        log.error(f"Pre-close check error:\n{traceback.format_exc()}")
        return

    if is_absorption:
        pre_alert_state[key] = direction
        await send_telegram(f"{symbol.upper()} — possible {direction_label(direction)} forming")


# ==================== CORE LOGIC ====================
async def finalize_candle(symbol: str, open_time: int, k: dict):
    """Runs after a short grace period so late/out-of-order aggTrades can still land."""
    key = (symbol, open_time)

    await asyncio.sleep(GRACE_PERIOD_SECONDS)

    trades = candle_buckets.pop(key, [])
    bucket_created_at.pop(key, None)
    latest_kline.pop(key, None)
    scheduled_preclose.discard(key)
    pre_direction = pre_alert_state.pop(key, None)

    try:
        open_price = float(k["o"])
        close_price = float(k["c"])
        high_price = float(k["h"])
        low_price = float(k["l"])

        is_absorption, direction = evaluate_absorption(trades, open_price, close_price, high_price, low_price)

        if is_absorption:
            await send_telegram(f"{symbol.upper()} — {direction_label(direction)}")
        elif pre_direction:
            # a pre-close warning was sent for this candle, but it didn't hold at close
            await send_telegram(f"{symbol.upper()} — possible {direction_label(pre_direction)} failed at close")
        else:
            log.info(f"[{symbol.upper()}] Candle closed. No absorption. O:{open_price} C:{close_price}")

        # First few candles after every (re)connect: report either way, so you
        # can confirm klines + trades are actually flowing without waiting for
        # a real absorption signal.
        if verify_remaining[symbol] > 0:
            n = VERIFY_CANDLES - verify_remaining[symbol] + 1
            if close_price == open_price:
                await send_telegram(
                    f"\U0001F50D Verification candle ({n}/{VERIFY_CANDLES})\n"
                    f"{symbol.upper()} {INTERVAL}: DOJI (O==C=={open_price}) — skipped, data is flowing."
                )
            elif not is_absorption:
                candle_dir = "GREEN" if close_price > open_price else "RED"
                await send_telegram(
                    f"\U0001F50D Verification candle ({n}/{VERIFY_CANDLES})\n"
                    f"{symbol.upper()} {INTERVAL} closed {candle_dir} — "
                    f"no absorption, this is just confirming data is live."
                )
            verify_remaining[symbol] -= 1

    except Exception:
        err = traceback.format_exc()
        log.error(err)
        await send_telegram(f"\u26A0\uFE0F Logic error while evaluating {symbol.upper()} candle close:\n{err[-500:]}")


async def process_message(raw: str):
    try:
        msg = json.loads(raw)
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        # Stream names look like "btcusdt@kline_3m" / "seiusdt@aggTrade" — pull the symbol
        # out of the stream name itself so each message is routed to the right coin.
        symbol = stream.split("@")[0]
        if symbol not in SYMBOLS:
            return

        if stream.endswith("@kline_" + INTERVAL):
            k = data["k"]
            open_time = k["t"]
            key = (symbol, open_time)
            latest_kline[key] = k

            if key not in scheduled_preclose:
                scheduled_preclose.add(key)
                asyncio.create_task(pre_close_check_task(symbol, open_time))

            if k["x"]:  # candle closed -> schedule finalize after grace period, don't block the loop
                asyncio.create_task(finalize_candle(symbol, open_time, k))

        elif stream.endswith("@aggTrade"):
            price = float(data["p"])
            qty = float(data["q"])
            is_buyer_maker = data["m"]
            trade_time_ms = int(data["T"])
            ot = trade_open_time(trade_time_ms)
            key = (symbol, ot)

            if key not in bucket_created_at:
                bucket_created_at[key] = time.time()
            candle_buckets[key].append((price, qty, is_buyer_maker))

    except Exception:
        err = traceback.format_exc()
        log.error(err)
        await send_telegram(f"\u26A0\uFE0F Error processing message:\n{err[-500:]}")


# ==================== WEBSOCKET LOOP ====================
async def run_forever():
    symbols_str = ", ".join(s.upper() for s in SYMBOLS)
    await send_telegram(
        f"\U0001F680 Bot started — watching {symbols_str} {INTERVAL} candles.\n"
        f"Will confirm the first {VERIFY_CANDLES} closed candles per coin so you can verify it's working."
    )
    first_connect = True

    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=20, ping_timeout=20) as ws:
                if not first_connect:
                    await send_telegram("\u2705 Reconnected successfully.")
                first_connect = False
                for sym in SYMBOLS:
                    verify_remaining[sym] = VERIFY_CANDLES
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
