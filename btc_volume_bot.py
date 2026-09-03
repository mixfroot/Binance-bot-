import asyncio
import json
import logging
import os
import traceback
from datetime import datetime, timezone

import requests
import websockets

# ==================== CONFIG ====================
# Put these in environment variables instead of hardcoding them.
# (You pasted a real token in chat earlier — revoke it via @BotFather and use a new one.)
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "6263967739")

SYMBOL_RAW = "btcusdt.p"    # ".p" (TradingView perpetual notation) is stripped automatically
SYMBOL = SYMBOL_RAW.lower().replace(".p", "")
INTERVAL = "1m"

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


# ==================== STATE ====================
class CandleState:
    def __init__(self):
        self.reset()

    def reset(self):
        self.open_time = None
        self.buy_vol = 0.0
        self.sell_vol = 0.0

    def add_trade(self, qty: float, is_buyer_maker: bool):
        # isBuyerMaker True  -> taker was a SELLER -> counts as sell volume
        # isBuyerMaker False -> taker was a BUYER  -> counts as buy volume
        if is_buyer_maker:
            self.sell_vol += qty
        else:
            self.buy_vol += qty

    @property
    def delta(self):
        return self.buy_vol - self.sell_vol


state = CandleState()
verify_remaining = 0   # counts down on each closed candle after a (re)connect


# ==================== HEARTBEAT ====================
async def heartbeat_task():
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        await send_telegram(f"\U0001F493 Heartbeat — bot is alive ({now})")


# ==================== CORE LOGIC ====================
async def handle_kline(k: dict):
    global verify_remaining

    open_time = k["t"]
    is_closed = k["x"]

    # New candle started -> reset accumulator
    if state.open_time != open_time:
        state.reset()
        state.open_time = open_time

    if is_closed:
        try:
            open_price = float(k["o"])
            close_price = float(k["c"])
            candle_positive = close_price > open_price
            candle_negative = close_price < open_price
            delta = state.delta

            is_absorption = (delta < 0 and candle_positive) or (delta > 0 and candle_negative)

            if delta < 0 and candle_positive:
                await send_telegram(
                    f"\U0001F7E2\U0001F53B ABSORPTION (buy side)\n"
                    f"{SYMBOL.upper()} {INTERVAL}\n"
                    f"Candle closed GREEN while delta was NEGATIVE ({delta:.4f})\n"
                    f"O:{open_price} C:{close_price}"
                )
            elif delta > 0 and candle_negative:
                await send_telegram(
                    f"\U0001F534\U0001F53A ABSORPTION (sell side)\n"
                    f"{SYMBOL.upper()} {INTERVAL}\n"
                    f"Candle closed RED while delta was POSITIVE ({delta:.4f})\n"
                    f"O:{open_price} C:{close_price}"
                )
            else:
                log.info(
                    f"Candle closed. No absorption. delta={delta:.4f} "
                    f"O:{open_price} C:{close_price}"
                )

            # First few candles after every (re)connect: report either way, so you
            # can confirm klines + trades are actually flowing without waiting for
            # a real absorption signal.
            if verify_remaining > 0:
                if not is_absorption:
                    direction = "GREEN" if candle_positive else ("RED" if candle_negative else "FLAT")
                    await send_telegram(
                        f"\U0001F50D Verification candle ({VERIFY_CANDLES - verify_remaining + 1}/{VERIFY_CANDLES})\n"
                        f"{SYMBOL.upper()} {INTERVAL} closed {direction}, delta={delta:.4f}\n"
                        f"O:{open_price} C:{close_price}\n"
                        f"No absorption — this is just confirming data is live."
                    )
                verify_remaining -= 1

        except Exception:
            err = traceback.format_exc()
            log.error(err)
            await send_telegram(f"\u26A0\uFE0F Logic error while evaluating candle close:\n{err[-500:]}")

        state.reset()


async def process_message(raw: str):
    try:
        msg = json.loads(raw)
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if stream.endswith("@kline_" + INTERVAL):
            await handle_kline(data["k"])

        elif stream.endswith("@aggTrade"):
            qty = float(data["q"])
            is_buyer_maker = data["m"]
            state.add_trade(qty, is_buyer_maker)

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
    await asyncio.gather(run_forever(), heartbeat_task())


if __name__ == "__main__":
    asyncio.run(main())
