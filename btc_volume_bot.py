#!/usr/bin/env python3
"""
BTCUSDT.P (Binance USDT-M Futures) 1m volume-spike Telegram bot.

- Connects to Binance Futures websocket for BTCUSDT 1m klines (no REST/API key needed).
- On every CLOSED 1m candle, keeps a rolling window of the last 60 closed candles' volumes.
- If a closed candle's volume > mean + 2*stdev (of the trailing 60), sends a Telegram alert.
- Auto-reconnects on any error, waiting 6 seconds, and alerts on error + on successful reconnect.
- Sends a startup alert, and a "still alive" heartbeat every 5 minutes, 24/7.

Run:
    pip install websockets requests
    python3 btc_volume_bot.py
"""

import asyncio
import json
import logging
import statistics
from collections import deque
from datetime import datetime, timezone

import requests
import websockets

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"

SYMBOL = "btcusdt"
INTERVAL = "1m"
WS_URL = f"wss://fstream.binance.com/ws/{SYMBOL}@kline_{INTERVAL}"

WINDOW_SIZE = 60          # rolling number of closed candles used for mean/std
STD_MULTIPLIER = 2.0      # alert threshold multiplier
RETRY_SECONDS = 6         # reconnect delay on error
HEARTBEAT_SECONDS = 5 * 60  # "still alive" interval

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("btc-vol-bot")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def send_telegram_sync(text: str) -> None:
    """Blocking Telegram send, used inside asyncio.to_thread()."""
    try:
        resp = requests.post(
            TELEGRAM_API,
            data={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code != 200:
            log.error("Telegram send failed (%s): %s", resp.status_code, resp.text)
    except Exception as e:
        log.error("Telegram send exception: %s", e)


async def send_alert(text: str) -> None:
    await asyncio.to_thread(send_telegram_sync, text)


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# Heartbeat task
# ---------------------------------------------------------------------------
async def heartbeat_loop():
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        await send_alert(
            f"✅ Bot alive — {SYMBOL.upper()}.P {INTERVAL} volume watcher still running.\n"
            f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )


# ---------------------------------------------------------------------------
# Main websocket loop with auto-reconnect
# ---------------------------------------------------------------------------
async def kline_loop():
    volumes: deque = deque(maxlen=WINDOW_SIZE)
    first_connection = True

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                if first_connection:
                    await send_alert(
                        f"🚀 Bot started — watching {SYMBOL.upper()}.P {INTERVAL} candles.\n"
                        f"Alert rule: closed-candle volume > mean + {STD_MULTIPLIER}×stdev "
                        f"(trailing {WINDOW_SIZE} candles)."
                    )
                    first_connection = False
                else:
                    await send_alert("✅ Reconnected successfully to Binance websocket.")
                    log.info("Reconnected successfully.")

                log.info("Connected to %s", WS_URL)

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                    except Exception:
                        continue

                    k = msg.get("k")
                    if not k:
                        continue

                    is_closed = k.get("x", False)
                    if not is_closed:
                        continue  # only act on fully closed candles

                    close_time = k["T"]
                    close_price = float(k["c"])
                    volume = float(k["v"])

                    volumes.append(volume)

                    if len(volumes) >= WINDOW_SIZE:
                        baseline = list(volumes)[:-1] if len(volumes) == WINDOW_SIZE else list(volumes)
                        if len(baseline) >= 2:
                            mean_v = statistics.mean(baseline)
                            std_v = statistics.stdev(baseline)
                            threshold = mean_v + STD_MULTIPLIER * std_v

                            log.info(
                                "Closed candle %s | vol=%.3f mean=%.3f std=%.3f thresh=%.3f",
                                fmt_ts(close_time), volume, mean_v, std_v, threshold,
                            )

                            if std_v > 0 and volume > threshold:
                                z = (volume - mean_v) / std_v
                                await send_alert(
                                    "🚨 <b>Volume Spike Alert</b> — BTCUSDT.P 1m\n"
                                    f"Candle close: {fmt_ts(close_time)}\n"
                                    f"Close price: {close_price}\n"
                                    f"Volume: {volume:.3f}\n"
                                    f"60-candle mean: {mean_v:.3f}\n"
                                    f"60-candle stdev: {std_v:.3f}\n"
                                    f"Threshold (mean+{STD_MULTIPLIER}σ): {threshold:.3f}\n"
                                    f"Z-score: {z:.2f}"
                                )
                    else:
                        log.info(
                            "Warming up: %d/%d closed candles collected.",
                            len(volumes), WINDOW_SIZE,
                        )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Websocket error: %s", e)
            await send_alert(f"⚠️ Error: {e}\nRetrying in {RETRY_SECONDS}s...")
            await asyncio.sleep(RETRY_SECONDS)
            continue


async def main():
    await asyncio.gather(
        kline_loop(),
        heartbeat_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
