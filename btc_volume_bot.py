import os
import asyncio
import aiohttp
import pandas as pd
import json
import warnings
import socket
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

# === CONFIG ===
SYMBOL = "BTCUSDT"
selected_tf = "1m"
warmup_candles = 200
rsi_period = 14

# Cooldown for RSI alerts
cooldown_period = timedelta(minutes=15)

# RSI thresholds
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# Binance endpoints
binance_rest_url = "https://fapi.binance.com/fapi/v1/klines"
binance_ws_base = "wss://fstream.binance.com/stream"

# Telegram credentials — set these in Railway's Variables tab, NOT hardcoded
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

def telegram_url():
    return f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# === GLOBAL STATE ===
df_current = pd.DataFrame()
last_alert_time = None
message_queue = asyncio.Queue()

# === TELEGRAM SENDER (QUEUED) ===
async def _try_send_telegram(msg, session, retries=3):
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[TEL] {msg}")
        return True
    for attempt in range(retries):
        try:
            async with session.post(
                telegram_url(),
                json={"chat_id": CHAT_ID, "text": msg},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    return True
                else:
                    txt = await resp.text()
                    print(f"Telegram error {resp.status}: {txt}")
        except asyncio.TimeoutError:
            print(f"Telegram timeout, attempt {attempt+1}")
        except Exception as e:
            print(f"Telegram failed: {type(e).__name__}: {e}")
        await asyncio.sleep(2 ** attempt)
    print(f"Telegram send ultimately failed: {msg}")
    return False

async def telegram_worker():
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            msg = await message_queue.get()
            await _try_send_telegram(msg, session)
            await asyncio.sleep(1)

def send_telegram(msg):
    message_queue.put_nowait(msg)

# === RSI ALERT WITH COOLDOWN ===
async def check_rsi_and_alert(rsi_value):
    global last_alert_time
    now = datetime.utcnow()
    if last_alert_time and now - last_alert_time < cooldown_period:
        return
    if rsi_value > RSI_OVERBOUGHT or rsi_value < RSI_OVERSOLD:
        side = f"Overbought (> {RSI_OVERBOUGHT})" if rsi_value > RSI_OVERBOUGHT else f"Oversold (< {RSI_OVERSOLD})"
        msg = f"⚠️ {SYMBOL} ({selected_tf}) {side} — RSI: {rsi_value:.2f}"
        print(msg)
        send_telegram(msg)
        last_alert_time = now

# === HELPERS ===
async def fetch_candles(session, symbol, interval, limit):
    url = f"{binance_rest_url}?symbol={symbol}&interval={interval}&limit={limit}"
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()

def build_ohlc_df(raw):
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close",
        "volume", "close_time", "quote_volume", "trades",
        "taker_base_vol", "taker_quote_vol", "ignore"
    ])
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df.set_index("close_time")[["open", "high", "low", "close"]]

def compute_rsi(df):
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/rsi_period, adjust=False, min_periods=rsi_period).mean()
    avg_loss = loss.ewm(alpha=1/rsi_period, adjust=False, min_periods=rsi_period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# === STREAM HANDLER ===
async def process_stream(session):
    global df_current
    while True:
        try:
            stream = f"{SYMBOL.lower()}@kline_{selected_tf}"
            url = f"{binance_ws_base}?streams={stream}"
            print(f"[WS] Connecting {SYMBOL} ({selected_tf})")
            async with session.ws_connect(url, autoping=True, heartbeat=30) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        k = data.get("data", {}).get("k", {})
                        if not k or not k.get("x", False):
                            continue
                        close_time = pd.to_datetime(k["T"], unit="ms", utc=True)
                        row = pd.DataFrame(
                            [[float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"])]],
                            index=[close_time],
                            columns=["open", "high", "low", "close"]
                        )
                        if close_time in df_current.index:
                            df_current = df_current.drop(close_time)
                        df_current = pd.concat([df_current, row]).iloc[-warmup_candles:]

                        if len(df_current) < rsi_period + 1:
                            continue

                        rsi_series = compute_rsi(df_current)
                        await check_rsi_and_alert(rsi_series.iloc[-1])

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError("WebSocket error")
        except Exception as e:
            print(f"[WS] {SYMBOL} error: {type(e).__name__}: {e}")
            send_telegram(f"⛔ Stream error for {SYMBOL}, reconnecting: {e}")
            await asyncio.sleep(3)
            continue

# === MAIN ===
async def main():
    global df_current
    asyncio.create_task(telegram_worker())
    async with aiohttp.ClientSession() as session:
        start_msg = (
            f"🚀 RSI scanner started for {SYMBOL} ({selected_tf}) | "
            f"OB>{RSI_OVERBOUGHT} / OS<{RSI_OVERSOLD} | Cooldown: {int(cooldown_period.total_seconds()//60)}m"
        )
        print(start_msg)
        send_telegram(start_msg)

        raw = await fetch_candles(session, SYMBOL, selected_tf, warmup_candles)
        df_current = build_ohlc_df(raw)

        await process_stream(session)

# === RESTART LOOP ===
if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("Interrupted by user. Exiting.")
            break
        except Exception as e:
            print(f"Bot error, restarting: {type(e).__name__} - {e}")
            asyncio.run(asyncio.sleep(3))
            continue