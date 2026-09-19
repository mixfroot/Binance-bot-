import os
import asyncio
import aiohttp
import pandas as pd
import json
import warnings
import socket
from datetime import datetime, timedelta
from collections import deque

warnings.filterwarnings("ignore")

# === CONFIG ===
selected_tf = "1m"
warmup_candles = 200
rsi_period = 14

# Filters
min_move_pct_15m = 1.0
min_quote_volume_24h = 50_000_000

# Hard cap for Railway free plan
MAX_TRACKED = 16

# RSI thresholds
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# Binance
binance_rest_url = "https://fapi.binance.com/fapi/v1/klines"
binance_24h_url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
binance_ws_base = "wss://fstream.binance.com/stream"

# Telegram
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"

def telegram_url():
    return f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# === GLOBAL STATE ===
df_map = {}                 # symbol -> DataFrame
stream_tasks = {}           # symbol -> task
tracking_symbols = set()
fail_count = {}
message_queue = deque()

# === TELEGRAM ===
async def _try_send_telegram(msg, session, retries=3):
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[TEL] {msg}")
        return True
    for attempt in range(retries):
        try:
            async with session.post(
                telegram_url(),
                json={"chat_id": CHAT_ID, "text": msg},
                timeout=30
            ) as resp:
                if resp.status == 200:
                    return True
                txt = await resp.text()
                print(f"Telegram error {resp.status}: {txt}")
        except Exception as e:
            print(f"❌ Telegram failed: {type(e).__name__}: {e}")
        await asyncio.sleep(2 ** attempt)
    return False

async def telegram_worker():
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                if message_queue:
                    msg = message_queue.popleft()
                    await _try_send_telegram(msg, session)
                    await asyncio.sleep(1.1)
                else:
                    await asyncio.sleep(0.15)
            except Exception as e:
                print(f"[TEL worker] {e}")
                await asyncio.sleep(1)

def send_telegram(msg):
    message_queue.append(msg)

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

# === DITCH COIN ===
async def ditch_symbol(symbol, reason=""):
    if symbol in stream_tasks:
        task = stream_tasks.pop(symbol, None)
        if task and not task.done():
            task.cancel()
    df_map.pop(symbol, None)
    tracking_symbols.discard(symbol)
    print(f"[Ditch] {symbol} removed. Reason: {reason}. Now tracking {len(tracking_symbols)}")

# === RSI CHECK + IMMEDIATE DITCH ===
async def check_rsi_and_alert(symbol, rsi_value):
    if rsi_value > RSI_OVERBOUGHT or rsi_value < RSI_OVERSOLD:
        side = "Overbought" if rsi_value > RSI_OVERBOUGHT else "Oversold"
        msg = f"⚠️ {symbol} {side} — RSI: {rsi_value:.2f}"
        print(msg)
        send_telegram(msg)
        await ditch_symbol(symbol, reason=f"RSI {side}")

# === GET NEW CANDIDATES (15m movers) ===
async def get_new_movers(session):
    try:
        async with session.get(binance_24h_url) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as e:
        send_telegram(f"Volume fetch error: {e}")
        return []

    # Volume filter first
    candidates = [
        item["symbol"]
        for item in data
        if item.get("symbol", "").endswith("USDT")
        and float(item.get("quoteVolume", 0)) >= min_quote_volume_24h
        and not any(c.isdigit() for c in item["symbol"].replace("USDT", ""))
    ]

    if not candidates:
        return []

    sem = asyncio.Semaphore(8)

    async def check_one(sym):
        async with sem:
            try:
                raw = await fetch_candles(session, sym, "15m", 3)
                if not raw or len(raw) < 2:
                    return None
                # Take the last closed candle (second last)
                k = raw[-2]
                o = float(k[1])
                c = float(k[4])
                if o <= 0:
                    return None
                move = abs((c - o) / o) * 100
                if move >= min_move_pct_15m:
                    return sym
            except Exception:
                return None
            return None

    tasks = [asyncio.create_task(check_one(s)) for s in candidates]
    results = await asyncio.gather(*tasks)
    movers = [r for r in results if r]
    print(f"[Filter] New 15m movers found: {len(movers)}")
    return movers

# === BOOTSTRAP + START STREAM ===
async def bootstrap_and_start(session, symbol):
    if symbol in tracking_symbols or len(tracking_symbols) >= MAX_TRACKED:
        return

    try:
        raw = await fetch_candles(session, symbol, selected_tf, warmup_candles)
        df = build_ohlc_df(raw)
        if df.empty or len(df) < rsi_period + 5:
            return

        df_map[symbol] = df
        tracking_symbols.add(symbol)
        stream_tasks[symbol] = asyncio.create_task(process_stream(symbol, session))
        print(f"[Track] Added {symbol}. Tracking: {len(tracking_symbols)}/{MAX_TRACKED}")
    except Exception as e:
        print(f"[Bootstrap] {symbol} failed: {e}")

# === WEBSOCKET HANDLER ===
async def process_stream(symbol, session):
    for attempt in range(2):
        try:
            stream = f"{symbol.lower()}@kline_{selected_tf}"
            url = f"{binance_ws_base}?streams={stream}"
            print(f"[WS] Connecting {symbol}")
            async with session.ws_connect(url, autoping=True, heartbeat=30) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        k = data.get("data", {}).get("k", {})
                        if not k or not k.get("x"):
                            continue

                        close_time = pd.to_datetime(k["T"], unit="ms", utc=True)
                        row = pd.DataFrame(
                            [[float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"])]],
                            index=[close_time],
                            columns=["open", "high", "low", "close"]
                        )

                        df = df_map.get(symbol)
                        if df is None:
                            return

                        if close_time in df.index:
                            df = df.drop(close_time)
                        df = pd.concat([df, row]).iloc[-warmup_candles:]
                        df_map[symbol] = df

                        if len(df) < rsi_period + 1:
                            continue

                        # Same RSI calculation as your original code
                        delta = df["close"].diff()
                        gain = delta.clip(lower=0)
                        loss = -delta.clip(upper=0)
                        avg_gain = gain.ewm(alpha=1/rsi_period, adjust=False, min_periods=rsi_period).mean()
                        avg_loss = loss.ewm(alpha=1/rsi_period, adjust=False, min_periods=rsi_period).mean()
                        rs = avg_gain / avg_loss
                        rsi = 100 - (100 / (1 + rs))
                        await check_rsi_and_alert(symbol, rsi.iloc[-1])

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError("WebSocket error")

        except asyncio.CancelledError:
            print(f"[WS] {symbol} cancelled")
            return
        except Exception as e:
            print(f"[WS] {symbol} error: {type(e).__name__}: {e}")
            if attempt < 1:
                await asyncio.sleep(3)
                continue
            fail_count[symbol] = fail_count.get(symbol, 0) + 1
            await ditch_symbol(symbol, reason="WS failed")
            return

# === 15-MINUTE MAINTENANCE ===
async def maintenance_loop(session):
    while True:
        await asyncio.sleep(900)  # 15 minutes
        try:
            print("[Maint] Running 15m scan...")
            movers = await get_new_movers(session)

            # Add new ones only if we have free slots
            free_slots = MAX_TRACKED - len(tracking_symbols)
            if free_slots > 0 and movers:
                to_add = [s for s in movers if s not in tracking_symbols][:free_slots]
                for sym in to_add:
                    await bootstrap_and_start(session, sym)

            # Clean dead tasks
            for sym, task in list(stream_tasks.items()):
                if task.done():
                    await ditch_symbol(sym, reason="task died")

            print(f"[Maint] Tracking {len(tracking_symbols)}/{MAX_TRACKED}")
        except Exception as e:
            print(f"[Maint] Error: {e}")
            send_telegram(f"Maintenance error: {e}")

# === MAIN ===
async def main():
    asyncio.create_task(telegram_worker())

    async with aiohttp.ClientSession() as session:
        start_msg = (
            f"🚀 RSI Mover Bot started\n"
            f"TF: {selected_tf} | OB>{RSI_OVERBOUGHT} / OS<{RSI_OVERSOLD}\n"
            f"Only tracks coins with ≥{min_move_pct_15m}% 15m move + ≥${min_quote_volume_24h//1_000_000}M vol\n"
            f"Max {MAX_TRACKED} coins | Ditch after alert"
        )
        print(start_msg)
        send_telegram(start_msg)

        # Initial fill
        movers = await get_new_movers(session)
        for sym in movers[:MAX_TRACKED]:
            await bootstrap_and_start(session, sym)

        asyncio.create_task(maintenance_loop(session))

        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("Stopped by user")
            break
        except Exception as e:
            print(f"Bot crashed: {e} — restarting in 5s")
            try:
                asyncio.run(asyncio.sleep(5))
            except Exception:
                pass