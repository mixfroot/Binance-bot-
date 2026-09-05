import aiohttp
import asyncio
import json
import time
import requests
from datetime import datetime

# ====================== CONFIG ======================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"

SYMBOL    = "BTCUSDT"
TIMEFRAME = "1m"
LOOKBACK  = 6
PERCENTILE = 50
HEARTBEAT_SECONDS = 6 * 60
# ====================================================

def _timeframe_to_seconds(tf):
    unit = tf[-1]
    value = int(tf[:-1])
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    return 60

STALE_THRESHOLD_SECONDS = _timeframe_to_seconds(TIMEFRAME) * 2

closes = []
previous_median = None
current_state = None
last_kline_open_time = None
last_data_received = None

active_ws = None
ws_should_reconnect = False


# ====================================================
# TELEGRAM
# ====================================================
def send_telegram(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=12
        )
    except Exception as e:
        print("Telegram error:", e)


# ====================================================
# PERCENTILE
# ====================================================
def percentile_linear_interpolation(data, length, percentile):
    if len(data) < length:
        return None

    window = sorted(data[-length:])
    n = len(window)

    rank = (percentile / 100) * (n - 1)
    low_idx = int(rank)
    high_idx = min(low_idx + 1, n - 1)
    frac = rank - low_idx

    return window[low_idx] + (window[high_idx] - window[low_idx]) * frac


# ====================================================
# SYMBOL VALIDATION
# ====================================================
def validate_symbol():
    try:
        resp = requests.get(
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        valid_symbols = {s["symbol"] for s in data.get("symbols", [])}

        if SYMBOL not in valid_symbols:
            send_telegram(
                f"❌ <b>Invalid Symbol</b>\n\n"
                f"'{SYMBOL}' was not found on Binance Futures."
            )
            return False
        return True

    except Exception as e:
        send_telegram(
            f"⚠️ Could not verify symbol '{SYMBOL}':\n<code>{str(e)}</code>"
        )
        return True


# ====================================================
# REST BACKFILL
# ====================================================
def backfill_closes():
    global closes, last_kline_open_time, last_data_received
    try:
        limit = LOOKBACK + 5
        resp = requests.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": SYMBOL, "interval": TIMEFRAME, "limit": limit},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        now_ms = time.time() * 1000
        closed = [row for row in data if row[6] <= now_ms]

        if closed:
            closes = [float(row[4]) for row in closed]
            last_kline_open_time = closed[-1][0]
            last_data_received = time.time()
            print(f"Backfilled {len(closes)} closes")

    except Exception as e:
        print("Backfill error:", e)


# ====================================================
# HEARTBEAT
# ====================================================
async def heartbeat_loop():
    global ws_should_reconnect

    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)

        if last_data_received is None:
            send_telegram(f"💓 {SYMBOL} - no candles received yet")
            continue

        seconds_since_data = time.time() - last_data_received
        if seconds_since_data > STALE_THRESHOLD_SECONDS:
            minutes_since = int(seconds_since_data // 60)
            send_telegram(
                f"⚠️ <b>{SYMBOL} No data received in {minutes_since}m</b>\n"
                f"Feed stalled - reconnecting..."
            )
            ws_should_reconnect = True
            continue

        state_str = current_state if current_state else "Waiting for first signal"
        send_telegram(f"💓 {SYMBOL} State: <b>{state_str}</b>")


# ====================================================
# WEBSOCKET HANDLER
# ====================================================
async def process_message(msg):
    global previous_median, current_state, closes, last_kline_open_time, last_data_received

    try:
        data = json.loads(msg)
        k = data.get("k")
        if not k or k.get("x") is not True:
            return

        open_time = k.get("t")
        if open_time == last_kline_open_time:
            return
        last_kline_open_time = open_time
        last_data_received = time.time()

        close_price = float(k["c"])
        closes.append(close_price)

        if len(closes) > LOOKBACK + 5:
            closes = closes[-(LOOKBACK + 5):]

        median = percentile_linear_interpolation(closes, LOOKBACK, PERCENTILE)
        if median is None:
            return

        if previous_median is None:
            previous_median = median
            return

        new_state = None
        if median > previous_median:
            new_state = "Uptrend"
        elif median < previous_median:
            new_state = "Downtrend"

        if new_state and new_state != current_state:
            current_state = new_state
            emoji = "🟢" if new_state == "Uptrend" else "🔴"
            time_str = datetime.fromtimestamp(k["T"] / 1000).strftime("%H:%M")
            msg = (
                f"{emoji} <b>{SYMBOL} Trend Change</b>\n\n"
                f"New State: <b>{new_state}</b>\n"
                f"Timeframe: {TIMEFRAME}\n"
                f"Time: {time_str}\n"
                f"Median: {median:.2f}"
            )
            send_telegram(msg)

        previous_median = median

    except Exception as e:
        send_telegram(f"⚠️ Error:\n<code>{str(e)}</code>")


# ====================================================
# MAIN WS LOOP (aiohttp)
# ====================================================
async def run_websocket():
    global ws_should_reconnect

    url = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_{TIMEFRAME}"

    while True:
        ws_should_reconnect = False

        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    print("Connected to Binance")
                    send_telegram(
                        f"✅ <b>Quantile Bot Started</b>\n\n"
                        f"Symbol: {SYMBOL}\n"
                        f"Timeframe: {TIMEFRAME}\n"
                        f"Lookback: {LOOKBACK}"
                    )

                    backfill_closes()

                    async for msg in ws:
                        if ws_should_reconnect:
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await process_message(msg.data)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            send_telegram("⚠️ WS Error - reconnecting...")
                            break

        except Exception as e:
            send_telegram(f"❌ WS Crash:\n<code>{str(e)}</code>")

        await asyncio.sleep(3)


# ====================================================
# ENTRYPOINT
# ====================================================
async def main():
    if not validate_symbol():
        return

    asyncio.create_task(heartbeat_loop())
    await run_websocket()


if __name__ == "__main__":
    asyncio.run(main())