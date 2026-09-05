import json
import time
import threading
import requests
from datetime import datetime
from websocket import WebSocketApp

# ====================== CONFIG ======================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"

SYMBOL    = "BTCUSDT"
TIMEFRAME = "1m"       # change freely: 1m, 5m, 15m, 1h, etc.
LOOKBACK  = 6           # same as "Lookback Length" in the Pine indicator
PERCENTILE = 50         # 50 = median (matches your script's `median` line)
HEARTBEAT_SECONDS = 6 * 60   # send an "alive" ping every 6 minutes
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
    return 60  # fallback

STALE_THRESHOLD_SECONDS = _timeframe_to_seconds(TIMEFRAME) * 2  # no data for 2 candles = stale

closes = []
previous_median = None
current_state = None
last_kline_open_time = None  # de-dupe guard
last_data_received = None    # timestamp of last processed candle, for stale-feed detection


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


def percentile_linear_interpolation(data, length, percentile):
    """
    Replicates Pine Script's ta.percentile_linear_interpolation exactly.
    Uses the most recent `length` values, sorts them, and linearly
    interpolates between the two nearest ranks.
    """
    if len(data) < length:
        return None

    window = sorted(data[-length:])
    n = len(window)

    # Pine's rank formula: (percentile / 100) * (n - 1)
    rank = (percentile / 100) * (n - 1)
    low_idx = int(rank)
    high_idx = min(low_idx + 1, n - 1)
    frac = rank - low_idx

    return window[low_idx] + (window[high_idx] - window[low_idx]) * frac


def on_message(ws, message):
    global previous_median, current_state, closes, last_kline_open_time, last_data_received

    try:
        data = json.loads(message)
        k = data.get("k")
        if not k or k.get("x") is not True:
            return

        # de-dupe: skip if this candle's open time was already processed
        open_time = k.get("t")
        if open_time == last_kline_open_time:
            return
        last_kline_open_time = open_time
        last_data_received = time.time()

        close_price = float(k["c"])
        closes.append(close_price)

        # keep a small rolling buffer, just enough for the lookback
        if len(closes) > LOOKBACK + 5:
            closes = closes[-(LOOKBACK + 5):]

        median = percentile_linear_interpolation(closes, LOOKBACK, PERCENTILE)
        if median is None:
            return  # not enough closes yet to fill the lookback window

        if previous_median is None:
            previous_median = median
            return

        new_state = None
        if median > previous_median:
            new_state = "Uptrend"
        elif median < previous_median:
            new_state = "Downtrend"
        # if median == previous_median: no change, stay silent

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
        send_telegram(f"⚠️ Quantile Bot Error:\n<code>{str(e)}</code>")


def on_error(ws, error):
    send_telegram(f"⚠️ Quantile Bot WebSocket error:\n<code>{str(error)}</code>")


def on_close(ws, close_status_code, close_msg):
    send_telegram("🔌 Quantile Bot stopped / disconnected. Reconnecting in 9 seconds...")


def heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_SECONDS)

        if last_data_received is None:
            send_telegram(f"💓 {SYMBOL} - no candles received yet")
            continue

        seconds_since_data = time.time() - last_data_received
        if seconds_since_data > STALE_THRESHOLD_SECONDS:
            minutes_since = int(seconds_since_data // 60)
            send_telegram(
                f"⚠️ <b>{SYMBOL} No data received in {minutes_since}m</b>\n"
                f"Feed may be stalled - check connection"
            )
            continue

        state_str = current_state if current_state else "Waiting for first signal"
        send_telegram(f"💓 {SYMBOL} State: <b>{state_str}</b>")


def validate_symbol():
    """Check SYMBOL actually exists on Binance USDT-M Futures before connecting."""
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
                f"'{SYMBOL}' was not found on Binance Futures.\n"
                f"Check spelling (e.g. BTCUSDT, ETHUSDT) - bot will not start."
            )
            print(f"Invalid symbol: {SYMBOL}. Exiting.")
            return False
        return True

    except Exception as e:
        send_telegram(
            f"⚠️ Could not verify symbol '{SYMBOL}' (network/API issue):\n"
            f"<code>{str(e)}</code>\nBot will attempt to start anyway."
        )
        return True  # don't block startup just because the check itself failed


def on_open(ws):
    print("Quantile Median Bot connected")
    send_telegram(
        f"✅ <b>Quantile Bot Started</b>\n\n"
        f"Symbol: {SYMBOL}\n"
        f"Timeframe: {TIMEFRAME}\n"
        f"Lookback: {LOOKBACK}"
    )


def run_websocket():
    stream = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_{TIMEFRAME}"
    while True:
        try:
            ws = WebSocketApp(
                stream,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            send_telegram(f"❌ Quantile Bot Crash:\n<code>{str(e)}</code>\nReconnecting in 9s...")
        time.sleep(9)


if __name__ == "__main__":
    print("Quiet Quantile Median Bot started...")
    if not validate_symbol():
        exit(1)
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    try:
        run_websocket()
    except Exception as e:
        send_telegram(f"💀 <b>Quantile Bot FATAL - process exiting</b>\n<code>{str(e)}</code>")
        raise
