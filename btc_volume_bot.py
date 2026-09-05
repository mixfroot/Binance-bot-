import json
import time
import statistics
import requests
from datetime import datetime
from websocket import WebSocketApp

# ====================== CONFIG ======================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"

SYMBOL = "BTCUSDT"
TIMEFRAME = "1m"               # change to "5m", "15m" etc if you want
LOOKBACK = 6                   # Median lookback (you said 6)
HEARTBEAT_EVERY = 60           # minutes
# ====================================================

closes = []                    # stores last closes
previous_median = None
current_state = None           # "Uptrend" or "Downtrend"
last_heartbeat = time.time()

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

def calculate_median(data):
    if len(data) < LOOKBACK:
        return None
    return statistics.median(data[-LOOKBACK:])

def on_message(ws, message):
    global previous_median, current_state, last_heartbeat, closes

    try:
        data = json.loads(message)
        k = data.get("k")
        if not k or k.get("x") is not True:
            return  # only closed candles

        close_price = float(k["c"])
        candle_time = k["T"]
        time_str = datetime.fromtimestamp(candle_time / 1000).strftime("%H:%M")

        closes.append(close_price)
        if len(closes) > LOOKBACK + 5:
            closes = closes[-(LOOKBACK + 5):]

        median = calculate_median(closes)
        if median is None:
            return

        print(f"{time_str} | Close: {close_price} | Median: {median:.2f}")

        # First median calculation
        if previous_median is None:
            previous_median = median
            send_telegram(
                f"✅ <b>{SYMBOL} Median Bot Started</b>\n\n"
                f"First Median: <b>{median:.2f}</b>\n"
                f"Lookback: {LOOKBACK} | Timeframe: {TIMEFRAME}\n"
                f"Waiting for trend change..."
            )
            return

        # Detect state change
        new_state = None
        if median > previous_median:
            new_state = "Uptrend"
        elif median < previous_median:
            new_state = "Downtrend"

        # Only alert if state actually changed
        if new_state and new_state != current_state:
            current_state = new_state
            emoji = "🟢" if new_state == "Uptrend" else "🔴"
            msg = (
                f"{emoji} <b>{SYMBOL} Trend Change</b>\n\n"
                f"New State: <b>{new_state}</b>\n"
                f"Time: {time_str}\n"
                f"Median: {median:.2f}\n"
                f"Previous Median: {previous_median:.2f}"
            )
            send_telegram(msg)
            print(f"STATE CHANGE → {new_state}")

        previous_median = median

        # Heartbeat
        if time.time() - last_heartbeat > HEARTBEAT_EVERY * 60:
            send_telegram(
                f"❤️ Heartbeat — Median Bot alive\n"
                f"Current State: {current_state or 'None'}\n"
                f"Median: {median:.2f}\n"
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            last_heartbeat = time.time()

    except Exception as e:
        print("Error:", e)
        send_telegram(f"⚠️ Error:\n<code>{str(e)}</code>")

def on_error(ws, error):
    send_telegram(f"⚠️ WebSocket error:\n<code>{str(error)}</code>")

def on_close(ws, close_status_code, close_msg):
    send_telegram("🔌 WebSocket closed. Reconnecting in 9 seconds...")

def on_open(ws):
    send_telegram(f"🔗 Connected to {SYMBOL} {TIMEFRAME} stream")

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
            send_telegram(f"❌ Crash:\n<code>{str(e)}</code>\nReconnecting in 9s...")
        time.sleep(9)

if __name__ == "__main__":
    send_telegram(
        f"🚀 <b>{SYMBOL} Median Trend Bot Started</b>\n\n"
        f"Timeframe: {TIMEFRAME}\n"
        f"Lookback: {LOOKBACK}\n"
        f"Alerts only when Median trend changes"
    )
    run_websocket()