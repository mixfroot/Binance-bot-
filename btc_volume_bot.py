import json
import time
import os
import statistics
import requests
from datetime import datetime
from websocket import WebSocketApp

# ====================== CONFIG ======================
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "PUT_YOUR_TOKEN_HERE")
CHAT_ID   = os.environ.get("TG_CHAT_ID", "PUT_YOUR_CHAT_ID_HERE")

SYMBOL = "BTCUSDT"
STD_MULTIPLIER = 2.0
LOOKBACK = 180
HEARTBEAT_EVERY = 60          # minutes
MIN_HISTORY = 30

STATE_FILE = "oi_delta_state.json"
# ====================================================

delta_history = []
last_oi = None
first_sent = False
last_heartbeat = time.time()

# Tracks the candle open-time of the LAST candle we actually processed.
# Used to detect + skip a stale/mid-formation candle right after reconnect.
last_processed_candle_open = None
just_reconnected = False


# ---------------------- persistence ----------------------

def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "delta_history": delta_history,
                "last_oi": last_oi,
                "first_sent": first_sent,
                "last_processed_candle_open": last_processed_candle_open
            }, f)
    except Exception as e:
        print("State save error:", e)


def load_state():
    global delta_history, last_oi, first_sent, last_processed_candle_open
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        delta_history = data.get("delta_history", [])[-LOOKBACK:]
        last_oi = data.get("last_oi")
        first_sent = data.get("first_sent", False)
        last_processed_candle_open = data.get("last_processed_candle_open")
        print(f"Loaded state: {len(delta_history)} deltas, last_oi={last_oi}")
    except Exception as e:
        print("State load error:", e)


# ---------------------- telegram ----------------------

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


def get_current_oi():
    r = requests.get(
        f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL}",
        timeout=10
    )
    r.raise_for_status()
    return float(r.json()["openInterest"])


# ---------------------- core logic ----------------------

def process_new_oi(current_oi, candle_open_time, candle_close_time):
    global last_oi, first_sent, delta_history, last_processed_candle_open

    now_str = datetime.fromtimestamp(candle_close_time / 1000).strftime("%H:%M")

    if last_oi is None:
        last_oi = current_oi
        last_processed_candle_open = candle_open_time
        save_state()
        print(f"Initial OI set: {current_oi:,.0f}")
        return

    delta = current_oi - last_oi
    last_oi = current_oi
    last_processed_candle_open = candle_open_time

    delta_history.append(delta)
    if len(delta_history) > LOOKBACK:
        delta_history.pop(0)

    save_state()

    print(f"{now_str} | Delta: {delta:,.0f} | History: {len(delta_history)}/{LOOKBACK}")

    if len(delta_history) < MIN_HISTORY:
        return

    mean = statistics.mean(delta_history)
    std = statistics.stdev(delta_history) if len(delta_history) > 1 else 0
    upper = mean + STD_MULTIPLIER * std
    lower = mean - STD_MULTIPLIER * std

    if not first_sent:
        msg = (
            f"✅ <b>{SYMBOL} 1m OI - First Update</b>\n\n"
            f"Time: {now_str}\n"
            f"Latest Delta: <b>{delta:,.0f}</b>\n"
            f"History: {len(delta_history)}/{LOOKBACK}\n"
            f"Mean: {mean:,.0f} | 2σ: ±{std*2:,.0f}\n\n"
            f"<i>Now only 2σ alerts will be sent.</i>"
        )
        send_telegram(msg)
        first_sent = True
        save_state()
        return

    if delta > upper or delta < lower:
        direction = "🟢 Positive" if delta > 0 else "🔴 Negative"
        msg = (
            f"🚨 <b>{SYMBOL} 1m OI ALERT (2σ)</b>\n\n"
            f"Time: {now_str}\n"
            f"1m Delta: <b>{delta:,.0f}</b>\n"
            f"Mean: {mean:,.0f}\n"
            f"2σ Range: {lower:,.0f} → {upper:,.0f}\n"
            f"{direction} extreme move"
        )
        send_telegram(msg)


# ---------------------- websocket handlers ----------------------

def on_message(ws, message):
    global last_heartbeat, just_reconnected

    try:
        data = json.loads(message)
        k = data.get("k")
        if not k or k.get("x") is not True:
            return  # only fully closed candles

        candle_open_time = k["t"]
        candle_close_time = k["T"]

        # Right after a reconnect: the very first closed-candle event we get
        # could be for a candle that was already mid-formation (or already
        # processed) before the drop. Skip exactly one closed-candle event
        # after reconnect if it's not strictly newer than the last one we
        # processed, so we don't double-count or process a stale partial.
        if just_reconnected:
            just_reconnected = False
            if last_processed_candle_open is not None and candle_open_time <= last_processed_candle_open:
                print(f"Skipping stale post-reconnect candle at {candle_open_time}")
                return
            # otherwise it's a genuinely new closed candle -> fine to process

        try:
            current_oi = get_current_oi()
            process_new_oi(current_oi, candle_open_time, candle_close_time)
        except Exception as e:
            send_telegram(f"⚠️ OI fetch error:\n<code>{str(e)}</code>")

        if time.time() - last_heartbeat > HEARTBEAT_EVERY * 60:
            send_telegram(
                f"❤️ Heartbeat\n"
                f"History: {len(delta_history)}/{LOOKBACK}\n"
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            last_heartbeat = time.time()

    except Exception as e:
        print("Message error:", e)


def on_error(ws, error):
    send_telegram(f"⚠️ WebSocket error:\n<code>{str(error)}</code>")


def on_close(ws, close_status_code, close_msg):
    global just_reconnected
    just_reconnected = True
    send_telegram("🔌 WebSocket closed. Reconnecting in 9 seconds... (forming candle at reconnect will be ignored)")


def on_open(ws):
    send_telegram(f"🔗 Connected to {SYMBOL} 1m stream")


def run_websocket():
    while True:
        try:
            ws = WebSocketApp(
                f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_1m",
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
    load_state()
    send_telegram(
        f"🚀 <b>{SYMBOL} 1m OI Delta Bot Started</b>\n\n"
        f"• History loaded: {len(delta_history)}/{LOOKBACK}\n"
        f"• Lookback: {LOOKBACK}\n"
        f"• Alert: 2σ\n"
        f"• On reconnect → skips the forming/stale candle, resumes on next fresh close"
    )
    run_websocket()
