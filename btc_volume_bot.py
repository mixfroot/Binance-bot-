import requests
import time
from datetime import datetime

# ====== YOUR TELEGRAM ======
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"
# ===========================

SYMBOL = "TUTUSDT"
PERIOD = "15m"
LIMIT  = 180          # last 180 candles

def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print("Failed to send Telegram:", e)

def get_oi_history():
    url = "https://fapi.binance.com/futures/data/openInterestHist"
    params = {
        "symbol": SYMBOL,
        "period": PERIOD,
        "limit": LIMIT
    }
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()          # raises error if not 200
    return response.json()

def main():
    print("Bot starting...")

    # ========== TEST ON STARTUP ==========
    try:
        data = get_oi_history()
        if not data:
            raise Exception("Empty response from Binance")
        
        newest = data[-1]
        msg = (
            f"✅ <b>API Working!</b>\n\n"
            f"Symbol: {SYMBOL}\n"
            f"Period: {PERIOD}\n"
            f"Candles received: {len(data)}\n"
            f"Latest OI: {float(newest['sumOpenInterest']):,.0f}\n"
            f"Time: {datetime.fromtimestamp(newest['timestamp']/1000)}"
        )
        send_telegram(msg)
        print("Startup test successful")

    except Exception as e:
        error_msg = f"❌ <b>ERROR on startup</b>\n\n{str(e)}"
        send_telegram(error_msg)
        print("Startup error:", e)
        return          # stop the bot if API is blocked
    # =====================================

    print("Now checking every 60 seconds...")

    last_timestamp = None

    while True:
        try:
            data = get_oi_history()
            data = sorted(data, key=lambda x: x["timestamp"])

            newest = data[-1]
            newest_ts = newest["timestamp"]

            # Only process when new candle appears
            if last_timestamp is None or newest_ts > last_timestamp:
                print(f"New candle: {datetime.fromtimestamp(newest_ts/1000)}")

                # Calculate last 30 deltas (you can change 30)
                deltas = []
                for i in range(1, min(31, len(data))):
                    curr = float(data[-i]["sumOpenInterest"])
                    prev = float(data[-i-1]["sumOpenInterest"])
                    deltas.append(curr - prev)

                latest_delta = deltas[0]
                avg_delta = sum(deltas) / len(deltas)

                msg = (
                    f"<b>{SYMBOL} 15m OI Update</b>\n\n"
                    f"Latest Delta: <b>{latest_delta:,.0f}</b>\n"
                    f"Average (last {len(deltas)}): {avg_delta:,.0f}\n"
                    f"Candles loaded: {len(data)}"
                )
                send_telegram(msg)

                last_timestamp = newest_ts

        except Exception as e:
            error_msg = f"❌ Error while running:\n{str(e)}"
            send_telegram(error_msg)
            print("Error:", e)

        time.sleep(60)

if __name__ == "__main__":
    main()