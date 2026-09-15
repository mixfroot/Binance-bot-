import requests
import time
import statistics
from datetime import datetime
import traceback

# ====================== CONFIG ======================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"

SYMBOLS = ["TUTUSDT", "BTCUSDT", "SEIUSDT", "SUIUSDT"]
PERIOD = "5m"
LOOKBACK = 180
STD_MULTIPLIER = 2.0
HEARTBEAT_EVERY = 60        # minutes
CHECK_INTERVAL = 40
# ====================================================

def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=15)
    except Exception as e:
        print("Telegram send failed:", e)

def get_oi_history(symbol, limit=180):
    url = "https://fapi.binance.com/futures/data/openInterestHist"
    params = {
        "symbol": symbol,
        "period": PERIOD,
        "limit": limit
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    data = sorted(data, key=lambda x: x["timestamp"])
    return data

def calculate_deltas(data):
    deltas = []
    for i in range(1, len(data)):
        curr = float(data[i]["sumOpenInterest"])
        prev = float(data[i-1]["sumOpenInterest"])
        deltas.append(curr - prev)
    return deltas

def process_symbol(symbol):
    data = get_oi_history(symbol, LOOKBACK + 5)
    
    if len(data) < 30:
        return None

    deltas = calculate_deltas(data)
    latest_delta = deltas[-1]
    
    window = deltas[-LOOKBACK:] if len(deltas) >= LOOKBACK else deltas
    
    mean = statistics.mean(window)
    std = statistics.stdev(window) if len(window) > 1 else 0
    
    upper = mean + (STD_MULTIPLIER * std)
    lower = mean - (STD_MULTIPLIER * std)
    
    is_extreme = latest_delta > upper or latest_delta < lower
    
    return {
        "symbol": symbol,
        "latest_delta": latest_delta,
        "mean": mean,
        "std": std,
        "upper": upper,
        "lower": lower,
        "is_extreme": is_extreme,
        "timestamp": data[-1]["timestamp"]
    }

def main():
    send_telegram(
        "🚀 <b>OI Delta Bot Started (5m)</b>\n"
        "Tracking: TUT | BTC | SEI | SUI\n"
        "I will send the latest delta once per coin, then only extreme (1σ) alerts."
    )
    
    last_timestamps = {s: None for s in SYMBOLS}
    first_sent = {s: False for s in SYMBOLS}   # to send latest only once
    last_heartbeat = time.time()
    
    print("Bot is running...")

    while True:
        try:
            for symbol in SYMBOLS:
                try:
                    result = process_symbol(symbol)
                    
                    if result is None:
                        continue
                    
                    # New candle detected
                    if last_timestamps[symbol] is None or result["timestamp"] > last_timestamps[symbol]:
                        last_timestamps[symbol] = result["timestamp"]
                        time_str = datetime.fromtimestamp(result["timestamp"] / 1000).strftime("%H:%M")
                        
                        # Send latest delta only once (first time)
                        if not first_sent[symbol]:
                            msg = (
                                f"✅ <b>{result['symbol']} Latest 5m OI Delta</b>\n\n"
                                f"Time: {time_str}\n"
                                f"Latest Delta: <b>{result['latest_delta']:,.0f}</b>\n"
                                f"Mean: {result['mean']:,.0f} | 1σ: ±{result['std']:,.0f}\n\n"
                                f"<i>Now only extreme alerts will be sent.</i>"
                            )
                            send_telegram(msg)
                            first_sent[symbol] = True
                            print(f"First update sent → {symbol}")
                        
                        # After first time → only send if extreme
                        elif result["is_extreme"]:
                            direction = "🟢 Positive" if result["latest_delta"] > 0 else "🔴 Negative"
                            msg = (
                                f"🚨 <b>{result['symbol']} 5m OI ALERT</b>\n\n"
                                f"Time: {time_str}\n"
                                f"Latest Delta: <b>{result['latest_delta']:,.0f}</b>\n"
                                f"Mean: {result['mean']:,.0f}\n"
                                f"1σ Range: {result['lower']:,.0f} → {result['upper']:,.0f}\n"
                                f"{direction} extreme move"
                            )
                            send_telegram(msg)
                            print(f"ALERT → {symbol} | {result['latest_delta']:,.0f}")

                except Exception as e:
                    error_msg = f"⚠️ Error on {symbol}:\n<code>{str(e)}</code>"
                    send_telegram(error_msg)
                    print(f"Error {symbol}:", e)

            # Heartbeat
            if time.time() - last_heartbeat > HEARTBEAT_EVERY * 60:
                send_telegram(f"❤️ Heartbeat — Bot is alive\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                last_heartbeat = time.time()

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            error_msg = f"❌ Critical error:\n<code>{str(e)}</code>\n\nReconnecting in 9 seconds..."
            send_telegram(error_msg)
            print("Critical error:", traceback.format_exc())
            time.sleep(9)

if __name__ == "__main__":
    main()