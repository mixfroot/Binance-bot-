import requests
import time
import statistics
from datetime import datetime
import traceback

# ====================== CONFIG ======================
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"

SYMBOLS = ["TUTUSDT", "BTCUSDT", "SEIUSDT", "SUIUSDT"]
PERIOD = "15m"
LOOKBACK = 180          # last 180 candles
STD_MULTIPLIER = 1.0    # 1 standard deviation
HEARTBEAT_EVERY = 60    # minutes
CHECK_INTERVAL = 60     # seconds
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
    
    # Use last 180 deltas (or as many as we have)
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
        "candles": len(data),
        "timestamp": data[-1]["timestamp"]
    }

def main():
    send_telegram("🚀 <b>OI Delta Bot Started</b>\nTracking: TUT | BTC | SEI | SUI\nInterval: 15m | Lookback: 180 | Alert: 1σ")
    
    last_timestamps = {s: None for s in SYMBOLS}
    last_heartbeat = time.time()
    
    print("Bot is running 24/7...")

    while True:
        try:
            for symbol in SYMBOLS:
                try:
                    result = process_symbol(symbol)
                    
                    if result is None:
                        continue
                    
                    # Only act on new candle
                    if last_timestamps[symbol] is None or result["timestamp"] > last_timestamps[symbol]:
                        last_timestamps[symbol] = result["timestamp"]
                        
                        time_str = datetime.fromtimestamp(result["timestamp"]/1000).strftime("%H:%M")
                        
                        if result["is_extreme"]:
                            direction = "🟢 LONG bias" if result["latest_delta"] > 0 else "🔴 SHORT bias"
                            msg = (
                                f"🚨 <b>{result['symbol']} OI Delta Alert</b>\n\n"
                                f"Time: {time_str}\n"
                                f"Latest Delta: <b>{result['latest_delta']:,.0f}</b>\n"
                                f"Mean: {result['mean']:,.0f}\n"
                                f"1σ Range: {result['lower']:,.0f}  →  {result['upper']:,.0f}\n"
                                f"{direction}"
                            )
                            send_telegram(msg)
                            print(f"ALERT → {symbol} | Delta: {result['latest_delta']:,.0f}")
                        else:
                            print(f"Normal → {symbol} | Delta: {result['latest_delta']:,.0f}")

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