#!/usr/bin/env python3
"""
Outlier Trades Chart → Hawkes version
- Same structure as your original script
- Every trade is an event
- Hawkes intensity at each bar close
- Rolling z-score of intensity over last CALC_LOOKBACK bars
- Same candles + lower panel style, same Telegram send
"""

import asyncio
import io
import traceback

import aiohttp
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SYMBOL = "BTCUSDT"

VISIBLE_CANDLES = 200
CALC_LOOKBACK = 30          # same as your original
STD_MULT = 2.0

# Hawkes parameters (seconds)
MU = 0.05
ALPHA = 0.8
BETA = 0.15

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# --------------------------------------------------------------------------
# Fetch Klines  (identical to your original)
# --------------------------------------------------------------------------
async def fetch_klines(session, symbol, total_needed):
    all_data = []
    remaining = total_needed
    end_time = None

    while remaining > 0:
        limit = min(1500, remaining)
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}"
        if end_time:
            url += f"&endTime={end_time}"

        async with session.get(url, timeout=15) as resp:
            data = await resp.json()

        if not data:
            break

        all_data = data + all_data
        remaining -= len(data)
        end_time = data[0][0] - 1

        if len(data) < limit:
            break
        await asyncio.sleep(0.2)

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    return df.drop_duplicates(subset=["open_time"]).reset_index(drop=True)


# --------------------------------------------------------------------------
# Fetch ALL trade timestamps (instead of only largest)
# --------------------------------------------------------------------------
async def fetch_all_trade_times(session, symbol, start_ms, end_ms):
    times = []
    current_start = start_ms

    print("   Fetching aggTrades...")
    while current_start < end_ms:
        url = (f"https://fapi.binance.com/fapi/v1/aggTrades"
               f"?symbol={symbol}&startTime={current_start}&endTime={end_ms}&limit=1000")

        async with session.get(url, timeout=15) as resp:
            trades = await resp.json()

        if not trades or not isinstance(trades, list):
            break

        for t in trades:
            times.append(t["T"])

        if len(trades) < 1000:
            break

        current_start = trades[-1]["T"] + 1
        await asyncio.sleep(0.12)

    times.sort()
    return times


# --------------------------------------------------------------------------
# Hawkes intensity (recursive)
# then rolling z-score exactly like your original mean/std logic
# --------------------------------------------------------------------------
def compute_hawkes_intensities(trade_times_ms, bar_close_ms,
                               mu=MU, alpha=ALPHA, beta=BETA):
    if not trade_times_ms:
        return [mu] * len(bar_close_ms)

    trade_t = np.array(trade_times_ms, dtype=np.float64) / 1000.0
    bar_t   = np.array(bar_close_ms, dtype=np.float64) / 1000.0

    intensities = []
    trade_idx = 0
    n_trades = len(trade_t)

    current_time = min(trade_t[0], bar_t[0]) - 1.0
    current_lambda = mu

    for b_close in bar_t:
        while trade_idx < n_trades and trade_t[trade_idx] <= b_close:
            dt = trade_t[trade_idx] - current_time
            if dt > 0:
                current_lambda = mu + (current_lambda - mu) * np.exp(-beta * dt)
            current_lambda += alpha
            current_time = trade_t[trade_idx]
            trade_idx += 1

        dt = b_close - current_time
        if dt > 0:
            current_lambda = mu + (current_lambda - mu) * np.exp(-beta * dt)
            current_time = b_close

        intensities.append(max(current_lambda, 0.0))

    return intensities


# --------------------------------------------------------------------------
# Create Chart  (kept as close as possible to your original)
# --------------------------------------------------------------------------
def create_chart(kline_df, intensities):
    display_df = kline_df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)
    display_intensities = intensities[-VISIBLE_CANDLES:]

    bar_values = []
    bar_colors = []

    for i in range(len(display_df)):
        # rolling window of intensities (exactly like your original mean/std)
        window = display_intensities[max(0, i - CALC_LOOKBACK + 1): i + 1]

        if len(window) < 5:
            bar_values.append(0)
            bar_colors.append("#555555")
            continue

        mean = np.mean(window)
        std = np.std(window)
        if std == 0:
            bar_values.append(0)
            bar_colors.append("#555555")
            continue

        z = (display_intensities[i] - mean) / std

        # only show the bar when it is an outlier (same spirit as original)
        if abs(z) >= STD_MULT:
            bar_values.append(z)
            bar_colors.append("#00ff88" if z > 0 else "#ff4466")
        else:
            bar_values.append(0)
            bar_colors.append("#555555")

    # ---------- Plot ----------
    fig = plt.figure(figsize=(18, 10), facecolor="black")
    gs = fig.add_gridspec(2, 1, height_ratios=[2.8, 1.4], hspace=0.07)

    ax_candle = fig.add_subplot(gs[0])
    ax_out = fig.add_subplot(gs[1], sharex=ax_candle)

    for ax in [ax_candle, ax_out]:
        ax.set_facecolor("black")
        ax.tick_params(colors="white")
        for spine in ax.spines.values():
            spine.set_color("white")

    # Candlesticks (identical)
    width = 0.6
    for idx, row in display_df.iterrows():
        color = "#00ff88" if row["close"] >= row["open"] else "#ff4466"
        ax_candle.plot([idx, idx], [row["low"], row["high"]], color=color, linewidth=0.7)
        body_low = min(row["open"], row["close"])
        body_height = abs(row["close"] - row["open"]) or 0.01
        rect = Rectangle((idx - width/2, body_low), width, body_height,
                         facecolor=color, edgecolor=color)
        ax_candle.add_patch(rect)

    ax_candle.set_xlim(-1, VISIBLE_CANDLES)
    ax_candle.set_title(
        f"{SYMBOL} 1m  |  Visible: {VISIBLE_CANDLES}  |  Lookback: {CALC_LOOKBACK}  |  Std: {STD_MULT}σ\n"
        f"Hawkes Intensity Z-Score  |  Green = elevated  |  Red = suppressed",
        color="white", fontsize=12, pad=8
    )
    ax_candle.grid(True, color="#333333", alpha=0.4)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # Bars (same style as original)
    ax_out.bar(range(len(display_df)), bar_values, color=bar_colors, width=0.65, alpha=0.85)
    ax_out.axhline(0, color="white", linewidth=0.8, alpha=0.5)

    ax_out.set_ylabel("Hawkes Z-Score", color="white")
    ax_out.grid(True, color="#333333", alpha=0.4)

    step = max(1, VISIBLE_CANDLES // 12)
    ax_out.set_xticks(range(0, VISIBLE_CANDLES, step))
    labels = [display_df["open_time"].iloc[i].strftime("%m-%d %H:%M") for i in range(0, VISIBLE_CANDLES, step)]
    ax_out.set_xticklabels(labels, rotation=45, color="white", fontsize=8)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=120, facecolor="black", edgecolor="none")
    buf.seek(0)
    plt.close()
    return buf.getvalue()


# --------------------------------------------------------------------------
# Send Photo (identical)
# --------------------------------------------------------------------------
async def send_photo(photo_bytes, caption=""):
    url = f"{TELEGRAM_API_URL}/sendPhoto"
    data = aiohttp.FormData()
    data.add_field("chat_id", str(CHAT_ID))
    data.add_field("caption", caption)
    data.add_field("photo", photo_bytes, filename="hawkes_outlier.png", content_type="image/png")

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, timeout=60) as resp:
            text = await resp.text()
            print(f"Telegram status: {resp.status}")
            if resp.status == 200:
                print("Photo sent successfully!")
            else:
                print("Failed:", text[:300])


# --------------------------------------------------------------------------
# MAIN (same flow as your original)
# --------------------------------------------------------------------------
async def main():
    try:
        print("1. Fetching klines...")
        async with aiohttp.ClientSession() as session:
            kline_df = await fetch_klines(session, SYMBOL, VISIBLE_CANDLES + CALC_LOOKBACK + 30)
            print(f"   Got {len(kline_df)} klines")

            start_ms = int(kline_df["open_time"].iloc[0].timestamp() * 1000)
            end_ms = int(kline_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

            print("2. Fetching all trades...")
            trade_times = await fetch_all_trade_times(session, SYMBOL, start_ms, end_ms)
            print(f"   Got {len(trade_times)} trades")

        bar_close_ms = [int(ts.timestamp() * 1000) + 60_000 for ts in kline_df["open_time"]]

        print("3. Computing Hawkes intensities...")
        intensities = compute_hawkes_intensities(trade_times, bar_close_ms)
        print(f"   Intensity series ready")

        print("4. Creating chart...")
        photo = create_chart(kline_df, intensities)
        print(f"   Chart size: {len(photo)/1024:.1f} KB")

        caption = (f"{SYMBOL} – Hawkes Intensity Outliers\n"
                   f"Visible: {VISIBLE_CANDLES}\n"
                   f"Lookback: {CALC_LOOKBACK} | Std: {STD_MULT}σ\n"
                   f"μ={MU} α={ALPHA} β={BETA}")

        print("5. Sending...")
        await send_photo(photo, caption)
        print("6. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
