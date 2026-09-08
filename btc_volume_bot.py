#!/usr/bin/env python3
"""
Outlier Trades Chart
- Uses largest trade of each candle in the rolling lookback
- Threshold = 99th Percentile (instead of Std)
"""

import asyncio
import io
import traceback
from collections import defaultdict

import aiohttp
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SYMBOL = "BTCUSDT"

VISIBLE_CANDLES = 400
CALC_LOOKBACK = 60
QUANTILE = 0.99          # 99th percentile

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# --------------------------------------------------------------------------
# Fetch Klines
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
# Fetch largest trade per candle
# --------------------------------------------------------------------------
async def fetch_largest_per_candle(session, symbol, start_ms, end_ms):
    largest = {}
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
            ts = t["T"]
            candle_ts = ts - (ts % 60000)
            quote_qty = float(t["q"]) * float(t["p"])
            is_buyer_maker = t["m"]

            if candle_ts not in largest or quote_qty > largest[candle_ts][0]:
                largest[candle_ts] = (quote_qty, is_buyer_maker)

        if len(trades) < 1000:
            break

        current_start = trades[-1]["T"] + 1
        await asyncio.sleep(0.12)

    return largest


# --------------------------------------------------------------------------
# Create Chart
# --------------------------------------------------------------------------
def create_chart(kline_df, largest_map):
    display_df = kline_df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)
    all_ts = [int(ts.timestamp() * 1000) for ts in display_df["open_time"]]

    bar_values = []
    bar_colors = []
    bar_counts = []

    for i in range(len(display_df)):
        # Collect largest trades from the rolling lookback
        window_sizes = []
        for j in range(max(0, i - CALC_LOOKBACK + 1), i + 1):
            ts = all_ts[j]
            if ts in largest_map:
                window_sizes.append(largest_map[ts][0])

        if len(window_sizes) < 5:
            bar_values.append(0)
            bar_colors.append("#555555")
            bar_counts.append(0)
            continue

        # 99th percentile threshold
        threshold = np.quantile(window_sizes, QUANTILE)

        # Current candle largest trade
        curr_ts = all_ts[i]
        if curr_ts not in largest_map:
            bar_values.append(0)
            bar_colors.append("#555555")
            bar_counts.append(0)
            continue

        qty, is_buyer_maker = largest_map[curr_ts]

        if qty > threshold:
            direction = -1 if is_buyer_maker else 1
            bar_values.append(direction * qty)
            bar_colors.append("#ff4466" if is_buyer_maker else "#00ff88")
            bar_counts.append(1)
        else:
            bar_values.append(0)
            bar_colors.append("#555555")
            bar_counts.append(0)

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

    # Candlesticks
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
        f"{SYMBOL} 1m  |  Visible: {VISIBLE_CANDLES}  |  Lookback: {CALC_LOOKBACK}  |  Quantile: {int(QUANTILE*100)}th\n"
        f"Using Largest Trade of each candle  |  Green = large Buy  |  Red = large Sell",
        color="white", fontsize=12, pad=8
    )
    ax_candle.grid(True, color="#333333", alpha=0.4)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # Bars
    ax_out.bar(range(len(display_df)), bar_values, color=bar_colors, width=0.65, alpha=0.85)
    ax_out.axhline(0, color="white", linewidth=0.8, alpha=0.5)

    # Count labels
    max_abs = max([abs(v) for v in bar_values] + [1])
    for i, (val, count) in enumerate(zip(bar_values, bar_counts)):
        if count > 0:
            y_pos = val + (max_abs * 0.04 * (1 if val >= 0 else -1))
            ax_out.text(i, y_pos, str(count), color="white", fontsize=7,
                        ha="center", va="bottom" if val >= 0 else "top", fontweight="bold")

    ax_out.set_ylabel("Largest Trade (USDT)", color="white")
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
# Send Photo
# --------------------------------------------------------------------------
async def send_photo(photo_bytes, caption=""):
    url = f"{TELEGRAM_API_URL}/sendPhoto"
    data = aiohttp.FormData()
    data.add_field("chat_id", str(CHAT_ID))
    data.add_field("caption", caption)
    data.add_field("photo", photo_bytes, filename="outlier_trades.png", content_type="image/png")

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, timeout=60) as resp:
            text = await resp.text()
            print(f"Telegram status: {resp.status}")
            if resp.status == 200:
                print("Photo sent successfully!")
            else:
                print("Failed:", text[:300])


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
async def main():
    try:
        print("1. Fetching klines...")
        async with aiohttp.ClientSession() as session:
            kline_df = await fetch_klines(session, SYMBOL, VISIBLE_CANDLES + CALC_LOOKBACK + 30)
            print(f"   Got {len(kline_df)} klines")

            start_ms = int(kline_df["open_time"].iloc[0].timestamp() * 1000)
            end_ms = int(kline_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

            print("2. Fetching largest trades per candle...")
            largest_map = await fetch_largest_per_candle(session, SYMBOL, start_ms, end_ms)
            print(f"   Got largest trade for {len(largest_map)} candles")

        print("3. Creating chart...")
        photo = create_chart(kline_df, largest_map)
        print(f"4. Chart size: {len(photo)/1024:.1f} KB")

        caption = (f"{SYMBOL} – Largest Trade Outliers\n"
                   f"Visible: {VISIBLE_CANDLES}\n"
                   f"Lookback: {CALC_LOOKBACK} | Quantile: {int(QUANTILE*100)}th\n"
                   f"Using largest trade of each candle")

        print("5. Sending...")
        await send_photo(photo, caption)
        print("6. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())