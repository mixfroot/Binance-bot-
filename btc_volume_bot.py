#!/usr/bin/env python3
"""
Largest Trade per 1m Candle Chart
- Shows the biggest aggTrade that happened inside each 1-minute candle
- Green = biggest trade was Buy | Red = biggest trade was Sell
- Custom Std bands (no average line)
"""

import asyncio
import io
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone

import aiohttp
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Rectangle

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SYMBOL = "BTCUSDT"

VISIBLE_CANDLES = 600
CALC_LOOKBACK = 9
STD_MULT = 2.0

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# --------------------------------------------------------------------------
# Fetch 1m Klines (for candlesticks)
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
# Fetch aggTrades and find largest trade per 1m candle
# --------------------------------------------------------------------------
async def fetch_largest_trades(session, symbol, start_ms, end_ms):
    """
    Returns dict: {candle_open_time_ms: (max_quote_qty, is_buyer_maker)}
    is_buyer_maker=True → Sell (red), False → Buy (green)
    """
    largest = {}
    current_start = start_ms

    while current_start < end_ms:
        url = (f"https://fapi.binance.com/fapi/v1/aggTrades"
               f"?symbol={symbol}&startTime={current_start}&endTime={end_ms}&limit=1000")

        async with session.get(url, timeout=15) as resp:
            trades = await resp.json()

        if not trades or not isinstance(trades, list):
            break

        for t in trades:
            ts = t["T"]  # trade time
            # floor to 1-minute candle
            candle_ts = ts - (ts % 60000)
            quote_qty = float(t["q"]) * float(t["p"])  # size in USDT
            is_buyer_maker = t["m"]  # True = seller is taker → Sell

            if candle_ts not in largest or quote_qty > largest[candle_ts][0]:
                largest[candle_ts] = (quote_qty, is_buyer_maker)

        if len(trades) < 1000:
            break

        # move forward
        current_start = trades[-1]["T"] + 1
        await asyncio.sleep(0.15)

    return largest


# --------------------------------------------------------------------------
# Create Chart
# --------------------------------------------------------------------------
def create_chart(kline_df, largest_map):
    display_df = kline_df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)

    # Build series of largest trade size + color
    sizes = []
    colors = []
    for _, row in display_df.iterrows():
        ts_ms = int(row["open_time"].timestamp() * 1000)
        if ts_ms in largest_map:
            size, is_buyer_maker = largest_map[ts_ms]
            sizes.append(size)
            colors.append("#ff4466" if is_buyer_maker else "#00ff88")  # red=sell, green=buy
        else:
            sizes.append(0)
            colors.append("#555555")

    display_df["largest_size"] = sizes
    display_df["bar_color"] = colors

    # Std calculation
    size_series = pd.Series(sizes)
    rolling_std = size_series.rolling(CALC_LOOKBACK).std()
    rolling_mean = size_series.rolling(CALC_LOOKBACK).mean()

    upper = rolling_mean + STD_MULT * rolling_std
    lower = rolling_mean - STD_MULT * rolling_std

    # ---------- Plot ----------
    fig = plt.figure(figsize=(18, 10), facecolor="black")
    gs = fig.add_gridspec(2, 1, height_ratios=[3, 1.3], hspace=0.06)

    ax_candle = fig.add_subplot(gs[0])
    ax_big = fig.add_subplot(gs[1], sharex=ax_candle)

    for ax in [ax_candle, ax_big]:
        ax.set_facecolor("black")
        ax.tick_params(colors="white")
        for spine in ax.spines.values():
            spine.set_color("white")

    # Candlesticks
    width = 0.6
    for idx, row in display_df.iterrows():
        color = "#00ff88" if row["close"] >= row["open"] else "#ff4466"
        ax_candle.plot([idx, idx], [row["low"], row["high"]], color=color, linewidth=0.8)
        body_low = min(row["open"], row["close"])
        body_height = abs(row["close"] - row["open"]) or 0.01
        rect = Rectangle((idx - width/2, body_low), width, body_height,
                         facecolor=color, edgecolor=color)
        ax_candle.add_patch(rect)

    ax_candle.set_xlim(-1, VISIBLE_CANDLES)
    ax_candle.set_title(
        f"{SYMBOL} 1m  |  Visible: {VISIBLE_CANDLES}  |  BigTrade Lookback: {CALC_LOOKBACK}  |  Std: {STD_MULT}σ",
        color="white", fontsize=13, pad=10
    )
    ax_candle.grid(True, color="#333333", alpha=0.5)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # Largest Trade bars
    ax_big.bar(range(len(display_df)), display_df["largest_size"],
               color=display_df["bar_color"], width=0.7, alpha=0.85)

    # Std lines only (no average)
    ax_big.plot(range(len(display_df)), upper, color="cyan", linestyle="--", linewidth=1.1, alpha=0.9)
    ax_big.plot(range(len(display_df)), lower, color="cyan", linestyle="--", linewidth=1.1, alpha=0.9)

    ax_big.set_ylabel("Largest Trade (USDT)", color="white")
    ax_big.grid(True, color="#333333", alpha=0.5)

    # X labels
    step = max(1, VISIBLE_CANDLES // 10)
    ax_big.set_xticks(range(0, VISIBLE_CANDLES, step))
    labels = [display_df["open_time"].iloc[i].strftime("%m-%d %H:%M") for i in range(0, VISIBLE_CANDLES, step)]
    ax_big.set_xticklabels(labels, rotation=45, color="white", fontsize=8)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=130, facecolor="black", edgecolor="none")
    buf.seek(0)
    plt.close()
    return buf.getvalue()


# --------------------------------------------------------------------------
# Send to Telegram
# --------------------------------------------------------------------------
async def send_photo(photo_bytes, caption=""):
    url = f"{TELEGRAM_API_URL}/sendPhoto"
    data = aiohttp.FormData()
    data.add_field("chat_id", str(CHAT_ID))
    data.add_field("caption", caption)
    data.add_field("photo", photo_bytes, filename="bigtrade_chart.png", content_type="image/png")

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, timeout=60) as resp:
            text = await resp.text()
            print(f"Telegram status: {resp.status}")
            print(f"Telegram response: {text[:300]}")
            if resp.status == 200:
                print("Photo sent successfully!")
            else:
                print("Failed to send photo")


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
async def main():
    try:
        print("1. Fetching klines...")
        async with aiohttp.ClientSession() as session:
            kline_df = await fetch_klines(session, SYMBOL, VISIBLE_CANDLES + 50)
            print(f"   Klines: {len(kline_df)}")

            start_ms = int(kline_df["open_time"].iloc[0].timestamp() * 1000)
            end_ms = int(kline_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

            print("2. Fetching aggTrades (this can take a while for 600 candles)...")
            largest_map = await fetch_largest_trades(session, SYMBOL, start_ms, end_ms)
            print(f"   Found largest trades for {len(largest_map)} candles")

        print("3. Creating chart...")
        photo = create_chart(kline_df, largest_map)
        print(f"4. Chart size: {len(photo)} bytes")

        caption = (f"{SYMBOL} – Largest Trade per 1m Candle\n"
                   f"Visible: {VISIBLE_CANDLES}\n"
                   f"Lookback: {CALC_LOOKBACK}\n"
                   f"Std: {STD_MULT}σ\n"
                   f"Green = Biggest trade was Buy | Red = Sell")

        print("5. Sending to Telegram...")
        await send_photo(photo, caption)
        print("6. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())