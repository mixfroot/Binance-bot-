#!/usr/bin/env python3
"""
Largest Outlier Trades Chart
- 1m Candlesticks
- Only trades outside rolling Std (lookback=9) are shown
- Green up = large Buy | Red down = large Sell
"""

import asyncio
import io
import traceback
from collections import defaultdict
from datetime import datetime, timezone

import aiohttp
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle, FancyBboxPatch

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
# Fetch all aggTrades in range and group by 1m candle
# --------------------------------------------------------------------------
async def fetch_all_trades(session, symbol, start_ms, end_ms):
    """
    Returns: dict[candle_ts] = list of (quote_qty, is_buyer_maker)
    """
    trades_by_candle = defaultdict(list)
    current_start = start_ms

    print("   Fetching aggTrades (this will take time)...")
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
            is_buyer_maker = t["m"]  # True = Sell (taker sell)
            trades_by_candle[candle_ts].append((quote_qty, is_buyer_maker))

        if len(trades) < 1000:
            break

        current_start = trades[-1]["T"] + 1
        await asyncio.sleep(0.12)

    return trades_by_candle


# --------------------------------------------------------------------------
# Create Chart
# --------------------------------------------------------------------------
def create_chart(kline_df, trades_by_candle):
    display_df = kline_df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)

    # Prepare lists for outlier trades
    outlier_x = []
    outlier_y = []
    outlier_colors = []
    outlier_sizes = []

    # We need enough history for rolling calculation
    all_candle_ts = [int(ts.timestamp() * 1000) for ts in display_df["open_time"]]

    for i, row in display_df.iterrows():
        ts_ms = int(row["open_time"].timestamp() * 1000)

        # Collect trades from the last CALC_LOOKBACK candles (including current)
        window_trades = []
        for j in range(max(0, i - CALC_LOOKBACK + 1), i + 1):
            prev_ts = all_candle_ts[j]
            window_trades.extend(trades_by_candle.get(prev_ts, []))

        if len(window_trades) < 5:
            continue

        sizes = np.array([t[0] for t in window_trades])
        mean = np.mean(sizes)
        std = np.std(sizes)
        if std == 0:
            continue

        upper = mean + STD_MULT * std
        lower = mean - STD_MULT * std

        # Check current candle trades for outliers
        current_trades = trades_by_candle.get(ts_ms, [])
        for qty, is_buyer_maker in current_trades:
            if qty > upper or qty < lower:
                # Outlier found
                outlier_x.append(i)
                # Buy = up (positive), Sell = down (negative)
                direction = -1 if is_buyer_maker else 1
                outlier_y.append(direction * qty)
                outlier_colors.append("#ff4466" if is_buyer_maker else "#00ff88")
                outlier_sizes.append(qty)

    # ---------- Plot ----------
    fig = plt.figure(figsize=(20, 11), facecolor="black")
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
        f"{SYMBOL} 1m  |  Visible: {VISIBLE_CANDLES}  |  Rolling Lookback: {CALC_LOOKBACK}  |  Std: {STD_MULT}σ\n"
        f"Only outlier trades shown  |  Green = large Buy  |  Red = large Sell",
        color="white", fontsize=12, pad=10
    )
    ax_candle.grid(True, color="#333333", alpha=0.4)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # Outlier trades panel
    if outlier_x:
        ax_out.bar(outlier_x, outlier_y, color=outlier_colors, width=0.6, alpha=0.85)
        ax_out.axhline(0, color="white", linewidth=0.8, alpha=0.6)

    ax_out.set_ylabel("Outlier Trade Size (USDT)", color="white")
    ax_out.grid(True, color="#333333", alpha=0.4)

    # X labels
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
            print(f"Telegram response: {text[:400]}")
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
            kline_df = await fetch_klines(session, SYMBOL, VISIBLE_CANDLES + CALC_LOOKBACK + 30)
            print(f"   Got {len(kline_df)} klines")

            start_ms = int(kline_df["open_time"].iloc[0].timestamp() * 1000)
            end_ms = int(kline_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

            print("2. Fetching all aggTrades (can take 1-3 minutes)...")
            trades_by_candle = await fetch_all_trades(session, SYMBOL, start_ms, end_ms)
            print(f"   Trades collected for {len(trades_by_candle)} candles")

        print("3. Creating chart...")
        photo = create_chart(kline_df, trades_by_candle)
        print(f"4. Chart size: {len(photo)/1024:.1f} KB")

        caption = (f"{SYMBOL} – Outlier Trades (>{STD_MULT}σ)\n"
                   f"Visible: {VISIBLE_CANDLES} candles\n"
                   f"Rolling Lookback: {CALC_LOOKBACK}\n"
                   f"Green ↑ = large Buy | Red ↓ = large Sell")

        print("5. Sending to Telegram...")
        await send_photo(photo, caption)
        print("6. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())