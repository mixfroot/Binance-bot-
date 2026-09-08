#!/usr/bin/env python3
"""
One-time 1m Chart Generator
- Candlesticks
- Taker Buy Volume
- Taker Sell Volume
- Delta (Buy - Sell)
- Custom visible candles + separate calculation lookback
- Multiple API calls if needed
"""

import asyncio
import io
import traceback

import aiohttp
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Rectangle

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SYMBOL = "BTCUSDT"

VISIBLE_CANDLES = 400       # How many candles you want to SEE on the chart
CALC_LOOKBACK = 60         # Lookback used for Std calculation
STD_MULT = 3.0              # Standard deviation multiplier

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# --------------------------------------------------------------------------
# Fetch data (supports multiple calls if needed)
# --------------------------------------------------------------------------
async def fetch_klines(symbol: str, total_needed: int):
    """
    Fetches enough candles. Binance max per request is 1500.
    If we need more, it will make multiple calls.
    """
    all_data = []
    remaining = total_needed
    end_time = None

    async with aiohttp.ClientSession() as session:
        while remaining > 0:
            limit = min(1500, remaining)
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}"
            if end_time:
                url += f"&endTime={end_time}"

            async with session.get(url, timeout=15) as resp:
                data = await resp.json()

            if not data:
                break

            all_data = data + all_data          # prepend older data
            remaining -= len(data)

            # prepare for next (older) batch
            end_time = data[0][0] - 1

            if len(data) < limit:
                break

            await asyncio.sleep(0.3)  # be nice to API

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    for col in ["open", "high", "low", "close", "volume", "taker_buy_base"]:
        df[col] = df[col].astype(float)

    df["taker_sell_base"] = df["volume"] - df["taker_buy_base"]
    df["delta"] = df["taker_buy_base"] - df["taker_sell_base"]

    # Remove duplicates just in case
    df = df.drop_duplicates(subset=["open_time"]).reset_index(drop=True)
    return df


# --------------------------------------------------------------------------
# Create Chart
# --------------------------------------------------------------------------
def create_chart(df: pd.DataFrame) -> bytes:
    # We need CALC_LOOKBACK history before the first visible candle
    required = VISIBLE_CANDLES + CALC_LOOKBACK
    if len(df) < required:
        print(f"Warning: Only {len(df)} candles available, needed {required}")

    display_df = df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)

    # Calculate rolling stats on the full dataframe (pre-warmed)
    buy_std  = df["taker_buy_base"].rolling(CALC_LOOKBACK).std().tail(VISIBLE_CANDLES).values
    sell_std = df["taker_sell_base"].rolling(CALC_LOOKBACK).std().tail(VISIBLE_CANDLES).values
    delta_std = df["delta"].rolling(CALC_LOOKBACK).std().tail(VISIBLE_CANDLES).values

    buy_mean  = df["taker_buy_base"].rolling(CALC_LOOKBACK).mean().tail(VISIBLE_CANDLES).values
    sell_mean = df["taker_sell_base"].rolling(CALC_LOOKBACK).mean().tail(VISIBLE_CANDLES).values
    delta_mean = df["delta"].rolling(CALC_LOOKBACK).mean().tail(VISIBLE_CANDLES).values

    # Create figure with 4 panels
    fig = plt.figure(figsize=(16, 12), facecolor="black")
    gs = fig.add_gridspec(4, 1, height_ratios=[3, 1, 1, 1], hspace=0.06)

    ax_candle = fig.add_subplot(gs[0])
    ax_buy    = fig.add_subplot(gs[1], sharex=ax_candle)
    ax_sell   = fig.add_subplot(gs[2], sharex=ax_candle)
    ax_delta  = fig.add_subplot(gs[3], sharex=ax_candle)

    for ax in [ax_candle, ax_buy, ax_sell, ax_delta]:
        ax.set_facecolor("black")
        ax.tick_params(colors="white")
        for spine in ax.spines.values():
            spine.set_color("white")

    # ---------- Candlesticks ----------
    width = 0.6
    for idx, row in display_df.iterrows():
        color = "#00ff88" if row["close"] >= row["open"] else "#ff4466"
        ax_candle.plot([idx, idx], [row["low"], row["high"]], color=color, linewidth=1)
        body_low = min(row["open"], row["close"])
        body_height = abs(row["close"] - row["open"]) or 0.01
        rect = Rectangle((idx - width/2, body_low), width, body_height,
                         facecolor=color, edgecolor=color)
        ax_candle.add_patch(rect)

    ax_candle.set_xlim(-1, VISIBLE_CANDLES)
    ax_candle.set_title(
        f"{SYMBOL} 1m  |  Visible: {VISIBLE_CANDLES}  |  Calc Lookback: {CALC_LOOKBACK}  |  Std: {STD_MULT}σ",
        color="white", fontsize=13, pad=10
    )
    ax_candle.grid(True, color="#333333", alpha=0.6)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # ---------- Taker Buy ----------
    ax_buy.bar(range(len(display_df)), display_df["taker_buy_base"],
               color="#00ff88", width=0.7, alpha=0.85)
    ax_buy.axhline(buy_mean[-1] + STD_MULT * buy_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_buy.axhline(buy_mean[-1] - STD_MULT * buy_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_buy.set_ylabel("Buy", color="white")
    ax_buy.grid(True, color="#333333", alpha=0.5)
    plt.setp(ax_buy.get_xticklabels(), visible=False)

    # ---------- Taker Sell ----------
    ax_sell.bar(range(len(display_df)), display_df["taker_sell_base"],
                color="#ff4466", width=0.7, alpha=0.85)
    ax_sell.axhline(sell_mean[-1] + STD_MULT * sell_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_sell.axhline(sell_mean[-1] - STD_MULT * sell_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_sell.set_ylabel("Sell", color="white")
    ax_sell.grid(True, color="#333333", alpha=0.5)
    plt.setp(ax_sell.get_xticklabels(), visible=False)

    # ---------- Delta (Buy - Sell) ----------
    colors = ["#00ff88" if v >= 0 else "#ff4466" for v in display_df["delta"]]
    ax_delta.bar(range(len(display_df)), display_df["delta"],
                 color=colors, width=0.7, alpha=0.85)
    ax_delta.axhline(delta_mean[-1] + STD_MULT * delta_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_delta.axhline(delta_mean[-1] - STD_MULT * delta_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_delta.axhline(0, color="white", linestyle="-", linewidth=0.8, alpha=0.5)
    ax_delta.set_ylabel("Delta", color="white")
    ax_delta.grid(True, color="#333333", alpha=0.5)

    # X-axis labels
    step = max(1, VISIBLE_CANDLES // 8)
    ax_delta.set_xticks(range(0, VISIBLE_CANDLES, step))
    labels = [display_df["open_time"].iloc[i].strftime("%H:%M") for i in range(0, VISIBLE_CANDLES, step)]
    ax_delta.set_xticklabels(labels, rotation=45, color="white")

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=140, facecolor="black", edgecolor="none")
    buf.seek(0)
    plt.close()
    return buf.getvalue()


# --------------------------------------------------------------------------
# Send Photo
# --------------------------------------------------------------------------
async def send_photo(photo_bytes: bytes, caption: str = ""):
    url = f"{TELEGRAM_API_URL}/sendPhoto"

    data = aiohttp.FormData()
    data.add_field("chat_id", str(CHAT_ID))
    data.add_field("caption", caption)
    data.add_field("photo", photo_bytes, filename="chart.png", content_type="image/png")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, timeout=30) as resp:
                result = await resp.text()
                print(f"Telegram status: {resp.status}")
                print(f"Telegram response: {result}")
                if resp.status == 200:
                    print("Photo sent successfully!")
                else:
                    print("Failed to send photo")
    except Exception as e:
        print("Exception while sending photo:", str(e))
        traceback.print_exc()


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
async def main():
    try:
        total_needed = VISIBLE_CANDLES + CALC_LOOKBACK + 50
        print(f"1. Fetching data (need \~{total_needed} candles)...")
        df = await fetch_klines(SYMBOL, total_needed)
        print(f"2. Data fetched: {len(df)} candles")

        print("3. Creating chart...")
        photo = create_chart(df)
        print(f"4. Chart created: {len(photo)} bytes")

        caption = (f"{SYMBOL} 1m Chart\n"
                   f"Visible: {VISIBLE_CANDLES}\n"
                   f"Calc Lookback: {CALC_LOOKBACK}\n"
                   f"Std: {STD_MULT}σ")

        print("5. Sending to Telegram...")
        await send_photo(photo, caption)
        print("6. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())