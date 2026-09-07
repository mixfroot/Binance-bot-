#!/usr/bin/env python3
"""
One-time 1m Chart Generator
- Candlesticks + Taker Buy/Sell Volume
- Sends picture to Telegram and exits
"""

import asyncio
import io

import aiohttp
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Rectangle

# --------------------------------------------------------------------------
# CONFIG (change these)
# --------------------------------------------------------------------------
SYMBOL = "BTCUSDT"

LOOKBACK = 100          # Used for both visible candles AND calculation
STD_MULT = 2.0          # Standard deviation multiplier (1.0 = 1σ, 2.0 = 2σ, etc.)

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# --------------------------------------------------------------------------
# Fetch data
# --------------------------------------------------------------------------
async def fetch_klines(symbol: str, limit: int = 300):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    for col in ["open", "high", "low", "close", "volume", "taker_buy_base"]:
        df[col] = df[col].astype(float)

    df["taker_sell_base"] = df["volume"] - df["taker_buy_base"]
    return df


# --------------------------------------------------------------------------
# Create Chart
# --------------------------------------------------------------------------
def create_chart(df: pd.DataFrame) -> bytes:
    # Keep only the last LOOKBACK candles for display
    # (we fetched extra so the first visible candle already has full history)
    display_df = df.tail(LOOKBACK).copy().reset_index(drop=True)

    # Rolling calculations on the full data (pre-warmed)
    buy_mean = df["taker_buy_base"].rolling(LOOKBACK).mean().tail(LOOKBACK).values
    buy_std  = df["taker_buy_base"].rolling(LOOKBACK).std().tail(LOOKBACK).values

    sell_mean = df["taker_sell_base"].rolling(LOOKBACK).mean().tail(LOOKBACK).values
    sell_std  = df["taker_sell_base"].rolling(LOOKBACK).std().tail(LOOKBACK).values

    # Create figure
    fig = plt.figure(figsize=(16, 10), facecolor="black")
    gs = fig.add_gridspec(3, 1, height_ratios=[3, 1, 1], hspace=0.05)

    ax_candle = fig.add_subplot(gs[0])
    ax_buy = fig.add_subplot(gs[1], sharex=ax_candle)
    ax_sell = fig.add_subplot(gs[2], sharex=ax_candle)

    for ax in [ax_candle, ax_buy, ax_sell]:
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

    ax_candle.set_xlim(-1, LOOKBACK)
    ax_candle.set_title(
        f"{SYMBOL} 1m  |  Lookback: {LOOKBACK}  |  Std: {STD_MULT}σ",
        color="white", fontsize=13, pad=10
    )
    ax_candle.grid(True, color="#333333", alpha=0.6)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # ---------- Taker Buy Volume ----------
    ax_buy.bar(range(len(display_df)), display_df["taker_buy_base"],
               color="#00ff88", width=0.7, alpha=0.85)

    ax_buy.axhline(buy_mean[-1], color="yellow", linestyle="--", linewidth=1.2, alpha=0.9)
    ax_buy.axhline(buy_mean[-1] + STD_MULT * buy_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_buy.axhline(buy_mean[-1] - STD_MULT * buy_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)

    ax_buy.set_ylabel("Taker Buy", color="white")
    ax_buy.grid(True, color="#333333", alpha=0.5)
    plt.setp(ax_buy.get_xticklabels(), visible=False)

    # ---------- Taker Sell Volume ----------
    ax_sell.bar(range(len(display_df)), display_df["taker_sell_base"],
                color="#ff4466", width=0.7, alpha=0.85)

    ax_sell.axhline(sell_mean[-1], color="yellow", linestyle="--", linewidth=1.2, alpha=0.9)
    ax_sell.axhline(sell_mean[-1] + STD_MULT * sell_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)
    ax_sell.axhline(sell_mean[-1] - STD_MULT * sell_std[-1], color="cyan", linestyle="--", linewidth=1, alpha=0.8)

    ax_sell.set_ylabel("Taker Sell", color="white")
    ax_sell.grid(True, color="#333333", alpha=0.5)

    # X-axis time labels
    step = max(1, LOOKBACK // 8)
    ax_sell.set_xticks(range(0, LOOKBACK, step))
    labels = [display_df["open_time"].iloc[i].strftime("%H:%M") for i in range(0, LOOKBACK, step)]
    ax_sell.set_xticklabels(labels, rotation=45, color="white")

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, facecolor="black", edgecolor="none")
    buf.seek(0)
    plt.close()
    return buf.getvalue()


# --------------------------------------------------------------------------
# Send to Telegram
# --------------------------------------------------------------------------
async def main():
    try:
        print("1. Fetching data...")
        df = await fetch_klines(SYMBOL, limit=LOOKBACK * 2 + 50)
        print(f"2. Data fetched: {len(df)} candles")

        print("3. Creating chart...")
        photo = create_chart(df)
        print(f"4. Chart created: {len(photo)} bytes")

        caption = (f"{SYMBOL} 1m Chart\n"
                   f"Lookback: {LOOKBACK}\n"
                   f"Std Multiplier: {STD_MULT}σ")

        print("5. Sending to Telegram...")
        await send_photo(photo, caption)
        print("6. Done. Exiting.")

    except Exception as e:
        print("ERROR:", str(e))
        import traceback
        traceback.print_exc()