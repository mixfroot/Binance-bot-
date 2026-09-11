#!/usr/bin/env python3
"""
Hawkes Intensity Chart – fixed rolling version
- Every trade is an event
- Intensity for each bar is calculated ONLY from the last HAWKES_WINDOW minutes
  (fully local → bursts appear anywhere on the chart)
- Full z-score series shown on every bar
- Volume on twin axis
- Same candles + Telegram style as your original
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
CALC_LOOKBACK = 30          # z-score lookback (bars)
HAWKES_WINDOW = 5           # minutes of tape used for each bar's intensity (short = more reactive)
STD_MULT = 2.0

# Hawkes parameters
MU = 0.1
ALPHA = 1.2
BETA = 0.25                 # faster decay so recent clusters stand out

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
# Fetch ALL trade times
# --------------------------------------------------------------------------
async def fetch_all_trade_times(session, symbol, start_ms, end_ms):
    times = []
    current_start = start_ms

    print("   Fetching aggTrades...")
    while current_start < end_ms:
        url = (f"https://fapi.binance.com/fapi/v1/aggTrades"
               f"?symbol={symbol}&startTime={current_start}&endTime={end_ms}&limit=1000")

        async with session.get(url, timeout=20) as resp:
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
    print(f"   Collected {len(times):,} trades")
    return np.array(times, dtype=np.int64)


# --------------------------------------------------------------------------
# Fully local / rolling Hawkes intensity for every bar
# --------------------------------------------------------------------------
def compute_rolling_hawkes(trade_times_ms, bar_close_ms,
                           window_minutes=HAWKES_WINDOW,
                           mu=MU, alpha=ALPHA, beta=BETA):
    """
    For each bar close T:
      use ONLY trades in [T - window_minutes, T]
      run recursive Hawkes on that local window
      sample intensity at T
    This keeps intensity reactive to local bursts anywhere on the chart.
    """
    window_ms = int(window_minutes * 60 * 1000)
    n = len(bar_close_ms)
    intensities = np.zeros(n)

    left = 0
    for i, t_close in enumerate(bar_close_ms):
        t_start = t_close - window_ms

        while left < len(trade_times_ms) and trade_times_ms[left] < t_start:
            left += 1

        right = left
        while right < len(trade_times_ms) and trade_times_ms[right] <= t_close:
            right += 1

        local = trade_times_ms[left:right]

        if len(local) == 0:
            intensities[i] = mu
            continue

        times_s = local.astype(np.float64) / 1000.0
        t_close_s = t_close / 1000.0

        lam = mu
        t_cur = times_s[0] - 1e-6

        for ts in times_s:
            dt = ts - t_cur
            if dt > 0:
                lam = mu + (lam - mu) * np.exp(-beta * dt)
            lam += alpha
            t_cur = ts

        dt = t_close_s - t_cur
        if dt > 0:
            lam = mu + (lam - mu) * np.exp(-beta * dt)

        intensities[i] = max(lam, 0.0)

    return intensities


# --------------------------------------------------------------------------
# Create Chart
# --------------------------------------------------------------------------
def create_chart(kline_df, intensities):
    display_df = kline_df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)
    display_intensities = intensities[-VISIBLE_CANDLES:]

    # Full z-score for every bar
    bar_values = []
    bar_colors = []

    for i in range(len(display_df)):
        window = display_intensities[max(0, i - CALC_LOOKBACK + 1): i + 1]

        if len(window) < 5:
            bar_values.append(0.0)
            bar_colors.append("#555555")
            continue

        mean = np.mean(window)
        std = np.std(window)
        if std < 1e-12:
            bar_values.append(0.0)
            bar_colors.append("#555555")
            continue

        z = (display_intensities[i] - mean) / std
        bar_values.append(z)

        if z >= STD_MULT:
            bar_colors.append("#00ff88")
        elif z <= -STD_MULT:
            bar_colors.append("#ff4466")
        elif z > 0:
            bar_colors.append("#66ffaa")
        else:
            bar_colors.append("#888888")

    volumes = display_df["volume"].astype(float).values

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
        f"{SYMBOL} 1m  |  Visible: {VISIBLE_CANDLES}  |  Hawkes window: {HAWKES_WINDOW}m  |  Z-lookback: {CALC_LOOKBACK}\n"
        f"Local Hawkes Z-Score (all bars) + Volume  |  Green ≥ +{STD_MULT}σ  |  Red ≤ –{STD_MULT}σ",
        color="white", fontsize=12, pad=8
    )
    ax_candle.grid(True, color="#333333", alpha=0.4)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # Hawkes z-score (all bars)
    ax_out.bar(range(len(display_df)), bar_values, color=bar_colors, width=0.65, alpha=0.85, zorder=2)
    ax_out.axhline(0, color="white", linewidth=0.8, alpha=0.5)
    ax_out.axhline(STD_MULT, color="#00ff88", linewidth=0.6, linestyle="--", alpha=0.5)
    ax_out.axhline(-STD_MULT, color="#ff4466", linewidth=0.6, linestyle="--", alpha=0.5)
    ax_out.set_ylabel("Hawkes Z-Score", color="white")
    ax_out.grid(True, color="#333333", alpha=0.4)

    # Volume twin axis
    ax_vol = ax_out.twinx()
    ax_vol.bar(range(len(display_df)), volumes, color="#4488ff", width=0.65, alpha=0.25, zorder=1)
    ax_vol.set_ylabel("Volume", color="#4488ff")
    ax_vol.tick_params(axis="y", colors="#4488ff")
    ax_vol.spines["right"].set_color("#4488ff")
    ax_vol.set_ylim(0, volumes.max() * 1.15 if len(volumes) and volumes.max() > 0 else 1)

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
    data.add_field("photo", photo_bytes, filename="hawkes_intensity.png", content_type="image/png")

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
            extra = max(HAWKES_WINDOW, CALC_LOOKBACK) + 40
            kline_df = await fetch_klines(session, SYMBOL, VISIBLE_CANDLES + extra)
            print(f"   Got {len(kline_df)} klines")

            start_ms = int(kline_df["open_time"].iloc[0].timestamp() * 1000)
            end_ms   = int(kline_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

            print("2. Fetching full trade tape...")
            trade_times = await fetch_all_trade_times(session, SYMBOL, start_ms, end_ms)

        bar_close_ms = np.array(
            [int(ts.timestamp() * 1000) + 60_000 for ts in kline_df["open_time"]],
            dtype=np.int64
        )

        print("3. Computing local rolling Hawkes intensity...")
        intensities = compute_rolling_hawkes(
            trade_times, bar_close_ms,
            window_minutes=HAWKES_WINDOW,
            mu=MU, alpha=ALPHA, beta=BETA
        )
        print(f"   Intensity ready | last 5: {[round(x, 2) for x in intensities[-5:]]}")

        print("4. Creating chart...")
        photo = create_chart(kline_df, intensities)
        print(f"   Chart size: {len(photo)/1024:.1f} KB")

        caption = (f"{SYMBOL} – Local Hawkes Z-Score (all bars) + Volume\n"
                   f"Hawkes window: {HAWKES_WINDOW}m | Z-lookback: {CALC_LOOKBACK}\n"
                   f"μ={MU} α={ALPHA} β={BETA}")

        print("5. Sending...")
        await send_photo(photo, caption)
        print("6. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())