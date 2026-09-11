#!/usr/bin/env python3
"""
Arrival rate VOLUME-style panel: buy_rate and sell_rate shown as separate bars
(green up = buy, red down = sell) — not netted into delta.
Z_LOOKBACK / Z_THRESHOLD control when a bar is highlighted as an outlier.
No lines plotted.
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
Z_LOOKBACK = 30
Z_THRESHOLD = 2.0

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
# Fetch ALL agg trades WITH buy/sell side
# --------------------------------------------------------------------------
async def fetch_all_agg_trades(session, symbol, start_ms, end_ms):
    """isBuyerMaker=True -> SELL aggressor. isBuyerMaker=False -> BUY aggressor."""
    times = []
    maker_flags = []
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
            maker_flags.append(t["m"])
        if len(trades) < 1000:
            break
        current_start = trades[-1]["T"] + 1
        await asyncio.sleep(0.12)

    order = np.argsort(times)
    times_ms = np.array(times, dtype=np.int64)[order]
    is_buyer_maker = np.array(maker_flags, dtype=bool)[order]
    print(f"   Collected {len(times_ms):,} trades")
    return times_ms, is_buyer_maker


# --------------------------------------------------------------------------
# Instantaneous arrival rate per candle (averaged 1/dt within each candle)
# --------------------------------------------------------------------------
def compute_candle_arrival_rate(times_ms, bar_open_ms, bar_close_ms):
    n = len(bar_open_ms)
    rates = np.zeros(n)
    left = 0
    for i in range(n):
        t_start, t_end = bar_open_ms[i], bar_close_ms[i]
        while left < len(times_ms) and times_ms[left] < t_start:
            left += 1
        right = left
        while right < len(times_ms) and times_ms[right] < t_end:
            right += 1
        local = times_ms[left:right]
        if len(local) >= 2:
            times_s = local.astype(np.float64) / 1000.0
            dts = np.diff(times_s)
            dts = dts[dts > 0]
            if len(dts) > 0:
                rates[i] = np.mean(1.0 / dts)
    return rates


def rolling_zscore(series, lookback=Z_LOOKBACK):
    n = len(series)
    z = np.zeros(n)
    for i in range(n):
        window = series[max(0, i - lookback + 1): i + 1]
        if len(window) < 5:
            continue
        mean = np.mean(window)
        std = np.std(window)
        if std > 1e-12:
            z[i] = (series[i] - mean) / std
    return z


# --------------------------------------------------------------------------
# Chart
# --------------------------------------------------------------------------
def create_chart(kline_df, buy_rate, sell_rate, buy_z, sell_z):
    display_df = kline_df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)
    n = len(display_df)
    b = buy_rate[-n:]
    s = sell_rate[-n:]
    bz = buy_z[-n:]
    sz = sell_z[-n:]

    fig = plt.figure(figsize=(18, 10), facecolor="black")
    gs = fig.add_gridspec(2, 1, height_ratios=[2.8, 1.4], hspace=0.07)
    ax_candle = fig.add_subplot(gs[0])
    ax_rate = fig.add_subplot(gs[1], sharex=ax_candle)

    for ax in [ax_candle, ax_rate]:
        ax.set_facecolor("black")
        ax.tick_params(colors="white")
        for spine in ax.spines.values():
            spine.set_color("white")

    # Candles
    width = 0.6
    for idx, row in display_df.iterrows():
        color = "#00ff88" if row["close"] >= row["open"] else "#ff4466"
        ax_candle.plot([idx, idx], [row["low"], row["high"]], color=color, linewidth=0.7)
        body_low = min(row["open"], row["close"])
        body_height = abs(row["close"] - row["open"]) or 0.01
        rect = Rectangle((idx - width / 2, body_low), width, body_height,
                          facecolor=color, edgecolor=color)
        ax_candle.add_patch(rect)

    # star flag if either side is an outlier
    span = display_df["high"].max() - display_df["low"].min()
    for idx in range(n):
        if abs(bz[idx]) >= Z_THRESHOLD or abs(sz[idx]) >= Z_THRESHOLD:
            y = display_df["high"].iloc[idx] + span * 0.02
            ax_candle.plot(idx, y, marker="*", color="#ffdd00", markersize=12, zorder=5)

    ax_candle.set_xlim(-1, n)
    ax_candle.set_title(
        f"{SYMBOL} 1m  |  Arrival Rate Volume Panel (buy up / sell down)  |  "
        f"z lookback={Z_LOOKBACK}  threshold={Z_THRESHOLD}σ",
        color="white", fontsize=12, pad=8
    )
    ax_candle.grid(True, color="#333333", alpha=0.4)
    plt.setp(ax_candle.get_xticklabels(), visible=False)

    # Volume-style panel: green up = buy rate, red down = sell rate, bars only
    buy_colors = ["#00ff88" if abs(bz[i]) >= Z_THRESHOLD else "#2e6b4a" for i in range(n)]
    sell_colors = ["#ff4466" if abs(sz[i]) >= Z_THRESHOLD else "#7a2e3a" for i in range(n)]

    ax_rate.bar(range(n), b, color=buy_colors, width=0.65, alpha=0.95, zorder=2)
    ax_rate.bar(range(n), -s, color=sell_colors, width=0.65, alpha=0.95, zorder=2)
    ax_rate.axhline(0, color="white", linewidth=0.8, alpha=0.6)
    ax_rate.set_ylabel("Arrival rate (trades/sec)", color="white", fontsize=9)
    ax_rate.grid(True, color="#333333", alpha=0.4)

    step = max(1, n // 12)
    ax_rate.set_xticks(range(0, n, step))
    labels = [display_df["open_time"].iloc[i].strftime("%m-%d %H:%M") for i in range(0, n, step)]
    ax_rate.set_xticklabels(labels, rotation=45, color="white", fontsize=8)

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
    data.add_field("photo", photo_bytes, filename="arrival_rate_volume.png", content_type="image/png")
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
            kline_df = await fetch_klines(session, SYMBOL, VISIBLE_CANDLES + 5)
            print(f"   Got {len(kline_df)} klines")

            start_ms = int(kline_df["open_time"].iloc[0].timestamp() * 1000)
            end_ms = int(kline_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

            print("2. Fetching full trade tape (buy/sell tagged)...")
            trade_times, is_buyer_maker = await fetch_all_agg_trades(session, SYMBOL, start_ms, end_ms)

        buy_times = trade_times[~is_buyer_maker]
        sell_times = trade_times[is_buyer_maker]
        print(f"   Buys: {len(buy_times):,}  Sells: {len(sell_times):,}")

        bar_open_ms = np.array(
            [int(ts.timestamp() * 1000) for ts in kline_df["open_time"]], dtype=np.int64
        )
        bar_close_ms = bar_open_ms + 60_000

        print("3. Computing per-candle arrival rates...")
        buy_rate = compute_candle_arrival_rate(buy_times, bar_open_ms, bar_close_ms)
        sell_rate = compute_candle_arrival_rate(sell_times, bar_open_ms, bar_close_ms)

        print("4. Computing rolling z-score per side...")
        buy_z = rolling_zscore(buy_rate, lookback=Z_LOOKBACK)
        sell_z = rolling_zscore(sell_rate, lookback=Z_LOOKBACK)
        n_flagged = int(np.sum((np.abs(buy_z) >= Z_THRESHOLD) | (np.abs(sell_z) >= Z_THRESHOLD)))
        print(f"   {n_flagged} candles with either side |z| >= {Z_THRESHOLD}")

        print("5. Creating chart...")
        photo = create_chart(kline_df, buy_rate, sell_rate, buy_z, sell_z)
        print(f"   Chart size: {len(photo)/1024:.1f} KB")

        caption = (f"{SYMBOL} – Arrival Rate Volume Panel\n"
                   f"z lookback={Z_LOOKBACK} | threshold={Z_THRESHOLD}σ | {n_flagged} flagged")

        print("6. Sending...")
        await send_photo(photo, caption)
        print("7. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())