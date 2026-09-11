#!/usr/bin/env python3
"""
Trade-level rolling 60s arrival rate, fully independent of candle boundaries.
- For every trade, look at the trailing 60s window ending at that trade
- rate = (number of trades in that window) / 60
- Take the 99th percentile of that rate series across the whole tape
- Any trade whose window-rate >= 99th percentile flags its containing candle
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
ROLLING_SECONDS = 60
PCTL = 99

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
# Trade-level rolling 60s rate, fully independent of candles
# --------------------------------------------------------------------------
def compute_trade_level_rolling_rate(trade_times_ms, window_sec=ROLLING_SECONDS):
    """
    For every trade i, count trades in [t_i - window_sec, t_i], rate = count/window_sec.
    Pure trade-tape computation, no candle involvement at all.
    """
    n = len(trade_times_ms)
    window_ms = window_sec * 1000
    rates = np.zeros(n)
    left = 0
    for i in range(n):
        t_i = trade_times_ms[i]
        t_start = t_i - window_ms
        while trade_times_ms[left] < t_start:
            left += 1
        count = i - left + 1
        rates[i] = count / window_sec
    return rates


def find_pctl_events(trade_times_ms, rates, pctl=PCTL):
    threshold = np.percentile(rates, pctl)
    mask = rates >= threshold
    event_times = trade_times_ms[mask]
    event_rates = rates[mask]
    return event_times, event_rates, threshold


# --------------------------------------------------------------------------
# Map trade-level events onto candles
# --------------------------------------------------------------------------
def flag_candles_from_events(event_times_ms, event_rates, kline_df):
    open_ms = (kline_df["open_time"].astype(np.int64) // 1_000_000).values
    flagged = {}
    for t_ms, r in zip(event_times_ms, event_rates):
        idx = np.searchsorted(open_ms, t_ms, side="right") - 1
        if idx < 0 or idx >= len(kline_df):
            continue
        if idx not in flagged or r > flagged[idx]:
            flagged[idx] = r
    return flagged  # {candle_index: peak rate}


# --------------------------------------------------------------------------
# Chart
# --------------------------------------------------------------------------
def create_chart(kline_df, flagged_full, threshold):
    display_df = kline_df.tail(VISIBLE_CANDLES).copy().reset_index(drop=True)
    offset = len(kline_df) - len(display_df)
    flagged = {idx - offset: r for idx, r in flagged_full.items()
               if 0 <= idx - offset < len(display_df)}

    volumes = display_df["volume"].astype(float).values

    fig, ax_candle = plt.subplots(figsize=(18, 8), facecolor="black")
    ax_candle.set_facecolor("black")
    ax_candle.tick_params(colors="white")
    for spine in ax_candle.spines.values():
        spine.set_color("white")

    width = 0.6
    for idx, row in display_df.iterrows():
        color = "#00ff88" if row["close"] >= row["open"] else "#ff4466"
        ax_candle.plot([idx, idx], [row["low"], row["high"]], color=color, linewidth=0.7)
        body_low = min(row["open"], row["close"])
        body_height = abs(row["close"] - row["open"]) or 0.01
        rect = Rectangle((idx - width / 2, body_low), width, body_height,
                          facecolor=color, edgecolor=color)
        ax_candle.add_patch(rect)

    span = display_df["high"].max() - display_df["low"].min()
    for idx, r in flagged.items():
        y = display_df["high"].iloc[idx] + span * 0.02
        ax_candle.plot(idx, y, marker="*", color="#ffdd00", markersize=14, zorder=5)
        ax_candle.text(idx, y + span * 0.015, f"{r:.1f}/s", color="#ffdd00",
                        fontsize=7, ha="center", zorder=5)

    ax_candle.set_xlim(-1, len(display_df))
    ax_candle.set_title(
        f"{SYMBOL} 1m  |  Trade-level rolling {ROLLING_SECONDS}s rate  |  "
        f"{PCTL}th pctl = {threshold:.2f} trades/sec\n"
        f"Flagged candles: {len(flagged)}",
        color="white", fontsize=12, pad=10
    )
    ax_candle.grid(True, color="#333333", alpha=0.4)

    ax_vol = ax_candle.twinx()
    ax_vol.bar(range(len(display_df)), volumes, color="#4488ff", width=0.65, alpha=0.2, zorder=0)
    ax_vol.set_ylabel("Volume", color="#4488ff")
    ax_vol.tick_params(axis="y", colors="#4488ff")
    ax_vol.spines["right"].set_color("#4488ff")
    ax_vol.set_ylim(0, volumes.max() * 4 if len(volumes) and volumes.max() > 0 else 1)

    step = max(1, len(display_df) // 12)
    ax_candle.set_xticks(range(0, len(display_df), step))
    labels = [display_df["open_time"].iloc[i].strftime("%m-%d %H:%M") for i in range(0, len(display_df), step)]
    ax_candle.set_xticklabels(labels, rotation=45, color="white", fontsize=8)

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
    data.add_field("photo", photo_bytes, filename="rolling_rate_pctl.png", content_type="image/png")
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

            start_ms = int(kline_df["open_time"].iloc[0].timestamp() * 1000) - ROLLING_SECONDS * 1000
            end_ms = int(kline_df["open_time"].iloc[-1].timestamp() * 1000) + 60_000

            print("2. Fetching full trade tape...")
            trade_times = await fetch_all_trade_times(session, SYMBOL, start_ms, end_ms)

        print("3. Computing trade-level rolling 60s rate (candle-independent)...")
        rates = compute_trade_level_rolling_rate(trade_times)
        print(f"   rate range: min={rates.min():.2f} max={rates.max():.2f} "
              f"median={np.median(rates):.2f} trades/sec")

        print(f"4. Finding {PCTL}th percentile events...")
        event_times, event_rates, threshold = find_pctl_events(trade_times, rates)
        print(f"   threshold={threshold:.2f} trades/sec | {len(event_times)} trades crossed it")

        print("5. Mapping events onto candles...")
        flagged = flag_candles_from_events(event_times, event_rates, kline_df)
        print(f"   {len(flagged)} candles flagged")

        print("6. Creating chart...")
        photo = create_chart(kline_df, flagged, threshold)
        print(f"   Chart size: {len(photo)/1024:.1f} KB")

        caption = (f"{SYMBOL} – Trade-level {ROLLING_SECONDS}s rolling rate, {PCTL}th percentile\n"
                   f"threshold={threshold:.2f} trades/sec | {len(flagged)} candles flagged")

        print("7. Sending...")
        await send_photo(photo, caption)
        print("8. Done.")

    except Exception as e:
        print("ERROR:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())