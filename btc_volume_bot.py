#!/usr/bin/env python3
import argparse
import io
import warnings
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import requests
from matplotlib.patches import Rectangle

warnings.filterwarnings("ignore")

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"

DEFAULT_SYMBOL = "NEARUSDT"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_LOOKBACK = 400      # candles fetched
DEFAULT_WINDOW = 8         # rolling window (candles) for buy/sell ratio


def fetch_binance_klines(symbol, interval, limit):
    urls = [
        "https://data-api.binance.vision/api/v3/klines",
        "https://api.binance.com/api/v3/klines",
        "https://api1.binance.com/api/v3/klines",
    ]
    all_rows = []
    end_time = None
    remaining = limit
    while remaining > 0:
        batch = min(remaining, 1000)
        params = {"symbol": symbol.upper(), "interval": interval, "limit": batch}
        if end_time is not None:
            params["endTime"] = end_time
        data, last_err = None, None
        for url in urls:
            try:
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                last_err = e
        if not data:
            if not all_rows:
                raise RuntimeError(f"All Binance endpoints failed: {last_err}")
            break
        all_rows = data + all_rows
        remaining -= len(data)
        end_time = data[0][0] - 1
        if len(data) < batch:
            break

    df = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ])
    df = df.drop_duplicates(subset="open_time").sort_values("open_time")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume", "taker_buy_base"]:
        df[col] = df[col].astype(float)
    df = df.set_index("open_time")
    return df[["open", "high", "low", "close", "volume", "taker_buy_base"]].tail(limit)


def compute_rolling_taker_ratio(df, window=20):
    """
    Per-candle: buy_vol = taker_buy_base, sell_vol = volume - taker_buy_base.
    Rolling: sum buy_vol and sell_vol over the trailing `window` candles,
    THEN compute ratio on the sums -> volume-weighted, not just averaged ratios.
    ratio = (sum_buy - sum_sell) / (sum_buy + sum_sell)  in [-1, +1]
    """
    buy_vol = df["taker_buy_base"]
    sell_vol = df["volume"] - df["taker_buy_base"]

    roll_buy = buy_vol.rolling(window=window, min_periods=window).sum()
    roll_sell = sell_vol.rolling(window=window, min_periods=window).sum()
    roll_total = roll_buy + roll_sell

    ratio = (roll_buy - roll_sell) / roll_total.replace(0, np.nan)
    ratio = ratio.fillna(0.0)
    return ratio, roll_buy, roll_sell


def plot_chart(df, ratio, symbol, timeframe, window):
    fig = plt.figure(figsize=(14, 9), facecolor="#000000")
    gs = fig.add_gridspec(3, 1, height_ratios=[3.0, 1.1, 0.18], hspace=0.10)
    ax_price = fig.add_subplot(gs[0])
    ax_ratio = fig.add_subplot(gs[1], sharex=ax_price)
    ax_info = fig.add_subplot(gs[2])

    for ax in (ax_price, ax_ratio, ax_info):
        ax.set_facecolor("#000000")
        ax.tick_params(colors="#cccccc", labelsize=8)
        for spine in ax.spines.values():
            spine.set_color("#333333")

    # ---- Candlesticks ----
    width = 0.6 * (df.index[1] - df.index[0]).total_seconds() / 86400.0 if len(df) > 1 else 0.0005
    for ts, row in df.iterrows():
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        color = "#00e676" if c >= o else "#ff1744"
        ax_price.plot([ts, ts], [l, h], color=color, linewidth=0.9, solid_capstyle="round")
        body_low = min(o, c)
        body_h = abs(c - o) or ((h - l) * 0.02 or 1e-8)
        rect = Rectangle((mdates.date2num(ts) - width / 2, body_low), width, body_h,
                         facecolor=color, edgecolor=color, linewidth=0.5, alpha=0.95)
        ax_price.add_patch(rect)

    ax_price.set_ylabel("Price", color="#cccccc")
    ax_price.set_title(f"{symbol}  |  {timeframe}  |  Rolling Taker Buy/Sell Ratio (window={window})",
                       color="#ffffff", fontsize=12, pad=8)
    ax_price.grid(True, color="#222222", linestyle="--", linewidth=0.5)
    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz=timezone.utc))

    # ---- Rolling ratio panel ----
    ratio_plot = ratio.dropna()
    colors = np.where(ratio_plot.values >= 0, "#00e676", "#ff1744")
    ax_ratio.bar(ratio_plot.index, ratio_plot.values, width=width, color=colors, alpha=0.6, align="center")
    ax_ratio.plot(ratio_plot.index, ratio_plot.values, color="#00bcd4", linewidth=1.1, alpha=0.8)
    ax_ratio.axhline(0, color="#555555", linewidth=0.8)
    ax_ratio.set_ylim(-1.05, 1.05)
    ax_ratio.set_ylabel(f"Buy/Sell ratio\n(roll {window})", color="#cccccc")
    ax_ratio.grid(True, color="#222222", linestyle="--", linewidth=0.5)

    ax_info.axis("off")
    latest = ratio.iloc[-1]
    info_txt = (f"Latest rolling({window}) ratio = {latest:+.3f}   |   "
                f"candles = {len(df)}   |   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    ax_info.text(0.5, 0.5, info_txt, transform=ax_info.transAxes, ha="center", va="center",
                color="#aaaaaa", fontsize=9, family="monospace")

    fig.autofmt_xdate(rotation=30)
    try:
        plt.tight_layout()
    except Exception:
        pass

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, facecolor=fig.get_facecolor(), edgecolor="none", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def send_telegram_photo(image_bytes, caption=""):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("taker_ratio_chart.png", image_bytes, "image/png")}
    data = {"chat_id": CHAT_ID, "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()
    print("Telegram send status:", r.status_code, r.json().get("ok"))


def main():
    parser = argparse.ArgumentParser(description="Binance rolling taker buy/sell ratio -> Telegram")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK, help="candles to fetch")
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW, help="rolling window (candles) for ratio")
    args = parser.parse_args()

    print(f"Fetching {args.symbol} {args.timeframe} last {args.lookback} candles ...")
    df = fetch_binance_klines(args.symbol, args.timeframe, args.lookback)
    print(f"Got {len(df)} candles from {df.index[0]} to {df.index[-1]}")

    ratio, roll_buy, roll_sell = compute_rolling_taker_ratio(df, window=args.window)
    print(f"Latest rolling({args.window}) ratio = {ratio.iloc[-1]:+.3f}")

    print("Rendering chart ...")
    img = plot_chart(df, ratio, args.symbol, args.timeframe, args.window)

    caption = (f"{args.symbol} {args.timeframe} | Rolling Taker Buy/Sell Ratio (window={args.window})\n"
              f"latest={ratio.iloc[-1]:+.3f}")
    print("Sending to Telegram ...")
    send_telegram_photo(img, caption=caption)
    print("Done.")


if __name__ == "__main__":
    main()