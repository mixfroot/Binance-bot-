#!/usr/bin/env python3
import argparse
import io
import warnings
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import numpy as np
import pandas as pd
import requests
from matplotlib.patches import Rectangle

warnings.filterwarnings("ignore")

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_LOOKBACK = 1000
DEFAULT_SS_PERIOD = 6

COLOR_GREEN = "#00e676"
COLOR_RED = "#ff1744"
ALPHA_BRIGHT = 0.95
ALPHA_DIM = 0.35


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
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df = df.set_index("open_time")
    return df[["open", "high", "low", "close", "volume"]].tail(limit)


def super_smoother(series: pd.Series, period: int = 6) -> pd.Series:
    """
    Ehlers 2-pole SuperSmoother filter.
    a1 = exp(-1.414*pi/period)
    b1 = 2*a1*cos(1.414*pi/period)
    c2 = b1 ; c3 = -a1^2 ; c1 = 1 - c2 - c3
    filt[n] = c1*(x[n]+x[n-1])/2 + c2*filt[n-1] + c3*filt[n-2]
    """
    a1 = np.exp(-1.414 * np.pi / period)
    b1 = 2 * a1 * np.cos(1.414 * np.pi / period)
    c2 = b1
    c3 = -a1 * a1
    c1 = 1 - c2 - c3

    x = series.values
    n = len(x)
    filt = np.zeros(n)
    for i in range(n):
        if i < 2:
            filt[i] = x[i]
        else:
            filt[i] = c1 * (x[i] + x[i - 1]) / 2.0 + c2 * filt[i - 1] + c3 * filt[i - 2]
    return pd.Series(filt, index=series.index)


def compute_drift_histogram(df: pd.DataFrame, ss_period: int = 6):
    """
    drift  = close[i] - close[i-1]        (raw candle-to-candle move)
    smooth = SuperSmoother(drift, period) (Ehlers 2-pole, denoised)
    bar colors: green if smooth>=0 else red
    dimming: alpha is DIM if |smooth[i]| < |smooth[i-1]|  (bar shrinking vs prior),
             else BRIGHT (growing or flat)
    """
    drift = df["close"].diff().fillna(0.0)
    smooth = super_smoother(drift, period=ss_period)

    vals = smooth.values
    n = len(vals)
    face_colors = []
    for i in range(n):
        base = COLOR_GREEN if vals[i] >= 0 else COLOR_RED
        if i == 0:
            a = ALPHA_BRIGHT
        else:
            growing = abs(vals[i]) >= abs(vals[i - 1])
            a = ALPHA_BRIGHT if growing else ALPHA_DIM
        face_colors.append(mcolors.to_rgba(base, alpha=a))

    return smooth, face_colors


def plot_chart(df, smooth, face_colors, symbol, timeframe, ss_period):
    fig = plt.figure(figsize=(14, 9), facecolor="#000000")
    gs = fig.add_gridspec(3, 1, height_ratios=[3.0, 1.1, 0.18], hspace=0.10)
    ax_price = fig.add_subplot(gs[0])
    ax_hist = fig.add_subplot(gs[1], sharex=ax_price)
    ax_info = fig.add_subplot(gs[2])

    for ax in (ax_price, ax_hist, ax_info):
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
    ax_price.set_title(f"{symbol}  |  {timeframe}  |  Drift SuperSmoother({ss_period}) Histogram",
                       color="#ffffff", fontsize=12, pad=8)
    ax_price.grid(True, color="#222222", linestyle="--", linewidth=0.5)
    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz=timezone.utc))

    # ---- Drift histogram panel ----
    ax_hist.bar(smooth.index, smooth.values, width=width, color=face_colors, align="center")
    ax_hist.axhline(0, color="#555555", linewidth=0.8)
    ax_hist.set_ylabel(f"Drift SS({ss_period})", color="#cccccc")
    ax_hist.grid(True, color="#222222", linestyle="--", linewidth=0.5)

    ax_info.axis("off")
    latest = smooth.iloc[-1]
    info_txt = (f"Latest SS({ss_period}) drift = {latest:+.4f}   |   "
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
    files = {"photo": ("drift_ss_chart.png", image_bytes, "image/png")}
    data = {"chat_id": CHAT_ID, "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()
    print("Telegram send status:", r.status_code, r.json().get("ok"))


def main():
    parser = argparse.ArgumentParser(description="Binance drift SuperSmoother histogram -> Telegram")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK)
    parser.add_argument("--ss-period", type=int, default=DEFAULT_SS_PERIOD, help="Ehlers SuperSmoother period")
    args = parser.parse_args()

    print(f"Fetching {args.symbol} {args.timeframe} last {args.lookback} candles ...")
    df = fetch_binance_klines(args.symbol, args.timeframe, args.lookback)
    print(f"Got {len(df)} candles from {df.index[0]} to {df.index[-1]}")

    smooth, face_colors = compute_drift_histogram(df, ss_period=args.ss_period)
    print(f"Latest SS({args.ss_period}) drift = {smooth.iloc[-1]:+.4f}")

    print("Rendering chart ...")
    img = plot_chart(df, smooth, face_colors, args.symbol, args.timeframe, args.ss_period)

    caption = (f"{args.symbol} {args.timeframe} | Drift SuperSmoother({args.ss_period}) Histogram\n"
              f"latest={smooth.iloc[-1]:+.4f}")
    print("Sending to Telegram ...")
    send_telegram_photo(img, caption=caption)
    print("Done.")


if __name__ == "__main__":
    main()