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

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_LOOKBACK = 300
DEFAULT_EMA = 9  # smoothing on the ratio, 0/1 = no smoothing


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


def compute_taker_ratio(df, ema_period=9):
    """
    buy_vol  = taker_buy_base            (aggressive buys)
    sell_vol = volume - taker_buy_base   (aggressive sells)
    ratio    = (buy - sell) / (buy + sell)  -> bounded [-1, +1]
    """
    buy_vol = df["taker_buy_base"]
    sell_vol = df["volume"] - df["taker_buy_base"]
    total = buy_vol + sell_vol
    ratio = np.where(total > 0, (buy_vol - sell_vol) / total, 0.0)
    ratio = pd.Series(ratio, index=df.index)
    ratio_smooth = ratio.ewm(span=max(ema_period, 1), adjust=False).mean() if ema_period > 1 else ratio
    return ratio, ratio_smooth, buy_vol, sell_vol


def plot_chart(df, ratio, ratio_smooth, symbol, timeframe, ema_period):
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
    ax_price.set_title(f"{symbol}  |  {timeframe}  |  Taker Buy/Sell Volume Ratio",
                       color="#ffffff", fontsize=12, pad=8)
    ax_price.grid(True, color="#222222", linestyle="--", linewidth=0.5)
    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz=timezone.utc))

    # ---- Ratio panel: bars colored by sign, + smoothed line ----
    colors = np.where(ratio.values >= 0, "#00e676", "#ff1744")
    ax_ratio.bar(ratio.index, ratio.values, width=width, color=colors, alpha=0.55, align="center")
    if ema_period > 1:
        ax_ratio.plot(ratio_smooth.index, ratio_smooth.values, color="#00bcd4", linewidth=1.4,
                     label=f"EMA({ema_period})")
        ax_ratio.legend(loc="upper left", fontsize=8, facecolor="#111111",
                        edgecolor="#333333", labelcolor="#eeeeee")
    ax_ratio.axhline(0, color="#555555", linewidth=0.8)
    ax_ratio.set_ylim(-1.05, 1.05)
    ax_ratio.set_ylabel("Buy/Sell ratio", color="#cccccc")
    ax_ratio.grid(True, color="#222222", linestyle="--", linewidth=0.5)

    ax_info.axis("off")
    latest = ratio.iloc[-1]
    latest_smooth = ratio_smooth.iloc[-1]
    info_txt = (f"Latest ratio = {latest:+.3f}   |   EMA({ema_period}) = {latest_smooth:+.3f}   |   "
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
    parser = argparse.ArgumentParser(description="Binance taker buy/sell ratio chart -> Telegram")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK)
    parser.add_argument("--ema", type=int, default=DEFAULT_EMA, help="EMA smoothing period on ratio, 0/1=off")
    args = parser.parse_args()

    print(f"Fetching {args.symbol} {args.timeframe} last {args.lookback} candles ...")
    df = fetch_binance_klines(args.symbol, args.timeframe, args.lookback)
    print(f"Got {len(df)} candles from {df.index[0]} to {df.index[-1]}")

    ratio, ratio_smooth, buy_vol, sell_vol = compute_taker_ratio(df, ema_period=args.ema)
    print(f"Latest ratio = {ratio.iloc[-1]:+.3f}  EMA = {ratio_smooth.iloc[-1]:+.3f}")

    print("Rendering chart ...")
    img = plot_chart(df, ratio, ratio_smooth, args.symbol, args.timeframe, args.ema)

    caption = (f"{args.symbol} {args.timeframe} | Taker Buy/Sell Ratio (EMA{args.ema})\n"
              f"latest={ratio.iloc[-1]:+.3f}  ema={ratio_smooth.iloc[-1]:+.3f}")
    print("Sending to Telegram ...")
    send_telegram_photo(img, caption=caption)
    print("Done.")


if __name__ == "__main__":
    main()