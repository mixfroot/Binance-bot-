#!/usr/bin/env python3
"""
Fetch Binance OHLCV (custom timeframe & lookback), rolling GARCH(1,1),
plot candlestick + GARCH bands (black bg) + omega / vol panel,
send PNG to Telegram.

Rolling logic:
  - First `garch_lookback` candles (default 16) are used for the initial fit.
  - Then the window slides forward one bar at a time (fixed-size rolling window).
"""

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
from arch import arch_model
from matplotlib.patches import Rectangle

warnings.filterwarnings("ignore")

# -------------------- Telegram credentials --------------------
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"

# -------------------- Defaults --------------------
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_LOOKBACK = 400       # number of candles to fetch
DEFAULT_GARCH_LOOKBACK = 88     # rolling window size
DEFAULT_MULTIPLIER = 1.618


def fetch_binance_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """Fetch recent klines from Binance public API (vision endpoint for broader access)."""
    urls = [
        "https://data-api.binance.vision/api/v3/klines",
        "https://api.binance.com/api/v3/klines",
        "https://api1.binance.com/api/v3/klines",
    ]
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": min(limit, 1000),
    }
    last_err = None
    data = None
    for url in urls:
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            last_err = e
            continue
    if data is None:
        raise RuntimeError(f"All Binance endpoints failed: {last_err}")
    if not data:
        raise ValueError("Empty response from Binance")

    df = pd.DataFrame(
        data,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore",
        ],
    )
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    df = df.set_index("open_time")
    return df[["open", "high", "low", "close", "volume"]]


def rolling_garch11(returns: pd.Series, window: int = 16):
    """
    True rolling GARCH(1,1).

    - First `window` returns are used for the initial estimation.
    - Then the window slides forward one bar at a time.
    - At each step we keep only the *latest* conditional volatility
      and the fitted parameters (ω, α, β) of that window.
    """
    rets = returns.dropna() * 100.0          # percent scale for numerical stability
    n = len(rets)

    if n < window:
        raise ValueError(f"Need at least {window} returns, got {n}")

    vol_list = [np.nan] * n
    omega_list = [np.nan] * n
    alpha_list = [np.nan] * n
    beta_list = [np.nan] * n

    last_omega = last_alpha = last_beta = np.nan

    for i in range(window - 1, n):
        # fixed-size rolling window ending at i
        window_rets = rets.iloc[i - window + 1 : i + 1]

        try:
            model = arch_model(
                window_rets,
                vol="Garch",
                p=1,
                q=1,
                mean="Zero",
                rescale=False,
            )
            res = model.fit(disp="off", show_warning=False, options={"maxiter": 200})

            omega = float(res.params.get("omega", np.nan))
            alpha = float(res.params.get("alpha[1]", np.nan))
            beta = float(res.params.get("beta[1]", np.nan))

            # latest conditional vol of this window (in decimal)
            cond_vol_pct = float(res.conditional_volatility.iloc[-1])
            vol = cond_vol_pct / 100.0

            vol_list[i] = vol
            omega_list[i] = omega
            alpha_list[i] = alpha
            beta_list[i] = beta

            last_omega, last_alpha, last_beta = omega, alpha, beta

        except Exception:
            # if a window fails to converge, keep previous values
            if i > 0 and not np.isnan(vol_list[i - 1]):
                vol_list[i] = vol_list[i - 1]
                omega_list[i] = omega_list[i - 1]
                alpha_list[i] = alpha_list[i - 1]
                beta_list[i] = beta_list[i - 1]

    idx = rets.index
    cond_vol = pd.Series(vol_list, index=idx)
    omega_s = pd.Series(omega_list, index=idx)
    alpha_s = pd.Series(alpha_list, index=idx)
    beta_s = pd.Series(beta_list, index=idx)

    return cond_vol, omega_s, alpha_s, beta_s, last_omega, last_alpha, last_beta


def plot_chart(
    df: pd.DataFrame,
    cond_vol: pd.Series,
    omega_s: pd.Series,
    last_omega: float,
    last_alpha: float,
    last_beta: float,
    multiplier: float,
    symbol: str,
    timeframe: str,
    window: int,
) -> bytes:
    """Black-background chart with candles + rolling GARCH bands + bottom panels."""
    common_idx = df.index.intersection(cond_vol.dropna().index)
    df_plot = df.loc[common_idx]
    vol = cond_vol.loc[common_idx]
    omegas = omega_s.loc[common_idx]

    upper = df_plot["close"] * (1.0 + multiplier * vol)
    lower = df_plot["close"] * (1.0 - multiplier * vol)

    fig = plt.figure(figsize=(14, 10), facecolor="#000000")
    gs = fig.add_gridspec(4, 1, height_ratios=[3.0, 0.9, 0.7, 0.18], hspace=0.10)

    ax_price = fig.add_subplot(gs[0])
    ax_vol = fig.add_subplot(gs[1], sharex=ax_price)
    ax_omega = fig.add_subplot(gs[2], sharex=ax_price)
    ax_info = fig.add_subplot(gs[3])

    for ax in (ax_price, ax_vol, ax_omega, ax_info):
        ax.set_facecolor("#000000")
        ax.tick_params(colors="#cccccc", labelsize=8)
        for spine in ax.spines.values():
            spine.set_color("#333333")

    # ---- Candlesticks ----
    width = 0.6 * (df_plot.index[1] - df_plot.index[0]).total_seconds() / 86400.0 if len(df_plot) > 1 else 0.0005
    for ts, row in df_plot.iterrows():
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        color = "#00e676" if c >= o else "#ff1744"
        ax_price.plot([ts, ts], [l, h], color=color, linewidth=0.9, solid_capstyle="round")
        body_low = min(o, c)
        body_h = abs(c - o)
        if body_h < 1e-12:
            body_h = (h - l) * 0.02 or 1e-8
        rect = Rectangle(
            (mdates.date2num(ts) - width / 2, body_low),
            width,
            body_h,
            facecolor=color,
            edgecolor=color,
            linewidth=0.5,
            alpha=0.95,
        )
        ax_price.add_patch(rect)

    # GARCH bands
    ax_price.plot(df_plot.index, upper, color="#00bcd4", linewidth=1.4,
                  label=f"Upper ({multiplier}×σ)", alpha=0.9)
    ax_price.plot(df_plot.index, lower, color="#ff9800", linewidth=1.4,
                  label=f"Lower ({multiplier}×σ)", alpha=0.9)
    ax_price.fill_between(df_plot.index, lower, upper, color="#00bcd4", alpha=0.08)

    ax_price.set_ylabel("Price", color="#cccccc")
    ax_price.legend(loc="upper left", fontsize=8, facecolor="#111111",
                    edgecolor="#333333", labelcolor="#eeeeee")
    ax_price.set_title(
        f"{symbol}  |  {timeframe}  |  Rolling GARCH(1,1)  window={window}  mult={multiplier}",
        color="#ffffff", fontsize=12, pad=8,
    )
    ax_price.grid(True, color="#222222", linestyle="--", linewidth=0.5)
    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz=timezone.utc))

    # ---- Conditional volatility panel ----
    ax_vol.plot(vol.index, vol * 100, color="#e040fb", linewidth=1.3, label="Cond. Vol (%)")
    ax_vol.fill_between(vol.index, 0, vol * 100, color="#e040fb", alpha=0.15)
    ax_vol.set_ylabel("σ (%)", color="#cccccc")
    ax_vol.legend(loc="upper left", fontsize=8, facecolor="#111111",
                  edgecolor="#333333", labelcolor="#eeeeee")
    ax_vol.grid(True, color="#222222", linestyle="--", linewidth=0.5)

    # ---- Omega (ω) panel ----
    ax_omega.plot(omegas.index, omegas, color="#ffeb3b", linewidth=1.2, label="ω (omega)")
    ax_omega.set_ylabel("ω", color="#cccccc")
    ax_omega.legend(loc="upper left", fontsize=8, facecolor="#111111",
                    edgecolor="#333333", labelcolor="#eeeeee")
    ax_omega.grid(True, color="#222222", linestyle="--", linewidth=0.5)

    # ---- Info bar ----
    ax_info.axis("off")
    info_txt = (
        f"Latest →  ω = {last_omega:.6e}   |   α = {last_alpha:.4f}   |   β = {last_beta:.4f}   |   "
        f"α+β = {last_alpha + last_beta:.4f}   |   rolling window = {window}   |   "
        f"candles = {len(df_plot)}   |   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    ax_info.text(
        0.5, 0.5, info_txt,
        transform=ax_info.transAxes,
        ha="center", va="center",
        color="#aaaaaa", fontsize=9,
        family="monospace",
    )

    fig.autofmt_xdate(rotation=30)
    try:
        plt.tight_layout()
    except Exception:
        pass

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, facecolor=fig.get_facecolor(),
                edgecolor="none", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def send_telegram_photo(image_bytes: bytes, caption: str = "") -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("garch_chart.png", image_bytes, "image/png")}
    data = {"chat_id": CHAT_ID, "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()
    print("Telegram send status:", r.status_code, r.json().get("ok"))


def main():
    parser = argparse.ArgumentParser(description="Binance Rolling GARCH chart → Telegram")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Binance symbol e.g. BTCUSDT")
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME, help="Candle interval e.g. 1m, 5m, 15m, 1h")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK, help="Number of candles to fetch")
    parser.add_argument("--garch-lookback", type=int, default=DEFAULT_GARCH_LOOKBACK,
                        help="Rolling window size (first N candles used, then slides) default 16")
    parser.add_argument("--multiplier", type=float, default=DEFAULT_MULTIPLIER,
                        help="Band multiplier (price ± mult * σ * price)")
    args = parser.parse_args()

    print(f"Fetching {args.symbol} {args.timeframe} last {args.lookback} candles ...")
    df = fetch_binance_klines(args.symbol, args.timeframe, args.lookback)
    print(f"Got {len(df)} candles from {df.index[0]} to {df.index[-1]}")

    returns = np.log(df["close"]).diff().dropna()

    print(f"Running rolling GARCH(1,1) with window = {args.garch_lookback} ...")
    cond_vol, omega_s, alpha_s, beta_s, last_omega, last_alpha, last_beta = rolling_garch11(
        returns, window=args.garch_lookback
    )
    print(f"Latest → ω={last_omega:.6e}  α={last_alpha:.4f}  β={last_beta:.4f}")

    print("Rendering chart ...")
    img = plot_chart(
        df, cond_vol, omega_s,
        last_omega, last_alpha, last_beta,
        multiplier=args.multiplier,
        symbol=args.symbol,
        timeframe=args.timeframe,
        window=args.garch_lookback,
    )

    caption = (
        f"{args.symbol} {args.timeframe} | Rolling GARCH(1,1) window={args.garch_lookback} ×{args.multiplier}\n"
        f"ω={last_omega:.4e}  α={last_alpha:.3f}  β={last_beta:.3f}"
    )
    print("Sending to Telegram ...")
    send_telegram_photo(img, caption=caption)
    print("Done.")


if __name__ == "__main__":
    main()