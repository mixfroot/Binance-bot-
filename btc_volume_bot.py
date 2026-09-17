 

import argparse
import io
import time
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

# -------------------- Telegram credentials --------------------
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"

# -------------------- Defaults --------------------
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_LOOKBACK = 300          # number of candles to fetch
DEFAULT_GARCH_LOOKBACK = 89     # window used to estimate GARCH (or min obs)
DEFAULT_MULTIPLIER = 1.618


def fetch_binance_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """Fetch recent klines from Binance public API."""
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": min(limit, 1000),  # Binance max 1000
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
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


def fit_garch11(returns: pd.Series, min_obs: int = 16):
    """
    Fit GARCH(1,1) on returns (in percent).
    Returns: omega, alpha, beta, conditional_volatility series (aligned to returns index)
    """
    # scale to percent for numerical stability
    rets = returns.dropna() * 100.0
    if len(rets) < max(min_obs, 20):
        raise ValueError(f"Need at least ~20 observations for GARCH, got {len(rets)}")

    # Use last `min_obs` or full series (user "lookback 16" treated as min window)
    # We fit on the whole fetched series for a stable estimate, but allow short window
    model = arch_model(rets, vol="Garch", p=1, q=1, mean="Zero", rescale=False)
    res = model.fit(disp="off", show_warning=False)

    omega = float(res.params.get("omega", np.nan))
    alpha = float(res.params.get("alpha[1]", np.nan))
    beta = float(res.params.get("beta[1]", np.nan))

    # conditional volatility in percent → convert back to decimal
    cond_vol = res.conditional_volatility / 100.0
    cond_vol.index = rets.index
    return omega, alpha, beta, cond_vol


def plot_chart(
    df: pd.DataFrame,
    cond_vol: pd.Series,
    omega: float,
    alpha: float,
    beta: float,
    multiplier: float,
    symbol: str,
    timeframe: str,
) -> bytes:
    """Create black-background chart with candles + GARCH bands + bottom omega/vol panel."""
    # Align
    common_idx = df.index.intersection(cond_vol.index)
    df = df.loc[common_idx]
    vol = cond_vol.loc[common_idx]

    # Bands: close ± multiplier * σ * close  (relative)
    upper = df["close"] * (1.0 + multiplier * vol)
    lower = df["close"] * (1.0 - multiplier * vol)

    # Figure
    fig = plt.figure(figsize=(14, 9), facecolor="#000000")
    gs = fig.add_gridspec(3, 1, height_ratios=[3.2, 1.0, 0.15], hspace=0.08)

    ax_price = fig.add_subplot(gs[0])
    ax_vol = fig.add_subplot(gs[1], sharex=ax_price)
    ax_info = fig.add_subplot(gs[2])

    for ax in (ax_price, ax_vol, ax_info):
        ax.set_facecolor("#000000")
        ax.tick_params(colors="#cccccc", labelsize=8)
        for spine in ax.spines.values():
            spine.set_color("#333333")

    # ---- Candlesticks (manual