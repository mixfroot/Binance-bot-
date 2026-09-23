#!/usr/bin/env python3
"""
BTCUSDT - 1m candlesticks with a per-1H (or other HTF) volume profile
drawn inside each closed bucket, anchored to the left edge. No HTF
candles are drawn - just thin boundary lines marking each bucket.
"""

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
from matplotlib.patches import Rectangle

# -------------------- Telegram --------------------
BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID   = "6263967739"

# -------------------- Config --------------------
SYMBOL = "BTCUSDT"

HTF_INTERVAL = "1h"        # bucket size the profile is built on: "1h", "4h", "30min", ...
LOOKBACK_HOURS = 24        # how far back to cover with profiled buckets (excludes forming one)
VALUE_AREA_PCT = 0.68
VP_ROWS = 30                # price bins per bucket's profile
PROFILE_WIDTH_FRAC = 0.55   # how far profile bars reach across each bucket (fraction of bucket width)
NODE_GAP_FRAC = 0.85        # bar height as fraction of bin height -> leaves a gap, no merging
MIN_TRADES_FOR_PROFILE = 20
SHOW_FORMING_BUCKET_LINE = True   # just a marker line for the still-forming bucket, no profile

COLORS = {
    "bull": "#26a69a",
    "bear": "#ef5350",
    "buy_vol": "#00e676",
    "sell_vol": "#ff1744",
    "poc": "#ffd600",
    "value_area": "#12212e",   # dim shaded band, no lines
    "bucket_edge": "#555",     # thin boundary marker between buckets
}

API_CANDIDATES = [
    ("https://fapi.binance.com", True,  "/fapi/v1/klines", "/fapi/v1/aggTrades"),
    ("https://fapi1.binance.com", True, "/fapi/v1/klines", "/fapi/v1/aggTrades"),
    ("https://data-api.binance.vision", False, "/api/v3/klines", "/api/v3/aggTrades"),
]


def _request_json(kind: str, params: dict):
    last_err = None
    for base, is_futures, kpath, tpath in API_CANDIDATES:
        url = base + (kpath if kind == "klines" else tpath)
        try:
            r = requests.get(url, params=params, timeout=25)
            if r.status_code == 200:
                return r.json(), is_futures
            last_err = f"{r.status_code} from {url}"
        except Exception as e:
            last_err = str(e)
    raise RuntimeError(f"All endpoints failed: {last_err}")


def get_time_windows():
    now = datetime.now(timezone.utc)
    htf_delta = pd.Timedelta(HTF_INTERVAL).to_pytimedelta()
    current_bucket_start = pd.Timestamp(now).floor(HTF_INTERVAL).to_pydatetime()

    n_closed = max(1, int(pd.Timedelta(hours=LOOKBACK_HOURS) / htf_delta))
    chart_start = current_bucket_start - n_closed * htf_delta

    closed_bucket_bounds = [
        (chart_start + i * htf_delta, chart_start + (i + 1) * htf_delta)
        for i in range(n_closed)
    ]

    return {
        "now": now,
        "htf_delta": htf_delta,
        "chart_start": chart_start,
        "current_bucket_start": current_bucket_start,
        "closed_bucket_bounds": closed_bucket_bounds,
    }


def fetch_1m_klines(start_ms: int, end_ms: int):
    """Paginated: LOOKBACK_HOURS worth of 1m candles can exceed 1000 rows."""
    frames = []
    cursor = start_ms
    is_futures = True
    while cursor < end_ms:
        params = {
            "symbol": SYMBOL, "interval": "1m",
            "startTime": cursor, "endTime": end_ms, "limit": 1000,
        }
        data, is_futures = _request_json("klines", params)
        if not data:
            break
        frames.extend(data)
        last_open = int(data[-1][0])
        cursor = last_open + 60_000
        if len(data) < 1000:
            break
        time.sleep(0.04)

    if not frames:
        raise ValueError("No klines returned")

    df = pd.DataFrame(frames, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    df = df.set_index("open_time").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df[["open", "high", "low", "close", "volume"]], is_futures


def fetch_agg_trades(start_ms: int, end_ms: int) -> pd.DataFrame:
    all_trades = []
    from_id = None
    for _ in range(400):
        params = {"symbol": SYMBOL, "limit": 1000}
        if from_id is None:
            params["startTime"] = start_ms
            params["endTime"] = end_ms
        else:
            params["fromId"] = from_id
        try:
            batch, _ = _request_json("aggTrades", params)
        except Exception:
            break
        if not batch:
            break
        all_trades.extend(batch)
        last = batch[-1]
        if last["T"] >= end_ms or len(batch) < 1000:
            break
        from_id = last["a"] + 1
        time.sleep(0.04)

    if not all_trades:
        return pd.DataFrame(columns=["price", "qty", "T", "is_buyer_maker"])

    df = pd.DataFrame(all_trades)
    df["price"] = df["p"].astype(float)
    df["qty"] = df["q"].astype(float)
    df["T"] = df["T"].astype(int)
    df["is_buyer_maker"] = df["m"].astype(bool)
    df = df[(df["T"] >= start_ms) & (df["T"] <= end_ms)]
    return df[["price", "qty", "T", "is_buyer_maker"]]


def build_volume_profile(trades: pd.DataFrame, n_rows: int = VP_ROWS):
    prices = trades["price"].values
    qtys = trades["qty"].values
    is_bm = trades["is_buyer_maker"].values

    p_min, p_max = prices.min(), prices.max()
    if p_max <= p_min:
        p_max = p_min + 1.0

    edges = np.linspace(p_min, p_max, n_rows + 1)
    height = edges[1] - edges[0]

    total = np.zeros(n_rows)
    buy = np.zeros(n_rows)
    sell = np.zeros(n_rows)

    idxs = np.clip(np.digitize(prices, edges) - 1, 0, n_rows - 1)
    for i, q, maker in zip(idxs, qtys, is_bm):
        total[i] += q
        if maker:
            sell[i] += q
        else:
            buy[i] += q

    poc_idx = int(np.argmax(total))
    poc_price = (edges[poc_idx] + edges[poc_idx + 1]) / 2

    target = total.sum() * VALUE_AREA_PCT
    order = np.argsort(total)[::-1]
    cum = 0.0
    va_idx = []
    for idx in order:
        cum += total[idx]
        va_idx.append(idx)
        if cum >= target:
            break
    va_idx = sorted(va_idx)
    val = edges[va_idx[0]]
    vah = edges[va_idx[-1] + 1]

    return {
        "edges": edges, "height": height, "total": total, "buy": buy, "sell": sell,
        "poc_price": poc_price, "poc_idx": poc_idx, "vah": vah, "val": val,
        "total_volume": float(total.sum()),
    }


def build_bucket_profiles(closed_bucket_bounds):
    profiles = {}
    for start, end in closed_bucket_bounds:
        s_ms = int(start.timestamp() * 1000)
        e_ms = int(end.timestamp() * 1000) - 1
        trades = fetch_agg_trades(s_ms, e_ms)
        if len(trades) >= MIN_TRADES_FOR_PROFILE:
            profiles[start] = build_volume_profile(trades, VP_ROWS)
    return profiles


def plot_chart(df_1m, bucket_profiles, windows, market_label):
    fig, ax = plt.subplots(figsize=(16, 9), facecolor="#0d0d0d")
    ax.set_facecolor("#0d0d0d")
    for sp in ax.spines.values():
        sp.set_color("#333")

    # ---- 1m candlesticks (the only candles drawn) ----
    w = 0.00055
    for ts, row in df_1m.iterrows():
        o, h, l, c = row.open, row.high, row.low, row.close
        color = COLORS["bull"] if c >= o else COLORS["bear"]
        ax.plot([ts, ts], [l, h], color=color, lw=0.9, solid_capstyle="round", zorder=6)
        body_h = abs(c - o) or (h - l) * 0.04 or 0.5
        ax.add_patch(Rectangle(
            (mdates.date2num(ts) - w / 2, min(o, c)), w, body_h,
            facecolor=color, edgecolor=color, lw=0.5, alpha=0.95, zorder=6
        ))

    htf_delta = windows["htf_delta"]
    bucket_width_num = mdates.date2num(windows["chart_start"] + htf_delta) - mdates.date2num(windows["chart_start"])
    current_bucket_start = windows["current_bucket_start"]

    # ---- bucket boundary markers + per-bucket volume profile ----
    for start, end in windows["closed_bucket_bounds"]:
        start_num = mdates.date2num(start)
        end_num = mdates.date2num(end)

        ax.axvline(start_num, color=COLORS["bucket_edge"], lw=0.6, alpha=0.4, zorder=1)

        vp = bucket_profiles.get(start)
        if vp is None:
            continue

        # dim value-area shading, no lines
        ax.add_patch(Rectangle(
            (start_num, vp["val"]), bucket_width_num, vp["vah"] - vp["val"],
            facecolor=COLORS["value_area"], edgecolor="none", alpha=0.55, zorder=2
        ))

        max_vol = vp["total"].max() or 1.0
        max_bar = bucket_width_num * PROFILE_WIDTH_FRAC
        gap = vp["height"] * (1 - NODE_GAP_FRAC) / 2

        for i, edge_low in enumerate(vp["edges"][:-1]):
            total_i = vp["total"][i]
            if total_i <= 0:
                continue
            buy_i, sell_i = vp["buy"][i], vp["sell"][i]
            row_low = edge_low + gap
            row_h = vp["height"] * NODE_GAP_FRAC

            # anchored to the LEFT edge of the bucket, growing rightward
            sell_w = sell_i / max_vol * max_bar
            buy_w = buy_i / max_vol * max_bar

            if sell_w > 0:
                ax.add_patch(Rectangle(
                    (start_num, row_low), sell_w, row_h,
                    facecolor=COLORS["sell_vol"], edgecolor="none", alpha=0.85, zorder=3
                ))
            if buy_w > 0:
                ax.add_patch(Rectangle(
                    (start_num + sell_w, row_low), buy_w, row_h,
                    facecolor=COLORS["buy_vol"], edgecolor="none", alpha=0.85, zorder=3
                ))
            if i == vp["poc_idx"]:
                ax.add_patch(Rectangle(
                    (start_num, row_low), sell_w + buy_w, row_h,
                    facecolor=COLORS["poc"], edgecolor="none", alpha=0.35, zorder=3.5
                ))

    if SHOW_FORMING_BUCKET_LINE:
        ax.axvline(mdates.date2num(current_bucket_start), color=COLORS["bucket_edge"],
                    lw=0.8, alpha=0.6, ls="--", zorder=1)

    ax.set_ylabel("Price (USDT)", color="#ccc", fontsize=10)
    ax.tick_params(colors="#aaa", labelsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=timezone.utc))
    ax.grid(True, color="#1a1a1a", ls="--", lw=0.5)

    ax.set_title(
        f"{market_label}  |  1m Candles + {HTF_INTERVAL} Volume Profile (VA {int(VALUE_AREA_PCT*100)}%)",
        color="#fff", fontsize=11, pad=9
    )

    closed = windows["closed_bucket_bounds"]
    last_closed = closed[-1][0] if closed else None
    if last_closed is not None and last_closed in bucket_profiles:
        vp = bucket_profiles[last_closed]
        info = (f"Last closed {HTF_INTERVAL}  ->  POC {vp['poc_price']:.1f}   "
                f"VAH {vp['vah']:.1f}   VAL {vp['val']:.1f}   "
                f"Total {vp['total_volume']:.1f} BTC   |   "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC")
        fig.text(0.5, 0.012, info, ha="center", color="#aaa", fontsize=9, family="monospace")

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def send_telegram(photo: bytes, caption: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("btc_1m_vp.png", photo, "image/png")}
    data = {"chat_id": CHAT_ID, "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()
    print("Telegram OK:", r.json().get("ok"))


def main():
    print("Calculating time windows ...")
    w = get_time_windows()
    print(f"Covering {len(w['closed_bucket_bounds'])} closed {HTF_INTERVAL} buckets "
          f"({w['chart_start']} -> {w['current_bucket_start']}) for the profile")

    start_ms = int(w["chart_start"].timestamp() * 1000)
    end_ms = int(w["now"].timestamp() * 1000)

    print("Fetching 1m candles ...")
    df_1m, is_futures = fetch_1m_klines(start_ms, end_ms)
    market = "BTCUSDT.P" if is_futures else "BTCUSDT (Spot)"
    print(f"Got {len(df_1m)} x 1m candles [{market}]")

    print("Building volume profile for every closed bucket (one tape fetch per bucket) ...")
    bucket_profiles = build_bucket_profiles(w["closed_bucket_bounds"])
    print(f"Profiles built for {len(bucket_profiles)} / {len(w['closed_bucket_bounds'])} closed buckets")

    print("Rendering ...")
    img = plot_chart(df_1m, bucket_profiles, w, market)

    closed = w["closed_bucket_bounds"]
    last_closed = closed[-1][0] if closed else None
    if last_closed in bucket_profiles:
        vp = bucket_profiles[last_closed]
        caption = (
            f"{market} | 1m Candles + {HTF_INTERVAL} Volume Profile (VA {int(VALUE_AREA_PCT*100)}%)\n"
            f"Covering {len(bucket_profiles)} closed buckets, last: {last_closed.strftime('%H:%M')} UTC\n"
            f"POC {vp['poc_price']:.1f} | VAH {vp['vah']:.1f} | VAL {vp['val']:.1f}"
        )
    else:
        caption = f"{market} | 1m Candles + {HTF_INTERVAL} Volume Profile"

    send_telegram(img, caption)
    print("Done.")


if __name__ == "__main__":
    main()