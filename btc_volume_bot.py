
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

HTF_INTERVAL = "1h"        # <- higher timeframe for the boxes/profile. e.g. "1h", "4h", "30min", "15min"
N_HTF_BOXES = 4           # number of CLOSED HTF candles shown (plus the still-forming one)
VALUE_AREA_PCT = 0.68
VP_ROWS = 30                # rows per per-candle profile (fewer = thicker, cleaner bars)
PROFILE_WIDTH_FRAC = 0.55   # how far profile bars reach into each box (fraction of box width)
MIN_TRADES_FOR_PROFILE = 20 # skip drawing a profile if a bucket has too few trades

COLORS = {
    "bull": "#26a69a",
    "bear": "#ef5350",
    "buy_vol": "#00e676",   # bright green
    "sell_vol": "#ff1744",  # bright red
    "poc": "#ffd600",       # gold highlight on the POC row only
    "val_vah": "#0d47a1",   # dark navy, deliberately muted
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
    """
    current_bucket_start -> start of the still-forming HTF bucket
    chart_start           -> start of the earliest bucket to display
    bucket_bounds          -> all buckets (closed + forming) as (start, end)
    closed_bucket_bounds   -> only the fully closed ones (safe to build a profile for)
    """
    now = datetime.now(timezone.utc)
    htf_delta = pd.Timedelta(HTF_INTERVAL).to_pytimedelta()
    current_bucket_start = pd.Timestamp(now).floor(HTF_INTERVAL).to_pydatetime()
    chart_start = current_bucket_start - N_HTF_BOXES * htf_delta

    bucket_bounds = [
        (chart_start + i * htf_delta, chart_start + (i + 1) * htf_delta)
        for i in range(N_HTF_BOXES + 1)  # +1 = the forming bucket
    ]
    closed_bucket_bounds = [b for b in bucket_bounds if b[1] <= current_bucket_start]

    return {
        "now": now,
        "htf_delta": htf_delta,
        "chart_start": chart_start,
        "current_bucket_start": current_bucket_start,
        "bucket_bounds": bucket_bounds,
        "closed_bucket_bounds": closed_bucket_bounds,
    }


def fetch_1m_klines(start_ms: int, end_ms: int):
    params = {
        "symbol": SYMBOL, "interval": "1m",
        "startTime": start_ms, "endTime": end_ms, "limit": 1000,
    }
    data, is_futures = _request_json("klines", params)
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    df = df.set_index("open_time")
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
        time.sleep(0.05)

    if not all_trades:
        raise ValueError("No trades")

    df = pd.DataFrame(all_trades)
    df["price"] = df["p"].astype(float)
    df["qty"] = df["q"].astype(float)
    df["T"] = df["T"].astype(int)
    df["is_buyer_maker"] = df["m"].astype(bool)
    df = df[(df["T"] >= start_ms) & (df["T"] <= end_ms)]
    return df


def build_volume_profile(trades: pd.DataFrame, n_rows: int = VP_ROWS):
    prices = trades["price"].values
    qtys = trades["qty"].values
    is_bm = trades["is_buyer_maker"].values

    p_min, p_max = prices.min(), prices.max()
    if p_max <= p_min:
        p_max = p_min + 1.0

    edges = np.linspace(p_min, p_max, n_rows + 1)
    centers = (edges[:-1] + edges[1:]) / 2
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
    poc_price = centers[poc_idx]

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


def build_bucket_profiles(trades: pd.DataFrame, closed_bucket_bounds):
    profiles = {}
    t_ms = trades["T"].values
    for start, end in closed_bucket_bounds:
        s_ms = int(start.timestamp() * 1000)
        e_ms = int(end.timestamp() * 1000)
        sub = trades[(t_ms >= s_ms) & (t_ms < e_ms)]
        if len(sub) >= MIN_TRADES_FOR_PROFILE:
            profiles[start] = build_volume_profile(sub, VP_ROWS)
    return profiles


def get_htf_candles(df_1m: pd.DataFrame):
    return df_1m.resample(HTF_INTERVAL, label="left", closed="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna()


def plot_chart(df_1m, htf, bucket_profiles, windows, market_label):
    fig, ax = plt.subplots(figsize=(16, 9), facecolor="#0d0d0d")
    ax.set_facecolor("#0d0d0d")
    for sp in ax.spines.values():
        sp.set_color("#333")

    # ---- 1m candlesticks ----
    w = 0.00055
    for ts, row in df_1m.iterrows():
        o, h, l, c = row.open, row.high, row.low, row.close
        color = COLORS["bull"] if c >= o else COLORS["bear"]
        ax.plot([ts, ts], [l, h], color=color, lw=0.9, solid_capstyle="round", zorder=4)
        body_h = abs(c - o) or (h - l) * 0.04 or 0.5
        ax.add_patch(Rectangle(
            (mdates.date2num(ts) - w / 2, min(o, c)), w, body_h,
            facecolor=color, edgecolor=color, lw=0.5, alpha=0.95, zorder=5
        ))

    htf_delta = windows["htf_delta"]
    box_width_num = mdates.date2num(windows["chart_start"] + htf_delta) - mdates.date2num(windows["chart_start"])

    # ---- HTF boxes + per-candle volume profile ----
    for ts, row in htf.iterrows():
        start_num = mdates.date2num(ts)
        end_num = mdates.date2num(ts + htf_delta)
        bullish = row.close >= row.open
        body_color = COLORS["bull"] if bullish else COLORS["bear"]

        # thin high-low outline (unfilled - no heavy background wash)
        ax.add_patch(Rectangle(
            (start_num, row.low), end_num - start_num, row.high - row.low,
            facecolor="none", edgecolor=body_color, lw=0.8, alpha=0.35, zorder=1
        ))
        # open-close body
        body_low = min(row.open, row.close)
        body_h = abs(row.close - row.open) or (row.high - row.low) * 0.08
        ax.add_patch(Rectangle(
            (start_num, body_low), end_num - start_num, body_h,
            facecolor=body_color, edgecolor=body_color, lw=1.2, alpha=0.15, zorder=2
        ))

        vp = bucket_profiles.get(ts)
        if vp is None:
            continue

        max_vol = vp["total"].max() or 1.0
        max_bar = box_width_num * PROFILE_WIDTH_FRAC
        for i, (edge_low, buy, sell, total) in enumerate(
            zip(vp["edges"][:-1], vp["buy"], vp["sell"], vp["total"])
        ):
            if total <= 0:
                continue
            sell_w = sell / max_vol * max_bar
            buy_w = buy / max_vol * max_bar
            if sell_w > 0:
                ax.add_patch(Rectangle(
                    (end_num - sell_w, edge_low), sell_w, vp["height"],
                    facecolor=COLORS["sell_vol"], edgecolor="none", alpha=0.55, zorder=3
                ))
            if buy_w > 0:
                ax.add_patch(Rectangle(
                    (end_num - sell_w - buy_w, edge_low), buy_w, vp["height"],
                    facecolor=COLORS["buy_vol"], edgecolor="none", alpha=0.55, zorder=3
                ))
            if i == vp["poc_idx"]:
                ax.add_patch(Rectangle(
                    (end_num - sell_w - buy_w, edge_low), sell_w + buy_w, vp["height"],
                    facecolor=COLORS["poc"], edgecolor="none", alpha=0.30, zorder=3.5
                ))

        # VAL / VAH - darker, confined to this box only
        ax.hlines(vp["val"], start_num, end_num, colors=COLORS["val_vah"], lw=1.1, ls="--", alpha=0.9, zorder=4)
        ax.hlines(vp["vah"], start_num, end_num, colors=COLORS["val_vah"], lw=1.1, ls="--", alpha=0.9, zorder=4)

    ax.set_ylabel("Price (USDT)", color="#ccc", fontsize=10)
    ax.tick_params(colors="#aaa", labelsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=timezone.utc))
    ax.grid(True, color="#1a1a1a", ls="--", lw=0.5)

    title = (f"{market_label}  |  1m Chart + {HTF_INTERVAL} HTF Boxes & Volume Profile "
             f"(VA {int(VALUE_AREA_PCT * 100)}%)")
    ax.set_title(title, color="#fff", fontsize=11, pad=9)

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
    files = {"photo": ("btc_htf_vp.png", photo, "image/png")}
    data = {"chat_id": CHAT_ID, "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=60)
    r.raise_for_status()
    print("Telegram OK:", r.json().get("ok"))


def main():
    print("Calculating time windows ...")
    w = get_time_windows()

    start_ms = int(w["chart_start"].timestamp() * 1000)
    end_ms = int(w["now"].timestamp() * 1000)

    print("Fetching 1m candles ...")
    df_1m, is_futures = fetch_1m_klines(start_ms, end_ms)
    market = "BTCUSDT.P" if is_futures else "BTCUSDT (Spot)"
    print(f"Got {len(df_1m)} x 1m candles [{market}]")

    htf = get_htf_candles(df_1m)
    print(f"HTF {HTF_INTERVAL} candles: {len(htf)}")

    trades_start_ms = int(w["chart_start"].timestamp() * 1000)
    trades_end_ms = int(w["current_bucket_start"].timestamp() * 1000) - 1

    print("Fetching trade tape for closed HTF buckets ...")
    trades = fetch_agg_trades(trades_start_ms, trades_end_ms)
    print(f"Trades: {len(trades)}")

    print("Building per-candle volume profiles ...")
    bucket_profiles = build_bucket_profiles(trades, w["closed_bucket_bounds"])
    print(f"Profiles built for {len(bucket_profiles)} / {len(w['closed_bucket_bounds'])} closed buckets")

    print("Rendering ...")
    img = plot_chart(df_1m, htf, bucket_profiles, w, market)

    closed = w["closed_bucket_bounds"]
    last_closed = closed[-1][0] if closed else None
    if last_closed in bucket_profiles:
        vp = bucket_profiles[last_closed]
        caption = (
            f"{market} | 1m + {HTF_INTERVAL} HTF Boxes & Volume Profile (VA {int(VALUE_AREA_PCT*100)}%)\n"
            f"Last closed {HTF_INTERVAL}: {last_closed.strftime('%H:%M')} UTC\n"
            f"POC {vp['poc_price']:.1f} | VAH {vp['vah']:.1f} | VAL {vp['val']:.1f}"
        )
    else:
        caption = f"{market} | 1m + {HTF_INTERVAL} HTF Boxes & Volume Profile"

    send_telegram(img, caption)
    print("Done.")


if __name__ == "__main__":
    main()