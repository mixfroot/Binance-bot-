#!/usr/bin/env python3
"""
Binance USDT-M Futures
- CVD + OI → LONG/SHORT BUILDING/COVER (very short alerts)
- Signal only fires when current CVD is outside 1 std of its own 30s rolling
  average; outside 2 std gets a 🔥 on the same alert name; inside 1 std = no
  signal, no action.
"""

import asyncio
import json
import statistics
import time
from collections import deque

import aiohttp
import websockets

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SYMBOL = "BTCUSDT"

WS_AGGTRADE = f"wss://fstream.binance.com/market/ws/{SYMBOL.lower()}@aggTrade"
OI_URL      = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL}"

CVD_WINDOW_SEC = 30
OI_POLL_INTERVAL_SEC = 1.0
OI_LOOKBACK_SEC = 30
CONFIRM_TICKS = 3

BOT_TOKEN = "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs"
CHAT_ID = "6263967739"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

ALERT_ON_CONFIRM_ONLY = True

# --------------------------------------------------------------------------
# STATE
# --------------------------------------------------------------------------
trades = deque()
oi_hist = deque()
cvd_hist = deque()   # (ts, cvd_ratio) samples over the last CVD_WINDOW_SEC, for mean/std

_http_session = None
_last_alerted_label = None


def now():
    return time.time()


def prune(dq, window_sec):
    cutoff = now() - window_sec
    while dq and dq[0][0] < cutoff:
        dq.popleft()


def rolling_cvd_ratio():
    prune(trades, CVD_WINDOW_SEC)
    buy_vol = sum(q for _, q in trades if q > 0)
    sell_vol = -sum(q for _, q in trades if q < 0)
    total = buy_vol + sell_vol
    ratio = (buy_vol - sell_vol) / total if total else 0.0
    return ratio, buy_vol, sell_vol


def cvd_stats():
    """Mean and population std of the CVD ratio itself, over the last CVD_WINDOW_SEC."""
    prune(cvd_hist, CVD_WINDOW_SEC)
    values = [r for _, r in cvd_hist]
    if len(values) < 2:
        return None, None
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)
    return mean, stdev


def oi_change(lookback_sec=OI_LOOKBACK_SEC):
    if len(oi_hist) < 2:
        return None, None, None, None
    latest_ts, latest_oi = oi_hist[-1]
    target_ts = latest_ts - lookback_sec
    ref_ts, ref_oi = oi_hist[0]
    for ts, oi in oi_hist:
        if ts <= target_ts:
            ref_ts, ref_oi = ts, oi
        else:
            break
    abs_delta = latest_oi - ref_oi
    pct_delta = (abs_delta / ref_oi * 100) if ref_oi else 0.0
    return pct_delta, abs_delta, latest_oi, ref_oi


# --------------------------------------------------------------------------
# TELEGRAM
# --------------------------------------------------------------------------
async def send_telegram_alert(text):
    global _http_session
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        if _http_session is None or _http_session.closed:
            _http_session = aiohttp.ClientSession()
        payload = {"chat_id": CHAT_ID, "text": text}
        async with _http_session.post(
            TELEGRAM_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"[TG] failed ({resp.status}): {body}")
    except Exception as e:
        print(f"[TG] error: {e}")


def maybe_alert_cvd_oi(label, streak):
    global _last_alerted_label
    if label is None:
        return
    if ALERT_ON_CONFIRM_ONLY and streak < CONFIRM_TICKS:
        return
    if label == _last_alerted_label:
        return
    _last_alerted_label = label
    asyncio.create_task(send_telegram_alert(label))


# --------------------------------------------------------------------------
# WebSocket: aggTrade
# --------------------------------------------------------------------------
async def aggtrade_listener():
    backoff = 2
    while True:
        try:
            async with websockets.connect(WS_AGGTRADE, ping_interval=20, ping_timeout=10) as ws:
                print(f"[WS] aggTrade connected")
                backoff = 2
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        qty = float(msg["q"])
                        is_buyer_maker = msg["m"]
                        signed_qty = -qty if is_buyer_maker else qty
                        trades.append((now(), signed_qty))
                        prune(trades, CVD_WINDOW_SEC)
                    except Exception:
                        pass
        except Exception as e:
            print(f"[WS] aggTrade disconnected: {e}")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


# --------------------------------------------------------------------------
# OI poller
# --------------------------------------------------------------------------
async def oi_poller():
    last_value = None
    session = None
    backoff = 2
    while True:
        try:
            if session is None or session.closed:
                session = aiohttp.ClientSession()
            async with session.get(OI_URL, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                data = await resp.json()
                oi_val = float(data["openInterest"])
                ts = now()
                if oi_val != last_value:
                    oi_hist.append((ts, oi_val))
                    last_value = oi_val
                cutoff = now() - max(OI_LOOKBACK_SEC * 3, 180)
                while oi_hist and oi_hist[0][0] < cutoff:
                    oi_hist.popleft()
            backoff = 2
        except Exception as e:
            print(f"[OI] error: {e}")
            if session and not session.closed:
                await session.close()
            session = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        await asyncio.sleep(OI_POLL_INTERVAL_SEC)


# --------------------------------------------------------------------------
# Signal classification
# --------------------------------------------------------------------------
def classify(cvd_ratio, oi_pct):
    if oi_pct is None:
        return "warming up..."

    oi_up = oi_pct > 0
    oi_down = oi_pct < 0

    if cvd_ratio == 0:
        if oi_up:
            return "LONG BUILDING"
        if oi_down:
            return "SHORT BUILDING"
        return "neutral"

    cvd_buy = cvd_ratio > 0
    cvd_sell = cvd_ratio < 0

    if oi_up and cvd_buy:
        return "LONG BUILDING"
    if oi_down and cvd_buy:
        return "SHORT COVER"
    if oi_up and cvd_sell:
        return "SHORT BUILDING"
    if oi_down and cvd_sell:
        return "LONG COVER"

    return "neutral"


def apply_std_gate(base_label, cvd_ratio, cvd_mean, cvd_std):
    """
    Only let a signal through if the current CVD ratio sits outside 1 std of
    its own 30s rolling average. Outside 2 std -> same label + 🔥.
    Inside 1 std, or not enough data yet -> no signal (None).
    """
    if base_label in ("warming up...", "neutral"):
        return None, 0
    if cvd_mean is None or cvd_std is None or cvd_std <= 0:
        return None, 0

    deviation = abs(cvd_ratio - cvd_mean)

    if deviation >= 2 * cvd_std:
        return f"{base_label} \U0001F525", 2
    if deviation >= 1 * cvd_std:
        return base_label, 1
    return None, 0


# --------------------------------------------------------------------------
# Monitor loop
# --------------------------------------------------------------------------
async def monitor_loop():
    last_gated_label = None
    streak = 0
    while True:
        try:
            cvd_ratio, buy_vol, sell_vol = rolling_cvd_ratio()
            cvd_hist.append((now(), cvd_ratio))
            prune(cvd_hist, CVD_WINDOW_SEC)
            cvd_mean, cvd_std = cvd_stats()

            oi_pct, _, latest_oi, _ = oi_change()
            base_label = classify(cvd_ratio, oi_pct)

            gated_label, tier = apply_std_gate(base_label, cvd_ratio, cvd_mean, cvd_std)

            if gated_label is not None and gated_label == last_gated_label:
                streak += 1
            elif gated_label is not None:
                streak = 1
                last_gated_label = gated_label
            else:
                streak = 0
                last_gated_label = None

            ts_str = time.strftime("%H:%M:%S")
            mean_str = f"{cvd_mean:+.3f}" if cvd_mean is not None else "n/a"
            std_str = f"{cvd_std:.3f}" if cvd_std is not None else "n/a"

            if oi_pct is not None:
                shown = gated_label if gated_label is not None else base_label
                print(
                    f"{ts_str} | CVD {cvd_ratio:+.2f} (avg {mean_str} std {std_str}) "
                    f"| OI {oi_pct:+.3f}% | {shown} [{streak}]"
                )
                if gated_label is not None:
                    maybe_alert_cvd_oi(gated_label, streak)
            else:
                print(f"{ts_str} | CVD {cvd_ratio:+.2f} | OI gathering...")

        except Exception as e:
            print(f"[MONITOR] {e}")
        await asyncio.sleep(1)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
async def main():
    print(f"[BOOT] {SYMBOL} monitor started")
    await send_telegram_alert(f"Monitor started {SYMBOL}")
    try:
        await asyncio.gather(
            aggtrade_listener(),
            oi_poller(),
            monitor_loop(),
        )
    finally:
        if _http_session and not _http_session.closed:
            await _http_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped.")
