#!/usr/bin/env python3
"""
Binance USDT-M Futures — real-time CVD (rolling window) + fastest possible OI polling
"""

import asyncio
import json
import time
from collections import deque

import aiohttp
import websockets

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SYMBOL = "BTCUSDT"
WS_URL = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@aggTrade"
OI_URL = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL}"

CVD_WINDOW_SEC = 30
OI_POLL_INTERVAL_SEC = 1.0
OI_LOOKBACK_SEC = 30

CVD_RATIO_THRESHOLD = 0.15
OI_PCT_THRESHOLD = 0.05
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

_http_session = None


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
        payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
        async with _http_session.post(
            TELEGRAM_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"[TG] alert failed ({resp.status}): {body}")
    except Exception as e:
        print(f"[TG] alert error: {e}")


def maybe_alert(label, streak, cvd_ratio, buy_vol, sell_vol, oi_pct, latest_oi):
    if label in ("warming up...", "neutral"):
        return

    if ALERT_ON_CONFIRM_ONLY and streak < CONFIRM_TICKS:
        return

    msg = (
        f"⚡ <b>{SYMBOL}</b> — {label}\n"
        f"CVD ratio: {cvd_ratio:+.2f} (buy {buy_vol:.2f} / sell {sell_vol:.2f})\n"
        f"OI: {latest_oi:.3f} (Δ{oi_pct:+.3f}%)\n"
        f"Confirmed for {streak} ticks"
    )
    asyncio.create_task(send_telegram_alert(msg))


# --------------------------------------------------------------------------
# CVD: aggTrade websocket
# --------------------------------------------------------------------------
async def aggtrade_listener():
    backoff = 2
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                print(f"[WS] connected: {WS_URL}")
                backoff = 2
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        qty = float(msg["q"])
                        is_buyer_maker = msg["m"]
                        signed_qty = -qty if is_buyer_maker else qty
                        trades.append((now(), signed_qty))
                        prune(trades, CVD_WINDOW_SEC)
                    except Exception as e:
                        print(f"[WS] bad message skipped: {e}")
        except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            print(f"[WS] disconnected: {e} — reconnecting in {backoff}s")
        except Exception as e:
            print(f"[WS] unexpected error: {e} — reconnecting in {backoff}s")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


# --------------------------------------------------------------------------
# OI: REST polling
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
            print(f"[OI] poll error: {e} — backing off {backoff}s")
            if session and not session.closed:
                await session.close()
            session = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        await asyncio.sleep(OI_POLL_INTERVAL_SEC)


# --------------------------------------------------------------------------
# Signal: OI and CVD independent — either can fire on its own
# --------------------------------------------------------------------------
def classify(cvd_ratio, oi_pct):
    labels = []

    if oi_pct is not None:
        if oi_pct >= OI_PCT_THRESHOLD:
            labels.append("OI RISING")
        elif oi_pct <= -OI_PCT_THRESHOLD:
            labels.append("OI FALLING")

    if cvd_ratio is not None:
        if cvd_ratio > CVD_RATIO_THRESHOLD:
            labels.append("CVD BUY SKEW")
        elif cvd_ratio < -CVD_RATIO_THRESHOLD:
            labels.append("CVD SELL SKEW")

    if not labels:
        return "neutral" if oi_pct is not None else "warming up..."

    return " + ".join(labels)


async def monitor_loop():
    last_label = None
    streak = 0
    while True:
        try:
            cvd_ratio, buy_vol, sell_vol = rolling_cvd_ratio()
            oi_pct, oi_abs, latest_oi, ref_oi = oi_change()
            label = classify(cvd_ratio, oi_pct)

            if label in ("warming up...", "neutral"):
                streak, last_label = 0, label
            elif label == last_label:
                streak += 1
            else:
                streak, last_label = 1, label

            tag = f"CONFIRMED x{streak}" if streak >= CONFIRM_TICKS else f"building {streak}/{CONFIRM_TICKS}"
            ts_str = time.strftime("%H:%M:%S")

            if oi_pct is not None:
                print(f"{ts_str} | CVD ratio: {cvd_ratio:+.2f} (buy {buy_vol:.2f} / sell {sell_vol:.2f}) | "
                      f"OI: {latest_oi:.3f} (Δ{oi_pct:+.3f}%) | {label} [{tag}]")
                maybe_alert(label, streak, cvd_ratio, buy_vol, sell_vol, oi_pct, latest_oi)
            else:
                print(f"{ts_str} | CVD ratio: {cvd_ratio:+.2f} | OI: gathering history...")
        except Exception as e:
            print(f"[MONITOR] loop error: {e}")
        await asyncio.sleep(1)


async def main():
    print(f"[BOOT] starting monitor for {SYMBOL} — Telegram alerts -> chat {CHAT_ID}")
    await send_telegram_alert(f"🟢 CVD/OI monitor started for {SYMBOL}")
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