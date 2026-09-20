#!/usr/bin/env python3
"""
BTCUSDT LOB Microstructure + Thermodynamics Telegram Bot
- Multi-Level Order-Flow Imbalance (MLOFI) with Ridge weights
- Order Book Thermodynamics (Temperature + Delta Entropy)
- Edge-triggered alerts + 60s cooldown
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import aiohttp
import numpy as np
import websockets

# =====================================================================
# CONFIGURATION
# =====================================================================
SYMBOL = "btcusdt"
BOT_TOKEN = os.getenv("BOT_TOKEN", "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs")
CHAT_ID = os.getenv("CHAT_ID", "6263967739")

# Future-proof dual streams
DEPTH_WS = f"wss://fstream.binance.com/public/stream?streams={SYMBOL}@depth20@100ms"
TRADE_WS = f"wss://fstream.binance.com/market/stream?streams={SYMBOL}@aggTrade"

# Paper constants
GRAVITY_G = 0.29581494
ACTIVE_DEPTH_ALPHA_TICKS = 50
WINDOW_DT = 10.0
TICK_SIZE = 0.1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("LOB-Thermo")


# =====================================================================
# TELEGRAM
# =====================================================================
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"

    async def send(self, text: str, parse_mode: str = "HTML"):
        if not self.token or not self.chat_id:
            log.warning("Telegram credentials missing – alert skipped")
            return
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log.error(f"Telegram error {resp.status}: {body}")
        except Exception as e:
            log.error(f"Telegram send failed: {e}")


# =====================================================================
# ENGINE
# =====================================================================
class BinanceLOBThermodynamicsEngine:
    def __init__(self, symbol: str = "btcusdt"):
        self.symbol = symbol.lower()
        self.notifier = TelegramNotifier(BOT_TOKEN, CHAT_ID)

        # Load calibrated Ridge β (or use defaults)
        weights_file = f"mlofi_weights_{self.symbol}.json"
        if os.path.exists(weights_file):
            with open(weights_file) as f:
                data = json.load(f)
            self.ridge_beta = np.array(data["ridge_beta"], dtype=float)
            log.info(f"Loaded Ridge weights from {weights_file}")
        else:
            self.ridge_beta = np.array(
                [2.17, 1.99, 1.85, 1.44, 1.21, 1.09, 1.01, 0.92, 0.89, 1.01],
                dtype=float
            )
            log.warning(f"{weights_file} not found – using default β")

        self.snapshots = deque()   # (timestamp, bids, asks)
        self.trades = deque()      # (timestamp, price, qty, is_buyer_maker)

        self.cooldowns = {"MLOFI": 0.0, "ENTROPY": 0.0}
        self.mlofi_armed = True
        self.mlofi_history = deque(maxlen=600)

        self.running = True
        self.last_eval = 0.0

    def _clean_buffers(self, now: float):
        cutoff = now - WINDOW_DT
        while self.snapshots and self.snapshots[0][0] < cutoff:
            self.snapshots.popleft()
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    def _calc_mlofi(self):
        if len(self.snapshots) < 2:
            return 0.0, 0.0

        _, t0_bids, t0_asks = self.snapshots[0]
        _, t1_bids, t1_asks = self.snapshots[-1]

        e_m = np.zeros(10)
        for m in range(10):
            # Bid ΔW
            if m < len(t1_bids) and m < len(t0_bids):
                b1, r1 = t1_bids[m]
                b0, r0 = t0_bids[m]
                if b1 > b0:
                    delta_w = r1
                elif b1 == b0:
                    delta_w = r1 - r0
                else:
                    delta_w = -r0
            else:
                delta_w = 0.0

            # Ask ΔV
            if m < len(t1_asks) and m < len(t0_asks):
                a1, q1 = t1_asks[m]
                a0, q0 = t0_asks[m]
                if a1 > a0:
                    delta_v = -q0
                elif a1 == a0:
                    delta_v = q1 - q0
                else:
                    delta_v = q1
            else:
                delta_v = 0.0

            e_m[m] = delta_w - delta_v

        raw_score = float(np.dot(e_m, self.ridge_beta))
        self.mlofi_history.append(raw_score)

        if len(self.mlofi_history) > 10:
            mu = np.mean(self.mlofi_history)
            sigma = np.std(self.mlofi_history) + 1e-8
            z = (raw_score - mu) / sigma
        else:
            z = 0.0
        return raw_score, z

    def _calc_thermodynamics(self):
        if not self.snapshots:
            return 0.0, 0.0

        _, bids, asks = self.snapshots[-1]
        if not bids or not asks:
            return 0.0, 0.0

        b1 = bids[0][0]
        a1 = asks[0][0]
        alpha = ACTIVE_DEPTH_ALPHA_TICKS * TICK_SIZE

        pe_bid = sum(s * GRAVITY_G * max(0.0, p - (b1 - alpha)) for p, s in bids[:20])
        pe_ask = sum(s * GRAVITY_G * max(0.0, (a1 + alpha) - p) for p, s in asks[:20])
        total_pe = pe_bid + pe_ask

        delta_ke = 0.0
        for ts, price, qty, is_buyer_maker in self.trades:
            mid = (b1 + a1) / 2.0
            v = abs(price - mid)
            delta_ke += 0.5 * qty * (v ** 2)

        temperature = delta_ke + total_pe
        delta_entropy = delta_ke / max(temperature, 1e-6)
        return temperature, delta_entropy

    async def evaluate_and_alert(self):
        now = time.time()
        if now - self.last_eval < 1.0:
            return
        self.last_eval = now

        self._clean_buffers(now)
        raw_mlofi, z_mlofi = self._calc_mlofi()
        temperature, delta_entropy = self._calc_thermodynamics()

        # MLOFI Impulse (edge + hysteresis)
        if abs(z_mlofi) >= 2.0 and self.mlofi_armed and now > self.cooldowns["MLOFI"]:
            direction = "🟢 BULLISH IMPULSE" if z_mlofi > 0 else "🔴 BEARISH IMPULSE"
            msg = (
                f"<b>{direction} [{self.symbol.upper()} Perp]</b>\n"
                f"• MLOFI Score: {raw_mlofi:.1f}  (Z = {z_mlofi:.2f})\n"
                f"• Action: Heavy net order flow accumulating deep in the LOB.\n"
                f"• Time: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
            )
            await self.notifier.send(msg)
            self.cooldowns["MLOFI"] = now + 60.0
            self.mlofi_armed = False
            log.info(f"MLOFI alert  Z={z_mlofi:.2f}")

        elif abs(z_mlofi) <= 1.0:
            self.mlofi_armed = True

        # Volatility Expansion
        if delta_entropy > 0.35 and now > self.cooldowns["ENTROPY"]:
            msg = (
                f"<b>⚠️ VOLATILITY EXPANSION WARNING [{self.symbol.upper()} Perp]</b>\n"
                f"• Delta Entropy (ΔS): {delta_entropy:.3f}\n"
                f"• Temperature (T): {temperature:.1f}\n"
                f"• Action: Order-book chaos detected – expect local vol expansion.\n"
                f"• Time: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
            )
            await self.notifier.send(msg)
            self.cooldowns["ENTROPY"] = now + 60.0
            log.info(f"Entropy alert  ΔS={delta_entropy:.3f}")

    async def depth_loop(self):
        while self.running:
            try:
                log.info(f"Connecting Depth WS → {DEPTH_WS}")
                async with websockets.connect(DEPTH_WS, ping_interval=20, ping_timeout=10) as ws:
                    log.info("Depth WebSocket connected")
                    async for raw in ws:
                        data = json.loads(raw)
                        payload = data.get("data", {})
                        bids = [[float(p), float(q)] for p, q in payload.get("b", [])]
                        asks = [[float(p), float(q)] for p, q in payload.get("a", [])]
                        now = time.time()
                        self.snapshots.append((now, bids, asks))
                        while len(self.snapshots) > 200:
                            self.snapshots.popleft()
                        await self.evaluate_and_alert()
            except Exception as e:
                log.error(f"Depth WS error: {e}")
                await self.notifier.send(f"⚠️ Depth WS disconnected: {e}")
                await asyncio.sleep(5)

    async def trade_loop(self):
        while self.running:
            try:
                log.info(f"Connecting Trade WS → {TRADE_WS}")
                async with websockets.connect(TRADE_WS, ping_interval=20, ping_timeout=10) as ws:
                    log.info("Trade WebSocket connected")
                    async for raw in ws:
                        data = json.loads(raw)
                        payload = data.get("data", {})
                        price = float(payload.get("p", 0))
                        qty = float(payload.get("q", 0))
                        is_buyer_maker = bool(payload.get("m", False))
                        ts = float(payload.get("T", time.time() * 1000)) / 1000.0
                        self.trades.append((ts, price, qty, is_buyer_maker))
                        while len(self.trades) > 500:
                            self.trades.popleft()
            except Exception as e:
                log.error(f"Trade WS error: {e}")
                await self.notifier.send(f"⚠️ Trade WS disconnected: {e}")
                await asyncio.sleep(5)

    async def heartbeat(self):
        while self.running:
            await asyncio.sleep(300)
            raw, z = self._calc_mlofi()
            temp, ds = self._calc_thermodynamics()
            msg = (
                f"<b>🟢 Heartbeat – {self.symbol.upper()} LOB Engine</b>\n"
                f"• MLOFI Z: {z:.2f}\n"
                f"• Temperature: {temp:.1f}\n"
                f"• ΔEntropy: {ds:.3f}\n"
                f"• Snapshots in window: {len(self.snapshots)}\n"
                f"• Trades in window: {len(self.trades)}"
            )
            await self.notifier.send(msg)

    async def start(self):
        log.info("Starting BTCUSDT LOB Thermodynamics Bot…")
        await self.notifier.send(
            "🚀 <b>BTCUSDT LOB Microstructure + Thermodynamics Bot Online</b>\n"
            "• Engines: MLOFI (Ridge) + Order-Book Thermodynamics\n"
            "• Window: 10 s rolling\n"
            "• Alerts: Edge-triggered + 60 s cooldown"
        )
        await asyncio.gather(
            self.depth_loop(),
            self.trade_loop(),
            self.heartbeat(),
        )


# =====================================================================
if __name__ == "__main__":
    engine = BinanceLOBThermodynamicsEngine(symbol="btcusdt")
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("Bot stopped by user")