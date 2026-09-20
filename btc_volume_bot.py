#!/usr/bin/env python3
"""
BTCUSDT TWI + CVD State Change Bot
- 7-second Trade Weighted Imbalance (TWI)
- 30-second majority vote
- Extra rule: SHORT only allowed when 30s CVD is also negative
- Alerts only on state change
- First 30 seconds = warm-up
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import aiohttp
import websockets

# =====================================================================
# CONFIG
# =====================================================================
SYMBOL = "btcusdt"
BOT_TOKEN = os.getenv("BOT_TOKEN", "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs")
CHAT_ID = os.getenv("CHAT_ID", "6263967739")

TRADE_WS = f"wss://fstream.binance.com/market/stream?streams={SYMBOL}@aggTrade"
DEPTH_WS = f"wss://fstream.binance.com/public/stream?streams={SYMBOL}@depth5@100ms"

TWI_WINDOW = 7.0
STATE_WINDOW = 30.0
WARMUP_SECONDS = 30.0
STATE_COOLDOWN = 5.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("TWI-CVD")


# =====================================================================
# TELEGRAM
# =====================================================================
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"

    async def send(self, text: str):
        if not self.token or not self.chat_id:
            return
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        log.error(f"Telegram error: {await resp.text()}")
        except Exception as e:
            log.error(f"Telegram failed: {e}")


# =====================================================================
# ENGINE
# =====================================================================
class TWICVDStateBot:
    def __init__(self):
        self.notifier = TelegramNotifier(BOT_TOKEN, CHAT_ID)

        self.best_bid = 0.0
        self.best_ask = 0.0
        self.mid = 0.0

        # trades: (timestamp, qty, is_buyer_maker)
        self.trades = deque()

        # history of 7s TWI: (timestamp, twi_value)
        self.twi_history = deque()

        self.start_time = time.time()
        self.current_state = None          # "LONG" or "SHORT"
        self.last_alert_time = 0.0
        self.running = True

    def update_depth(self, bids, asks):
        if not bids or not asks:
            return
        try:
            self.best_bid = float(bids[0][0])
            self.best_ask = float(asks[0][0])
            self.mid = (self.best_bid + self.best_ask) / 2.0
        except Exception:
            pass

    def add_trade(self, price: float, qty: float, is_buyer_maker: bool, ts: float):
        self.trades.append((ts, qty, is_buyer_maker))
        # keep last \~40 seconds of trades (enough for 30s CVD + 7s TWI)
        cutoff = ts - 40.0
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    def compute_7s_twi(self, now: float) -> float:
        cutoff = now - TWI_WINDOW
        buy_vol = 0.0
        sell_vol = 0.0
        for ts, qty, is_buyer_maker in self.trades:
            if ts >= cutoff:
                if is_buyer_maker:
                    sell_vol += qty
                else:
                    buy_vol += qty
        total = buy_vol + sell_vol
        if total < 1e-8:
            return 0.0
        return (buy_vol - sell_vol) / total

    def compute_30s_cvd(self, now: float) -> float:
        """Cumulative Volume Delta over last 30 seconds.
        Positive = more aggressive buying, Negative = more aggressive selling
        """
        cutoff = now - STATE_WINDOW
        cvd = 0.0
        for ts, qty, is_buyer_maker in self.trades:
            if ts >= cutoff:
                if is_buyer_maker:
                    cvd -= qty          # sell aggressor
                else:
                    cvd += qty          # buy aggressor
        return cvd

    def update_state(self, now: float):
        # 1. Current 7-second TWI
        twi = self.compute_7s_twi(now)
        self.twi_history.append((now, twi))

        # keep last \~40s of TWI samples
        cutoff = now - 40.0
        while self.twi_history and self.twi_history[0][0] < cutoff:
            self.twi_history.popleft()

        if len(self.twi_history) < 5:
            return

        # 2. Last 30 seconds of TWI values
        recent_twi = [v for t, v in self.twi_history if t >= now - STATE_WINDOW]
        if len(recent_twi) < 3:
            return

        bid_heavy = sum(1 for v in recent_twi if v > 0.05)
        ask_heavy = sum(1 for v in recent_twi if v < -0.05)

        # 3. 30-second CVD
        cvd_30s = self.compute_30s_cvd(now)

        # 4. Decide candidate state
        new_state = None
        if bid_heavy > ask_heavy:
            new_state = "LONG"
        elif ask_heavy > bid_heavy and cvd_30s < 0:          # ← extra CVD filter only for SHORT
            new_state = "SHORT"
        else:
            return   # mixed or SHORT without negative CVD → stay silent

        # 5. Warm-up
        if now - self.start_time < WARMUP_SECONDS:
            self.current_state = new_state
            return

        # 6. Alert only on real state change
        if new_state != self.current_state and (now - self.last_alert_time) >= STATE_COOLDOWN:
            self.current_state = new_state
            self.last_alert_time = now
            asyncio.create_task(self.send_state_alert(new_state, twi, recent_twi, cvd_30s))

    async def send_state_alert(self, state: str, current_twi: float, recent_twis: list, cvd: float):
        avg_twi = sum(recent_twis) / len(recent_twis)
        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        if state == "LONG":
            emoji = "🟢"
            title = "STATE → LONG (Bid Heavy)"
            reason = "Last 30s of 7s-TWI mostly bid-heavy."
        else:
            emoji = "🔴"
            title = "STATE → SHORT (Ask Heavy + Negative CVD)"
            reason = "Last 30s of 7s-TWI mostly ask-heavy AND 30s CVD is negative."

        msg = (
            f"{emoji} <b>{title}</b>\n\n"
            f"⏰ {time_str}\n"
            f"• Current 7s TWI: <code>{current_twi:+.3f}</code>\n"
            f"• 30s Average TWI: <code>{avg_twi:+.3f}</code>\n"
            f"• 30s CVD: <code>{cvd:+.3f}</code>\n"
            f"• Best Bid / Ask: <code>{self.best_bid:.1f} / {self.best_ask:.1f}</code>\n"
            f"• Mid: <code>{self.mid:.1f}</code>\n\n"
            f"💡 {reason}"
        )
        await self.notifier.send(msg)
        log.info(f"STATE → {state} | 7s TWI={current_twi:+.3f} | 30s CVD={cvd:+.3f}")

    async def depth_loop(self):
        while self.running:
            try:
                log.info("Connecting Depth WS...")
                async with websockets.connect(DEPTH_WS, ping_interval=20, ping_timeout=10) as ws:
                    log.info("Depth connected")
                    async for raw in ws:
                        data = json.loads(raw)
                        payload = data.get("data", {})
                        self.update_depth(payload.get("b", []), payload.get("a", []))
            except Exception as e:
                log.error(f"Depth error: {e}")
                await asyncio.sleep(5)

    async def trade_loop(self):
        while self.running:
            try:
                log.info("Connecting Trade WS...")
                async with websockets.connect(TRADE_WS, ping_interval=20, ping_timeout=10) as ws:
                    log.info("Trade connected")
                    async for raw in ws:
                        data = json.loads(raw)
                        payload = data.get("data", {})
                        price = float(payload.get("p", 0))
                        qty = float(payload.get("q", 0))
                        is_buyer_maker = bool(payload.get("m", False))
                        ts = float(payload.get("T", time.time() * 1000)) / 1000.0
                        self.add_trade(price, qty, is_buyer_maker, ts)
                        self.update_state(time.time())
            except Exception as e:
                log.error(f"Trade error: {e}")
                await asyncio.sleep(5)

    async def start(self):
        log.info("Starting TWI + CVD State Bot...")
        await self.notifier.send(
            "🚀 <b>BTCUSDT TWI + CVD State Bot Online</b>\n\n"
            "• 7-second TWI\n"
            "• 30-second majority vote\n"
            "• SHORT only when 30s CVD is also negative\n"
            "• Alerts only on state change\n"
            "• First 30 seconds = warm-up"
        )
        await asyncio.gather(
            self.depth_loop(),
            self.trade_loop(),
        )


if __name__ == "__main__":
    bot = TWICVDStateBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        log.info("Bot stopped")