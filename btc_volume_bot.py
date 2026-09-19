import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
import numpy as np
import aiohttp
import websockets

# =====================================================================
# CONFIGURATION & TELEGRAM CREDENTIALS
# =====================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "7541584197:AAGZuuVygk54j3P6p_pcXZzplXEmQSpT7bs")
CHAT_ID = os.getenv("CHAT_ID", "6263967739")
SYMBOL = "BTCUSDT"

# Future-Proofed Endpoint Paths
DEPTH_WS_URL = "wss://fstream.binance.com/public/stream?streams=btcusdt@depth20@100ms"
TRADE_WS_URL = "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade"
BINANCE_OI_REST_URL = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL}"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# =====================================================================
# TELEGRAM NOTIFIER MODULE
# =====================================================================
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    async def send_message(self, text: str, parse_mode: str = "Markdown"):
        if not self.token or not self.chat_id:
            logging.warning("Telegram token or chat_id missing. Message skipped.")
            return
        
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        err_txt = await resp.text()
                        logging.error(f"Telegram API Error ({resp.status}): {err_txt}")
        except Exception as e:
            logging.error(f"Failed to send Telegram message: {e}")

# =====================================================================
# MICROSTRUCTURE ENGINE (Event-Driven & O'Hara Microstructure Models)
# =====================================================================
class MicrostructureEngine:
    def __init__(self):
        # Top of book
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.bid_qty = 0.0
        self.ask_qty = 0.0
        self.midpoint = 0.0
        self.spread = 0.0
        
        # Order Book Depth (Top 20)
        self.total_bid_depth_20 = 0.0
        self.total_ask_depth_20 = 0.0
        self.obi = 0.5  # Order Book Imbalance
        
        # Open Interest
        self.current_oi = 0.0
        self.oi_history = []  # list of tuples: (timestamp, oi_value)
        
        # Rolling Data Buffers
        self.trades = []          # list of dicts: {'time', 'price', 'qty', 'is_buyer_maker'}
        self.spread_history = []  # list of dicts: {'time', 'spread'}
        self.ofi_history = []     # list of dicts: {'time', 'ofi'}
        
    def update_depth(self, bids, asks):
        if not bids or not asks:
            return
        try:
            # Parse top level [price, quantity]
            self.best_bid = float(bids[0][0])
            self.bid_qty  = float(bids[0][1])
            self.best_ask = float(asks[0][0])
            self.ask_qty  = float(asks[0][1])
            
            self.midpoint = (self.best_bid + self.best_ask) / 2.0
            self.spread = max(self.best_ask - self.best_bid, 0.0)
            
            # Sum quantities across top 20 levels
            self.total_bid_depth_20 = sum(float(b[1]) for b in bids[:20])
            self.total_ask_depth_20 = sum(float(a[1]) for a in asks[:20])
            
            total_depth = self.total_bid_depth_20 + self.total_ask_depth_20
            if total_depth > 0:
                self.obi = self.total_bid_depth_20 / total_depth
            
            now = time.time()
            self.spread_history.append({'time': now, 'spread': self.spread})
            self._clean_buffers(now)
        except (IndexError, ValueError) as e:
            logging.error(f"Error parsing depth payload: {e}")

    def add_trade(self, price: float, qty: float, is_buyer_maker: bool, ts: float):
        self.trades.append({
            'time': ts,
            'price': price,
            'qty': qty,
            'is_buyer_maker': is_buyer_maker  # True = sell market order, False = buy market order
        })
        self._clean_buffers(ts)

    def update_open_interest(self, oi: float, ts: float):
        self.current_oi = oi
        self.oi_history.append((ts, oi))
        cutoff = ts - 900  # keep 15m history
        self.oi_history = [item for item in self.oi_history if item[0] >= cutoff]

    def _clean_buffers(self, now: float):
        cutoff_5m = now - 300
        self.trades = [t for t in self.trades if t['time'] >= cutoff_5m]
        self.spread_history = [s for s in self.spread_history if s['time'] >= cutoff_5m]

    def compute_features(self):
        now = time.time()
        
        # 1. Rolling 30s VWAP
        trades_30s = [t for t in self.trades if t['time'] >= (now - 30)]
        if trades_30s:
            v_vol = sum(t['qty'] for t in trades_30s)
            vwap_30s = sum(t['price'] * t['qty'] for t in trades_30s) / v_vol if v_vol > 0 else self.midpoint
        else:
            vwap_30s = self.midpoint
            
        # 2. Quote Placement Skew (QS_t)
        spread_safe = max(self.spread, 0.1)
        quote_skew = (self.midpoint - vwap_30s) / spread_safe
        
        # 3. Spread Width Expansion Ratio (S_r)
        spreads_5m = [s['spread'] for s in self.spread_history]
        baseline_spread = np.mean(spreads_5m) if spreads_5m else spread_safe
        baseline_spread = max(baseline_spread, 0.1)
        spread_ratio = self.spread / baseline_spread
        
        # 4. Order Flow Imbalance (OFI) over 30s
        buy_vol_30s = sum(t['qty'] for t in trades_30s if not t['is_buyer_maker'])
        sell_vol_30s = sum(t['qty'] for t in trades_30s if t['is_buyer_maker'])
        net_ofi_30s = buy_vol_30s - sell_vol_30s
        
        self.ofi_history.append({'time': now, 'ofi': net_ofi_30s})
        self.ofi_history = [o for o in self.ofi_history if o['time'] >= (now - 300)]
        all_ofi = [o['ofi'] for o in self.ofi_history]
        
        mean_ofi = np.mean(all_ofi) if all_ofi else 0.0
        std_ofi = np.std(all_ofi) if len(all_ofi) > 5 else 1.0
        std_ofi = max(std_ofi, 0.0001)
        ofi_zscore = (net_ofi_30s - mean_ofi) / std_ofi
        
        # 5. Open Interest Delta (1-minute change)
        oi_1m_delta = 0.0
        if len(self.oi_history) > 1:
            ts_60s_ago = now - 60
            past_oi_tuple = min(self.oi_history, key=lambda x: abs(x[0] - ts_60s_ago))
            oi_1m_delta = self.current_oi - past_oi_tuple[1]

        return {
            "midpoint": self.midpoint,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread": self.spread,
            "vwap_30s": vwap_30s,
            "quote_skew": quote_skew,
            "spread_ratio": spread_ratio,
            "net_ofi_30s": net_ofi_30s,
            "ofi_zscore": ofi_zscore,
            "obi": self.obi,
            "current_oi": self.current_oi,
            "oi_1m_delta": oi_1m_delta
        }

    def classify_regime_and_signal(self, features: dict):
        qs = features['quote_skew']
        sr = features['spread_ratio']
        ofi_z = features['ofi_zscore']
        oi_delta = features['oi_1m_delta']
        
        regime = "Regime D: Discretionary Noise / Range"
        signal = "PASS"
        action_reason = "Order flow is balanced or noise-driven."
        
        # Regime A: Inventory Rebalancing
        if abs(qs) > 1.5 and sr <= 1.3 and oi_delta <= 10.0:
            regime = "Regime A: Inventory Rebalancing (Mean-Reverting)"
            if qs < -1.8 and ofi_z < -0.5:
                signal = "🟢 LONG (Inventory Rebound)"
                action_reason = "MM shifted quotes down symmetrically to offload long inventory. Expect mean-reverting bounce to VWAP."
            elif qs > 1.8 and ofi_z > 0.5:
                signal = "🔴 SHORT (Inventory Exhaustion)"
                action_reason = "MM shifted quotes up symmetrically to cover short inventory. Expect mean-reverting dip to VWAP."

        # Regime B: Toxic Informed Breakout
        elif sr > 1.4 and oi_delta > 15.0:
            regime = "Regime B: Toxic Informed Breakout (Trend Following)"
            if ofi_z > 1.5:
                signal = "🟢 LONG (Informed Buy Momentum)"
                action_reason = "Spiking Open Interest + Asymmetric quote ratcheting indicates toxic informed buy flow. Ride breakout."
            elif ofi_z < -1.5:
                signal = "🔴 SHORT (Informed Sell Momentum)"
                action_reason = "Spiking Open Interest + Asymmetric quote markdown indicates toxic informed sell flow. Ride downside breakout."

        # Regime C: Post-Block Event Recovery
        elif sr > 1.3 and oi_delta <= 0:
            regime = "Regime C: Post-Block Event Uncertainty"
            if ofi_z < -2.0:
                signal = "🟢 LONG (Post-Block Rebound)"
                action_reason = "Large sell block absorbed. Uninformed follow-up trades lower MM's event probability. Expect V-bounce."

        return regime, signal, action_reason

# =====================================================================
# BOT APP & WORKFLOW MANAGERS
# =====================================================================
class BTCFuturesMicrostructureBot:
    def __init__(self):
        self.notifier = TelegramNotifier(BOT_TOKEN, CHAT_ID)
        self.engine = MicrostructureEngine()
        self.running = True
        self.last_heartbeat = 0.0
        self.last_signal_time = 0.0
        self.signal_cooldown = 5.0  # 5-second Telegram alert cooldown

    async def start(self):
        logging.info("Starting BTCUSDT Futures Microstructure Bot (v2)...")
        startup_msg = (
            "🚀 *BTCUSDT Futures Microstructure Bot (v2) Online*\n\n"
            "• *Symbol*: BTCUSDT.P (Binance Futures)\n"
            "• *Architecture*: Instant Event-Driven (Tick Time)\n"
            "• *Endpoints*: Dual Public/Market WS Streams\n"
            "• *Models*: Garman / Stoll / Glosten-Milgrom / Kyle / O'Hara\n"
            "• *Cooldown*: 5 Seconds between alerts\n"
            "• *Heartbeat*: Every 5 minutes"
        )
        await self.notifier.send_message(startup_msg)
        
        await asyncio.gather(
            self.depth_websocket_loop(),
            self.trade_websocket_loop(),
            self.open_interest_polling_loop(),
            self.heartbeat_loop(),
            self.event_driven_signal_loop()
        )

    async def open_interest_polling_loop(self):
        """Polls Binance REST API for Open Interest every 10 seconds."""
        async with aiohttp.ClientSession() as session:
            while self.running:
                try:
                    async with session.get(BINANCE_OI_REST_URL, timeout=5) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            oi_val = float(data.get("openInterest", 0.0))
                            self.engine.update_open_interest(oi_val, time.time())
                except Exception as e:
                    logging.error(f"Error fetching Open Interest: {e}")
                await asyncio.sleep(10)

    async def heartbeat_loop(self):
        """Sends Telegram Heartbeat every 5 minutes (300s)."""
        while self.running:
            await asyncio.sleep(10)
            now = time.time()
            if now - self.last_heartbeat >= 300:
                self.last_heartbeat = now
                feats = self.engine.compute_features()
                regime, _, _ = self.engine.classify_regime_and_signal(feats)
                
                hb_msg = (
                    "🟢 *Heartbeat Check-In* | Bot Operating Smoothly\n\n"
                    f"• *BTC Mid Price*: `${feats['midpoint']:,.2f}`\n"
                    f"• *Spread*: `${feats['spread']:.2f}` (Ratio: `{feats['spread_ratio']:.2f}x`)\n"
                    f"• *Quote Skew*: `{feats['quote_skew']:.2f}`\n"
                    f"• *OFI Z-Score*: `{feats['ofi_zscore']:.2f}`\n"
                    f"• *Order Book Imbalance (B/A)*: `{feats['obi']*100:.1f}%` Bids\n"
                    f"• *Open Interest*: `{feats['current_oi']:,.2f} BTC` (1m Δ: `{feats['oi_1m_delta']:+.2f}`)\n"
                    f"• *Active MM Regime*: `{regime}`"
                )
                await self.notifier.send_message(hb_msg)

    async def event_driven_signal_loop(self):
        """Evaluates ticks continuously and triggers instant alerts with a 5s cooldown."""
        while self.running:
            await asyncio.sleep(0.2)  # Check 5 times per second
            now = time.time()
            
            # Enforce 5-second alert cooldown
            if now - self.last_signal_time < self.signal_cooldown:
                continue
                
            feats = self.engine.compute_features()
            regime, signal, reason = self.engine.classify_regime_and_signal(feats)
            
            if signal != "PASS":
                self.last_signal_time = now  # Reset 5s cooldown timer
                time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                mid = feats['midpoint']
                spread = feats['spread']
                tp_price = feats['vwap_30s'] if "Inventory" in regime else (mid + (spread * 10) if "LONG" in signal else mid - (spread * 10))
                sl_price = mid - (spread * 6) if "LONG" in signal else mid + (spread * 6)
                
                alert_msg = (
                    f"⚡ *INSTANT MICROSTRUCTURE ALERT* ⚡\n\n"
                    f"⏰ *Event Time*: `{time_str}`\n"
                    f"🎯 *Signal*: *{signal}*\n"
                    f"📊 *Active Regime*: `{regime}`\n\n"
                    f"*Market Metrics*:\n"
                    f"• *Price*: `\( {mid:,.2f}` | *VWAP (30s)*: ` \){feats['vwap_30s']:,.2f}`\n"
                    f"• *Spread*: `${spread:.2f}` (Ratio: `{feats['spread_ratio']:.2f}x`)\n"
                    f"• *Quote Skew (QS)*: `{feats['quote_skew']:.2f}`\n"
                    f"• *OFI Z-Score*: `{feats['ofi_zscore']:.2f}`\n"
                    f"• *Open Interest 1m Δ*: `{feats['oi_1m_delta']:+.2f} BTC`\n"
                    f"• *Top 20 Book Imbalance*: `{feats['obi']*100:.1f}% Bids`\n\n"
                    f"💡 *Microstructure Mechanics*:\n_{reason}_\n\n"
                    f"📍 *Execution Guidance*:\n"
                    f"• *Est. Target Price*: `${tp_price:,.2f}`\n"
                    f"• *Est. Stop Loss*: `${sl_price:,.2f}`"
                )
                await self.notifier.send_message(alert_msg)

    async def depth_websocket_loop(self):
        """Streams Depth 20 order book updates from Binance Public WS."""
        while self.running:
            try:
                logging.info(f"Connecting to Depth WS Stream: {DEPTH_WS_URL}")
                async with websockets.connect(DEPTH_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    logging.info("Depth WebSocket Connected!")
                    while self.running:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        payload = data.get("data", {})
                        bids = payload.get("b", [])
                        asks = payload.get("a", [])
                        self.engine.update_depth(bids, asks)
            except Exception as e:
                err_msg = f"⚠️ *Depth WS Warning*: `{e}`. Reconnecting in 5s..."
                logging.error(err_msg)
                await self.notifier.send_message(err_msg)
                await asyncio.sleep(5)

    async def trade_websocket_loop(self):
        """Streams aggregated trade prints from Binance Market WS."""
        while self.running:
            try:
                logging.info(f"Connecting to Trade WS Stream: {TRADE_WS_URL}")
                async with websockets.connect(TRADE_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    logging.info("Trade WebSocket Connected!")
                    while self.running:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        payload = data.get("data", {})
                        price = float(payload.get("p", 0.0))
                        qty = float(payload.get("q", 0.0))
                        is_buyer_maker = bool(payload.get("m", False))
                        ts = float(payload.get("T", time.time() * 1000)) / 1000.0
                        self.engine.add_trade(price, qty, is_buyer_maker, ts)
            except Exception as e:
                err_msg = f"⚠️ *Trade WS Warning*: `{e}`. Reconnecting in 5s..."
                logging.error(err_msg)
                await self.notifier.send_message(err_msg)
                await asyncio.sleep(5)

if __name__ == "__main__":
    bot = BTCFuturesMicrostructureBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")