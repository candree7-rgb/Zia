import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import db_export
import telegram_alerts

from config import (
    CATEGORY, ACCOUNT_TYPE, QUOTE, LEVERAGE, RISK_PCT,
    ENTRY_EXPIRATION_MIN, ENTRY_TOO_FAR_PCT, ENTRY_TRIGGER_BUFFER_PCT, ENTRY_LIMIT_PRICE_OFFSET_PCT,
    ENTRY_EXPIRATION_PRICE_PCT,
    TP_SPLITS, TP_SPLITS_AUTO, DCA_QTY_MULTS, INITIAL_SL_PCT, FALLBACK_TP_PCT,
    MOVE_SL_TO_BE_ON_TP1, BE_BUFFER_PCT,
    FOLLOW_TP_ENABLED, FOLLOW_TP_BUFFER_PCT, MAX_SL_DISTANCE_PCT,
    TRAIL_AFTER_TP_INDEX, TRAIL_DISTANCE_PCT, TRAIL_ACTIVATE_ON_TP,
    DRY_RUN
)

def _opposite_side(side: str) -> str:
    return "Sell" if side == "Buy" else "Buy"

def _pos_side(side: str) -> str:
    return "Long" if side == "Buy" else "Short"

class TradeEngine:
    def __init__(self, bybit, state: dict, logger):
        self.bybit = bybit
        self.state = state
        self.log = logger
        self._instrument_cache: Dict[str, Dict[str, float]] = {}  # symbol -> rules
        self._cache_ttl = 300  # 5 min cache
        self._cache_times: Dict[str, float] = {}
        self._last_stats_day: str = ""

    # ---------- startup sync ----------
    def startup_sync(self) -> None:
        """Check for orphaned positions at startup and log warnings."""
        if DRY_RUN:
            self.log.info("DRY_RUN: Skipping startup sync")
            return

        try:
            # Get all open positions
            positions = self.bybit.positions(CATEGORY, "")  # Empty = all symbols
            open_positions = [p for p in positions if float(p.get("size") or 0) > 0]

            if not open_positions:
                self.log.info("✅ Startup sync: No open positions found")
                return

            # Check which positions are tracked in state
            tracked_symbols = set()
            for tr in self.state.get("open_trades", {}).values():
                if tr.get("status") in ("pending", "open"):
                    tracked_symbols.add(tr.get("symbol"))

            orphaned = []
            for pos in open_positions:
                symbol = pos.get("symbol")
                size = float(pos.get("size") or 0)
                side = pos.get("side")
                entry = float(pos.get("avgPrice") or 0)
                pnl = float(pos.get("unrealisedPnl") or 0)

                if symbol not in tracked_symbols:
                    orphaned.append(f"{symbol} ({side} {size} @ {entry}, PnL: {pnl:.2f})")

            if orphaned:
                self.log.warning(f"⚠️ Orphaned positions (not tracked by bot):")
                for o in orphaned:
                    self.log.warning(f"   → {o}")
                self.log.warning("   These positions will NOT be managed automatically!")
            else:
                self.log.info(f"✅ Startup sync: {len(open_positions)} position(s), all tracked")

            # Log performance report at startup
            if self.state.get("trade_history"):
                self.log_performance_report()

        except Exception as e:
            self.log.warning(f"Startup sync failed: {e}")

    def log_daily_stats(self) -> None:
        """Log daily trade statistics once per day."""
        from state import utc_day_key
        today = utc_day_key()

        if self._last_stats_day == today:
            return  # Already logged today

        # Get yesterday's stats
        yesterday_trades = 0
        for tr in self.state.get("open_trades", {}).values():
            placed_ts = tr.get("placed_ts") or 0
            if placed_ts:
                trade_day = utc_day_key(placed_ts)
                if trade_day == self._last_stats_day:
                    yesterday_trades += 1

        if self._last_stats_day and yesterday_trades > 0:
            daily_count = self.state.get("daily_counts", {}).get(self._last_stats_day, 0)
            self.log.info(f"📊 Stats for {self._last_stats_day}: {daily_count} trades placed")

            # Log full performance report once per day
            self.log_performance_report()

        self._last_stats_day = today

    # ---------- precision helpers ----------
    @staticmethod
    def _floor_to_step(x: float, step: float) -> float:
        if step <= 0:
            return x
        return math.floor(x / step) * step

    def _get_instrument_rules(self, symbol: str) -> Dict[str, float]:
        """Get instrument rules with caching to avoid repeated API calls."""
        now = time.time()
        cached_time = self._cache_times.get(symbol, 0)

        if symbol in self._instrument_cache and (now - cached_time) < self._cache_ttl:
            return self._instrument_cache[symbol]

        info = self.bybit.instruments_info(CATEGORY, symbol)
        lot = info.get("lotSizeFilter") or {}
        price_filter = info.get("priceFilter") or {}
        qty_step = float(lot.get("qtyStep") or lot.get("basePrecision") or "0.000001")
        min_qty  = float(lot.get("minOrderQty") or "0")
        tick_size = float(price_filter.get("tickSize") or "0.0001")

        rules = {"qty_step": qty_step, "min_qty": min_qty, "tick_size": tick_size}
        self._instrument_cache[symbol] = rules
        self._cache_times[symbol] = now
        return rules

    def _round_price(self, price: float, tick_size: float) -> float:
        """Round price to valid tick size."""
        if tick_size <= 0:
            return price
        return round(round(price / tick_size) * tick_size, 10)

    def _round_qty(self, qty: float, qty_step: float, min_qty: float) -> float:
        """Round qty down to valid step and ensure min qty."""
        qty = self._floor_to_step(qty, qty_step)
        if qty < min_qty:
            qty = min_qty
        return float(f"{qty:.10f}")

    def calc_base_qty(self, symbol: str, entry_price: float) -> float:
        # Risk model: margin = equity * RISK_PCT; notional = margin * LEVERAGE; qty = notional / price
        equity = self.bybit.wallet_equity(ACCOUNT_TYPE)
        margin = equity * (RISK_PCT / 100.0)
        notional = margin * LEVERAGE
        qty = notional / entry_price

        rules = self._get_instrument_rules(symbol)
        return self._round_qty(qty, rules["qty_step"], rules["min_qty"])

    # ---------- entry gatekeepers ----------
    def _too_far(self, side: str, last: float, trigger: float, tp1: float = None) -> bool:
        """
        Check if price has moved too far past trigger to enter.

        If tp1 is provided, use it as threshold (skip if TP1 already reached).
        But only apply TP1 check if price has already crossed entry in profit direction.
        """
        if tp1 and tp1 > 0:
            if side == "Sell":
                # SHORT: Entry is high, TP1 is below entry
                # Price approaches from ABOVE (falling to entry) = normal
                # Price approaches from BELOW (rising to entry) = also valid, don't skip
                if last < trigger:
                    # Price is below entry - will rise to trigger entry
                    # Don't skip - the trade hasn't started yet
                    return False
                # Price is at/above entry - check if it dropped past TP1
                # (This shouldn't normally happen for a fresh signal)
                return last <= tp1
            else:
                # LONG: Entry is low, TP1 is above entry
                # Price approaches from BELOW (rising to entry) = normal
                # Price approaches from ABOVE (falling to entry) = also valid, don't skip
                if last > trigger:
                    # Price is above entry - will fall to trigger entry
                    # Don't skip - the trade hasn't started yet
                    return False
                # Price is at/below entry - check if it rose past TP1
                return last >= tp1

        # Fallback to percentage-based check if no TP1
        if side == "Sell":
            return last <= trigger * (1 - ENTRY_TOO_FAR_PCT / 100.0)
        return last >= trigger * (1 + ENTRY_TOO_FAR_PCT / 100.0)

    def _beyond_expiry_price(self, side: str, last: float, trigger: float) -> bool:
        """
        Skip if price moved too far past entry in profit direction.
        Only applies when price has already crossed entry level.
        """
        if ENTRY_EXPIRATION_PRICE_PCT <= 0:
            return False

        if side == "Sell":
            # SHORT: only check if price is at/below entry (crossed into profit zone)
            if last < trigger:
                # Price is below entry - will rise to trigger, don't skip
                return False
            return last <= trigger * (1 - ENTRY_EXPIRATION_PRICE_PCT / 100.0)
        else:
            # LONG: only check if price is at/above entry (crossed into profit zone)
            if last > trigger:
                # Price is above entry - will fall to trigger, don't skip
                return False
            return last >= trigger * (1 + ENTRY_EXPIRATION_PRICE_PCT / 100.0)

    def _trigger_direction(self, last: float, trigger: float) -> int:
        # Bybit: 1=rises to trigger, 2=falls to trigger
        if last < trigger:
            return 1
        if last > trigger:
            return 2
        return 1

    # ---------- order / position helpers ----------
    def _position(self, symbol: str) -> Optional[Dict[str, Any]]:
        plist = self.bybit.positions(CATEGORY, symbol)
        for p in plist:
            if p.get("symbol") == symbol:
                return p
        return None

    def position_size_avg(self, symbol: str) -> tuple[float, float]:
        p = self._position(symbol)
        if not p:
            return 0.0, 0.0
        size = float(p.get("size") or 0)
        avg  = float(p.get("avgPrice") or 0)
        return size, avg

    # ---------- core actions ----------
    def place_conditional_entry(self, sig: Dict[str, Any], trade_id: str) -> Optional[str]:
        symbol = sig["symbol"]
        side   = "Sell" if sig["side"] == "sell" else "Buy"
        trigger = float(sig["trigger"])

        # ensure leverage set
        try:
            if not DRY_RUN:
                self.bybit.set_leverage(CATEGORY, symbol, LEVERAGE)
        except Exception as e:
            self.log.warning(f"set_leverage failed for {symbol}: {e}")

        last = self.bybit.last_price(CATEGORY, symbol)
        tp_prices = sig.get("tp_prices") or []
        tp1 = float(tp_prices[0]) if tp_prices else None
        if self._too_far(side, last, trigger, tp1):
            self.log.info(f"SKIP {symbol} – too far past trigger (last={last}, trigger={trigger}, tp1={tp1})")
            return None
        if self._beyond_expiry_price(side, last, trigger):
            self.log.info(f"SKIP {symbol} – beyond expiry-price rule (last={last}, trigger={trigger})")
            return None

        # Check max SL distance - skip if SL is too far from entry
        if MAX_SL_DISTANCE_PCT > 0:
            sl_price = sig.get("sl_price")
            if sl_price:
                sl_distance_pct = abs(float(sl_price) - trigger) / trigger * 100
                if sl_distance_pct > MAX_SL_DISTANCE_PCT:
                    self.log.info(f"SKIP {symbol} – SL too far from entry ({sl_distance_pct:.1f}% > {MAX_SL_DISTANCE_PCT}%)")
                    return None

        # Get instrument rules for price/qty rounding
        rules = self._get_instrument_rules(symbol)
        tick_size = rules["tick_size"]

        # buffer: slightly earlier trigger if desired
        trigger_adj = trigger * (1 - ENTRY_TRIGGER_BUFFER_PCT / 100.0) if side == "Buy" else trigger * (1 + ENTRY_TRIGGER_BUFFER_PCT / 100.0)
        trigger_adj = self._round_price(trigger_adj, tick_size)

        # We use LIMIT conditional by default for exact pricing; optionally offset the limit to improve fill odds
        limit_price = trigger
        if ENTRY_LIMIT_PRICE_OFFSET_PCT != 0:
            off = abs(ENTRY_LIMIT_PRICE_OFFSET_PCT) / 100.0
            if side == "Sell":
                limit_price = trigger * (1 + off)
            else:
                limit_price = trigger * (1 - off)
        limit_price = self._round_price(limit_price, tick_size)

        qty = self.calc_base_qty(symbol, trigger)
        td = self._trigger_direction(last, trigger_adj)

        body = {
            "category": CATEGORY,
            "symbol": symbol,
            "side": side,
            "orderType": "Limit",
            "qty": f"{qty:.10f}",
            "price": f"{limit_price:.10f}",
            "timeInForce": "GTC",
            "triggerDirection": td,
            "triggerPrice": f"{trigger_adj:.10f}",
            "triggerBy": "LastPrice",
            "reduceOnly": False,
            "closeOnTrigger": False,
            "orderLinkId": trade_id,
        }

        if DRY_RUN:
            self.log.info(f"DRY_RUN ENTRY {symbol}: {body}")
            return "DRY_RUN"

        try:
            self.log.debug(f"Bybit place_order request: {body}")
            resp = self.bybit.place_order(body)
            self.log.debug(f"Bybit place_order response: {resp}")
            oid = (resp.get("result") or {}).get("orderId")
            if oid:
                self.log.info(f"✅ Bybit order created: {symbol} orderId={oid}")
            else:
                self.log.warning(f"⚠️ Bybit response has no orderId: {resp}")
            return oid
        except Exception as e:
            self.log.error(f"❌ Bybit place_order FAILED for {symbol}: {e}")
            return None

    def cancel_entry(self, symbol: str, order_id: str) -> None:
        body = {"category": CATEGORY, "symbol": symbol, "orderId": order_id}
        if DRY_RUN:
            self.log.info(f"DRY_RUN cancel entry: {body}")
            return
        self.bybit.cancel_order(body)

    def _generate_fallback_tps(self, entry: float, side: str, tick_size: float) -> List[float]:
        """Generate fallback TP prices based on % distance from entry."""
        tps = []
        for pct in FALLBACK_TP_PCT:
            if side == "Sell":  # SHORT: TPs are below entry
                tp = entry * (1 - pct / 100.0)
            else:  # LONG: TPs are above entry
                tp = entry * (1 + pct / 100.0)
            tps.append(self._round_price(tp, tick_size))
        return tps

    def place_post_entry_orders(self, trade: Dict[str, Any]) -> None:
        """Places SL + TP ladder + DCA conditionals after entry is filled.

        OPTIMIZED: Gets position size first, then places SL + TPs + DCAs in parallel.
        """
        symbol = trade["symbol"]
        side   = trade["order_side"]  # Buy/Sell
        entry  = float(trade["entry_price"])
        base_qty = float(trade["base_qty"])

        # Get instrument rules for price/qty rounding (cached)
        rules = self._get_instrument_rules(symbol)
        tick_size = rules["tick_size"]
        qty_step = rules["qty_step"]
        min_qty = rules["min_qty"]

        # ---- Get position size FIRST (needed for TP quantities) ----
        size, _avg = self.position_size_avg(symbol)
        if size <= 0:
            # sometimes position size appears a bit later; retry via main loop
            self.log.warning(f"No position size yet for {symbol}; will retry post-orders")
            return

        # ---- Calculate SL price ----
        # Priority: 1) Signal SL, 2) DCA+4%, 3) INITIAL_SL_PCT fallback
        signal_sl = trade.get("sl_price")
        dca_prices: List[float] = trade.get("dca_prices") or []

        if signal_sl:
            # Use SL from signal
            sl_price = self._round_price(float(signal_sl), tick_size)
            self.log.info(f"📍 SL from signal: {sl_price}")
        elif dca_prices:
            # No SL in signal but has DCA: set SL at DCA + 4%
            dca_buffer_pct = 0.04  # 4% above/below DCA
            first_dca = float(dca_prices[0])
            if side == "Sell":  # Short: SL above DCA
                sl_price = first_dca * (1 + dca_buffer_pct)
            else:  # Long: SL below DCA
                sl_price = first_dca * (1 - dca_buffer_pct)
            sl_price = self._round_price(sl_price, tick_size)
            self.log.info(f"📍 SL at DCA+4%: {sl_price} (DCA1: {first_dca})")
        else:
            # No SL and no DCA: use fallback percentage
            sl_pct = INITIAL_SL_PCT / 100.0
            sl_price = entry * (1 + sl_pct) if side == "Sell" else entry * (1 - sl_pct)
            sl_price = self._round_price(sl_price, tick_size)
            self.log.info(f"📍 SL at {INITIAL_SL_PCT}% from entry: {sl_price}")

        tp_prices: List[float] = trade.get("tp_prices") or []

        # Fallback TPs if signal has none
        if not tp_prices:
            tp_prices = self._generate_fallback_tps(entry, side, tick_size)
            self.log.info(f"Using fallback TPs for {symbol}: {tp_prices}")

        # Calculate TP splits - either from config or auto-calculate based on TP count
        if TP_SPLITS_AUTO:
            # Auto-calculate equal splits based on number of TPs
            tp_count = len(tp_prices)
            if tp_count > 0:
                split_pct = 100.0 / tp_count
                splits = [split_pct] * tp_count
                self.log.info(f"Auto-calculated TP splits for {tp_count} TPs: {splits}")
            else:
                splits = TP_SPLITS
        else:
            splits = trade.get("tp_splits") or TP_SPLITS

        # Store original TP percentages for recalculation after DCA
        tp_percentages = []
        for tp in tp_prices:
            if side == "Buy":  # Long: TP is above entry
                pct = (float(tp) / entry - 1)
            else:  # Short: TP is below entry
                pct = (1 - float(tp) / entry)
            tp_percentages.append(pct)
        trade["tp_percentages"] = tp_percentages

        # Prepare all orders first, then place in parallel
        tp_orders = []
        dca_orders = []

        # Build TP orders
        tp_to_place = min(len(tp_prices), len(splits))
        self.log.info(f"📊 Placing {tp_to_place} TPs (splits: {splits[:tp_to_place]}, remaining {100-sum(splits[:tp_to_place]):.0f}% runner)")
        for idx in range(tp_to_place):
            pct = float(splits[idx])
            if pct <= 0:
                continue
            tp = self._round_price(float(tp_prices[idx]), tick_size)
            qty = self._round_qty(size * (pct / 100.0), qty_step, min_qty)
            tp_orders.append({
                "idx": idx,
                "body": {
                    "category": CATEGORY,
                    "symbol": symbol,
                    "side": _opposite_side(side),
                    "orderType": "Limit",
                    "qty": f"{qty}",
                    "price": f"{tp:.10f}",
                    "timeInForce": "GTC",
                    "reduceOnly": True,
                    "closeOnTrigger": False,
                    "orderLinkId": f"{trade['id']}:TP{idx+1}",
                }
            })

        # Build DCA orders (dca_prices already loaded above for SL calculation)
        dca_to_place = min(len(dca_prices), len(DCA_QTY_MULTS))
        self.log.info(f"📊 Placing {dca_to_place} DCAs (mults: {DCA_QTY_MULTS[:dca_to_place]})")
        last = self.bybit.last_price(CATEGORY, symbol)

        for j in range(1, dca_to_place + 1):
            price = self._round_price(float(dca_prices[j-1]), tick_size)
            mult = DCA_QTY_MULTS[j-1]
            qty = self._round_qty(base_qty * mult, qty_step, min_qty)
            td = self._trigger_direction(last, price)
            dca_orders.append({
                "idx": j,
                "body": {
                    "category": CATEGORY,
                    "symbol": symbol,
                    "side": side,
                    "orderType": "Limit",
                    "qty": f"{qty}",
                    "price": f"{price:.10f}",
                    "timeInForce": "GTC",
                    "triggerDirection": td,
                    "triggerPrice": f"{price:.10f}",
                    "triggerBy": "LastPrice",
                    "reduceOnly": False,
                    "closeOnTrigger": False,
                    "orderLinkId": f"{trade['id']}:DCA{j}",
                }
            })

        # Place SL + TPs + DCAs in parallel for speed
        ts_body = {
            "category": CATEGORY,
            "symbol": symbol,
            "positionIdx": 0,
            "stopLoss": f"{sl_price:.10f}",
            "tpslMode": "Full",
        }

        if DRY_RUN:
            self.log.info(f"DRY_RUN set SL: {ts_body}")
            for o in tp_orders:
                self.log.info(f"DRY_RUN TP{o['idx']+1}: {o['body']}")
                trade.setdefault("tp_order_ids", {})[str(o['idx']+1)] = f"DRY_TP{o['idx']+1}"
                if o['idx'] == 0:
                    trade["tp1_order_id"] = f"DRY_TP1"
            for o in dca_orders:
                self.log.info(f"DRY_RUN DCA{o['idx']}: {o['body']}")
        else:
            # Build list of all operations to run in parallel
            all_orders = [("TP", o) for o in tp_orders] + [("DCA", o) for o in dca_orders]

            def place_order(order_tuple):
                order_type, o = order_tuple
                resp = self.bybit.place_order(o["body"])
                return order_type, o["idx"], (resp.get("result") or {}).get("orderId")

            def set_sl():
                self.bybit.set_trading_stop(ts_body)
                return "SL", 0, None

            # Run SL + all orders in parallel (max 6 workers: 1 SL + 3 TPs + 2 DCAs)
            with ThreadPoolExecutor(max_workers=6) as executor:
                # Submit SL first (highest priority)
                sl_future = executor.submit(set_sl)
                # Submit all TP and DCA orders
                order_futures = [executor.submit(place_order, o) for o in all_orders]

                # Wait for SL first
                try:
                    sl_future.result()
                    self.log.info(f"✅ SL set successfully")
                except Exception as e:
                    self.log.warning(f"SL setting failed: {e}")

                # Process order results
                for future in as_completed(order_futures):
                    try:
                        order_type, idx, oid = future.result()
                        if order_type == "TP":
                            trade.setdefault("tp_order_ids", {})[str(idx+1)] = oid
                            if idx == 0:
                                trade["tp1_order_id"] = oid
                    except Exception as e:
                        self.log.warning(f"Order placement failed: {e}")

        trade["post_orders_placed"] = True

    def _recalculate_tps_after_dca(self, trade: Dict[str, Any]) -> None:
        """Recalculates and replaces unfilled TPs after DCA fill.

        Uses Bybit's avgPrice (automatically calculated) and original TP percentages
        to place new TPs at correct distances from the new average entry.
        """
        symbol = trade["symbol"]
        side = trade["order_side"]

        # Get new average entry from Bybit position
        size, new_avg = self.position_size_avg(symbol)
        if size <= 0 or new_avg <= 0:
            self.log.warning(f"Cannot recalculate TPs: no position data for {symbol}")
            return

        # Get original TP percentages
        tp_percentages = trade.get("tp_percentages", [])
        if not tp_percentages:
            self.log.debug(f"No TP percentages stored, cannot recalculate")
            return

        # Get unfilled TPs
        filled_tps = trade.get("tp_fills_list", [])
        tp_order_ids = trade.get("tp_order_ids", {})
        splits = trade.get("tp_splits") or TP_SPLITS

        # Get instrument rules
        rules = self._get_instrument_rules(symbol)
        tick_size = rules["tick_size"]
        qty_step = rules["qty_step"]
        min_qty = rules["min_qty"]

        # Track version for unique orderLinkId (avoids conflicts)
        tp_version = trade.get("tp_version", 1) + 1
        trade["tp_version"] = tp_version

        old_entry = trade.get("entry_price", new_avg)
        self.log.info(f"🔄 Recalculating TPs: entry {old_entry:.4f} → avg {new_avg:.4f}")

        # Calculate new TP prices and replace unfilled TPs
        new_tp_prices = []
        for i, pct in enumerate(tp_percentages):
            tp_num = i + 1

            # Calculate new TP price based on original percentage
            if side == "Buy":  # Long
                new_tp_price = new_avg * (1 + pct)
            else:  # Short
                new_tp_price = new_avg * (1 - pct)
            new_tp_prices.append(new_tp_price)

            # Skip already filled TPs
            if tp_num in filled_tps:
                continue

            # Skip if no split for this TP
            if i >= len(splits) or splits[i] <= 0:
                continue

            # Cancel existing TP order
            old_order_id = tp_order_ids.get(str(tp_num))
            if old_order_id:
                try:
                    self.bybit.cancel_order({
                        "category": CATEGORY,
                        "symbol": symbol,
                        "orderId": old_order_id
                    })
                    self.log.debug(f"Cancelled old TP{tp_num} order {old_order_id}")
                except Exception as e:
                    self.log.debug(f"Failed to cancel TP{tp_num}: {e}")

            # Place new TP order
            new_tp = self._round_price(new_tp_price, tick_size)
            qty = self._round_qty(size * (splits[i] / 100.0), qty_step, min_qty)

            body = {
                "category": CATEGORY,
                "symbol": symbol,
                "side": _opposite_side(side),
                "orderType": "Limit",
                "qty": f"{qty}",
                "price": f"{new_tp:.10f}",
                "timeInForce": "GTC",
                "reduceOnly": True,
                "closeOnTrigger": False,
                "orderLinkId": f"{trade['id']}:TP{tp_num}v{tp_version}",
            }

            if DRY_RUN:
                self.log.info(f"DRY_RUN new TP{tp_num}: {new_tp}")
                continue

            try:
                resp = self.bybit.place_order(body)
                new_oid = (resp.get("result") or {}).get("orderId")
                if new_oid:
                    tp_order_ids[str(tp_num)] = new_oid
                    if tp_num == 1:
                        trade["tp1_order_id"] = new_oid
                    self.log.info(f"   TP{tp_num}: {new_tp:.4f} (was {tp_percentages[i]*100:+.2f}% from entry)")
            except Exception as e:
                self.log.warning(f"Failed to place new TP{tp_num}: {e}")

        # Update trade's TP prices and average entry
        trade["tp_prices"] = new_tp_prices
        trade["avg_entry"] = new_avg

    # ---------- reactive events ----------
    def on_execution(self, ev: Dict[str, Any]) -> None:
        link = ev.get("orderLinkId") or ev.get("orderLinkID") or ""
        if not link:
            return

        # Entry filled?
        if link in self.state.get("open_trades", {}):
            tr = self.state["open_trades"][link]
            if tr.get("status") == "pending":
                # some execution payloads contain execPrice/lastPrice
                exec_price = ev.get("execPrice") or ev.get("price") or ev.get("lastPrice") or tr.get("trigger")
                try:
                    tr["entry_price"] = float(exec_price)
                except Exception:
                    pass
                tr["status"] = "open"
                tr["filled_ts"] = time.time()
                # Initialize tracking fields
                tr.setdefault("dca_fills", 0)
                tr.setdefault("tp_fills", 0)
                tr.setdefault("tp_fills_list", [])
                self.log.info(f"✅ ENTRY FILLED {tr['symbol']} @ {tr.get('entry_price')}")

                # Send Telegram notification
                telegram_alerts.send_trade_opened(
                    symbol=tr["symbol"],
                    side=tr["order_side"],
                    entry=tr.get("entry_price", 0),
                    qty=tr.get("base_qty", 0),
                )

                # Place post-entry orders IMMEDIATELY (SL, TPs, DCAs)
                try:
                    self.place_post_entry_orders(tr)
                except Exception as e:
                    self.log.warning(f"Post-entry orders failed (will retry in main loop): {e}")
            return

        # DCA fills: orderLinkId pattern "<trade_id>:DCA1"
        if ":DCA" in link:
            trade_id, dca_tag = link.split(":", 1)
            tr = self.state.get("open_trades", {}).get(trade_id)
            if not tr:
                return
            import re as _re
            m = _re.search(r"DCA(\d+)", dca_tag)
            if m:
                dca_num = int(m.group(1))
                # Track DCA fill (avoid double counting)
                filled_dcas = tr.get("dca_fills_list", [])
                if dca_num not in filled_dcas:
                    filled_dcas.append(dca_num)
                    tr["dca_fills_list"] = filled_dcas
                    tr["dca_fills"] = len(filled_dcas)
                    dca_count = len(DCA_QTY_MULTS)
                    self.log.info(f"📈 DCA{dca_num} FILLED {tr['symbol']} ({tr['dca_fills']}/{dca_count})")

                    # Recalculate TPs based on new average entry
                    try:
                        self._recalculate_tps_after_dca(tr)
                    except Exception as e:
                        self.log.warning(f"TP recalculation failed after DCA{dca_num}: {e}")
            return

        # TP fills / other events: orderLinkId pattern "<trade_id>:TP1"
        if ":TP" in link:
            trade_id, tp_tag = link.split(":", 1)
            tr = self.state.get("open_trades", {}).get(trade_id)
            if not tr:
                return
            tp_num = None
            m = None
            import re as _re
            m = _re.search(r"TP(\d+)", tp_tag)
            if m:
                tp_num = int(m.group(1))
            if not tp_num:
                return

            # Track TP fill (avoid double counting)
            filled_tps = tr.get("tp_fills_list", [])
            if tp_num not in filled_tps:
                filled_tps.append(tp_num)
                tr["tp_fills_list"] = filled_tps
                tr["tp_fills"] = len(filled_tps)
                tp_count = len(tr.get("tp_prices") or FALLBACK_TP_PCT)
                self.log.info(f"🎯 TP{tp_num} HIT {tr['symbol']} ({tr['tp_fills']}/{tp_count})")

            # Follow-TP: Move SL to previous TP level after each TP hit
            # OR just move to BE on TP1 (legacy behavior)
            if FOLLOW_TP_ENABLED:
                # Follow-TP mode: move SL progressively
                tp_prices = tr.get("tp_prices") or []
                side = tr.get("order_side")  # "Buy" or "Sell"
                entry = float(tr.get("avg_entry") or tr.get("entry_price") or tr.get("trigger"))

                if tp_num == 1:
                    # TP1 hit -> SL to Entry (BE)
                    new_sl = entry
                    # Add buffer so BE is slightly in profit
                    if BE_BUFFER_PCT > 0:
                        if side == "Sell":  # SHORT
                            new_sl = entry * (1 - BE_BUFFER_PCT / 100.0)
                        else:  # LONG
                            new_sl = entry * (1 + BE_BUFFER_PCT / 100.0)
                    self._move_sl(tr["symbol"], new_sl)
                    tr["sl_moved_to_be"] = True
                    tr["last_sl_level"] = 0  # 0 = Entry/BE
                    self.log.info(f"✅ SL -> BE {tr['symbol']} @ {new_sl:.6f} (TP1 hit)")
                    # Cancel DCA orders on TP1
                    self._cancel_dca_orders(tr)

                elif tp_num > 1 and len(tp_prices) >= tp_num - 1:
                    # TP2+ hit -> SL to previous TP level
                    prev_tp = float(tp_prices[tp_num - 2])  # TP2 -> TP1, TP3 -> TP2, etc.

                    # Add buffer to the TP level
                    if FOLLOW_TP_BUFFER_PCT > 0:
                        if side == "Sell":  # SHORT: SL should be ABOVE prev TP
                            new_sl = prev_tp * (1 + FOLLOW_TP_BUFFER_PCT / 100.0)
                        else:  # LONG: SL should be BELOW prev TP
                            new_sl = prev_tp * (1 - FOLLOW_TP_BUFFER_PCT / 100.0)
                    else:
                        new_sl = prev_tp

                    self._move_sl(tr["symbol"], new_sl)
                    tr["last_sl_level"] = tp_num - 1
                    self.log.info(f"✅ SL -> TP{tp_num-1} {tr['symbol']} @ {new_sl:.6f} (TP{tp_num} hit)")

            elif MOVE_SL_TO_BE_ON_TP1 and tp_num == 1 and not tr.get("sl_moved_to_be"):
                # Legacy behavior: only move to BE on TP1
                be = float(tr.get("avg_entry") or tr.get("entry_price") or tr.get("trigger"))
                # Add buffer so BE is slightly in profit (covers fees)
                if BE_BUFFER_PCT > 0:
                    if tr.get("order_side") == "Sell":  # SHORT
                        be = be * (1 - BE_BUFFER_PCT / 100.0)
                    else:  # LONG
                        be = be * (1 + BE_BUFFER_PCT / 100.0)
                self._move_sl(tr["symbol"], be)
                tr["sl_moved_to_be"] = True
                self.log.info(f"✅ SL -> BE {tr['symbol']} @ {be} (buffer {BE_BUFFER_PCT}%)")

                # Cancel all pending DCA orders when TP1 hits
                self._cancel_dca_orders(tr)

            # start trailing after TPn
            if TRAIL_ACTIVATE_ON_TP and tp_num == TRAIL_AFTER_TP_INDEX and not tr.get("trailing_started"):
                self._start_trailing(tr, tp_num)
                tr["trailing_started"] = True
                self.log.info(f"✅ TRAILING STARTED {tr['symbol']} after TP{tp_num}")

    def _move_sl(self, symbol: str, sl_price: float, max_retries: int = 3) -> bool:
        """Move SL with retry logic for volatile markets."""
        rules = self._get_instrument_rules(symbol)
        sl_price = self._round_price(sl_price, rules["tick_size"])
        body = {
            "category": CATEGORY,
            "symbol": symbol,
            "positionIdx": 0,
            "stopLoss": f"{sl_price:.10f}",
            "tpslMode": "Full",
        }
        if DRY_RUN:
            self.log.info(f"DRY_RUN move SL: {body}")
            return True

        for attempt in range(max_retries):
            try:
                self.bybit.set_trading_stop(body)
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    self.log.warning(f"SL move attempt {attempt+1} failed for {symbol}: {e} - retrying in 100ms...")
                    time.sleep(0.1)  # 100ms wait
                else:
                    self.log.error(f"❌ SL move FAILED after {max_retries} attempts for {symbol}: {e}")
                    self.log.error(f"   Trade continues with original SL!")
                    return False
        return False

    def _start_trailing(self, tr: Dict[str, Any], tp_num: int) -> None:
        """Start trailing stop after TPn is hit.

        For Bybit V5:
        - activePrice: price at which trailing activates
        - trailingStop: distance to trail behind price

        For SHORT: activePrice must be BELOW current price to activate later,
                   OR we skip activePrice if price already passed the level.
        For LONG: activePrice must be ABOVE current price to activate later.
        """
        symbol = tr["symbol"]
        side = tr["order_side"]  # Buy/Sell
        tp_prices = tr.get("tp_prices") or []

        rules = self._get_instrument_rules(symbol)
        tick_size = rules["tick_size"]

        # Get current market price
        current_price = self.bybit.last_price(CATEGORY, symbol)

        if len(tp_prices) < tp_num:
            anchor = current_price
        else:
            anchor = float(tp_prices[tp_num-1])

        anchor = self._round_price(anchor, tick_size)
        dist = self._round_price(anchor * (TRAIL_DISTANCE_PCT / 100.0), tick_size)

        body = {
            "category": CATEGORY,
            "symbol": symbol,
            "positionIdx": 0,
            "tpslMode": "Full",
            "trailingStop": f"{dist:.10f}",
        }

        # Only set activePrice if price hasn't already passed the activation level
        # For SHORT: activePrice should be below current price (activate when dropping further)
        # For LONG: activePrice should be above current price (activate when rising further)
        if side == "Sell":  # SHORT
            if anchor < current_price:
                # Price hasn't reached anchor yet - set activation price
                body["activePrice"] = f"{anchor:.10f}"
            # else: price already at/past anchor - don't set activePrice, activate immediately
        else:  # LONG
            if anchor > current_price:
                # Price hasn't reached anchor yet - set activation price
                body["activePrice"] = f"{anchor:.10f}"
            # else: price already at/past anchor - don't set activePrice, activate immediately

        # keep SL at BE if already moved; otherwise keep existing stopLoss unchanged
        if tr.get("sl_moved_to_be"):
            be_price = float(tr.get("avg_entry") or tr.get("entry_price") or tr.get("trigger"))
            # Add buffer so BE is slightly in profit
            if BE_BUFFER_PCT > 0:
                if tr.get("order_side") == "Sell":  # SHORT
                    be_price = be_price * (1 - BE_BUFFER_PCT / 100.0)
                else:  # LONG
                    be_price = be_price * (1 + BE_BUFFER_PCT / 100.0)
            be_price = self._round_price(be_price, tick_size)
            body["stopLoss"] = f"{be_price:.10f}"

        if DRY_RUN:
            self.log.info(f"DRY_RUN set trailing: {body}")
            return

        try:
            self.bybit.set_trading_stop(body)
            self.log.info(f"🔄 Trailing started for {symbol} (dist: {dist})")
        except Exception as e:
            self.log.warning(f"Failed to set trailing for {symbol}: {e}")

    # ---------- maintenance ----------
    def check_tp_fills_fallback(self) -> None:
        """Polling fallback: Check if TP1 was filled OR price went through TP1 level.

        This handles two scenarios:
        1. TP1 order was filled but WebSocket missed the event
        2. Price shot through TP1 so fast the limit order wasn't filled

        In both cases, we should move SL to BE.
        """
        if DRY_RUN:
            return

        for tid, tr in list(self.state.get("open_trades", {}).items()):
            if tr.get("status") != "open":
                continue
            if not tr.get("post_orders_placed"):
                continue
            if tr.get("sl_moved_to_be"):
                continue  # Already moved

            symbol = tr["symbol"]
            side = tr["order_side"]  # Buy/Sell
            tp_prices = tr.get("tp_prices") or []

            if not tp_prices:
                continue

            tp1_price = float(tp_prices[0])
            should_move_to_be = False

            # Check 1: Did TP1 order get filled?
            tp1_oid = tr.get("tp1_order_id")
            if tp1_oid:
                try:
                    open_orders = self.bybit.open_orders(CATEGORY, symbol)
                    tp1_still_open = any(o.get("orderId") == tp1_oid for o in open_orders)
                    if not tp1_still_open:
                        should_move_to_be = True
                        self.log.debug(f"TP1 order no longer open for {symbol}")
                except Exception as e:
                    self.log.debug(f"TP1 order check failed for {symbol}: {e}")

            # Check 2: Did price go THROUGH TP1 level? (even if order wasn't filled)
            if not should_move_to_be:
                try:
                    current_price = self.bybit.last_price(CATEGORY, symbol)
                    if side == "Buy":  # LONG: TP1 is above entry
                        if current_price >= tp1_price:
                            should_move_to_be = True
                            self.log.info(f"📈 Price passed TP1 level for {symbol} ({current_price} >= {tp1_price})")
                    else:  # SHORT: TP1 is below entry
                        if current_price <= tp1_price:
                            should_move_to_be = True
                            self.log.info(f"📉 Price passed TP1 level for {symbol} ({current_price} <= {tp1_price})")
                except Exception as e:
                    self.log.debug(f"Price check failed for {symbol}: {e}")

            if should_move_to_be:
                be = float(tr.get("avg_entry") or tr.get("entry_price") or tr.get("trigger"))
                # Add buffer so BE is slightly in profit
                if BE_BUFFER_PCT > 0:
                    if tr.get("order_side") == "Sell":  # SHORT
                        be = be * (1 - BE_BUFFER_PCT / 100.0)
                    else:  # LONG
                        be = be * (1 + BE_BUFFER_PCT / 100.0)
                if self._move_sl(symbol, be):
                    tr["sl_moved_to_be"] = True
                    # Also track TP1 as filled if not already
                    if 1 not in tr.get("tp_fills_list", []):
                        tr.setdefault("tp_fills_list", []).append(1)
                        tr["tp_fills"] = len(tr["tp_fills_list"])
                    self.log.info(f"✅ SL -> BE (fallback) {symbol} @ {be} (buffer {BE_BUFFER_PCT}%)")

    def cancel_expired_entries(self) -> None:
        now = time.time()
        for tid, tr in list(self.state.get("open_trades", {}).items()):
            if tr.get("status") != "pending":
                continue
            placed = float(tr.get("placed_ts") or 0)
            if placed and now - placed > ENTRY_EXPIRATION_MIN * 60:
                oid = tr.get("entry_order_id")
                if oid and oid != "DRY_RUN":
                    try:
                        self.cancel_entry(tr["symbol"], oid)
                        self.log.info(f"⏳ Canceled expired entry {tr['symbol']} ({tid})")
                    except Exception as e:
                        self.log.warning(f"Cancel failed {tr['symbol']} ({tid}): {e}")
                tr["status"] = "expired"

    def check_position_alerts(self) -> None:
        """Check all open positions and send Telegram alerts if thresholds crossed."""
        if not telegram_alerts.is_enabled():
            return

        for tid, tr in list(self.state.get("open_trades", {}).items()):
            if tr.get("status") != "open":
                continue

            symbol = tr["symbol"]
            side = tr["order_side"]
            avg_entry = float(tr.get("avg_entry") or tr.get("entry_price") or 0)

            if not avg_entry:
                continue

            try:
                current_price = self.bybit.last_price(CATEGORY, symbol)
                if not current_price:
                    continue

                telegram_alerts.check_position_alerts(
                    trade_id=tid,
                    symbol=symbol,
                    side=side,
                    avg_entry=avg_entry,
                    current_price=current_price,
                    leverage=LEVERAGE,
                    dca_fills=tr.get("dca_fills", 0),
                    dca_count=len(DCA_QTY_MULTS),
                )
            except Exception as e:
                self.log.debug(f"Position alert check failed for {symbol}: {e}")

    def cleanup_closed_trades(self) -> None:
        """Remove trades from state if position is closed (size = 0)."""
        for tid, tr in list(self.state.get("open_trades", {}).items()):
            if tr.get("status") not in ("open",):
                continue
            try:
                size, _ = self.position_size_avg(tr["symbol"])
                if size == 0:
                    # Position closed - cancel all pending orders for this trade!
                    self._cancel_all_trade_orders(tr)
                    tr["status"] = "closed"
                    tr["closed_ts"] = time.time()

                    # Fetch final PnL from Bybit
                    self._fetch_and_store_trade_stats(tr)

                    # Export to Database IMMEDIATELY (not waiting for archive)
                    if db_export.is_enabled():
                        self._export_trade_to_db(tr)

                    # Send Telegram notification
                    telegram_alerts.send_trade_closed(
                        symbol=tr["symbol"],
                        side=tr["order_side"],
                        pnl=tr.get("realized_pnl", 0),
                        exit_reason=tr.get("exit_reason", "unknown"),
                        tp_fills=tr.get("tp_fills", 0),
                        dca_fills=tr.get("dca_fills", 0),
                    )
                    telegram_alerts.clear_alerts_for_trade(tid)

                    self.log.info(f"✅ TRADE CLOSED {tr['symbol']} ({tid})")
            except Exception as e:
                self.log.warning(f"Cleanup check failed for {tr['symbol']}: {e}")

        # Prune old closed/expired trades (keep last 24h for reference)
        cutoff = time.time() - 86400
        for tid, tr in list(self.state.get("open_trades", {}).items()):
            if tr.get("status") in ("closed", "expired"):
                closed_at = tr.get("closed_ts") or tr.get("placed_ts") or 0
                if closed_at < cutoff:
                    # Move to trade_history before deleting
                    self._archive_trade(tr)
                    del self.state["open_trades"][tid]

    def _cancel_dca_orders(self, trade: Dict[str, Any]) -> None:
        """Cancel all pending DCA orders for a trade (called when TP1 hits)."""
        if DRY_RUN:
            self.log.info(f"DRY_RUN: Would cancel DCA orders for {trade['symbol']}")
            return

        symbol = trade["symbol"]
        dca_order_ids = trade.get("dca_order_ids") or {}

        if not dca_order_ids:
            return

        cancelled = 0
        for dca_idx, order_id in dca_order_ids.items():
            if not order_id:
                continue
            try:
                self.bybit.cancel_order({
                    "category": CATEGORY,
                    "symbol": symbol,
                    "orderId": order_id,
                })
                cancelled += 1
                self.log.info(f"🗑️ Cancelled DCA{dca_idx} order: {order_id}")
            except Exception as e:
                # Order might already be filled or cancelled
                self.log.debug(f"Could not cancel DCA{dca_idx}: {e}")

        if cancelled > 0:
            self.log.info(f"✅ Cancelled {cancelled} DCA order(s) for {symbol} (TP1 hit)")
            trade["dca_order_ids"] = {}  # Clear the order IDs

    def _cancel_all_trade_orders(self, trade: Dict[str, Any]) -> None:
        """Cancel all pending DCA and TP orders for a closed trade."""
        if DRY_RUN:
            self.log.info(f"DRY_RUN: Would cancel orders for {trade['symbol']}")
            return

        symbol = trade["symbol"]
        trade_id = trade["id"]

        try:
            # Get all open orders for this symbol
            open_orders = self.bybit.open_orders(CATEGORY, symbol)

            cancelled = 0
            for order in open_orders:
                link_id = order.get("orderLinkId") or ""
                # Check if this order belongs to our trade (DCA or TP)
                if link_id.startswith(trade_id + ":"):
                    order_id = order.get("orderId")
                    if order_id:
                        try:
                            self.bybit.cancel_order({
                                "category": CATEGORY,
                                "symbol": symbol,
                                "orderId": order_id
                            })
                            cancelled += 1
                            self.log.info(f"🗑️ Cancelled orphan order: {link_id}")
                        except Exception as e:
                            # Ignore "order not found" errors
                            if "not found" not in str(e).lower():
                                self.log.warning(f"Failed to cancel {link_id}: {e}")

            if cancelled > 0:
                self.log.info(f"🧹 Cleaned up {cancelled} pending order(s) for {symbol}")

        except Exception as e:
            self.log.warning(f"Failed to cleanup orders for {symbol}: {e}")

    def _export_trade_to_db(self, trade: Dict[str, Any]) -> None:
        """Export trade to PostgreSQL database immediately after close."""
        try:
            # Calculate margin used for PnL % calculation
            entry_price = trade.get("entry_price") or trade.get("trigger") or 0
            base_qty = trade.get("base_qty") or 0
            margin_used = (entry_price * base_qty) / LEVERAGE if entry_price and base_qty else 0

            # Fetch current equity from Bybit for Equity PnL % calculation
            equity_at_close = 0
            try:
                equity_at_close = self.bybit.wallet_equity(ACCOUNT_TYPE)
            except Exception as e:
                self.log.debug(f"Could not fetch equity: {e}")

            # Add margin and equity to trade for export
            trade["margin_used"] = margin_used
            trade["equity_at_close"] = equity_at_close

            if db_export.export_trade(trade):
                self.log.info(f"📊 Trade exported to database")
            else:
                self.log.warning(f"⚠️ Database export failed (check DATABASE_URL)")
        except Exception as e:
            self.log.warning(f"Database export error: {e}")

    def _fetch_and_store_trade_stats(self, trade: Dict[str, Any]) -> None:
        """Fetch final PnL from Bybit and determine exit reason."""
        if DRY_RUN:
            trade["realized_pnl"] = 0.0
            trade["exit_reason"] = "dry_run"
            return

        symbol = trade["symbol"]
        filled_ts = trade.get("filled_ts") or trade.get("placed_ts") or 0

        try:
            # Fetch closed PnL records around the time of this trade
            start_time = int((filled_ts - 60) * 1000) if filled_ts else None
            pnl_records = self.bybit.closed_pnl(CATEGORY, symbol, start_time=start_time, limit=20)

            # Sum all PnL records for this symbol in the timeframe
            total_pnl = 0.0
            for rec in pnl_records:
                rec_time = int(rec.get("createdTime") or 0)
                if rec_time >= int(filled_ts * 1000):
                    total_pnl += float(rec.get("closedPnl") or 0)

            trade["realized_pnl"] = total_pnl
            trade["is_win"] = total_pnl > 0

            # Determine exit reason based on what happened
            trade["exit_reason"] = self._determine_exit_reason(trade)

            # Log trade summary
            self._log_trade_summary(trade)

        except Exception as e:
            self.log.warning(f"Failed to fetch PnL for {symbol}: {e}")
            trade["realized_pnl"] = None
            trade["exit_reason"] = "unknown"

    def _determine_exit_reason(self, trade: Dict[str, Any]) -> str:
        """Determine how the trade was closed."""
        tp_fills = trade.get("tp_fills", 0)
        tp_count = len(trade.get("tp_prices") or FALLBACK_TP_PCT)
        trailing_started = trade.get("trailing_started", False)
        sl_moved_to_be = trade.get("sl_moved_to_be", False)
        pnl = trade.get("realized_pnl", 0)

        if trailing_started and pnl and pnl > 0:
            return "trailing_stop"
        elif tp_fills >= tp_count:
            return "all_tps_hit"
        elif tp_fills > 0 and sl_moved_to_be and pnl is not None and abs(pnl) < 1:
            return "breakeven"
        elif tp_fills > 0:
            return f"tp{tp_fills}_then_sl"
        elif pnl and pnl < 0:
            return "stop_loss"
        else:
            return "unknown"

    def _log_trade_summary(self, trade: Dict[str, Any]) -> None:
        """Log a nice trade summary."""
        symbol = trade["symbol"]
        side = trade.get("pos_side", "")
        entry = trade.get("entry_price", trade.get("trigger"))
        pnl = trade.get("realized_pnl", 0) or 0
        exit_reason = trade.get("exit_reason", "unknown")
        tp_fills = trade.get("tp_fills", 0)
        # TP count = how many we actually placed (limited by TP_SPLITS)
        signal_tp_count = len(trade.get("tp_prices") or FALLBACK_TP_PCT)
        tp_count = min(signal_tp_count, len(TP_SPLITS))
        dca_fills = trade.get("dca_fills", 0)
        dca_count = len(DCA_QTY_MULTS)
        is_win = pnl > 0

        emoji = "🟢" if is_win else "🔴"
        result = "WIN" if is_win else "LOSS"

        self.log.info(f"")
        self.log.info(f"{'='*50}")
        self.log.info(f"{emoji} TRADE {result}: {symbol} {side}")
        self.log.info(f"{'='*50}")
        self.log.info(f"   Entry: ${entry:.6f}")
        self.log.info(f"   PnL: ${pnl:.2f} USDT")
        self.log.info(f"   TPs Hit: {tp_fills}/{tp_count}")
        self.log.info(f"   DCAs Filled: {dca_fills}/{dca_count}")
        self.log.info(f"   Exit: {exit_reason}")
        self.log.info(f"{'='*50}")
        self.log.info(f"")

    def _archive_trade(self, trade: Dict[str, Any]) -> None:
        """Move closed trade to trade_history for long-term stats."""
        history = self.state.setdefault("trade_history", [])

        # TP count = how many we actually placed (limited by TP_SPLITS)
        signal_tp_count = len(trade.get("tp_prices") or FALLBACK_TP_PCT)
        actual_tp_count = min(signal_tp_count, len(TP_SPLITS))

        # Keep only essential fields for history
        archived = {
            "id": trade.get("id"),
            "symbol": trade.get("symbol"),
            "side": trade.get("pos_side"),
            "entry_price": trade.get("entry_price"),
            "trigger": trade.get("trigger"),
            "placed_ts": trade.get("placed_ts"),
            "filled_ts": trade.get("filled_ts"),
            "closed_ts": trade.get("closed_ts"),
            "realized_pnl": trade.get("realized_pnl"),
            "is_win": trade.get("is_win"),
            "exit_reason": trade.get("exit_reason"),
            "tp_fills": trade.get("tp_fills", 0),
            "tp_count": actual_tp_count,
            "dca_fills": trade.get("dca_fills", 0),
            "dca_count": len(DCA_QTY_MULTS),
            "trailing_used": trade.get("trailing_started", False),
        }
        history.append(archived)

        # Note: Google Sheets export happens immediately at trade close,
        # not here at archive time (to avoid 24h delay)

        # Keep max 500 trades in history (oldest pruned)
        if len(history) > 500:
            self.state["trade_history"] = history[-500:]

    def get_trade_stats(self, days: Optional[int] = None) -> Dict[str, Any]:
        """Calculate trade statistics for the given period (None = all time)."""
        history = self.state.get("trade_history", [])
        now = time.time()

        if days:
            cutoff = now - (days * 86400)
            trades = [t for t in history if (t.get("closed_ts") or 0) >= cutoff]
        else:
            trades = history

        if not trades:
            return {
                "period_days": days or "all",
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
                "avg_tp_fills": 0.0,
                "avg_dca_fills": 0.0,
                "trailing_exits": 0,
                "sl_exits": 0,
                "be_exits": 0,
            }

        wins = [t for t in trades if t.get("is_win")]
        losses = [t for t in trades if not t.get("is_win")]
        pnls = [t.get("realized_pnl") or 0 for t in trades]
        tp_fills = [t.get("tp_fills") or 0 for t in trades]
        dca_fills = [t.get("dca_fills") or 0 for t in trades]

        exit_reasons = [t.get("exit_reason") or "" for t in trades]
        trailing_exits = sum(1 for r in exit_reasons if r == "trailing_stop")
        sl_exits = sum(1 for r in exit_reasons if r == "stop_loss")
        be_exits = sum(1 for r in exit_reasons if r == "breakeven")

        return {
            "period_days": days or "all",
            "total_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0.0,
            "total_pnl": round(sum(pnls), 2),
            "avg_pnl": round(sum(pnls) / len(trades), 2) if trades else 0.0,
            "best_trade": round(max(pnls), 2) if pnls else 0.0,
            "worst_trade": round(min(pnls), 2) if pnls else 0.0,
            "avg_tp_fills": round(sum(tp_fills) / len(trades), 1) if trades else 0.0,
            "avg_dca_fills": round(sum(dca_fills) / len(trades), 1) if trades else 0.0,
            "trailing_exits": trailing_exits,
            "sl_exits": sl_exits,
            "be_exits": be_exits,
        }

    def log_performance_report(self) -> None:
        """Log a comprehensive performance report."""
        stats_7d = self.get_trade_stats(7)
        stats_30d = self.get_trade_stats(30)
        stats_all = self.get_trade_stats()

        self.log.info("")
        self.log.info("=" * 60)
        self.log.info("📊 PERFORMANCE REPORT")
        self.log.info("=" * 60)

        for label, stats in [("7 Days", stats_7d), ("30 Days", stats_30d), ("All Time", stats_all)]:
            if stats["total_trades"] == 0:
                self.log.info(f"\n{label}: No trades")
                continue

            self.log.info(f"\n📈 {label}:")
            self.log.info(f"   Trades: {stats['total_trades']} | Wins: {stats['wins']} | Losses: {stats['losses']}")
            self.log.info(f"   Win Rate: {stats['win_rate']}%")
            self.log.info(f"   Total PnL: ${stats['total_pnl']:.2f} | Avg: ${stats['avg_pnl']:.2f}")
            self.log.info(f"   Best: ${stats['best_trade']:.2f} | Worst: ${stats['worst_trade']:.2f}")
            self.log.info(f"   Avg TPs Hit: {stats['avg_tp_fills']:.1f} | Avg DCAs: {stats['avg_dca_fills']:.1f}")
            self.log.info(f"   Exits: {stats['trailing_exits']} trailing, {stats['sl_exits']} SL, {stats['be_exits']} BE")

        self.log.info("")
        self.log.info("=" * 60)

        # Update daily equity snapshot
        if db_export.is_enabled():
            try:
                equity = self.bybit.wallet_equity(ACCOUNT_TYPE)
                # Count today's trades
                from state import utc_day_key
                today = utc_day_key()
                today_trades = sum(1 for tr in self.state.get("open_trades", {}).values()
                                 if utc_day_key(tr.get("closed_ts") or 0) == today)
                today_wins = sum(1 for tr in self.state.get("open_trades", {}).values()
                               if utc_day_key(tr.get("closed_ts") or 0) == today and tr.get("is_win"))
                today_losses = today_trades - today_wins
                db_export.update_daily_equity(equity, today_trades, today_wins, today_losses)
            except Exception as e:
                self.log.debug(f"Failed to update daily equity: {e}")

    def place_dca_orders(self, trade: Dict[str, Any]) -> bool:
        """Place DCA orders for a trade (called when DCA is added to signal after entry)."""
        symbol = trade["symbol"]
        side = trade.get("order_side")  # "Sell" or "Buy"
        base_qty = float(trade.get("base_qty", 0))
        dca_prices: List[float] = trade.get("dca_prices") or []

        if not dca_prices:
            self.log.debug(f"No DCA prices for {symbol}")
            return False

        if trade.get("dca_orders_placed"):
            self.log.debug(f"DCA orders already placed for {symbol}")
            return False

        # Get instrument rules
        rules = self._get_instrument_rules(symbol)
        tick_size = rules["tick_size"]
        qty_step = rules["qty_step"]
        min_qty = rules["min_qty"]

        dca_to_place = min(len(dca_prices), len(DCA_QTY_MULTS))
        self.log.info(f"📊 Placing {dca_to_place} DCAs for {symbol} (mults: {DCA_QTY_MULTS[:dca_to_place]})")

        if dca_to_place == 0:
            return False

        last = self.bybit.last_price(CATEGORY, symbol)

        dca_orders = []
        for j in range(1, dca_to_place + 1):
            price = self._round_price(float(dca_prices[j-1]), tick_size)
            mult = DCA_QTY_MULTS[j-1]
            qty = self._round_qty(base_qty * mult, qty_step, min_qty)
            td = self._trigger_direction(last, price)
            dca_orders.append({
                "idx": j,
                "body": {
                    "category": CATEGORY,
                    "symbol": symbol,
                    "side": side,
                    "orderType": "Limit",
                    "qty": f"{qty}",
                    "price": f"{price:.10f}",
                    "timeInForce": "GTC",
                    "triggerDirection": td,
                    "triggerPrice": f"{price:.10f}",
                    "triggerBy": "LastPrice",
                    "reduceOnly": False,
                    "closeOnTrigger": False,
                    "orderLinkId": f"{trade['id']}:DCA{j}",
                }
            })

        if DRY_RUN:
            for o in dca_orders:
                self.log.info(f"DRY_RUN DCA{o['idx']}: {o['body']}")
            trade["dca_orders_placed"] = True
            return True

        # Place DCA orders
        success = True
        for o in dca_orders:
            try:
                resp = self.bybit.place_order(o["body"])
                oid = (resp.get("result") or {}).get("orderId")
                trade.setdefault("dca_order_ids", {})[str(o["idx"])] = oid
                self.log.info(f"✅ DCA{o['idx']} placed for {symbol} @ {dca_prices[o['idx']-1]}")
            except Exception as e:
                self.log.error(f"❌ DCA{o['idx']} failed for {symbol}: {e}")
                success = False

        trade["dca_orders_placed"] = True

        # Also update SL if no SL was set before (use DCA+4% rule)
        if not trade.get("sl_price") and not trade.get("sl_moved_to_be"):
            dca_buffer_pct = 0.04
            first_dca = float(dca_prices[0])
            if side == "Sell":
                new_sl = first_dca * (1 + dca_buffer_pct)
            else:
                new_sl = first_dca * (1 - dca_buffer_pct)
            new_sl = self._round_price(new_sl, tick_size)
            self.log.info(f"📍 Setting SL at DCA+4% for {symbol}: {new_sl} (DCA1: {first_dca})")
            if self._move_sl(symbol, new_sl):
                trade["sl_price"] = new_sl

        return success
