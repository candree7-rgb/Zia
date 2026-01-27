import re
import hashlib
from typing import Any, Dict, Optional, List

NUM = r"([0-9]+(?:\.[0-9]+)?)"

# ============================================================
# ETC TRADE FORMAT (AO Trading Embeds in ETC Channel)
# ============================================================
# Beispiel-Signal (echtes Format aus Discord):
# AO Trading • New Trade Signal
# 🔴 SHORT SIGNAL - AGLD/USDT
# Leverage: 25x • Trader: Ziad
#
# 📊 Entry: 0.398 ✅ Triggered
#
# 🎯 Profit Targets:
# ✅ TP1: 0.39402 HIT (+25.00%)
# ✅ TP2: 0.389244 HIT (+55.00%)
# ✅ TP3: 0.3822392 HIT (+99.00%)
#
# 📊 DCA Levels:
# DCA1: 0.98382
# DCA2: 54
#
# 🛡️ Breakeven: 0.863 ✅ Moved to Breakeven
#
# 📝 Notes: Caller: Ziad
# ============================================================

# Symbol and Side: "🔴 SHORT SIGNAL - AGLD/USDT"
RE_SYMBOL_SIDE = re.compile(
    r"(LONG|SHORT)\s+SIGNAL\s*[-–—]\s*([A-Z0-9]+)/([A-Z]+)",
    re.I
)

# Entry: "📊 Entry: 0.398" or "Entry: $0.398"
RE_ENTRY = re.compile(
    r"Entry:\s*\$?" + NUM,
    re.I
)

# TP: "TP1: 0.39402" or "✅ TP1: 0.39402 HIT"
RE_TP = re.compile(
    r"TP(\d+):\s*\$?" + NUM,
    re.I
)

# DCA: "DCA1: 0.98382" or "✅ DCA1: 47.75916 HIT"
RE_DCA = re.compile(
    r"DCA(\d+):\s*\$?" + NUM,
    re.I
)

# Leverage: "Leverage: 25x"
RE_LEVERAGE = re.compile(
    r"Leverage:\s*(\d+)x",
    re.I
)

# Caller: "📝 Notes: Caller: Ziad" or "Caller: Ziad"
RE_CALLER = re.compile(
    r"Caller:\s*(\w+)",
    re.I
)

# Trader in signal line: "Trader: Ziad"
RE_TRADER = re.compile(
    r"Trader:\s*(\w+)",
    re.I
)

# Status patterns to detect if trade is still valid for entry
RE_CLOSED = re.compile(
    r"TRADE\s+CLOSED|closed\s+at\s+breakeven|TRADE\s+CANCELLED|⏳\s*closed",
    re.I
)

# Signal markers
RE_NEW_SIGNAL = re.compile(r"NEW\s+SIGNAL|New\s+Trade\s+Signal", re.I)


def parse_signal(text: str, quote: str = "USDT", allowed_callers: List[str] = None) -> Optional[Dict[str, Any]]:
    """
    Parse ETC Trade signal format (AO Trading embeds).

    Returns None if:
    - Not a NEW SIGNAL message
    - Trade is already CLOSED/CANCELLED
    - Cannot parse symbol/side or entry price
    - Caller not in allowed_callers list (if specified)
    """
    # We only want fresh "NEW SIGNAL" or "New Trade Signal" entries
    if not RE_NEW_SIGNAL.search(text):
        return None

    # Skip already closed/cancelled trades
    if RE_CLOSED.search(text):
        return None

    # Parse symbol and side
    ms = RE_SYMBOL_SIDE.search(text)
    if not ms:
        return None

    side_word = ms.group(1).upper()
    base = ms.group(2).upper()
    quote_from_signal = ms.group(3).upper()

    side = "sell" if side_word == "SHORT" else "buy"
    symbol = f"{base}{quote_from_signal}"

    # Parse caller for filtering
    caller = None
    mc = RE_CALLER.search(text)
    if mc:
        caller = mc.group(1)

    # If no caller found in Notes, try Trader field
    if not caller:
        mt = RE_TRADER.search(text)
        if mt:
            caller = mt.group(1)

    # Filter by allowed callers if specified
    if allowed_callers and len(allowed_callers) > 0:
        if not caller or caller.lower() not in [c.lower() for c in allowed_callers]:
            return None

    # Parse entry/trigger price
    mtr = RE_ENTRY.search(text)
    if not mtr:
        return None
    trigger = float(mtr.group(1))

    # Parse TP prices (can be 3-4 or more)
    tps: List[float] = []
    for m in RE_TP.finditer(text):
        idx = int(m.group(1))
        price = float(m.group(2))
        # Keep in order
        while len(tps) < idx:
            tps.append(0.0)
        tps[idx-1] = price
    tps = [p for p in tps if p > 0]

    # Parse DCA prices (0, 1, 2 or more)
    dcas: List[float] = []
    for m in RE_DCA.finditer(text):
        idx = int(m.group(1))
        price = float(m.group(2))
        while len(dcas) < idx:
            dcas.append(0.0)
        dcas[idx-1] = price
    dcas = [p for p in dcas if p > 0]

    # No Stop Loss in ETC format - will use INITIAL_SL_PCT fallback
    sl = None

    # Parse leverage (optional, for logging/filtering)
    leverage = None
    mlev = RE_LEVERAGE.search(text)
    if mlev:
        leverage = int(mlev.group(1))

    return {
        "base": base,
        "symbol": symbol,
        "side": side,          # buy / sell
        "trigger": trigger,
        "tp_prices": tps,
        "dca_prices": dcas,
        "sl_price": sl,        # Always None for ETC - uses fallback
        "leverage": leverage,
        "trader": caller,      # Caller name (Ziad, etc.)
        "raw": text,
    }


def parse_signal_update(text: str) -> Dict[str, Any]:
    """
    Parse signal for DCA updates only.

    Unlike parse_signal(), this does NOT require "NEW SIGNAL" in text.
    Used for checking if an existing signal was updated with new DCA values.

    Note: ETC format has no Stop Loss in signals, so sl_price is always None.

    Returns dict with sl_price and dca_prices (may be None/empty if not found).
    """
    result = {
        "sl_price": None,
        "dca_prices": [],
        "tp_prices": [],
    }

    # Parse DCA prices
    dcas: List[float] = []
    for m in RE_DCA.finditer(text):
        idx = int(m.group(1))
        price = float(m.group(2))
        while len(dcas) < idx:
            dcas.append(0.0)
        dcas[idx-1] = price
    result["dca_prices"] = [p for p in dcas if p > 0]

    # Parse TP prices (for updates)
    tps: List[float] = []
    for m in RE_TP.finditer(text):
        idx = int(m.group(1))
        price = float(m.group(2))
        while len(tps) < idx:
            tps.append(0.0)
        tps[idx-1] = price
    result["tp_prices"] = [p for p in tps if p > 0]

    return result


# ============================================================
# DCA TRIGGERED Message Parser
# ============================================================
# Beispiel:
# 🔵 DCA 1 TRIGGERED
# RIVER/USDT SHORT • Leverage: 1x
# Trader: Ziad
#
# 📊 POSITION UPDATE
# Original Entry: $53.72
# New Average: $58.39
#
# 🎯 RECALCULATED TARGETS
# TP1: $53.18 → $57.92
# TP2: $52.54 → $57.46
# TP3: $51.59 → $56.05
# ============================================================

RE_DCA_TRIGGERED = re.compile(r"DCA\s*(\d+)\s+TRIGGERED", re.I)
RE_NEW_AVERAGE = re.compile(r"New\s+Average:\s*\$?" + NUM, re.I)
RE_RECALC_TP = re.compile(r"TP(\d+):[^→]+→\s*\$?" + NUM, re.I)


def parse_dca_triggered(text: str) -> Optional[Dict[str, Any]]:
    """
    Parse DCA TRIGGERED message to get new average entry and recalculated TPs.

    Returns None if not a DCA TRIGGERED message.
    Returns dict with:
        - dca_index: which DCA was triggered (1, 2, etc.)
        - symbol: e.g. "RIVERUSDT"
        - side: "buy" or "sell"
        - new_average: new average entry price
        - new_tp_prices: list of recalculated TP prices
    """
    # Check if this is a DCA TRIGGERED message
    m_dca = RE_DCA_TRIGGERED.search(text)
    if not m_dca:
        return None

    dca_index = int(m_dca.group(1))

    # Parse symbol and side
    ms = RE_SYMBOL_SIDE.search(text)
    if not ms:
        return None

    side_word = ms.group(1).upper()
    base = ms.group(2).upper()
    quote_from_signal = ms.group(3).upper()

    side = "sell" if side_word == "SHORT" else "buy"
    symbol = f"{base}{quote_from_signal}"

    # Parse new average
    m_avg = RE_NEW_AVERAGE.search(text)
    new_average = float(m_avg.group(1)) if m_avg else None

    # Parse recalculated TPs (the "→ $57.92" part)
    new_tps: List[float] = []
    for m in RE_RECALC_TP.finditer(text):
        idx = int(m.group(1))
        price = float(m.group(2))
        while len(new_tps) < idx:
            new_tps.append(0.0)
        new_tps[idx-1] = price
    new_tps = [p for p in new_tps if p > 0]

    return {
        "dca_index": dca_index,
        "symbol": symbol,
        "side": side,
        "new_average": new_average,
        "new_tp_prices": new_tps,
    }


def signal_hash(sig: Dict[str, Any]) -> str:
    """Generate unique hash for signal deduplication."""
    core = f"{sig.get('symbol')}|{sig.get('side')}|{sig.get('trigger')}|{sig.get('tp_prices')}|{sig.get('dca_prices')}"
    return hashlib.md5(core.encode("utf-8")).hexdigest()
