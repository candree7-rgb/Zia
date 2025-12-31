import os
from dotenv import load_dotenv

load_dotenv()

def _get(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def _get_bool(name: str, default: str = "false") -> bool:
    return _get(name, default).lower() in ("1","true","yes","y","on")

def _get_int(name: str, default: str) -> int:
    return int(_get(name, default))

def _get_float(name: str, default: str) -> float:
    return float(_get(name, default))

# Discord
DISCORD_TOKEN = _get("DISCORD_TOKEN")
CHANNEL_ID    = _get("CHANNEL_ID")

# Bybit
BYBIT_API_KEY    = _get("BYBIT_API_KEY")
BYBIT_API_SECRET = _get("BYBIT_API_SECRET")
BYBIT_TESTNET    = _get_bool("BYBIT_TESTNET","false")
BYBIT_DEMO       = _get_bool("BYBIT_DEMO","false")  # Demo trading (paper trading)
ACCOUNT_TYPE     = _get("ACCOUNT_TYPE","UNIFIED")  # UNIFIED / CONTRACT etc (depends on your Bybit account)

# Bot identification (for multi-bot dashboard support)
BOT_ID = _get("BOT_ID", "ao")  # Unique identifier for this bot instance

# Signal Parser Version: "v1" = original embed format, "v2" = plain text format
SIGNAL_PARSER_VERSION = _get("SIGNAL_PARSER_VERSION", "v1").lower()

RECV_WINDOW = _get("RECV_WINDOW","5000")

# Trading
CATEGORY = _get("CATEGORY","linear")   # linear for USDT perpetual
QUOTE    = _get("QUOTE","USDT").upper()

LEVERAGE = _get_int("LEVERAGE","5")
RISK_PCT = _get_float("RISK_PCT","5")

# Limits / Safety
MAX_CONCURRENT_TRADES = _get_int("MAX_CONCURRENT_TRADES","3")
MAX_TRADES_PER_DAY    = _get_int("MAX_TRADES_PER_DAY","20")
TC_MAX_LAG_SEC        = _get_int("TC_MAX_LAG_SEC","300")

# Entry rules
ENTRY_EXPIRATION_MIN         = _get_int("ENTRY_EXPIRATION_MIN","180")
ENTRY_TOO_FAR_PCT            = _get_float("ENTRY_TOO_FAR_PCT","0.5")
ENTRY_TRIGGER_BUFFER_PCT     = _get_float("ENTRY_TRIGGER_BUFFER_PCT","0.0")
ENTRY_LIMIT_PRICE_OFFSET_PCT = _get_float("ENTRY_LIMIT_PRICE_OFFSET_PCT","0.0")
ENTRY_EXPIRATION_PRICE_PCT   = _get_float("ENTRY_EXPIRATION_PRICE_PCT","0.6")

# TP/SL
MOVE_SL_TO_BE_ON_TP1 = _get_bool("MOVE_SL_TO_BE_ON_TP1","true")
BE_BUFFER_PCT = _get_float("BE_BUFFER_PCT","0.15")  # Buffer for BE SL (0.15% = slight profit instead of exact entry)
INITIAL_SL_PCT = _get_float("INITIAL_SL_PCT","19.0")  # SL distance from entry in %

# Follow-TP: Move SL to previous TP level after each TP hit
# Example: TP1 hit → SL to BE, TP2 hit → SL to TP1, TP3 hit → SL to TP2
FOLLOW_TP_ENABLED = _get_bool("FOLLOW_TP_ENABLED", "false")
FOLLOW_TP_BUFFER_PCT = _get_float("FOLLOW_TP_BUFFER_PCT", "0.1")  # Buffer above/below TP level

# Max SL distance filter: Skip signals where SL is more than X% from entry
# Set to 0 to disable this filter
MAX_SL_DISTANCE_PCT = _get_float("MAX_SL_DISTANCE_PCT", "0")

# TP_SPLITS: percentage of position to close at each TP level
# Example: 30,30,30 means 90% total, leaving 10% as runner for trailing stop
# For V2 signals with 3-5 TPs, use: 20,20,20,20,20 (adjust as needed)
# DO NOT normalize - allow sum < 100% for runner positions
TP_SPLITS = [float(x) for x in _get("TP_SPLITS","30,30,30").split(",") if x.strip()]
if sum(TP_SPLITS) > 100.0:
    # Only normalize if over 100% (user error)
    s = sum(TP_SPLITS)
    TP_SPLITS = [x * 100.0 / s for x in TP_SPLITS]

# TP_SPLITS_AUTO: if true, automatically calculate equal splits based on number of TPs
# Example: 5 TPs = 20% each, 4 TPs = 25% each, 3 TPs = 33% each
TP_SPLITS_AUTO = _get_bool("TP_SPLITS_AUTO", "false")

# Fallback TP distances (% from entry) if signal has no TPs
FALLBACK_TP_PCT = [float(x) for x in _get("FALLBACK_TP_PCT","0.85,1.65,4.0").split(",") if x.strip()]

TRAIL_AFTER_TP_INDEX = _get_int("TRAIL_AFTER_TP_INDEX","3")  # start trailing when TPn filled
TRAIL_DISTANCE_PCT   = _get_float("TRAIL_DISTANCE_PCT","2.0")
TRAIL_ACTIVATE_ON_TP = _get_bool("TRAIL_ACTIVATE_ON_TP","true")

# DCA sizing multipliers vs BASE qty
# Example: 1.5 means DCA1 = 1.5x base qty
# Only places as many DCAs as there are multipliers (ignores extra DCA prices from signal)
# AO Trading typically has only 1 DCA or none
DCA_QTY_MULTS = [float(x) for x in _get("DCA_QTY_MULTS","1.5").split(",") if x.strip()]

# Timing
POLL_SECONDS    = _get_int("POLL_SECONDS","15")
POLL_JITTER_MAX = _get_int("POLL_JITTER_MAX","5")

# Misc
DRY_RUN     = _get_bool("DRY_RUN","true")
STATE_FILE  = _get("STATE_FILE","state.json")
LOG_LEVEL   = _get("LOG_LEVEL","INFO").upper()

# Telegram Alerts
TELEGRAM_BOT_TOKEN = _get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = _get("TELEGRAM_CHAT_ID")
# Position P&L thresholds to trigger alerts (e.g., 25,35,50 = alert at -25%, -35%, -50%)
POSITION_ALERT_THRESHOLDS = [float(x) for x in _get("POSITION_ALERT_THRESHOLDS", "25,35,50").split(",") if x.strip()]
