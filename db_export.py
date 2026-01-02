"""
PostgreSQL Database Export Module

Exports trade data to PostgreSQL database for dashboard visualization.

Setup:
1. Add PostgreSQL to Railway project
2. Set env var:
   - DATABASE_URL: PostgreSQL connection string (auto-set by Railway)

Example:
DATABASE_URL=postgresql://user:pass@host:5432/dbname
"""

import os
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from decimal import Decimal

log = logging.getLogger("db_export")

# Try to import psycopg2, fall back gracefully
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2.pool import SimpleConnectionPool
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    # Set to None to avoid NameError in type hints
    psycopg2 = None
    RealDictCursor = None
    SimpleConnectionPool = None
    # Don't log warning at import time - causes Railway build issues

# Connection pool (singleton) - use comment-style type hint to avoid NameError
_connection_pool = None  # type: Optional[SimpleConnectionPool]


def _get_connection_pool():
    """Get or create PostgreSQL connection pool."""
    global _connection_pool

    if _connection_pool is not None:
        return _connection_pool

    if not PSYCOPG2_AVAILABLE:
        return None

    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        log.warning("DATABASE_URL not set")
        return None

    try:
        # Create connection pool (min 1, max 5 connections)
        _connection_pool = SimpleConnectionPool(1, 5, db_url)
        log.info("PostgreSQL connection pool created")
        return _connection_pool
    except Exception as e:
        log.error(f"Failed to create connection pool: {e}")
        return None


def _get_connection():
    """Get a connection from the pool."""
    pool = _get_connection_pool()
    if not pool:
        return None
    try:
        return pool.getconn()
    except Exception as e:
        log.error(f"Failed to get connection from pool: {e}")
        return None


def _release_connection(conn):
    """Release connection back to pool."""
    if conn and _connection_pool:
        _connection_pool.putconn(conn)


def init_database() -> bool:
    """Initialize database schema. Returns True on success."""
    conn = _get_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            # First, run migration to add bot_id column if table already exists
            migration_sql = """
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'trades') THEN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name = 'trades' AND column_name = 'bot_id') THEN
                        ALTER TABLE trades ADD COLUMN bot_id VARCHAR(50) DEFAULT 'ao';
                        CREATE INDEX IF NOT EXISTS idx_trades_bot_id ON trades(bot_id);
                    END IF;
                END IF;
            END $$;
            """
            cur.execute(migration_sql)
            conn.commit()

            # Read schema file
            schema_path = os.path.join(os.path.dirname(__file__), "database", "schema.sql")
            if not os.path.exists(schema_path):
                log.error(f"Schema file not found: {schema_path}")
                return False

            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            # Execute schema
            cur.execute(schema_sql)
            conn.commit()
            log.info("Database schema initialized successfully")
            return True
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        conn.rollback()
        return False
    finally:
        _release_connection(conn)


def _ts_to_datetime(ts: Optional[float]) -> Optional[datetime]:
    """Convert Unix timestamp to datetime object."""
    if not ts:
        return None
    return datetime.fromtimestamp(ts)


def export_trade(trade: Dict[str, Any]) -> bool:
    """Export a single trade to database. Returns True on success."""
    conn = _get_connection()
    if not conn:
        return False

    try:
        filled_ts = trade.get("filled_ts") or 0
        closed_ts = trade.get("closed_ts") or 0
        duration_min = round((closed_ts - filled_ts) / 60) if filled_ts and closed_ts else None

        # Calculate PnL percentages
        pnl = trade.get("realized_pnl", 0) or 0
        margin_used = trade.get("margin_used", 0) or 0
        pnl_margin_pct = (pnl / margin_used) * 100 if margin_used > 0 else 0

        equity_after = trade.get("equity_at_close", 0) or 0
        equity_before = equity_after - pnl
        pnl_equity_pct = (pnl / equity_before) * 100 if equity_before > 0 else 0

        # TP count = how many we actually placed (limited by config)
        from config import TP_SPLITS, DCA_QTY_MULTS, BOT_ID
        signal_tp_count = len(trade.get("tp_prices") or [])
        actual_tp_count = min(signal_tp_count, len(TP_SPLITS)) if signal_tp_count else 3

        # Add bot_id to trade (future-proof for multi-bot support)
        bot_id = trade.get("bot_id", BOT_ID)

        with conn.cursor() as cur:
            # Try to insert with bot_id column (new schema)
            # Falls back gracefully if column doesn't exist yet (old schema)
            try:
                cur.execute("""
                    INSERT INTO trades (
                        id, symbol, side, order_side,
                        entry_price, trigger_price, avg_entry,
                        placed_at, filled_at, closed_at, duration_minutes,
                        realized_pnl, pnl_pct_margin, pnl_pct_equity, margin_used, equity_at_close,
                        is_win, exit_reason,
                        tp_fills, tp_count, dca_fills, dca_count, trailing_used,
                        bot_id
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        closed_at = EXCLUDED.closed_at,
                        duration_minutes = EXCLUDED.duration_minutes,
                        realized_pnl = EXCLUDED.realized_pnl,
                        pnl_pct_margin = EXCLUDED.pnl_pct_margin,
                        pnl_pct_equity = EXCLUDED.pnl_pct_equity,
                        equity_at_close = EXCLUDED.equity_at_close,
                        is_win = EXCLUDED.is_win,
                        exit_reason = EXCLUDED.exit_reason,
                        tp_fills = EXCLUDED.tp_fills,
                        dca_fills = EXCLUDED.dca_fills,
                        trailing_used = EXCLUDED.trailing_used,
                        avg_entry = EXCLUDED.avg_entry,
                        bot_id = EXCLUDED.bot_id
                """, (
                    trade.get("id"), trade.get("symbol"), trade.get("pos_side"), trade.get("order_side"),
                    trade.get("entry_price"), trade.get("trigger"), trade.get("avg_entry"),
                    _ts_to_datetime(trade.get("placed_ts")),
                    _ts_to_datetime(filled_ts),
                    _ts_to_datetime(closed_ts),
                    duration_min,
                    pnl, pnl_margin_pct, pnl_equity_pct, margin_used, equity_after,
                    trade.get("is_win", False), trade.get("exit_reason", "unknown"),
                    trade.get("tp_fills", 0), actual_tp_count,
                    trade.get("dca_fills", 0), len(DCA_QTY_MULTS),
                    trade.get("trailing_started", False),
                    bot_id
                ))
            except psycopg2.errors.UndefinedColumn:
                # Column doesn't exist yet - fall back to old schema without bot_id
                log.debug("bot_id column not found, using legacy schema (run migration to add bot_id support)")
                cur.execute("""
                    INSERT INTO trades (
                        id, symbol, side, order_side,
                        entry_price, trigger_price, avg_entry,
                        placed_at, filled_at, closed_at, duration_minutes,
                        realized_pnl, pnl_pct_margin, pnl_pct_equity, margin_used, equity_at_close,
                        is_win, exit_reason,
                        tp_fills, tp_count, dca_fills, dca_count, trailing_used
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        closed_at = EXCLUDED.closed_at,
                        duration_minutes = EXCLUDED.duration_minutes,
                        realized_pnl = EXCLUDED.realized_pnl,
                        pnl_pct_margin = EXCLUDED.pnl_pct_margin,
                        pnl_pct_equity = EXCLUDED.pnl_pct_equity,
                        equity_at_close = EXCLUDED.equity_at_close,
                        is_win = EXCLUDED.is_win,
                        exit_reason = EXCLUDED.exit_reason,
                        tp_fills = EXCLUDED.tp_fills,
                        dca_fills = EXCLUDED.dca_fills,
                        trailing_used = EXCLUDED.trailing_used,
                        avg_entry = EXCLUDED.avg_entry
                """, (
                    trade.get("id"), trade.get("symbol"), trade.get("pos_side"), trade.get("order_side"),
                    trade.get("entry_price"), trade.get("trigger"), trade.get("avg_entry"),
                    _ts_to_datetime(trade.get("placed_ts")),
                    _ts_to_datetime(filled_ts),
                    _ts_to_datetime(closed_ts),
                    duration_min,
                    pnl, pnl_margin_pct, pnl_equity_pct, margin_used, equity_after,
                    trade.get("is_win", False), trade.get("exit_reason", "unknown"),
                    trade.get("tp_fills", 0), actual_tp_count,
                    trade.get("dca_fills", 0), len(DCA_QTY_MULTS),
                    trade.get("trailing_started", False)
                ))
            conn.commit()
            log.info(f"Exported trade {trade.get('id')} to database")
            return True
    except Exception as e:
        log.error(f"Failed to export trade to database: {e}")
        conn.rollback()
        return False
    finally:
        _release_connection(conn)


def update_daily_equity(equity: float, trades_today: int = 0, wins_today: int = 0, losses_today: int = 0) -> bool:
    """Update daily equity snapshot. Returns True on success."""
    conn = _get_connection()
    if not conn:
        return False

    try:
        today = date.today()

        with conn.cursor() as cur:
            # Get previous day's equity for PnL calculation
            cur.execute("""
                SELECT equity FROM daily_equity
                WHERE date < %s
                ORDER BY date DESC
                LIMIT 1
            """, (today,))
            prev = cur.fetchone()
            prev_equity = float(prev[0]) if prev else equity

            daily_pnl = equity - prev_equity
            daily_pnl_pct = (daily_pnl / prev_equity * 100) if prev_equity > 0 else 0

            # Upsert daily equity
            cur.execute("""
                INSERT INTO daily_equity (date, equity, daily_pnl, daily_pnl_pct, trades_count, wins_count, losses_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    equity = EXCLUDED.equity,
                    daily_pnl = EXCLUDED.daily_pnl,
                    daily_pnl_pct = EXCLUDED.daily_pnl_pct,
                    trades_count = EXCLUDED.trades_count,
                    wins_count = EXCLUDED.wins_count,
                    losses_count = EXCLUDED.losses_count
            """, (today, equity, daily_pnl, daily_pnl_pct, trades_today, wins_today, losses_today))
            conn.commit()
            log.debug(f"Updated daily equity: ${equity:.2f} (PnL: ${daily_pnl:+.2f})")
            return True
    except Exception as e:
        log.error(f"Failed to update daily equity: {e}")
        conn.rollback()
        return False
    finally:
        _release_connection(conn)


def get_trades(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """Get trades from database. Returns list of trade dicts."""
    conn = _get_connection()
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM trades
                ORDER BY closed_at DESC NULLS LAST, placed_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            trades = cur.fetchall()
            return [dict(t) for t in trades]
    except Exception as e:
        log.error(f"Failed to fetch trades: {e}")
        return []
    finally:
        _release_connection(conn)


def get_daily_equity(days: int = 30) -> List[Dict[str, Any]]:
    """Get daily equity snapshots. Returns list of equity dicts."""
    conn = _get_connection()
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM daily_equity
                ORDER BY date DESC
                LIMIT %s
            """, (days,))
            equity = cur.fetchall()
            return [dict(e) for e in equity]
    except Exception as e:
        log.error(f"Failed to fetch daily equity: {e}")
        return []
    finally:
        _release_connection(conn)


def get_stats(days: Optional[int] = None) -> Dict[str, Any]:
    """Get trade statistics. days=None for all time."""
    conn = _get_connection()
    if not conn:
        return {}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            date_filter = ""
            params = []
            if days:
                date_filter = "WHERE closed_at >= NOW() - INTERVAL '%s days'"
                params = [days]

            cur.execute(f"""
                SELECT
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN NOT is_win THEN 1 ELSE 0 END) as losses,
                    SUM(realized_pnl) as total_pnl,
                    AVG(realized_pnl) as avg_pnl,
                    MAX(realized_pnl) as best_trade,
                    MIN(realized_pnl) as worst_trade,
                    AVG(tp_fills) as avg_tp_fills,
                    AVG(dca_fills) as avg_dca_fills,
                    SUM(CASE WHEN exit_reason = 'trailing_stop' THEN 1 ELSE 0 END) as trailing_exits,
                    SUM(CASE WHEN exit_reason = 'stop_loss' THEN 1 ELSE 0 END) as sl_exits,
                    SUM(CASE WHEN exit_reason = 'breakeven' THEN 1 ELSE 0 END) as be_exits
                FROM trades
                {date_filter}
            """, params)
            stats = cur.fetchone()

            if not stats or stats['total_trades'] == 0:
                return {"total_trades": 0}

            stats = dict(stats)
            stats['win_rate'] = round(float(stats['wins']) / float(stats['total_trades']) * 100, 1)
            stats['total_pnl'] = float(stats['total_pnl'] or 0)
            stats['avg_pnl'] = float(stats['avg_pnl'] or 0)
            stats['best_trade'] = float(stats['best_trade'] or 0)
            stats['worst_trade'] = float(stats['worst_trade'] or 0)
            stats['avg_tp_fills'] = float(stats['avg_tp_fills'] or 0)
            stats['avg_dca_fills'] = float(stats['avg_dca_fills'] or 0)

            return stats
    except Exception as e:
        log.error(f"Failed to fetch stats: {e}")
        return {}
    finally:
        _release_connection(conn)


def get_active_trade_for_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Check if there's an active trade for this symbol (from any bot).
    Returns the active trade dict if found, None otherwise.

    This is used for symbol locking when running multiple bots on same account.
    """
    conn = _get_connection()
    if not conn:
        return None

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, symbol, bot_id, placed_at, filled_at
                FROM trades
                WHERE symbol = %s AND closed_at IS NULL
                ORDER BY placed_at DESC
                LIMIT 1
                """,
                (symbol,)
            )
            result = cur.fetchone()
            return dict(result) if result else None
    except Exception as e:
        log.error(f"Failed to check active trade for {symbol}: {e}")
        return None
    finally:
        _release_connection(conn)


def is_enabled() -> bool:
    """Check if database export is configured."""
    if not PSYCOPG2_AVAILABLE and os.getenv("DATABASE_URL"):
        # Only warn once at runtime if DB is configured but psycopg2 missing
        if not hasattr(is_enabled, '_warned'):
            log.warning("DATABASE_URL set but psycopg2 not installed. Install with: pip install psycopg2-binary")
            is_enabled._warned = True
    return bool(os.getenv("DATABASE_URL")) and PSYCOPG2_AVAILABLE
