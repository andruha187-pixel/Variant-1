import os
import io
import csv
import json
import time
import math
import zipfile
import sqlite3
import asyncio
import logging
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Optional

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

try:
    from polymarket import AsyncSecureClient, RelayerApiKey
    from polymarket._internal.actions.orders.place import (
        post_order_with_allowance_recovery as sdk_post_order_with_allowance_recovery,
    )
except ImportError:
    AsyncSecureClient = None
    RelayerApiKey = None
    sdk_post_order_with_allowance_recovery = None

load_dotenv()

# ============================================================
# BNB + ETH G/H FIRST-V2 CONSENSUS — PAPER + LIVE
# ============================================================
# Trading targets: BNB and ETH only.
# Signal monitoring: BTC/XRP/BNB/SOL/ETH/DOGE/HYPE, because G/H require
# >=2 DISTINCT OTHER same-side FIRST-V2 votes within the previous 10 seconds.
#
# FIRST-V2 vote (all monitored tokens):
#   price 0.55..0.75, momentum 0.03..0.30, lookback 2 decision ticks.
# G ENTRY (BNB/ETH):
#   target 0.67..0.70, momentum 0.05..0.10,
#   >=2 other same-side FIRST-V2 votes in 10 sec, one ENTRY only.
# H ENTRY: exactly the same gate as G.
# H DCA: after actual ENTRY, arm when held-side ask <=0.50 and elapsed<=120s;
#   no buy on arming tick; later buy once at ask 0.30..0.60 with
#   rebound momentum +0.05..+0.15 and elapsed<=120s.
# No side switching. No strategy stop-loss.
# Default sizes: ENTRY 5 shares; H DCA 5 shares. Sizes are user-configurable.
# ============================================================

VERSION = "17.0-bnb-eth-gh-paper-live"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))

ASSET_CONFIG = {
    "BTC":  {"prefix": "btc-updown-5m",  "label": "Bitcoin"},
    "XRP":  {"prefix": "xrp-updown-5m",  "label": "XRP"},
    "BNB":  {"prefix": "bnb-updown-5m",  "label": "BNB"},
    "SOL":  {"prefix": "sol-updown-5m",  "label": "Solana"},
    "ETH":  {"prefix": "eth-updown-5m",  "label": "Ethereum"},
    "DOGE": {"prefix": "doge-updown-5m", "label": "Dogecoin"},
    "HYPE": {"prefix": "hype-updown-5m", "label": "Hyperliquid"},
}

# These seven chains are SIGNAL SOURCES. Only BNB and ETH can execute trades.
SYMBOLS = ["BTC", "XRP", "BNB", "SOL", "ETH", "DOGE", "HYPE"]
TRADE_SYMBOLS = ["BNB", "ETH"]

DECISION_INTERVAL = float(os.getenv("DECISION_INTERVAL", "3.0"))
TRADE_WINDOW_SECONDS = int(os.getenv("TRADE_WINDOW_SECONDS", "180"))
ENTRY_ORDER_SIZE = float(os.getenv("ENTRY_ORDER_SIZE", "5"))
DCA_ORDER_SIZE = float(os.getenv("DCA_ORDER_SIZE", "5"))
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
MIN_FREE_CASH = float(os.getenv("MIN_FREE_CASH", "5"))
CRYPTO_FEE_RATE = float(os.getenv("CRYPTO_FEE_RATE", "0.07"))
DISCOVERY_INTERVAL = float(os.getenv("DISCOVERY_INTERVAL", "10"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1000"))

MEMORY_CLEANUP_INTERVAL = int(os.getenv("MEMORY_CLEANUP_INTERVAL", "60"))
MEMORY_KEEP_RESOLVED_SEC = int(os.getenv("MEMORY_KEEP_RESOLVED_SEC", "900"))
WS_MAX_CONNECTION_AGE_SEC = int(os.getenv("WS_MAX_CONNECTION_AGE_SEC", "900"))
MEMORY_LOG_INTERVAL = int(os.getenv("MEMORY_LOG_INTERVAL", "300"))

ENTRY_MOVE = float(os.getenv("ENTRY_MOVE", "0.03"))
LOOKBACK_TICKS = int(os.getenv("LOOKBACK_TICKS", "2"))

V2_ELIGIBLE_PRICE_MIN = float(os.getenv("V2_ELIGIBLE_PRICE_MIN", "0.55"))
V2_ELIGIBLE_PRICE_MAX = float(os.getenv("V2_ELIGIBLE_PRICE_MAX", "0.75"))
V2_ELIGIBLE_MOM_MIN = float(os.getenv("V2_ELIGIBLE_MOM_MIN", "0.03"))
V2_ELIGIBLE_MOM_MAX = float(os.getenv("V2_ELIGIBLE_MOM_MAX", "0.30"))

SAFE_ENTRY_MOM_MIN = float(os.getenv("SAFE_ENTRY_MOM_MIN", "0.05"))
SAFE_ENTRY_MOM_MAX = float(os.getenv("SAFE_ENTRY_MOM_MAX", "0.10"))
TIGHT_ENTRY_PRICE_MIN = float(os.getenv("TIGHT_ENTRY_PRICE_MIN", "0.67"))
TIGHT_ENTRY_PRICE_MAX = float(os.getenv("TIGHT_ENTRY_PRICE_MAX", "0.70"))

CONSENSUS_WINDOW_SEC = float(os.getenv("CONSENSUS_WINDOW_SEC", "10"))
CONSENSUS_MIN_OTHER_TOKENS = int(os.getenv("CONSENSUS_MIN_OTHER_TOKENS", "2"))

DCA_ARM_PRICE = float(os.getenv("DCA_ARM_PRICE", "0.50"))
DCA_MIN_BUY_PRICE = float(os.getenv("DCA_MIN_BUY_PRICE", "0.30"))
DCA_MAX_BUY_PRICE = float(os.getenv("DCA_MAX_BUY_PRICE", "0.60"))
DCA_REBOUND_MOM_MIN = float(os.getenv("DCA_REBOUND_MOM_MIN", "0.05"))
DCA_REBOUND_MOM_MAX = float(os.getenv("DCA_REBOUND_MOM_MAX", "0.15"))
DCA_DEADLINE_SEC = float(os.getenv("DCA_DEADLINE_SEC", "120"))

MIN_PRICE = float(os.getenv("MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.95"))

# LIVE is deliberately guarded twice:
# 1) Render/env must explicitly set LIVE_MASTER_ENABLE=1.
# 2) Each BNB/ETH strategy must be switched from PAPER to LIVE in Telegram.
LIVE_MASTER_ENABLE = os.getenv("LIVE_MASTER_ENABLE", "0").strip().lower() in {"1", "true", "yes", "on"}
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
POLYMARKET_WALLET_ADDRESS = (
    os.getenv("POLYMARKET_WALLET_ADDRESS", "").strip()
    or os.getenv("POLYMARKET_DEPOSIT_WALLET", "").strip()
)
POLYMARKET_RELAYER_API_KEY = os.getenv("POLYMARKET_RELAYER_API_KEY", "").strip()
POLYMARKET_RELAYER_API_KEY_ADDRESS = os.getenv("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip()
LIVE_MAX_SHARES_PER_ORDER = float(os.getenv("LIVE_MAX_SHARES_PER_ORDER", "1000"))
LIVE_MIN_SHARES = float(os.getenv("LIVE_MIN_SHARES", "0.01"))

V2_PROBE = {
    "lookback": LOOKBACK_TICKS,
    "v2_price_min": V2_ELIGIBLE_PRICE_MIN,
    "v2_price_max": V2_ELIGIBLE_PRICE_MAX,
    "v2_mom_min": V2_ELIGIBLE_MOM_MIN,
    "v2_mom_max": V2_ELIGIBLE_MOM_MAX,
}


def _strategy_set(symbol):
    common = {
        "symbol": symbol,
        "entry_move": ENTRY_MOVE,
        "lookback": LOOKBACK_TICKS,
        "v2_price_min": V2_ELIGIBLE_PRICE_MIN,
        "v2_price_max": V2_ELIGIBLE_PRICE_MAX,
        "v2_mom_min": V2_ELIGIBLE_MOM_MIN,
        "v2_mom_max": V2_ELIGIBLE_MOM_MAX,
        "safe_entry_price_min": TIGHT_ENTRY_PRICE_MIN,
        "safe_entry_price_max": TIGHT_ENTRY_PRICE_MAX,
        "safe_entry_mom_min": SAFE_ENTRY_MOM_MIN,
        "safe_entry_mom_max": SAFE_ENTRY_MOM_MAX,
        "consensus_enabled": True,
        "consensus_window_sec": CONSENSUS_WINDOW_SEC,
        "consensus_min_other_tokens": CONSENSUS_MIN_OTHER_TOKENS,
        "max_buys_side": 1,
        "dca_enabled": False,
    }
    g = dict(common)
    g.update({
        "code": "G",
        "name": f"{symbol}_G_TIGHT_TWO_V2",
        "short": f"{symbol} / G TIGHT + 2 V2",
    })
    h = dict(common)
    h.update({
        "code": "H",
        "name": f"{symbol}_H_TIGHT_TWO_V2_SAFE_DCA",
        "short": f"{symbol} / H G + SAFE DCA",
        "max_buys_side": 2,
        "dca_enabled": True,
        "dca_arm_price": DCA_ARM_PRICE,
        "dca_min_buy_price": DCA_MIN_BUY_PRICE,
        "dca_max_buy_price": DCA_MAX_BUY_PRICE,
        "dca_rebound_mom": DCA_REBOUND_MOM_MIN,
        "dca_rebound_mom_max": DCA_REBOUND_MOM_MAX,
        "dca_deadline_sec": DCA_DEADLINE_SEC,
    })
    return [g, h]


STRATEGIES = [s for symbol in TRADE_SYMBOLS for s in _strategy_set(symbol)]
STRATEGIES_BY_SYMBOL = {
    symbol: [s for s in STRATEGIES if s["symbol"] == symbol]
    for symbol in TRADE_SYMBOLS
}
STRATEGY_BY_NAME = {x["name"]: x for x in STRATEGIES}

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    probe = DATA_DIR / ".write_test"
    probe.write_text("ok")
    probe.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "bnb_eth_gh_paper_live.db"
REPORT_DIR = DATA_DIR / "bnb_eth_gh_reports_DISABLED"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("bnb-eth-gh-live")

session: Optional[aiohttp.ClientSession] = None

books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()
price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=100)))
strategy_state = {}
settle_lock = asyncio.Lock()


# ============================================================
# HELPERS
# ============================================================

def now_ts():
    return int(time.time())


def now_ms():
    return int(time.time() * 1000)


def utc_iso(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def sf(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def si(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def jd(v):
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def parse_jsonish(v):
    if isinstance(v, list):
        return v
    if v is None:
        return []
    try:
        x = json.loads(v)
        return x if isinstance(x, list) else []
    except Exception:
        return []


def parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def fee_usdc(shares, price):
    fee = shares * CRYPTO_FEE_RATE * price * (1.0 - price)
    return round(fee, 5) if fee >= 0.000005 else 0.0



# ============================================================
# DATABASE / PERSISTENT PAPER ACCOUNTS
# ============================================================

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS discovered_markets (
            condition_id TEXT PRIMARY KEY,
            symbol TEXT,
            question TEXT,
            slug TEXT,
            start_ts INTEGER,
            end_ts INTEGER,
            up_asset TEXT,
            down_asset TEXT,
            discovered_ms INTEGER,
            resolved INTEGER DEFAULT 0,
            winning_asset TEXT,
            winning_outcome TEXT
        );

        CREATE TABLE IF NOT EXISTS gate_decisions (
            condition_id TEXT,
            variant TEXT,
            decision_ms INTEGER,
            elapsed_sec REAL,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            passed INTEGER,
            reason TEXT,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            signal_type TEXT,
            elapsed_sec REAL
        );

        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            signal_type TEXT,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_cost REAL,
            fee REAL,
            total_cost REAL,
            book_age_ms INTEGER,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS dca_events (
            condition_id TEXT,
            variant TEXT,
            armed_ms INTEGER,
            armed_elapsed_sec REAL,
            armed_ask REAL,
            filled_ms INTEGER,
            filled_elapsed_sec REAL,
            filled_ask REAL,
            filled_momentum REAL,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS v2_votes (
            condition_id TEXT PRIMARY KEY,
            symbol TEXT,
            decision_ms INTEGER,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            elapsed_sec REAL
        );

        CREATE TABLE IF NOT EXISTS consensus_events (
            condition_id TEXT,
            variant TEXT,
            decision_ms INTEGER,
            target_symbol TEXT,
            target_outcome TEXT,
            target_ask REAL,
            target_momentum REAL,
            window_sec REAL,
            required_count INTEGER,
            confirm_count INTEGER,
            confirm_symbols_json TEXT,
            confirm_ages_ms_json TEXT,
            passed INTEGER,
            reason TEXT,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS stop_events (
            condition_id TEXT,
            variant TEXT,
            trigger_ms INTEGER,
            trigger_bid REAL,
            stop_price REAL,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS paper_exits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exit_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            reason TEXT,
            trigger_price REAL,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_proceeds REAL,
            fee REAL,
            net_proceeds REAL,
            book_age_ms INTEGER,
            book_received_ms INTEGER,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS live_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            submitted_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            symbol TEXT,
            asset TEXT,
            outcome TEXT,
            action TEXT,
            reason TEXT,
            requested_shares REAL,
            limit_price REAL,
            order_id TEXT,
            status TEXT,
            filled_shares REAL,
            avg_price REAL,
            gross_amount REAL,
            fee_estimate REAL,
            net_or_total REAL,
            trade_ids_json TEXT,
            response_json TEXT,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS market_results (
            condition_id TEXT,
            variant TEXT,
            winning_asset TEXT,
            winning_outcome TEXT,
            buy_cost REAL,
            exit_proceeds REAL,
            payout REAL,
            pnl REAL,
            buy_trades INTEGER,
            exit_trades INTEGER,
            up_bought REAL,
            down_bought REAL,
            up_exited REAL,
            down_exited REAL,
            stopped_out INTEGER,
            execution_mode TEXT,
            settled_ms INTEGER,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS position_trajectory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            elapsed_sec REAL,
            primary_asset TEXT,
            primary_outcome TEXT,
            opposite_asset TEXT,
            bought_shares REAL,
            exited_shares REAL,
            remaining_shares REAL,
            gross_entry_cost REAL,
            entry_fees REAL,
            total_buy_cost REAL,
            exit_net_so_far REAL,
            primary_best_bid REAL,
            primary_best_ask REAL,
            opposite_best_bid REAL,
            opposite_best_ask REAL,
            mark_filled_shares REAL,
            mark_avg_price REAL,
            mark_fee REAL,
            mark_net_proceeds REAL,
            unrealized_total_pnl REAL,
            mfe_pnl REAL,
            mae_pnl REAL,
            stop_triggered INTEGER
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_gate_ms ON gate_decisions(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_ms ON paper_trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_exits_ms ON paper_exits(exit_ms);
        CREATE INDEX IF NOT EXISTS idx_dca_armed_ms ON dca_events(armed_ms);
        CREATE INDEX IF NOT EXISTS idx_consensus_ms ON consensus_events(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_v2_votes_ms ON v2_votes(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_results_ms ON market_results(settled_ms);
        CREATE INDEX IF NOT EXISTS idx_live_orders_ms ON live_orders(submitted_ms);
        CREATE INDEX IF NOT EXISTS idx_live_orders_cond ON live_orders(condition_id,variant,submitted_ms);
        CREATE INDEX IF NOT EXISTS idx_traj_ms ON position_trajectory(sample_ms);
        CREATE INDEX IF NOT EXISTS idx_traj_cond ON position_trajectory(condition_id,variant,sample_ms);
        """)

        defaults = {"trading_enabled": "0"}
        for strategy in STRATEGIES:
            name = strategy["name"]
            defaults[f"mode:{name}"] = "PAPER"
            defaults[f"entry_shares:{name}"] = str(ENTRY_ORDER_SIZE)
            defaults[f"dca_shares:{name}"] = str(DCA_ORDER_SIZE if strategy.get("dca_enabled") else 0)
            defaults[f"paper_initial:{name}"] = str(PAPER_START_BALANCE)
            defaults[f"paper_cash:{name}"] = str(PAPER_START_BALANCE)
        for key, value in defaults.items():
            if conn.execute("SELECT 1 FROM state WHERE key=?", (key,)).fetchone() is None:
                conn.execute("INSERT INTO state(key,value) VALUES(?,?)", (key, value))
        conn.commit()


def state_get(key, default=None):
    with db() as conn:
        row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def state_set(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        conn.commit()


def paper_cash(strategy_name):
    return sf(state_get(f"paper_cash:{strategy_name}", PAPER_START_BALANCE), PAPER_START_BALANCE)


def paper_initial(strategy_name):
    return sf(state_get(f"paper_initial:{strategy_name}", PAPER_START_BALANCE), PAPER_START_BALANCE)


def set_paper_cash(strategy_name, value):
    state_set(f"paper_cash:{strategy_name}", round(float(value), 10))


def trading_enabled():
    return state_get("trading_enabled", "0") == "1"


def strategy_mode(strategy_name):
    mode = str(state_get(f"mode:{strategy_name}", "PAPER") or "PAPER").upper()
    return mode if mode in {"PAPER", "LIVE", "OFF"} else "PAPER"


def entry_shares(strategy_or_name):
    name = strategy_or_name["name"] if isinstance(strategy_or_name, dict) else str(strategy_or_name)
    return max(0.0, sf(state_get(f"entry_shares:{name}", ENTRY_ORDER_SIZE), ENTRY_ORDER_SIZE))


def dca_shares(strategy_or_name):
    strategy = strategy_or_name if isinstance(strategy_or_name, dict) else STRATEGY_BY_NAME.get(str(strategy_or_name))
    name = strategy["name"] if strategy else str(strategy_or_name)
    default = DCA_ORDER_SIZE if strategy and strategy.get("dca_enabled") else 0.0
    return max(0.0, sf(state_get(f"dca_shares:{name}", default), default))


def requested_shares(variant, signal_type):
    return dca_shares(variant) if str(signal_type).upper() == "DCA" else entry_shares(variant)


def _valid_user_shares(value):
    x = sf(value, -1.0)
    return LIVE_MIN_SHARES <= x <= LIVE_MAX_SHARES_PER_ORDER


live_client = None
live_client_ready = False
live_client_error = ""
live_order_locks = defaultdict(asyncio.Lock)


async def init_live_client():
    global live_client, live_client_ready, live_client_error
    live_client_ready = False
    live_client_error = ""

    if AsyncSecureClient is None:
        live_client_error = "polymarket-client is not installed"
        log.warning("LIVE disabled: %s", live_client_error)
        return False

    if not POLYMARKET_PRIVATE_KEY:
        live_client_error = "POLYMARKET_PRIVATE_KEY not configured"
        log.info("LIVE signer not configured; PAPER remains available")
        return False

    try:
        api_key = None
        if POLYMARKET_RELAYER_API_KEY and POLYMARKET_RELAYER_API_KEY_ADDRESS:
            api_key = RelayerApiKey(
                key=POLYMARKET_RELAYER_API_KEY,
                address=POLYMARKET_RELAYER_API_KEY_ADDRESS,
            )

        live_client = await AsyncSecureClient.create(
            private_key=POLYMARKET_PRIVATE_KEY,
            wallet=POLYMARKET_WALLET_ADDRESS or None,
            api_key=api_key,
        )
        live_client_ready = True
        log.info(
            "LIVE wallet ready | wallet=%s | signer=%s | wallet_type=%s | master=%s",
            str(getattr(live_client, "wallet", POLYMARKET_WALLET_ADDRESS)),
            str(getattr(live_client, "signer", "")),
            str(getattr(live_client, "wallet_type", "")),
            "ON" if LIVE_MASTER_ENABLE else "OFF",
        )
        return True
    except Exception as e:
        live_client = None
        live_client_error = f"{type(e).__name__}: {e}"
        log.exception("LIVE wallet initialization failed")
        return False


async def close_live_client():
    global live_client, live_client_ready
    c = live_client
    live_client = None
    live_client_ready = False
    if c is not None:
        try:
            await c.close()
        except Exception:
            log.exception("LIVE client close failed")


async def live_collateral_balance():
    if not live_client_ready or live_client is None:
        return None
    try:
        b = await live_client.get_balance_allowance(asset_type="COLLATERAL")
        return sf(getattr(b, "balance", 0)) / 1_000_000.0
    except Exception:
        log.exception("LIVE balance read failed")
        return None


# ============================================================
# HTTP / BOOK
# ============================================================

async def get_json(url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                text = await r.text()
                if r.status == 200:
                    return json.loads(text)
                log.warning("HTTP %s %s %s -> %s", r.status, url, params, text[:200])
        except Exception as e:
            log.warning("GET %s failed: %s", url, e)
        await asyncio.sleep(0.3 * (attempt + 1))
    return None


def level_map(rows):
    out = {}
    for x in rows or []:
        if not isinstance(x, dict):
            continue
        p = sf(x.get("price"), math.nan)
        q = sf(x.get("size"), 0)
        if not math.isnan(p) and q > 0:
            out[p] = q
    return out


def apply_book(asset, payload, source="ws"):
    books[asset] = {
        "bids": level_map(payload.get("bids")),
        "asks": level_map(payload.get("asks")),
        "received_ms": now_ms(),
        "source": source,
    }


def apply_price_change(payload):
    changes = payload.get("price_changes") or payload.get("priceChanges") or []
    recv = now_ms()
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        asset = str(ch.get("asset_id") or ch.get("token_id") or ch.get("tokenId") or "")
        if not asset:
            continue
        b = books.setdefault(asset, {
            "bids": {}, "asks": {}, "received_ms": recv, "source": "ws-delta"
        })
        p = sf(ch.get("price"), math.nan)
        q = sf(ch.get("size"), 0)
        side = str(ch.get("side", "")).upper()
        if math.isnan(p):
            continue
        target = b["bids"] if side == "BUY" else b["asks"]
        if q <= 0:
            target.pop(p, None)
        else:
            target[p] = q
        b["received_ms"] = recv
        b["source"] = "ws"


def best_ask(asset):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return None
    return min(b["asks"])


def best_bid(asset):
    b = books.get(asset)
    if not b or not b.get("bids"):
        return None
    return max(b["bids"])


async def refresh_book(asset):
    data = await get_json(f"{CLOB_API}/book", params={"token_id": asset})
    if isinstance(data, dict):
        apply_book(asset, data, "rest")
        return True
    return False


async def ensure_book(asset):
    b = books.get(asset)
    if b and b.get("asks"):
        age = now_ms() - b["received_ms"]
        if age <= MAX_BOOK_AGE_MS:
            return age
    await refresh_book(asset)
    b = books.get(asset)
    if not b:
        return None
    return now_ms() - b["received_ms"]


def simulate_buy(asset, wanted):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return [], 0.0
    remaining = wanted
    fills = []
    for p in sorted(b["asks"]):
        q = b["asks"][p]
        take = min(q, remaining)
        if take > 0:
            fills.append((p, take))
            remaining -= take
        if remaining <= 1e-12:
            break
    return fills, wanted - remaining


def simulate_sell(asset, wanted):
    """Walk visible bids from best to worst for an executable PAPER exit mark."""
    b = books.get(asset)
    if not b or not b.get("bids"):
        return [], 0.0
    remaining = wanted
    fills = []
    for p in sorted(b["bids"], reverse=True):
        q = b["bids"][p]
        take = min(q, remaining)
        if take > 0:
            fills.append((p, take))
            remaining -= take
        if remaining <= 1e-12:
            break
    return fills, wanted - remaining


# ============================================================
# MARKET DISCOVERY
# ============================================================

def market_symbol(market):
    sym = str((market or {}).get("symbol") or "").upper()
    if sym in ASSET_CONFIG:
        return sym
    slug = str((market or {}).get("slug") or "").lower()
    for candidate, cfg in ASSET_CONFIG.items():
        if slug.startswith(cfg["prefix"] + "-"):
            return candidate
    return None


def strategies_for_market(market):
    return STRATEGIES_BY_SYMBOL.get(market_symbol(market), [])


def slot_start_from_slug(slug):
    try:
        return int(str(slug).rstrip("/").split("-")[-1])
    except Exception:
        return None


async def fetch_event_by_slug(slug):
    for url, params in (
        (f"{GAMMA_API}/events/slug/{slug}", None),
        (f"{GAMMA_API}/events", {"slug": slug}),
    ):
        data = await get_json(url, params=params)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
    return None


def parse_market_from_event(raw, event, symbol):
    if not isinstance(raw, dict) or symbol not in ASSET_CONFIG:
        return None
    cid = str(raw.get("conditionId") or raw.get("condition_id") or "")
    if not cid:
        return None
    title = str(raw.get("question") or raw.get("title") or event.get("title") or "Unknown")
    slug = str(raw.get("slug") or event.get("slug") or "")
    expected_prefix = ASSET_CONFIG[symbol]["prefix"] + "-"
    if slug and not slug.lower().startswith(expected_prefix):
        return None

    outcomes = [str(x).strip().upper() for x in parse_jsonish(raw.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]
    if len(tokens) < 2:
        return None

    up_asset = down_asset = None
    for i, outcome in enumerate(outcomes):
        if i >= len(tokens):
            break
        if outcome in {"UP", "YES"}:
            up_asset = tokens[i]
        elif outcome in {"DOWN", "NO"}:
            down_asset = tokens[i]
    up_asset = up_asset or tokens[0]
    down_asset = down_asset or tokens[1]

    start_ts = slot_start_from_slug(slug)
    if not start_ts:
        start_dt = parse_iso(raw.get("startDate")) or parse_iso(event.get("startDate"))
        start_ts = int(start_dt.timestamp()) if start_dt else None
    if not start_ts:
        return None

    return {
        "condition_id": cid,
        "symbol": symbol,
        "question": title,
        "slug": slug,
        "start_ts": int(start_ts),
        "end_ts": int(start_ts) + 300,
        "up_asset": str(up_asset),
        "down_asset": str(down_asset),
        "raw": raw,
    }

async def discover_slot_market(symbol, slot_start):
    cfg = ASSET_CONFIG.get(symbol)
    if not cfg:
        return None
    slug = f"{cfg['prefix']}-{slot_start}"
    event = await fetch_event_by_slug(slug)
    if not event or not isinstance(event.get("markets"), list):
        return None
    for raw in event["markets"]:
        market = parse_market_from_event(raw, event, symbol)
        if market:
            return market
    return None

def persist_market(m):
    with db() as conn:
        conn.execute("""
            INSERT INTO discovered_markets(
                condition_id,symbol,question,slug,start_ts,end_ts,up_asset,down_asset,discovered_ms
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                symbol=excluded.symbol, question=excluded.question, slug=excluded.slug,
                start_ts=excluded.start_ts, end_ts=excluded.end_ts,
                up_asset=excluded.up_asset, down_asset=excluded.down_asset
        """, (
            m["condition_id"], market_symbol(m), m["question"], m["slug"],
            m["start_ts"], m["end_ts"], m["up_asset"], m["down_asset"], now_ms(),
        ))
        conn.commit()

async def subscribe_asset(asset):
    if not asset or asset in subscribed_assets:
        return
    subscribed_assets.add(asset)
    await ws_send_queue.put({"operation": "subscribe", "assets_ids": [asset]})


async def discovery_loop():
    last_current_slot = {}
    while True:
        try:
            n = now_ts()
            current = (n // 300) * 300
            for symbol in SYMBOLS:
                candidates = []
                for slot_start in (current, current + 300, current - 300):
                    market = await discover_slot_market(symbol, slot_start)
                    if market:
                        candidates.append(market)

                if not candidates:
                    log.info("Discovery %s: market not found for slot %s", symbol, utc_iso(current))
                    continue

                active = [m for m in candidates if m["start_ts"] - 5 <= n <= m["end_ts"] + 5]
                chosen = min(active or candidates, key=lambda m: abs(n - m["start_ts"]))
                for market in candidates:
                    cid = market["condition_id"]
                    if cid in markets:
                        continue
                    markets[cid] = market
                    persist_market(market)
                    await subscribe_asset(market["up_asset"])
                    await subscribe_asset(market["down_asset"])
                    log.info(
                        "MARKET %s | %s | slug=%s | start=%s",
                        symbol, market["question"], market["slug"], utc_iso(market["start_ts"]),
                    )
                if last_current_slot.get(symbol) != current:
                    log.info("CURRENT %s %s | selected=%s", symbol, utc_iso(current), chosen["slug"])
                    last_current_slot[symbol] = current
        except Exception:
            log.exception("Discovery loop failed")
        await asyncio.sleep(DISCOVERY_INTERVAL)

def parse_ws(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if raw in ("", "PING", "PONG"):
        return []
    try:
        x = json.loads(raw)
        return x if isinstance(x, list) else [x]
    except Exception:
        return []


async def ws_sender(ws):
    while True:
        msg = await ws_send_queue.get()
        try:
            await ws.send(jd(msg))
        except Exception:
            await ws_send_queue.put(msg)
            return


async def ws_ping(ws):
    while True:
        try:
            await ws.send("PING")
        except Exception:
            return
        await asyncio.sleep(10)


async def ws_loop():
    while True:
        try:
            if not subscribed_assets:
                await asyncio.sleep(1)
                continue

            async with websockets.connect(
                MARKET_WS,
                ping_interval=None,
                close_timeout=5,
                max_size=20_000_000,
            ) as ws:
                await ws.send(jd({
                    "assets_ids": list(subscribed_assets),
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                log.info("WS connected | assets=%d", len(subscribed_assets))

                sender = asyncio.create_task(ws_sender(ws))
                ping = asyncio.create_task(ws_ping(ws))
                try:
                    ws_started = time.monotonic()
                    async for raw in ws:
                        if time.monotonic() - ws_started >= WS_MAX_CONNECTION_AGE_SEC:
                            log.info("WS periodic reconnect | active_assets=%d", len(subscribed_assets))
                            break
                        for ev in parse_ws(raw):
                            if not isinstance(ev, dict):
                                continue
                            et = str(ev.get("event_type") or ev.get("type") or "")
                            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else ev
                            if et == "book":
                                asset = str(payload.get("asset_id") or payload.get("token_id") or "")
                                if asset:
                                    apply_book(asset, payload)
                            elif et == "price_change":
                                apply_price_change(payload)
                            elif et == "market_resolved":
                                await settle_from_resolution(payload)
                finally:
                    sender.cancel()
                    ping.cancel()
        except Exception as e:
            log.warning("WS reconnect: %s", e)
            await asyncio.sleep(1)


# ============================================================
# MEMORY / RENDER STABILITY
# ============================================================

def current_rss_mb():
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0
    except Exception:
        pass
    return None


def cleanup_resolved_market_memory():
    cutoff = now_ts() - MEMORY_KEEP_RESOLVED_SEC
    with db() as conn:
        rows = conn.execute(
            "SELECT condition_id FROM discovered_markets WHERE resolved=1 AND end_ts < ?",
            (cutoff,),
        ).fetchall()
    old_cids = {str(r["condition_id"]) for r in rows}
    if not old_cids:
        return 0

    for cid in old_cids:
        markets.pop(cid, None)
        price_history.pop(cid, None)
    for key in list(strategy_state):
        if key[0] in old_cids:
            strategy_state.pop(key, None)

    keep_assets = set()
    for m in markets.values():
        if m.get("up_asset"):
            keep_assets.add(str(m["up_asset"]))
        if m.get("down_asset"):
            keep_assets.add(str(m["down_asset"]))
    for asset in list(books):
        if asset not in keep_assets:
            books.pop(asset, None)
    subscribed_assets.intersection_update(keep_assets)
    return len(old_cids)


async def memory_maintenance_loop():
    last_mem_log = 0.0
    while True:
        try:
            removed = cleanup_resolved_market_memory()
            mono = time.monotonic()
            if removed or mono - last_mem_log >= MEMORY_LOG_INTERVAL:
                rss = current_rss_mb()
                log.info(
                    "MEMORY | RSS=%s | removed_markets=%d | markets=%d | books=%d | state=%d | assets=%d",
                    f"{rss:.1f} MB" if rss is not None else "n/a",
                    removed, len(markets), len(books), len(strategy_state), len(subscribed_assets),
                )
                last_mem_log = mono
        except Exception:
            log.exception("Memory maintenance failed")
        await asyncio.sleep(MEMORY_CLEANUP_INTERVAL)




# ============================================================
# G/H FIRST-V2 CONSENSUS STRATEGY ENGINE
# ============================================================

def get_variant_state(condition, variant):
    key = (condition, variant["name"])
    if key in strategy_state:
        return strategy_state[key]

    st = {
        "buys": defaultdict(int),
        "last_buy": {},
        "started_sides": set(),
        "primary_asset": None,
        "gate_decided": False,
        "gate_passed": False,
        "gate_asset": None,
        "dca_armed": False,
        "dca_filled": False,
        "stopped_out": False,
    }

    with db() as conn:
        gate = conn.execute(
            "SELECT * FROM gate_decisions WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone()
        if gate:
            st["gate_decided"] = True
            st["gate_passed"] = bool(gate["passed"])
            st["gate_asset"] = str(gate["asset"]) if gate["passed"] else None

        rows = []
        for r in conn.execute(
            "SELECT trade_ms AS ms,asset,avg_price,signal_type,filled_shares "
            "FROM paper_trades WHERE condition_id=? AND variant=? AND filled_shares>0",
            (condition, variant["name"]),
        ).fetchall():
            rows.append(dict(r))
        for r in conn.execute(
            "SELECT submitted_ms AS ms,asset,avg_price,reason AS signal_type,filled_shares "
            "FROM live_orders WHERE condition_id=? AND variant=? AND action='BUY' AND filled_shares>0",
            (condition, variant["name"]),
        ).fetchall():
            rows.append(dict(r))
        rows.sort(key=lambda r: si(r.get("ms")))
        for r in rows:
            asset = str(r["asset"])
            st["buys"][asset] += 1
            st["last_buy"][asset] = sf(r["avg_price"])
            st["started_sides"].add(asset)
            if st["primary_asset"] is None:
                st["primary_asset"] = asset
            if str(r.get("signal_type", "")).upper() == "DCA":
                st["dca_filled"] = True

        dca = conn.execute(
            "SELECT * FROM dca_events WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone()
        if dca:
            st["dca_armed"] = True
            st["dca_filled"] = bool(dca["filled_ms"]) or st["dca_filled"]

    strategy_state[key] = st
    return st

def momentum_for(condition, asset, lookback):
    h = price_history[condition][asset]
    if len(h) <= lookback:
        return None, None
    current = h[-1][1]
    ref = h[-1 - lookback][1]
    return current - ref, ref


def store_gate_decision(condition, variant, asset, outcome, ask, ref, mom, elapsed, passed, reason):
    with db() as conn:
        conn.execute("""
            INSERT INTO gate_decisions(
                condition_id,variant,decision_ms,elapsed_sec,asset,outcome,ask,
                reference_ask,momentum,passed,reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (
            condition, variant["name"], now_ms(), elapsed, asset, outcome, ask,
            ref, mom, 1 if passed else 0, reason,
        ))
        conn.commit()


def store_signal(condition, variant, asset, outcome, ask, ref, mom, signal_type, elapsed):
    with db() as conn:
        conn.execute("""
            INSERT INTO signals(
                signal_ms,condition_id,variant,asset,outcome,ask,
                reference_ask,momentum,signal_type,elapsed_sec
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, variant["name"], asset, outcome,
            ask, ref, mom, signal_type, elapsed,
        ))
        conn.commit()


def arm_dca(condition, variant, ask, elapsed):
    st = get_variant_state(condition, variant)
    if st.get("dca_armed"):
        return False
    with db() as conn:
        conn.execute("""
            INSERT INTO dca_events(condition_id,variant,armed_ms,armed_elapsed_sec,armed_ask)
            VALUES(?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (condition, variant["name"], now_ms(), elapsed, ask))
        conn.commit()
    st["dca_armed"] = True
    return True


def mark_dca_filled(condition, variant, ask, mom, elapsed):
    st = get_variant_state(condition, variant)
    with db() as conn:
        conn.execute("""
            UPDATE dca_events
            SET filled_ms=?,filled_elapsed_sec=?,filled_ask=?,filled_momentum=?
            WHERE condition_id=? AND variant=?
        """, (now_ms(), elapsed, ask, mom, condition, variant["name"]))
        conn.commit()
    st["dca_filled"] = True


def trim_fills_to_budget(fills, max_total):
    if max_total <= 0:
        return [], 0.0
    out, spent, shares = [], 0.0, 0.0
    for price, qty in fills:
        price = sf(price)
        qty = sf(qty)
        if price <= 0 or qty <= 0:
            continue
        per_share = price + fee_usdc(1.0, price)
        affordable = max(0.0, (max_total - spent) / per_share)
        take = min(qty, affordable)
        if take <= 1e-9:
            break
        out.append((price, take))
        spent += price * take + fee_usdc(take, price)
        shares += take
        if spent >= max_total - 1e-8:
            break
    return out, shares


def stop_triggered(condition, variant_name):
    with db() as conn:
        return bool(conn.execute(
            "SELECT 1 FROM stop_events WHERE condition_id=? AND variant=?",
            (condition, variant_name),
        ).fetchone())


def _response_json(obj):
    try:
        if hasattr(obj, "model_dump"):
            return jd(obj.model_dump(mode="json"))
        if hasattr(obj, "__dict__"):
            return jd({k: str(v) for k, v in vars(obj).items()})
        return jd({"repr": repr(obj)})
    except Exception:
        return jd({"repr": repr(obj)})


def position_totals(condition, variant_name):
    """Aggregate one strategy/market across PAPER or LIVE execution.

    A mode change is blocked while a position is open, so one market/variant
    normally has exactly one execution mode.
    """
    with db() as conn:
        p_buys = conn.execute(
            "SELECT * FROM paper_trades WHERE condition_id=? AND variant=? ORDER BY id",
            (condition, variant_name),
        ).fetchall()
        p_exits = conn.execute(
            "SELECT * FROM paper_exits WHERE condition_id=? AND variant=? ORDER BY id",
            (condition, variant_name),
        ).fetchall()
        l_rows = conn.execute(
            """SELECT * FROM live_orders
               WHERE condition_id=? AND variant=? AND filled_shares>0
               ORDER BY submitted_ms,id""",
            (condition, variant_name),
        ).fetchall()

    buys = []
    exits = []

    for r in p_buys:
        buys.append({
            "_ms": si(r["trade_ms"]),
            "asset": str(r["asset"]),
            "outcome": str(r["outcome"]),
            "signal_type": str(r["signal_type"]),
            "filled_shares": sf(r["filled_shares"]),
            "avg_price": sf(r["avg_price"]),
            "gross_cost": sf(r["gross_cost"]),
            "fee": sf(r["fee"]),
            "total_cost": sf(r["total_cost"]),
            "mode": "PAPER",
        })
    for r in p_exits:
        exits.append({
            "_ms": si(r["exit_ms"]),
            "asset": str(r["asset"]),
            "outcome": str(r["outcome"]),
            "reason": str(r["reason"]),
            "filled_shares": sf(r["filled_shares"]),
            "avg_price": sf(r["avg_price"]),
            "gross_proceeds": sf(r["gross_proceeds"]),
            "fee": sf(r["fee"]),
            "net_proceeds": sf(r["net_proceeds"]),
            "mode": "PAPER",
        })

    for r in l_rows:
        action = str(r["action"]).upper()
        if action == "BUY":
            buys.append({
                "_ms": si(r["submitted_ms"]),
                "asset": str(r["asset"]),
                "outcome": str(r["outcome"]),
                "signal_type": str(r["reason"]),
                "filled_shares": sf(r["filled_shares"]),
                "avg_price": sf(r["avg_price"]),
                "gross_cost": sf(r["gross_amount"]),
                "fee": sf(r["fee_estimate"]),
                "total_cost": sf(r["net_or_total"]),
                "mode": "LIVE",
            })
        elif action == "SELL":
            exits.append({
                "_ms": si(r["submitted_ms"]),
                "asset": str(r["asset"]),
                "outcome": str(r["outcome"]),
                "reason": str(r["reason"]),
                "filled_shares": sf(r["filled_shares"]),
                "avg_price": sf(r["avg_price"]),
                "gross_proceeds": sf(r["gross_amount"]),
                "fee": sf(r["fee_estimate"]),
                "net_proceeds": sf(r["net_or_total"]),
                "mode": "LIVE",
            })

    buys.sort(key=lambda r: r["_ms"])
    exits.sort(key=lambda r: r["_ms"])

    bought = sum(sf(r["filled_shares"]) for r in buys)
    exited = sum(sf(r["filled_shares"]) for r in exits)
    buy_cost = sum(sf(r["total_cost"]) for r in buys)
    exit_net = sum(sf(r["net_proceeds"]) for r in exits)
    primary_asset = str(buys[0]["asset"]) if buys else None
    primary_outcome = str(buys[0]["outcome"]) if buys else None
    dca_trades = sum(1 for r in buys if str(r["signal_type"]).upper() == "DCA")

    modes = {str(r.get("mode", "")).upper() for r in buys + exits}
    execution_mode = "LIVE" if "LIVE" in modes else ("PAPER" if "PAPER" in modes else None)

    return {
        "buys": buys,
        "exits": exits,
        "bought": bought,
        "exited": exited,
        "remaining": max(0.0, bought - exited),
        "buy_cost": buy_cost,
        "exit_net": exit_net,
        "primary_asset": primary_asset,
        "primary_outcome": primary_outcome,
        "dca_trades": dca_trades,
        "has_dca": dca_trades > 0,
        "execution_mode": execution_mode,
    }


def live_action_ambiguous(condition, variant_name, action, reason):
    with db() as conn:
        return bool(conn.execute(
            """SELECT 1 FROM live_orders
               WHERE condition_id=? AND variant=? AND action=? AND reason=?
                 AND status IN ('AMBIGUOUS','DELAYED_AMBIGUOUS')
               LIMIT 1""",
            (condition, variant_name, str(action).upper(), str(reason)),
        ).fetchone())


def _visible_fak_limit(asset, wanted, side):
    """Worst visible price needed for up to `wanted` shares.

    This keeps LIVE execution close to the PAPER book walk: the FAK order can
    take liquidity at this price or better, but cannot chase beyond the
    snapshot used by PAPER.
    """
    b = books.get(asset) or {}
    side = str(side).upper()
    levels = b.get("asks") if side == "BUY" else b.get("bids")
    if not levels:
        return None, 0.0

    remaining = float(wanted)
    filled_visible = 0.0
    worst = None
    prices = sorted(levels) if side == "BUY" else sorted(levels, reverse=True)
    for px in prices:
        q = max(0.0, sf(levels[px]))
        take = min(q, remaining)
        if take > 0:
            worst = sf(px)
            filled_visible += take
            remaining -= take
        if remaining <= 1e-9:
            break
    return worst, filled_visible


async def execute_live_fak(condition, variant, asset, outcome, reason, action, wanted):
    """Place an exact-share IOC/FAK order using the current visible book.

    BUY: exact maximum share size via a signed LIMIT order converted to FAK.
    SELL: same for the stop liquidation.
    """
    name = variant["name"]
    symbol = variant["symbol"]
    action = str(action).upper()
    wanted = sf(wanted)

    if not LIVE_MASTER_ENABLE:
        log.error("LIVE BLOCK %s: LIVE_MASTER_ENABLE=0", name)
        return {"ok": False, "filled": 0.0, "error": "LIVE_MASTER_ENABLE=0"}
    if not live_client_ready or live_client is None:
        log.error("LIVE BLOCK %s: wallet client not ready (%s)", name, live_client_error)
        return {"ok": False, "filled": 0.0, "error": live_client_error or "wallet_not_ready"}
    if not _valid_user_shares(wanted):
        return {"ok": False, "filled": 0.0, "error": f"invalid shares {wanted}"}

    lock = live_order_locks[(condition, name)]
    async with lock:
        # If a network/API exception happened after submission, we cannot know
        # safely whether the exchange accepted the previous order. Never retry
        # the same action automatically: missing a trade is safer than duplicating
        # a real-money order. The block expires naturally with this 5-minute market.
        if live_action_ambiguous(condition, name, action, reason):
            log.error("LIVE FAIL-CLOSED %s %s %s: previous submission is ambiguous", name, action, reason)
            return {"ok": False, "filled": 0.0, "error": "previous_submission_ambiguous"}

        await ensure_book(asset)

        limit_price, visible = _visible_fak_limit(asset, wanted, action)
        if limit_price is None or visible <= 1e-9:
            return {"ok": False, "filled": 0.0, "error": "no_visible_liquidity"}

        # Book prices are already valid Polymarket ticks. Decimal(str(...)) avoids
        # adding binary-float noise to the signed price.
        limit_str = format(Decimal(str(limit_price)), "f")
        size_str = format(Decimal(str(wanted)), "f")
        submitted = now_ms()

        try:
            signed = await live_client.create_limit_order(
                token_id=str(asset),
                price=limit_str,
                size=size_str,
                side=action,
                post_only=False,
            )
            # SignedOrder is a frozen dataclass and supports order_type FAK.
            fak_order = replace(signed, order_type="FAK", post_only=False)
            if sdk_post_order_with_allowance_recovery is not None:
                response = await sdk_post_order_with_allowance_recovery(live_client, fak_order)
            else:
                # Test/offline fallback; production requirements pin the SDK version
                # that provides allowance-recovery placement.
                response = await live_client.post_order(fak_order)

            ok = bool(getattr(response, "ok", False))
            if not ok:
                error = f"{getattr(response, 'code', 'rejected')}: {getattr(response, 'message', '')}".strip()
                with db() as conn:
                    conn.execute("""
                        INSERT INTO live_orders(
                            submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                            requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                            gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        submitted, condition, name, symbol, asset, outcome, action, reason,
                        wanted, limit_price, "", "REJECTED", 0.0, None, 0.0, 0.0, 0.0,
                        "[]", _response_json(response), error,
                    ))
                    conn.commit()
                log.warning("LIVE REJECT %s %s %s | %s", name, action, reason, error)
                return {"ok": False, "filled": 0.0, "error": error}

            making = sf(getattr(response, "making_amount", 0))
            taking = sf(getattr(response, "taking_amount", 0))
            status = str(getattr(response, "status", ""))
            order_id = str(getattr(response, "order_id", ""))
            trade_ids = tuple(getattr(response, "trade_ids", ()) or ())

            # CLOB order denomination:
            # BUY makes collateral and takes shares; SELL makes shares and takes collateral.
            if action == "BUY":
                filled = taking
                gross = making
            else:
                filled = making
                gross = taking

            avg = gross / filled if filled > 1e-9 else 0.0
            fee = fee_usdc(filled, avg) if filled > 1e-9 else 0.0
            net_or_total = gross + fee if action == "BUY" else gross - fee

            stored_status = status
            if filled <= 1e-9 and status.lower() in {"delayed", "live", "matched"}:
                stored_status = "DELAYED_AMBIGUOUS"

            with db() as conn:
                conn.execute("""
                    INSERT INTO live_orders(
                        submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                        requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                        gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    submitted, condition, name, symbol, asset, outcome, action, reason,
                    wanted, limit_price, order_id, stored_status, filled, avg, gross, fee,
                    net_or_total, jd(list(trade_ids)), _response_json(response), "",
                ))
                conn.commit()

            if stored_status == "DELAYED_AMBIGUOUS":
                log.error("LIVE AMBIGUOUS %s %s %s | order_id=%s status=%s", name, action, reason, order_id, status)
                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    await tg_send(
                        f"⚠️ LIVE AMBIGUOUS {symbol}\n"
                        f"{action} {reason}: exchange returned {status} without a measurable fill.\n"
                        "This market/action is fail-closed: the bot will NOT retry automatically."
                    )

            if filled > 1e-9 and action == "BUY":
                st = get_variant_state(condition, variant)
                st["buys"][asset] += 1
                st["last_buy"][asset] = avg
                st["started_sides"].add(asset)
                if st["primary_asset"] is None:
                    st["primary_asset"] = asset

            if filled > 1e-9:
                log.warning(
                    "🔴 LIVE %s %-20s %-7s %s | %.4fsh @ %.4f | limit %.4f | status=%s",
                    action, name, reason, outcome, filled, avg, limit_price, status,
                )
                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    await tg_send(
                        f"🔴 LIVE {action} {symbol}\n"
                        f"{reason} {outcome}: {filled:.4f}sh @ {avg:.4f}\n"
                        f"limit {limit_price:.4f} | {status}"
                    )
            return {
                "ok": True,
                "filled": filled,
                "avg": avg,
                "gross": gross,
                "fee": fee,
                "net_or_total": net_or_total,
                "status": status,
                "order_id": order_id,
            }

        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            with db() as conn:
                conn.execute("""
                    INSERT INTO live_orders(
                        submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                        requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                        gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    submitted, condition, name, symbol, asset, outcome, action, reason,
                    wanted, limit_price, "", "AMBIGUOUS", 0.0, None, 0.0, 0.0, 0.0,
                    "[]", "{}", error,
                ))
                conn.commit()
            log.exception("LIVE order failed | %s %s %s", name, action, reason)
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"⚠️ LIVE ORDER AMBIGUOUS {symbol}\n"
                    f"{action} {reason}: {error}\n"
                    "Automatic retry for this market/action is blocked to prevent a duplicate real order."
                )
            return {"ok": False, "filled": 0.0, "error": error}


async def execute_paper(condition, variant, asset, outcome, signal_type):
    age = await ensure_book(asset)

    wanted = requested_shares(variant, signal_type)
    if not _valid_user_shares(wanted):
        log.warning("PAPER BLOCK %s %s invalid shares %.4f", variant["name"], signal_type, wanted)
        return False

    fills, filled = simulate_buy(asset, wanted)
    if filled <= 0:
        return False

    name = variant["name"]
    cash = paper_cash(name)
    available = max(0.0, cash - MIN_FREE_CASH)
    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    total = gross + fee

    if total > available + 1e-8:
        fills, filled = trim_fills_to_budget(fills, available)
        if filled <= 1e-8:
            log.warning("CASH BLOCK %s %s %s | cash=%.2f", name, signal_type, outcome, cash)
            return False
        gross = sum(p * q for p, q in fills)
        fee = sum(fee_usdc(q, p) for p, q in fills)
        total = gross + fee

    avg = gross / filled
    after = cash - total
    with db() as conn:
        conn.execute("""
            INSERT INTO paper_trades(
                trade_ms,condition_id,variant,asset,outcome,signal_type,
                requested_shares,filled_shares,avg_price,gross_cost,fee,
                total_cost,book_age_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, name, asset, outcome, signal_type,
            wanted, filled, avg, gross, fee, total, age,
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"paper_cash:{name}", str(after)),
        )
        conn.commit()

    st = get_variant_state(condition, variant)
    st["buys"][asset] += 1
    st["last_buy"][asset] = avg
    st["started_sides"].add(asset)
    if st["primary_asset"] is None:
        st["primary_asset"] = asset

    log.info(
        "PAPER BUY %-28s %-7s %-4s | %.2fsh @ %.4f fee=%.4f | cash %.2f -> %.2f",
        name, signal_type, outcome, filled, avg, fee, cash, after,
    )
    return True


async def execute_order(condition, variant, asset, outcome, signal_type):
    mode = strategy_mode(variant["name"])
    if mode == "OFF":
        return False
    if mode == "PAPER":
        return await execute_paper(condition, variant, asset, outcome, signal_type)
    wanted = requested_shares(variant, signal_type)
    result = await execute_live_fak(
        condition, variant, asset, outcome, signal_type, "BUY", wanted
    )
    return sf(result.get("filled")) > 1e-9


def _first_v2_eligible_candidates(market, variant):
    cid = market["condition_id"]
    out = []
    for asset, outcome in ((market["up_asset"], "Up"), (market["down_asset"], "Down")):
        ask = best_ask(asset)
        if ask is None or not (variant["v2_price_min"] <= ask <= variant["v2_price_max"]):
            continue
        mom, ref = momentum_for(cid, asset, variant["lookback"])
        if mom is None:
            continue
        if mom < variant["v2_mom_min"] or mom > variant["v2_mom_max"]:
            continue
        out.append((mom, asset, outcome, ask, ref))
    out.sort(reverse=True, key=lambda x: x[0])
    return out


def record_first_v2_vote(market, elapsed, decision_ms):
    """Record exactly one FIRST V2-eligible vote for every monitored token/market."""
    cid = market["condition_id"]
    with db() as conn:
        if conn.execute("SELECT 1 FROM v2_votes WHERE condition_id=? LIMIT 1", (cid,)).fetchone():
            return

    symbol = market_symbol(market)
    if symbol not in SYMBOLS:
        return
    candidates = _first_v2_eligible_candidates(market, V2_PROBE)
    if not candidates:
        return

    mom, asset, outcome, ask, ref = candidates[0]
    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO v2_votes(
                condition_id,symbol,decision_ms,asset,outcome,ask,
                reference_ask,momentum,elapsed_sec
            ) VALUES(?,?,?,?,?,?,?,?,?)
        """, (cid, symbol, int(decision_ms), asset, outcome, ask, ref, mom, elapsed))
        conn.commit()
    log.info("V2 VOTE %-4s %s | %s %.3f mom=%+.3f", symbol, cid[-6:], outcome, ask, mom)


def first_v2_vote(condition_id):
    with db() as conn:
        return conn.execute("SELECT * FROM v2_votes WHERE condition_id=?", (condition_id,)).fetchone()


def consensus_confirmations(target_symbol, outcome, at_ms, window_sec):
    cutoff = int(at_ms - float(window_sec) * 1000.0)
    with db() as conn:
        rows = conn.execute("""
            SELECT symbol,decision_ms,condition_id,ask,momentum,outcome
            FROM v2_votes
            WHERE outcome=? AND decision_ms>=? AND decision_ms<=? AND symbol<>?
            ORDER BY decision_ms DESC
        """, (outcome, cutoff, at_ms, target_symbol)).fetchall()
    latest = {}
    for r in rows:
        symbol = str(r["symbol"] or "").upper()
        if symbol not in SYMBOLS or symbol == target_symbol:
            continue
        if symbol not in latest:
            latest[symbol] = int(r["decision_ms"])
    ordered = sorted(latest.items(), key=lambda kv: kv[1], reverse=True)
    return [s for s, _ in ordered], [max(0, int(at_ms-ms)) for _, ms in ordered]


def store_consensus_event(condition, variant, symbol, outcome, ask, mom, at_ms,
                          confirm_symbols, confirm_ages, passed, reason):
    with db() as conn:
        conn.execute("""
            INSERT INTO consensus_events(
                condition_id,variant,decision_ms,target_symbol,target_outcome,
                target_ask,target_momentum,window_sec,required_count,confirm_count,
                confirm_symbols_json,confirm_ages_ms_json,passed,reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (
            condition, variant["name"], at_ms, symbol, outcome, ask, mom,
            float(variant["consensus_window_sec"]), int(variant["consensus_min_other_tokens"]),
            len(confirm_symbols), jd(confirm_symbols), jd(confirm_ages),
            1 if passed else 0, reason,
        ))
        conn.commit()


async def evaluate_consensus_variant(market, variant, elapsed):
    """Exact G/H target gate + H safer reversal DCA; execution routes PAPER/LIVE."""
    cid = market["condition_id"]
    symbol = market_symbol(market)
    st = get_variant_state(cid, variant)

    if not st["gate_decided"] and not st["started_sides"]:
        vote = first_v2_vote(cid)
        if vote is None:
            record_first_v2_vote(market, elapsed, now_ms())
            vote = first_v2_vote(cid)
        if vote is None:
            return

        mom = sf(vote["momentum"])
        asset = str(vote["asset"])
        outcome = str(vote["outcome"])
        ask = sf(vote["ask"])
        ref = sf(vote["reference_ask"])
        at_ms = si(vote["decision_ms"])

        price_ok = variant["safe_entry_price_min"] <= ask <= variant["safe_entry_price_max"]
        mom_ok = variant["safe_entry_mom_min"] <= mom <= variant["safe_entry_mom_max"]
        safe_ok = bool(price_ok and mom_ok)

        confirm_symbols, confirm_ages = [], []
        consensus_ok = False
        if safe_ok:
            confirm_symbols, confirm_ages = consensus_confirmations(
                symbol, outcome, at_ms, variant["consensus_window_sec"]
            )
            consensus_ok = len(confirm_symbols) >= int(variant["consensus_min_other_tokens"])

        passed = bool(safe_ok and consensus_ok)
        st["gate_decided"] = True
        st["gate_passed"] = passed
        st["gate_asset"] = asset if passed else None

        if ask < variant["safe_entry_price_min"]:
            reason = "SAFE_PRICE_LOW"
        elif ask > variant["safe_entry_price_max"]:
            reason = "SAFE_PRICE_HIGH"
        elif mom < variant["safe_entry_mom_min"]:
            reason = "SAFE_MOMENTUM_LOW"
        elif mom > variant["safe_entry_mom_max"]:
            reason = "SAFE_MOMENTUM_HIGH"
        elif not consensus_ok:
            reason = "V2_CONSENSUS_INSUFFICIENT"
        else:
            reason = "V2_CONSENSUS_OK"

        store_gate_decision(cid, variant, asset, outcome, ask, ref, mom, elapsed, passed, reason)
        store_consensus_event(
            cid, variant, symbol, outcome, ask, mom, at_ms,
            confirm_symbols, confirm_ages, passed, reason
        )
        log.info(
            "V2 CONS %-30s %s | %s %.3f mom=%+.3f | need=%d votes=%d [%s] | %s",
            variant["name"], cid[-6:], outcome, ask, mom,
            int(variant["consensus_min_other_tokens"]), len(confirm_symbols),
            ",".join(confirm_symbols) if confirm_symbols else "-",
            "PASS" if passed else f"SKIP {reason}",
        )
        if not passed:
            return

    if st["gate_decided"] and not st["gate_passed"]:
        return

    # ENTRY uses the current target book/momentum after the first-V2 gate passes.
    if not st["started_sides"]:
        asset = st.get("gate_asset")
        if not asset:
            return
        outcome = "Up" if asset == market["up_asset"] else "Down"
        ask = best_ask(asset)
        if ask is None:
            return
        mom, ref = momentum_for(cid, asset, variant["lookback"])
        if mom is None:
            return
        if not (variant["safe_entry_price_min"] <= ask <= variant["safe_entry_price_max"]):
            return
        if not (variant["safe_entry_mom_min"] <= mom <= variant["safe_entry_mom_max"]):
            return
        store_signal(cid, variant, asset, outcome, ask, ref, mom, "ENTRY", elapsed)
        await execute_order(cid, variant, asset, outcome, "ENTRY")
        return

    if not variant.get("dca_enabled"):
        return

    asset = st.get("primary_asset")
    if not asset or st["buys"][asset] >= variant["max_buys_side"] or st.get("dca_filled"):
        return
    if elapsed > variant["dca_deadline_sec"]:
        return

    ask = best_ask(asset)
    if ask is None or ask < MIN_PRICE or ask > MAX_PRICE:
        return
    outcome = "Up" if asset == market["up_asset"] else "Down"

    # Arm on weakness; never buy on the arm tick.
    if not st.get("dca_armed"):
        if ask <= variant["dca_arm_price"] + 1e-12:
            arm_dca(cid, variant, ask, elapsed)
            log.info(
                "DCA ARMED %-28s %s | %s ask=%.3f <= %.3f | elapsed=%.1fs",
                variant["name"], cid[-6:], outcome, ask, variant["dca_arm_price"], elapsed,
            )
        return

    mom, ref = momentum_for(cid, asset, variant["lookback"])
    if mom is None or mom < variant["dca_rebound_mom"]:
        return
    if mom > float(variant["dca_rebound_mom_max"]) + 1e-12:
        return
    if ask < float(variant["dca_min_buy_price"]) - 1e-12:
        return
    if ask > float(variant["dca_max_buy_price"]) + 1e-12:
        return

    store_signal(cid, variant, asset, outcome, ask, ref, mom, "DCA", elapsed)
    filled = await execute_order(cid, variant, asset, outcome, "DCA")
    if filled:
        mark_dca_filled(cid, variant, ask, mom, elapsed)
        log.info(
            "DCA FILLED %-27s %s | %s ask=%.3f mom=%+.3f | total buys=%d",
            variant["name"], cid[-6:], outcome, ask, mom, st["buys"][asset],
        )

# No stop-loss engine: G/H source strategy has no stop-loss.


def record_position_trajectory(market, variant, elapsed):
    cid = market["condition_id"]
    with db() as conn:
        if conn.execute(
            "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
            (cid, variant["name"]),
        ).fetchone():
            return False

    pos = position_totals(cid, variant["name"])
    if not pos["buys"]:
        return False

    primary_asset = pos["primary_asset"]
    primary_outcome = pos["primary_outcome"]
    opposite_asset = str(market["down_asset"] if primary_asset == str(market["up_asset"]) else market["up_asset"])
    remaining = pos["remaining"]

    p_bid = best_bid(primary_asset)
    p_ask = best_ask(primary_asset)
    o_bid = best_bid(opposite_asset)
    o_ask = best_ask(opposite_asset)

    mark_fills, mark_filled = simulate_sell(primary_asset, remaining) if remaining > 1e-9 else ([], 0.0)
    mark_gross = sum(sf(px) * sf(q) for px, q in mark_fills)
    mark_fee = sum(fee_usdc(sf(q), sf(px)) for px, q in mark_fills)
    mark_net = mark_gross - mark_fee
    mark_avg = mark_gross / mark_filled if mark_filled > 1e-9 else None

    # Total PnL if all remaining shares could be liquidated now.
    unrealized = None
    if remaining <= 1e-9:
        unrealized = pos["exit_net"] - pos["buy_cost"]
    elif mark_filled >= remaining - 1e-8:
        unrealized = pos["exit_net"] + mark_net - pos["buy_cost"]

    with db() as conn:
        prev = conn.execute("""
            SELECT MAX(unrealized_total_pnl) mfe, MIN(unrealized_total_pnl) mae
            FROM position_trajectory
            WHERE condition_id=? AND variant=? AND unrealized_total_pnl IS NOT NULL
        """, (cid, variant["name"])).fetchone()
        prev_mfe = sf(prev["mfe"]) if prev and prev["mfe"] is not None else None
        prev_mae = sf(prev["mae"]) if prev and prev["mae"] is not None else None
        mfe = prev_mfe if unrealized is None else (unrealized if prev_mfe is None else max(prev_mfe, unrealized))
        mae = prev_mae if unrealized is None else (unrealized if prev_mae is None else min(prev_mae, unrealized))

        conn.execute("""
            INSERT INTO position_trajectory(
                sample_ms,condition_id,variant,elapsed_sec,primary_asset,primary_outcome,
                opposite_asset,bought_shares,exited_shares,remaining_shares,gross_entry_cost,
                entry_fees,total_buy_cost,exit_net_so_far,primary_best_bid,primary_best_ask,
                opposite_best_bid,opposite_best_ask,mark_filled_shares,mark_avg_price,mark_fee,
                mark_net_proceeds,unrealized_total_pnl,mfe_pnl,mae_pnl,stop_triggered
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), cid, variant["name"], elapsed, primary_asset, primary_outcome,
            opposite_asset, pos["bought"], pos["exited"], remaining,
            sum(sf(r["gross_cost"]) for r in pos["buys"]),
            sum(sf(r["fee"]) for r in pos["buys"]), pos["buy_cost"], pos["exit_net"],
            p_bid, p_ask, o_bid, o_ask, mark_filled, mark_avg, mark_fee, mark_net,
            unrealized, mfe, mae, 0,
        ))
        conn.commit()
    return True


async def strategy_loop():
    while True:
        started = time.monotonic()
        n = time.time()
        try:
            trade_ready = []
            for cid, market in list(markets.items()):
                elapsed = n - market["start_ts"]
                if not (-30 <= elapsed <= 310):
                    continue

                # One shared WS-book sample per active market; no pre-decision REST refresh.
                for asset in (market["up_asset"], market["down_asset"]):
                    ask = best_ask(asset)
                    if ask is not None:
                        price_history[cid][asset].append((now_ms(), ask))

                variants = strategies_for_market(market)
                if 0 <= elapsed <= 305:
                    for variant in variants:
                        record_position_trajectory(market, variant, elapsed)

                if elapsed < 0 or elapsed > TRADE_WINDOW_SECONDS or not trading_enabled():
                    continue
                if best_ask(market["up_asset"]) is None or best_ask(market["down_asset"]) is None:
                    continue
                trade_ready.append((market, elapsed, variants))

            # Phase 1: all seven monitored tokens can write a FIRST-V2 vote using
            # exactly one decision-cycle timestamp. Monitor-only tokens never trade.
            cycle_ms = now_ms()
            for market, elapsed, _variants in trade_ready:
                record_first_v2_vote(market, elapsed, cycle_ms)

            # Phase 2: execute only BNB/ETH G/H strategies.
            for market, elapsed, variants in trade_ready:
                for variant in variants:
                    if strategy_mode(variant["name"]) == "OFF":
                        continue
                    await evaluate_consensus_variant(market, variant, elapsed)
        except Exception:
            log.exception("Strategy loop failed")

        spent = time.monotonic() - started
        await asyncio.sleep(max(0.05, DECISION_INTERVAL - spent))

async def settle_from_resolution(ev):
    cid = str(ev.get("market") or ev.get("condition_id") or "")
    winning_asset = str(ev.get("winning_asset_id") or ev.get("winning_asset") or "")
    winning_outcome = str(ev.get("winning_outcome") or "")
    if cid and winning_asset:
        await settle_market(cid, winning_asset, winning_outcome)


async def settle_market(cid, winning_asset, winning_outcome):
    async with settle_lock:
        market = markets.get(cid)
        if not market:
            with db() as conn:
                row = conn.execute(
                    "SELECT * FROM discovered_markets WHERE condition_id=?", (cid,)
                ).fetchone()
                if not row:
                    return
                market = dict(row)

        symbol = market_symbol(market)
        pair = STRATEGIES_BY_SYMBOL.get(symbol, [])
        messages = []

        for variant in pair:
            name = variant["name"]
            with db() as conn:
                if conn.execute(
                    "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchone():
                    continue

            pos = position_totals(cid, name)
            buys = pos["buys"]
            exits = pos["exits"]
            buy_cost = pos["buy_cost"]
            exit_proceeds = pos["exit_net"]

            up_bought = sum(
                sf(r["filled_shares"]) for r in buys
                if str(r["asset"]) == str(market["up_asset"])
            )
            down_bought = sum(
                sf(r["filled_shares"]) for r in buys
                if str(r["asset"]) == str(market["down_asset"])
            )
            up_exited = sum(
                sf(r["filled_shares"]) for r in exits
                if str(r["asset"]) == str(market["up_asset"])
            )
            down_exited = sum(
                sf(r["filled_shares"]) for r in exits
                if str(r["asset"]) == str(market["down_asset"])
            )
            winning_bought = sum(
                sf(r["filled_shares"]) for r in buys
                if str(r["asset"]) == str(winning_asset)
            )
            winning_exited = sum(
                sf(r["filled_shares"]) for r in exits
                if str(r["asset"]) == str(winning_asset)
            )

            payout = max(0.0, winning_bought - winning_exited)
            pnl = exit_proceeds + payout - buy_cost
            execution_mode = pos.get("execution_mode") or strategy_mode(name)

            with db() as conn:
                stopped = 0

                conn.execute("""
                    INSERT INTO market_results(
                        condition_id,variant,winning_asset,winning_outcome,buy_cost,
                        exit_proceeds,payout,pnl,buy_trades,exit_trades,up_bought,
                        down_bought,up_exited,down_exited,stopped_out,execution_mode,settled_ms
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    cid, name, winning_asset, winning_outcome, buy_cost,
                    exit_proceeds, payout, pnl, len(buys), len(exits), up_bought,
                    down_bought, up_exited, down_exited, stopped, execution_mode, now_ms(),
                ))

                cash_after = None
                # Only the PAPER ledger receives synthetic $1/share settlement.
                # LIVE winning shares remain on the actual Polymarket wallet and are
                # not auto-redeemed by this bot.
                if execution_mode == "PAPER":
                    cash_row = conn.execute(
                        "SELECT value FROM state WHERE key=?", (f"paper_cash:{name}",)
                    ).fetchone()
                    cash_before = sf(
                        cash_row["value"] if cash_row else PAPER_START_BALANCE,
                        PAPER_START_BALANCE,
                    )
                    cash_after = cash_before + payout
                    conn.execute(
                        "INSERT INTO state(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (f"paper_cash:{name}", str(cash_after)),
                    )
                conn.commit()

            if buys:
                mode_tag = "🔴 LIVE" if execution_mode == "LIVE" else "🟢 PAPER"
                tail = f" | paper cash ${cash_after:.2f}" if cash_after is not None else " | payout not auto-redeemed"
                messages.append(
                    f"{mode_tag} {variant['short']}: PnL~{pnl:+.2f}{tail}"
                    + (" | STOP" if stopped else "")
                )

        with db() as conn:
            conn.execute("""
                UPDATE discovered_markets
                SET resolved=1,winning_asset=?,winning_outcome=?
                WHERE condition_id=?
            """, (winning_asset, winning_outcome, cid))
            conn.commit()

        if cid in markets:
            markets[cid]["resolved"] = 1
        if messages:
            log.info("RESOLVED %s %s | %s", symbol, cid[-6:], " | ".join(messages))
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"✅ {symbol} MARKET SETTLED | {winning_outcome or winning_asset[-8:]}\n"
                    + "\n".join(messages)
                )


def resolve_winner_from_market(market_row):
    if not isinstance(market_row, dict):
        return None, None
    outcomes = [str(x) for x in parse_jsonish(market_row.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(market_row.get("clobTokenIds"))]
    prices_raw = parse_jsonish(market_row.get("outcomePrices"))

    if len(outcomes) >= 2 and len(tokens) >= 2 and len(prices_raw) >= 2:
        prices = [sf(x, -1) for x in prices_raw]
        best_idx = max(range(len(prices)), key=lambda i: prices[i])
        best = prices[best_idx]
        others = [prices[i] for i in range(len(prices)) if i != best_idx]
        second = max(others) if others else -1
        closed = bool(market_row.get("closed", False))
        resolved_flag = bool(
            market_row.get("resolved", False)
            or market_row.get("umaResolutionStatus") == "resolved"
        )
        if best >= 0.999 and second <= 0.001 and (closed or resolved_flag or best >= 0.9999):
            return tokens[best_idx], outcomes[best_idx]

    token_objs = market_row.get("tokens")
    if isinstance(token_objs, list):
        for tok in token_objs:
            if isinstance(tok, dict) and bool(tok.get("winner", False)):
                asset = str(tok.get("token_id") or tok.get("tokenId") or tok.get("id") or "")
                outcome = str(tok.get("outcome") or tok.get("name") or "")
                if asset:
                    return asset, outcome
    return None, None


async def fetch_resolved_market_by_slug(slug, condition_id):
    event = await fetch_event_by_slug(slug)
    if not isinstance(event, dict) or not isinstance(event.get("markets"), list):
        return None
    embedded = event["markets"]
    for m in embedded:
        if isinstance(m, dict):
            cid = str(m.get("conditionId") or m.get("condition_id") or "")
            if cid == str(condition_id):
                return m
    if len(embedded) == 1 and isinstance(embedded[0], dict):
        return embedded[0]
    return None


async def resolution_fallback_loop():
    while True:
        try:
            cutoff = now_ts() - 10
            with db() as conn:
                rows = conn.execute("""
                    SELECT condition_id,slug,question,end_ts
                    FROM discovered_markets
                    WHERE resolved=0 AND end_ts<?
                    ORDER BY end_ts LIMIT 50
                """, (cutoff,)).fetchall()

            for row in rows:
                cid = str(row["condition_id"])
                slug = str(row["slug"] or "")
                if not slug:
                    continue
                m = await fetch_resolved_market_by_slug(slug, cid)
                if not m:
                    continue
                winning_asset, winning_outcome = resolve_winner_from_market(m)
                if winning_asset:
                    log.info("RESOLUTION FALLBACK %s | winner=%s", slug, winning_outcome or winning_asset[-8:])
                    await settle_market(cid, winning_asset, winning_outcome)
        except Exception:
            log.exception("Resolution fallback failed")
        await asyncio.sleep(10)




# ============================================================
# PAPER/LIVE ACCOUNTS + TELEGRAM CONTROL
# ============================================================

pending_live_confirmations = {}


def strategy_for(symbol, code):
    symbol = str(symbol).upper()
    code = str(code).upper()
    for v in STRATEGIES_BY_SYMBOL.get(symbol, []):
        if v.get("code") == code:
            return v
    return None


def open_condition_ids(strategy_name):
    with db() as conn:
        rows = conn.execute("""
            SELECT condition_id FROM paper_trades WHERE variant=? AND filled_shares>0
            UNION
            SELECT condition_id FROM live_orders WHERE variant=? AND filled_shares>0
        """, (strategy_name, strategy_name)).fetchall()
    out = []
    for r in rows:
        cid = str(r["condition_id"])
        with db() as conn:
            settled = conn.execute(
                "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
                (cid, strategy_name),
            ).fetchone()
        if not settled and position_totals(cid, strategy_name)["remaining"] > 1e-8:
            out.append(cid)
    return out


def strategy_has_open_position(strategy_name):
    return bool(open_condition_ids(strategy_name))


def open_cost_basis(strategy_name):
    total = 0.0
    for cid in open_condition_ids(strategy_name):
        pos = position_totals(cid, strategy_name)
        if pos["bought"] > 1e-9 and pos["remaining"] > 1e-9:
            total += pos["buy_cost"] * pos["remaining"] / pos["bought"]
    return total


def account_stats(strategy_name):
    cash = paper_cash(strategy_name)
    initial = paper_initial(strategy_name)
    with db() as conn:
        realized = sf(conn.execute(
            "SELECT COALESCE(SUM(pnl),0) p FROM market_results WHERE variant=?", (strategy_name,)
        ).fetchone()["p"])
        traded = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0", (strategy_name,)
        ).fetchone()["c"])
        wins = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0 AND pnl>0", (strategy_name,)
        ).fetchone()["c"])
        losses = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0 AND pnl<0", (strategy_name,)
        ).fetchone()["c"])
        paper_buys = si(conn.execute(
            "SELECT COUNT(*) c FROM paper_trades WHERE variant=? AND filled_shares>0", (strategy_name,)
        ).fetchone()["c"])
        live_buys = si(conn.execute(
            "SELECT COUNT(*) c FROM live_orders WHERE variant=? AND action='BUY' AND filled_shares>0", (strategy_name,)
        ).fetchone()["c"])
        paper_fees = sf(conn.execute(
            "SELECT COALESCE(SUM(fee),0) f FROM paper_trades WHERE variant=?", (strategy_name,)
        ).fetchone()["f"])
        live_fee_est = sf(conn.execute(
            "SELECT COALESCE(SUM(fee_estimate),0) f FROM live_orders WHERE variant=? AND filled_shares>0", (strategy_name,)
        ).fetchone()["f"])
        avg_win = sf(conn.execute(
            "SELECT COALESCE(AVG(pnl),0) x FROM market_results WHERE variant=? AND pnl>0", (strategy_name,)
        ).fetchone()["x"])
        avg_loss = sf(conn.execute(
            "SELECT COALESCE(AVG(pnl),0) x FROM market_results WHERE variant=? AND pnl<0", (strategy_name,)
        ).fetchone()["x"])
        gate_pass = si(conn.execute(
            "SELECT COUNT(*) c FROM gate_decisions WHERE variant=? AND passed=1", (strategy_name,)
        ).fetchone()["c"])
        gate_skip = si(conn.execute(
            "SELECT COUNT(*) c FROM gate_decisions WHERE variant=? AND passed=0", (strategy_name,)
        ).fetchone()["c"])
    return {
        "initial": initial, "cash": cash, "open_cost": open_cost_basis(strategy_name),
        "realized": realized, "traded_markets": traded, "wins": wins, "losses": losses,
        "buy_trades": paper_buys + live_buys, "fees": paper_fees + live_fee_est,
        "avg_win": avg_win, "avg_loss": avg_loss, "gate_pass": gate_pass, "gate_skip": gate_skip,
    }


def keyboard():
    return {
        "keyboard": [
            [{"text": "▶️ START"}, {"text": "⏹ STOP"}],
            [{"text": "🎛 MODES"}, {"text": "📐 SIZES"}],
            [{"text": "💰 BALANCE"}, {"text": "📈 POSITIONS"}],
            [{"text": "📊 STATISTICS"}, {"text": "📜 TRADES"}],
            [{"text": "🔐 WALLET"}, {"text": "🚨 EMERGENCY STOP"}],
        ],
        "resize_keyboard": True,
    }


async def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or session is None:
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": str(text)[:4096], "reply_markup": keyboard()},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status != 200:
                log.warning("Telegram message failed: %s", await r.text())
                return False
        return True
    except Exception:
        log.exception("Telegram send failed")
        return False


def strategy_status_line(v):
    dca = f" | DCA {dca_shares(v):g}sh" if v.get("dca_enabled") else ""
    return f"{v['symbol']} {v['code']}: {strategy_mode(v['name'])} | ENTRY {entry_shares(v):g}sh{dca}"


async def send_modes():
    wallet_flag = "READY" if live_client_ready else f"NOT READY ({live_client_error or 'no credentials'})"
    await tg_send(
        "🎛 G/H MODES\n" + "\n".join(strategy_status_line(v) for v in STRATEGIES)
        + f"\n\nLIVE master: {'ON' if LIVE_MASTER_ENABLE else 'OFF'} | wallet: {wallet_flag}"
        + "\n\nMODE BNB G PAPER/LIVE/OFF"
        + "\nMODE BNB H PAPER/LIVE/OFF"
        + "\nLIVE confirmation: CONFIRM LIVE BNB G"
    )


async def send_sizes():
    await tg_send(
        "📐 SHARE SIZES\n" + "\n".join(strategy_status_line(v) for v in STRATEGIES)
        + "\n\nQuick per-token: SIZE BNB 7 7"
        + "\n  -> G entry=7, H entry=7, H DCA=7"
        + "\nPer-strategy: SIZE BNB G 7"
        + "\nPer-strategy H: SIZE BNB H 7 5"
        + "\nSizes cannot change while that strategy has an open position."
    )


async def send_wallet():
    live_balance = await live_collateral_balance() if live_client_ready else None
    if live_client_ready and live_client is not None:
        wallet = str(getattr(live_client, "wallet", POLYMARKET_WALLET_ADDRESS))
        signer = str(getattr(live_client, "signer", ""))
        wallet_type = str(getattr(live_client, "wallet_type", ""))
    else:
        wallet = POLYMARKET_WALLET_ADDRESS or "not configured"
        signer = "n/a"; wallet_type = "n/a"
    bal = f"${live_balance:.2f}" if live_balance is not None else "unavailable"
    await tg_send(
        "🔐 POLYMARKET WALLET\n"
        f"SDK: {'READY' if live_client_ready else 'NOT READY'}\n"
        f"LIVE master: {'ON' if LIVE_MASTER_ENABLE else 'OFF'}\n"
        f"Wallet: {wallet}\nSigner: {signer}\nType: {wallet_type}\nCollateral: {bal}\n"
        f"Error: {live_client_error or '-'}\n\nNever send the private key in Telegram."
    )


async def send_balance():
    live_balance = await live_collateral_balance() if live_client_ready else None
    lines = [
        "💰 BALANCE",
        f"Global START: {'ON' if trading_enabled() else 'OFF'}",
        f"Real wallet collateral: ${live_balance:.2f}" if live_balance is not None else "Real wallet collateral: unavailable",
        "",
    ]
    for v in STRATEGIES:
        s = account_stats(v["name"])
        lines.append(f"{v['symbol']} {v['code']} [{strategy_mode(v['name'])}] | paperCash ${s['cash']:.2f}")
    await tg_send("\n".join(lines))


def format_stats(v, s):
    d = s["wins"] + s["losses"]
    wr = s["wins"] / d * 100.0 if d else 0.0
    return (
        f"{v['symbol']} {v['code']} [{strategy_mode(v['name'])}] | "
        f"W/L {s['wins']}/{s['losses']} ({wr:.1f}%) | PnL~${s['realized']:+.2f} | "
        f"buys {s['buy_trades']} | fees~${s['fees']:.2f} | gate {s['gate_pass']}/{s['gate_skip']}"
    )


async def send_statistics():
    lines = ["📊 BNB/ETH G/H STATISTICS"]
    for v in STRATEGIES:
        lines.append(format_stats(v, account_stats(v["name"])))
    lines.append("\nLIVE PnL/fees are estimates from accepted fill amounts; winning LIVE shares are not auto-redeemed.")
    await tg_send("\n".join(lines))


async def send_positions():
    lines = ["📈 BOT-TRACKED OPEN POSITIONS"]
    found = False
    for v in STRATEGIES:
        for cid in open_condition_ids(v["name"]):
            pos = position_totals(cid, v["name"])
            if pos["remaining"] <= 1e-8:
                continue
            found = True
            lines.append(
                f"{v['symbol']} {v['code']} {pos.get('execution_mode') or strategy_mode(v['name'])} "
                f"{pos['primary_outcome']} | {pos['remaining']:.4f}sh | buy~${pos['buy_cost']:.2f}"
            )
    if not found:
        lines.append("None")
    await tg_send("\n".join(lines))


async def send_trades():
    with db() as conn:
        rows = conn.execute("""
            SELECT trade_ms AS ms,variant,outcome,signal_type AS reason,'BUY' AS action,
                   filled_shares,avg_price,'PAPER' AS mode,total_cost AS amount
            FROM paper_trades WHERE filled_shares>0
            UNION ALL
            SELECT submitted_ms AS ms,variant,outcome,reason,action,
                   filled_shares,avg_price,'LIVE' AS mode,net_or_total AS amount
            FROM live_orders WHERE filled_shares>0
            ORDER BY ms DESC LIMIT 30
        """).fetchall()
    lines = ["📜 LAST BOT ACTIONS"]
    for r in rows:
        dt = datetime.fromtimestamp(sf(r["ms"])/1000.0, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
        v = STRATEGY_BY_NAME.get(str(r["variant"]))
        tag = f"{v['symbol']} {v['code']}" if v else str(r["variant"])[-14:]
        lines.append(
            f"{dt} {r['mode']} {tag} {r['action']} {r['reason']} {r['outcome']} "
            f"{sf(r['filled_shares']):.4f}sh @ {sf(r['avg_price']):.3f}"
        )
    if not rows:
        lines.append("No trades yet.")
    await tg_send("\n".join(lines))


def _set_mode_direct(strategy, mode):
    mode = str(mode).upper()
    if mode not in {"PAPER", "LIVE", "OFF"}:
        return False, "invalid mode"
    current = strategy_mode(strategy["name"])
    if current == mode:
        return True, f"already {mode}"
    if mode != "OFF" and strategy_has_open_position(strategy["name"]):
        for cid in open_condition_ids(strategy["name"]):
            pos_mode = position_totals(cid, strategy["name"]).get("execution_mode")
            if pos_mode and pos_mode != mode:
                return False, f"open {pos_mode} position: switch to {mode} blocked until settlement"
    state_set(f"mode:{strategy['name']}", mode)
    return True, mode


async def request_live(symbol, code):
    v = strategy_for(symbol, code)
    if not v:
        await tg_send("Unknown BNB/ETH G/H strategy."); return
    if not LIVE_MASTER_ENABLE:
        await tg_send("🔒 LIVE_MASTER_ENABLE=0 on Render. Enable it there and redeploy first."); return
    if not live_client_ready:
        await tg_send(f"🔒 Wallet SDK is not ready: {live_client_error or 'credentials missing'}"); return
    if strategy_has_open_position(v["name"]) and strategy_mode(v["name"]) != "LIVE":
        await tg_send(f"🔒 {symbol} {code} has an open position; mode switch blocked."); return
    key = (str(symbol).upper(), str(code).upper())
    pending_live_confirmations[key] = time.time() + 60
    await tg_send(
        f"⚠️ REAL MONEY confirmation for {key[0]} {key[1]}.\n"
        f"Send exactly: CONFIRM LIVE {key[0]} {key[1]}\nExpires in 60 seconds."
    )


async def confirm_live(symbol, code):
    key = (str(symbol).upper(), str(code).upper())
    expiry = pending_live_confirmations.pop(key, 0)
    if expiry < time.time():
        await tg_send("LIVE confirmation missing or expired. Use MODE command again."); return
    v = strategy_for(*key)
    if not v or not LIVE_MASTER_ENABLE or not live_client_ready:
        await tg_send("LIVE cannot be enabled: wallet/master not ready."); return
    ok, msg = _set_mode_direct(v, "LIVE")
    await tg_send(f"🔴 {key[0]} {key[1]} = LIVE" if ok else f"LIVE switch blocked: {msg}")


def _can_resize(v):
    return not strategy_has_open_position(v["name"])


async def handle_tg(text):
    raw = str(text or "").strip()
    cmd = raw.upper()
    parts = cmd.split()

    if cmd in {"/START", "▶️ START", "START"}:
        state_set("trading_enabled", "1")
        await tg_send(
            "▶️ BNB/ETH G/H STARTED\n"
            "Monitoring votes: BTC, XRP, BNB, SOL, ETH, DOGE, HYPE\n"
            "Trading targets: BNB, ETH only\n"
            "G: 0.67–0.70, mom 0.05–0.10, >=2 other V2 votes/10s\n"
            "H: same entry + one safer reversal DCA\nNo stop-loss."
        ); return

    if cmd in {"⏹ STOP", "STOP", "/STOP", "🚨 EMERGENCY STOP", "EMERGENCY STOP"}:
        state_set("trading_enabled", "0")
        await tg_send("⏹ New G/H ENTRY/DCA actions stopped globally."); return

    if cmd in {"💰 BALANCE", "BALANCE", "/BALANCE"}: await send_balance(); return
    if cmd in {"📊 STATISTICS", "STATISTICS", "/STATS"}: await send_statistics(); return
    if cmd in {"📈 POSITIONS", "POSITIONS"}: await send_positions(); return
    if cmd in {"📜 TRADES", "TRADES"}: await send_trades(); return
    if cmd in {"🎛 MODES", "MODES", "LIVE", "🔴 LIVE", "PAPER", "🟢 PAPER"}: await send_modes(); return
    if cmd in {"📐 SIZES", "SIZES"}: await send_sizes(); return
    if cmd in {"🔐 WALLET", "WALLET", "/WALLET"}: await send_wallet(); return

    # MODE BNB G LIVE/PAPER/OFF
    if len(parts) == 4 and parts[0] == "MODE" and parts[1] in TRADE_SYMBOLS and parts[2] in {"G", "H"}:
        v = strategy_for(parts[1], parts[2])
        mode = parts[3]
        if mode == "LIVE":
            await request_live(parts[1], parts[2]); return
        if mode in {"PAPER", "OFF"}:
            ok, msg = _set_mode_direct(v, mode)
            await tg_send(f"{'🟢' if mode=='PAPER' else '⛔'} {parts[1]} {parts[2]}: {msg}"); return

    # CONFIRM LIVE BNB G
    if len(parts) == 4 and parts[0] == "CONFIRM" and parts[1] == "LIVE" and parts[2] in TRADE_SYMBOLS and parts[3] in {"G", "H"}:
        await confirm_live(parts[2], parts[3]); return

    # Quick token sizing: SIZE BNB 7 7 => both entries 7, H DCA 7.
    if len(parts) == 4 and parts[0] == "SIZE" and parts[1] in TRADE_SYMBOLS and parts[2] not in {"G", "H"}:
        symbol = parts[1]
        e, d = sf(parts[2], -1), sf(parts[3], -1)
        if not _valid_user_shares(e) or not _valid_user_shares(d):
            await tg_send(f"Invalid size. Allowed: {LIVE_MIN_SHARES:g}..{LIVE_MAX_SHARES_PER_ORDER:g} shares."); return
        targets = STRATEGIES_BY_SYMBOL[symbol]
        if any(not _can_resize(v) for v in targets):
            await tg_send(f"🔒 {symbol} has an open G/H position. Resize after settlement."); return
        for v in targets:
            state_set(f"entry_shares:{v['name']}", e)
            if v.get("dca_enabled"):
                state_set(f"dca_shares:{v['name']}", d)
        await tg_send(f"📐 {symbol}: G ENTRY {e:g} | H ENTRY {e:g} | H DCA {d:g} shares"); return

    # Strategy sizing: SIZE BNB G 7  OR  SIZE BNB H 7 5
    if len(parts) in {4,5} and parts[0] == "SIZE" and parts[1] in TRADE_SYMBOLS and parts[2] in {"G", "H"}:
        v = strategy_for(parts[1], parts[2])
        e = sf(parts[3], -1)
        d = sf(parts[4], -1) if len(parts) == 5 else None
        if not _valid_user_shares(e) or (v.get("dca_enabled") and d is not None and not _valid_user_shares(d)):
            await tg_send(f"Invalid size. Allowed: {LIVE_MIN_SHARES:g}..{LIVE_MAX_SHARES_PER_ORDER:g} shares."); return
        if not _can_resize(v):
            await tg_send(f"🔒 {parts[1]} {parts[2]} has an open position. Resize after settlement."); return
        state_set(f"entry_shares:{v['name']}", e)
        if v.get("dca_enabled"):
            if d is None:
                d = dca_shares(v)
            state_set(f"dca_shares:{v['name']}", d)
            await tg_send(f"📐 {parts[1]} H: ENTRY {e:g} | DCA {d:g} shares")
        else:
            await tg_send(f"📐 {parts[1]} G: ENTRY {e:g} shares")
        return

    await tg_send(
        "BNB/ETH G/H PAPER/LIVE BOT\n"
        "Trade targets: BNB, ETH. Seven tokens are still monitored for consensus votes.\n\n"
        "MODE BNB G PAPER/LIVE/OFF\nMODE BNB H PAPER/LIVE/OFF\n"
        "SIZE BNB 5 5\nSIZE BNB G 5\nSIZE BNB H 5 5\n"
        "LIVE requires: CONFIRM LIVE BNB G"
    )


async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return
    offset = 0
    await tg_send(
        f"🤖 {VERSION} online\n"
        f"Trading: {', '.join(TRADE_SYMBOLS)} | strategies G/H\n"
        f"Consensus monitoring: {', '.join(SYMBOLS)}\n"
        f"Global trading: {'ON' if trading_enabled() else 'OFF'}\n"
        f"Wallet: {'READY' if live_client_ready else 'NOT READY'} | LIVE master: {'ON' if LIVE_MASTER_ENABLE else 'OFF'}\n"
        "Default sizes: ENTRY 5sh | H DCA 5sh | No stop-loss"
    )
    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35),
            ) as r:
                data = await r.json()
            for update in data.get("result", []):
                offset = max(offset, si(update.get("update_id")) + 1)
                msg = update.get("message") or {}
                if str((msg.get("chat") or {}).get("id", "")) != str(TELEGRAM_CHAT_ID):
                    continue
                if msg.get("text"):
                    await handle_tg(msg["text"])
        except Exception as e:
            log.warning("Telegram polling: %s", e)
            await asyncio.sleep(2)


# No hourly ZIP reports in this trading build. Persistent SQLite logs remain.


async def health(request):
    return web.json_response({
        "ok": True,
        "version": VERSION,
        "paper_live": True,
        "trading_enabled": trading_enabled(),
        "live_master_enable": LIVE_MASTER_ENABLE,
        "live_client_ready": live_client_ready,
        "live_client_error": live_client_error,
        "trade_symbols": TRADE_SYMBOLS,
        "monitor_symbols": SYMBOLS,
        "strategies": {
            f"{v['symbol']}_{v['code']}": {
                "mode": strategy_mode(v["name"]),
                "entry_shares": entry_shares(v),
                "dca_shares": dca_shares(v) if v.get("dca_enabled") else None,
            } for v in STRATEGIES
        },
        "strategy_rules": {
            "first_v2_price": [V2_ELIGIBLE_PRICE_MIN, V2_ELIGIBLE_PRICE_MAX],
            "first_v2_momentum": [V2_ELIGIBLE_MOM_MIN, V2_ELIGIBLE_MOM_MAX],
            "target_entry_price": [TIGHT_ENTRY_PRICE_MIN, TIGHT_ENTRY_PRICE_MAX],
            "target_entry_momentum": [SAFE_ENTRY_MOM_MIN, SAFE_ENTRY_MOM_MAX],
            "consensus_min_other_tokens": CONSENSUS_MIN_OTHER_TOKENS,
            "consensus_window_sec": CONSENSUS_WINDOW_SEC,
            "H_dca": {
                "arm_ask_lte": DCA_ARM_PRICE,
                "buy_price": [DCA_MIN_BUY_PRICE, DCA_MAX_BUY_PRICE],
                "rebound_momentum": [DCA_REBOUND_MOM_MIN, DCA_REBOUND_MOM_MAX],
                "deadline_sec": DCA_DEADLINE_SEC,
            },
            "stop_loss": None,
        },
        "hourly_reports": False,
        "markets_tracked": len(markets),
        "assets_subscribed": len(subscribed_assets),
        "books": len(books),
        "memory_rss_mb": current_rss_mb(),
        "time_utc": utc_iso(),
    })

async def web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server on :%d", PORT)


async def main():
    global session
    init_db()
    session = aiohttp.ClientSession(headers={
        "User-Agent": f"BNBETHConsensusGH/{VERSION}",
        "Accept": "application/json",
    })
    await init_live_client()
    tasks = [
        asyncio.create_task(web_server()),
        asyncio.create_task(discovery_loop()),
        asyncio.create_task(ws_loop()),
        asyncio.create_task(strategy_loop()),
        asyncio.create_task(resolution_fallback_loop()),
        asyncio.create_task(telegram_loop()),
        asyncio.create_task(memory_maintenance_loop()),
    ]
    log.info(
        "%s started | trade=%s | monitor=%s | G/H tight+2V2 | H safe-DCA | "
        "window=%gs | live_master=%s | wallet=%s | trading=%s",
        VERSION, ",".join(TRADE_SYMBOLS), ",".join(SYMBOLS), CONSENSUS_WINDOW_SEC,
        "ON" if LIVE_MASTER_ENABLE else "OFF",
        "READY" if live_client_ready else "NOT READY",
        "ON" if trading_enabled() else "OFF",
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await close_live_client()
        if session:
            await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
