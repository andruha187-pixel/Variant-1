import os, io, csv, json, time, math, sqlite3, asyncio, logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Optional

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# THREE-WAY A/B/C PAPER BOT — BINANCE CONF65 EXACT SHADOW
# Polymarket BTC 5m
#
# A: M03_V3_NOSW90 + CONF65
# B: M03_V2_LOCK    + CONF65
# C: M03_V5_DYNAMIC + CONF65
#
# Every strategy has an independent $500 PAPER account.
# All three consume the same captured Polymarket book snapshot and the same
# Binance feature snapshot on each decision tick.
# ============================================================

VERSION = "3.0-paper-abc-m03-conf65-exact-shadow"
HOST = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = 137

PORT = int(os.getenv("PORT", "8080"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    test = DATA_DIR / ".write_test"
    test.write_text("ok")
    test.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

# New DB: old M05 results cannot contaminate this A/B/C run.
DB_PATH = DATA_DIR / "m03_threeway_conf65_ab.db"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# PAPER accounts: EACH strategy gets this amount independently.
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
ORDER_SIZE = float(os.getenv("ORDER_SIZE", "10"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1000"))
DECISION_INTERVAL = float(os.getenv("DECISION_INTERVAL", "3"))
TRADE_WINDOW_SECONDS = int(os.getenv("TRADE_WINDOW_SECONDS", "180"))
MIN_FREE_CASH = float(os.getenv("MIN_FREE_CASH", "5"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.95"))

# Exact candidate definitions from the old research simulator.
STRATEGIES = [
    {
        "name": "M03_V3_NOSW90",
        "short": "A / V3 NOSW90",
        "entry_move": 0.03,
        "pyramid_step": 0.08,
        "lookback": 2,
        "switch_move": 999.0,
        "max_buys_side": 5,
        "allow_switch": False,
        "entry_cutoff_sec": 90,
    },
    {
        "name": "M03_V2_LOCK",
        "short": "B / V2 LOCK",
        "entry_move": 0.03,
        "pyramid_step": 0.08,
        "lookback": 2,
        "switch_move": 999.0,
        "max_buys_side": 6,
        "entry_price_min": 0.55,
        "entry_price_max": 0.75,
        "momentum_cap": 0.30,
        "allow_switch": False,
    },
    {
        "name": "M03_V5_DYNAMIC",
        "short": "C / V5 DYNAMIC",
        "entry_move": 0.03,
        "pyramid_step": 0.08,
        "lookback": 2,
        "switch_move": 0.03,
        "max_buys_side": 5,
        "allow_switch": True,
        "dynamic_switch_v5": True,
    },
]
STRATEGY_BY_NAME = {x["name"]: x for x in STRATEGIES}

# Binance CONF65 — exact old V2 scoring/feed, threshold changed to 65.
BINANCE_SYMBOL = os.getenv("BINANCE_SYMBOL", "btcusdt").lower()
BINANCE_WS = (
    "wss://fstream.binance.com/market/stream?streams="
    f"{BINANCE_SYMBOL}@aggTrade/"
    f"{BINANCE_SYMBOL}@depth20@100ms"
)
BINANCE_LARGE_TRADE_USD = float(os.getenv("BINANCE_LARGE_TRADE_USD", "50000"))
BINANCE_SIGNAL_MAX_AGE_MS = int(os.getenv("BINANCE_SIGNAL_MAX_AGE_MS", "1500"))
REGIME_WINDOW_SEC = int(os.getenv("REGIME_WINDOW_SEC", "30"))
START_PRICE_CAPTURE_WINDOW_SEC = int(os.getenv("START_PRICE_CAPTURE_WINDOW_SEC", "3"))
CONF_MIN = float(os.getenv("CONF_MIN", "65"))
W_IMPULSE = float(os.getenv("W_IMPULSE", "22"))
W_FLOW = float(os.getenv("W_FLOW", "18"))
W_BOOK = float(os.getenv("W_BOOK", "14"))
W_LARGE = float(os.getenv("W_LARGE", "8"))
W_TREND = float(os.getenv("W_TREND", "14"))
W_DISTANCE = float(os.getenv("W_DISTANCE", "18"))
W_POLY_PRICE = float(os.getenv("W_POLY_PRICE", "6"))

# This build is intentionally PAPER-only. Three independent virtual accounts
# cannot safely map to one real-money wallet without a separate execution design.
ENABLE_LIVE = False

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("m03-threeway-conf65")

session: Optional[aiohttp.ClientSession] = None

# Shared runtime market state.
books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()
price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=120)))

# Independent BASE state per (market, strategy), never gated by CONF65.
strategy_state = {}

# Independent exact-shadow state per (market, strategy).
shadow_accepted_sides = defaultdict(set)

market_binance_start_price = {}

# Binance futures state, shared by all three.
binance_trades = deque(maxlen=50000)       # ts, price, quote, sign
binance_tick_prices = deque(maxlen=30000)  # ts, price
binance_second_prices = deque(maxlen=600)  # sec, price
binance_depth_bids = []
binance_depth_asks = []
binance_last_event_ms = 0
binance_last_trade_ms = 0
binance_last_depth_ms = 0
BINANCE_NO_TRADE_RECONNECT_MS = int(os.getenv("BINANCE_NO_TRADE_RECONNECT_MS", "5000"))
MEMORY_KEEP_RESOLVED_SEC = int(os.getenv("MEMORY_KEEP_RESOLVED_SEC", "900"))

settle_lock = asyncio.Lock()

# ============================================================
# Helpers / DB
# ============================================================

def now_ts():
    return int(time.time())

def now_ms():
    return int(time.time() * 1000)

def utc_iso(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()

def sf(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d

def si(v, d=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return d

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
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")) if s else None
    except Exception:
        return None

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c

def cash_key(strategy_name):
    return f"paper_cash::{strategy_name}"

def initial_key(strategy_name):
    return f"paper_initial::{strategy_name}"

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS state(
          key TEXT PRIMARY KEY,
          value TEXT
        );

        CREATE TABLE IF NOT EXISTS markets(
          condition_id TEXT PRIMARY KEY,
          question TEXT,
          slug TEXT,
          start_ts INTEGER,
          end_ts INTEGER,
          up_asset TEXT,
          down_asset TEXT,
          resolved INTEGER DEFAULT 0,
          winning_asset TEXT,
          winning_outcome TEXT
        );

        CREATE TABLE IF NOT EXISTS signals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_ms INTEGER,
          condition_id TEXT,
          strategy TEXT,
          asset TEXT,
          outcome TEXT,
          signal_type TEXT,
          ask REAL,
          reference_ask REAL,
          momentum REAL,
          elapsed_sec REAL,
          confidence REAL,
          binance_json TEXT,
          accepted INTEGER,
          reason TEXT
        );

        CREATE TABLE IF NOT EXISTS baseline_trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          trade_ms INTEGER,
          condition_id TEXT,
          strategy TEXT,
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

        CREATE TABLE IF NOT EXISTS trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          trade_ms INTEGER,
          mode TEXT,
          strategy TEXT,
          condition_id TEXT,
          asset TEXT,
          outcome TEXT,
          signal_type TEXT,
          requested_shares REAL,
          filled_shares REAL,
          avg_price REAL,
          gross_cost REAL,
          fee REAL,
          total_cost REAL,
          cash_before REAL,
          cash_after REAL,
          book_age_ms INTEGER,
          fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS results(
          condition_id TEXT,
          strategy TEXT,
          mode TEXT,
          winning_asset TEXT,
          winning_outcome TEXT,
          total_cost REAL,
          payout REAL,
          pnl REAL,
          trades INTEGER,
          settled_ms INTEGER,
          PRIMARY KEY(condition_id, strategy, mode)
        );

        CREATE INDEX IF NOT EXISTS idx_signals_ms ON signals(signal_ms);
        CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy, signal_ms);
        CREATE INDEX IF NOT EXISTS idx_baseline_trades_ms ON baseline_trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_baseline_strategy ON baseline_trades(strategy, trade_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_ms ON trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy, trade_ms);
        CREATE INDEX IF NOT EXISTS idx_results_strategy ON results(strategy, settled_ms);
        """)

        for strategy in STRATEGIES:
            name = strategy["name"]
            if c.execute("SELECT 1 FROM state WHERE key=?", (cash_key(name),)).fetchone() is None:
                c.execute(
                    "INSERT INTO state(key,value) VALUES(?,?)",
                    (cash_key(name), str(PAPER_START_BALANCE)),
                )
            if c.execute("SELECT 1 FROM state WHERE key=?", (initial_key(name),)).fetchone() is None:
                c.execute(
                    "INSERT INTO state(key,value) VALUES(?,?)",
                    (initial_key(name), str(PAPER_START_BALANCE)),
                )

        if c.execute("SELECT 1 FROM state WHERE key='trading_enabled'").fetchone() is None:
            c.execute("INSERT INTO state(key,value) VALUES('trading_enabled','0')")
        if c.execute("SELECT 1 FROM state WHERE key='mode'").fetchone() is None:
            c.execute("INSERT INTO state(key,value) VALUES('mode','PAPER')")
        c.commit()

def state_get(k, d=None):
    with db() as c:
        r = c.execute("SELECT value FROM state WHERE key=?", (k,)).fetchone()
        return r["value"] if r else d

def state_set(k, v):
    with db() as c:
        c.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (k, str(v)),
        )
        c.commit()

def paper_cash(strategy_name):
    return sf(state_get(cash_key(strategy_name), PAPER_START_BALANCE))

def paper_initial(strategy_name):
    return sf(state_get(initial_key(strategy_name), PAPER_START_BALANCE))

def set_paper_cash(strategy_name, value):
    state_set(cash_key(strategy_name), value)

def trading_enabled():
    return state_get("trading_enabled", "0") == "1"

def current_mode():
    return "PAPER"

# Crypto 5m taker fee formula used by the research simulator.
def fee_usdc(shares, price):
    fee = shares * 0.07 * price * (1.0 - price)
    return round(fee, 5) if fee >= 0.000005 else 0.0

# ============================================================
# HTTP / market discovery
# ============================================================

async def get_json(url, params=None):
    for i in range(3):
        try:
            async with session.get(url,params=params,timeout=aiohttp.ClientTimeout(total=12)) as r:
                t=await r.text()
                if r.status==200: return json.loads(t)
                log.warning("HTTP %s %s -> %s",r.status,url,t[:160])
        except Exception as e: log.warning("GET failed %s: %s",url,e)
        await asyncio.sleep(.3*(i+1))
    return None

def slot_start_from_slug(slug):
    try: return int(str(slug).rstrip("/").split("-")[-1])
    except Exception: return None

async def fetch_event_by_slug(slug):
    for url,params in ((f"{GAMMA}/events/slug/{slug}",None),(f"{GAMMA}/events",{"slug":slug})):
        d=await get_json(url,params)
        if isinstance(d,dict): return d
        if isinstance(d,list) and d and isinstance(d[0],dict): return d[0]
    return None

def parse_market(raw,event):
    if not isinstance(raw,dict): return None
    cid=str(raw.get("conditionId") or raw.get("condition_id") or "")
    if not cid: return None
    title=str(raw.get("question") or raw.get("title") or event.get("title") or "")
    slug=str(raw.get("slug") or event.get("slug") or "")
    text=(title+" "+slug).lower()
    if "bitcoin" not in text and "btc" not in text: return None
    outcomes=[str(x).strip().upper() for x in parse_jsonish(raw.get("outcomes"))]
    tokens=[str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]
    if len(tokens)<2: return None
    up=down=None
    for i,o in enumerate(outcomes):
        if i>=len(tokens): break
        if o in {"UP","YES"}: up=tokens[i]
        elif o in {"DOWN","NO"}: down=tokens[i]
    up=up or tokens[0]; down=down or tokens[1]
    start=slot_start_from_slug(slug)
    if not start:
        dt=parse_iso(raw.get("startDate")) or parse_iso(event.get("startDate"))
        start=int(dt.timestamp()) if dt else None
    if not start: return None
    end=start+300
    return dict(condition_id=cid,question=title,slug=slug,start_ts=start,end_ts=end,up_asset=up,down_asset=down)

async def discover_slot(slot):
    slug=f"btc-updown-5m-{slot}"
    ev=await fetch_event_by_slug(slug)
    if not ev or not isinstance(ev.get("markets"),list): return None
    for raw in ev["markets"]:
        m=parse_market(raw,ev)
        if m: return m
    return None

def persist_market(m):
    with db() as c:
        c.execute("""INSERT INTO markets(condition_id,question,slug,start_ts,end_ts,up_asset,down_asset)
          VALUES(?,?,?,?,?,?,?) ON CONFLICT(condition_id) DO UPDATE SET
          question=excluded.question,slug=excluded.slug,start_ts=excluded.start_ts,end_ts=excluded.end_ts,
          up_asset=excluded.up_asset,down_asset=excluded.down_asset""",
          (m["condition_id"],m["question"],m["slug"],m["start_ts"],m["end_ts"],m["up_asset"],m["down_asset"]))
        c.commit()

async def subscribe_asset(a):
    if a and a not in subscribed_assets:
        subscribed_assets.add(a)
        await ws_send_queue.put({"operation":"subscribe","assets_ids":[a]})

async def discovery_loop():
    while True:
        try:
            n=now_ts(); cur=(n//300)*300
            for slot in (cur-300,cur,cur+300):
                m=await discover_slot(slot)
                if m and m["condition_id"] not in markets:
                    markets[m["condition_id"]]=m; persist_market(m)
                    await subscribe_asset(m["up_asset"]); await subscribe_asset(m["down_asset"])
                    log.info("MARKET %s | %s",m["slug"],utc_iso(m["start_ts"]))
        except Exception: log.exception("Discovery failed")
        await asyncio.sleep(10)

# ============================================================
# Polymarket book
# ============================================================

def level_map(rows):
    out={}
    for x in rows or []:
        if isinstance(x,dict):
            p=sf(x.get("price"),math.nan); q=sf(x.get("size"),0)
            if not math.isnan(p) and q>0: out[p]=q
    return out

def apply_book(asset,payload,source="ws"):
    books[asset]={"bids":level_map(payload.get("bids")),"asks":level_map(payload.get("asks")),
                  "received_ms":now_ms(),"source":source}

def apply_price_change(payload):
    recv=now_ms()
    for ch in payload.get("price_changes") or payload.get("priceChanges") or []:
        if not isinstance(ch,dict): continue
        a=str(ch.get("asset_id") or ch.get("token_id") or ch.get("tokenId") or "")
        if not a: continue
        b=books.setdefault(a,{"bids":{},"asks":{},"received_ms":recv,"source":"delta"})
        p=sf(ch.get("price"),math.nan); q=sf(ch.get("size"),0); side=str(ch.get("side","")).upper()
        if math.isnan(p): continue
        target=b["bids"] if side=="BUY" else b["asks"]
        if q<=0: target.pop(p,None)
        else: target[p]=q
        b["received_ms"]=recv

def best_ask(a):
    b=books.get(a)
    return min(b["asks"]) if b and b["asks"] else None

async def refresh_book(a):
    d=await get_json(f"{HOST}/book",{"token_id":a})
    if isinstance(d,dict): apply_book(a,d,"rest"); return True
    return False

async def ensure_book(a):
    b=books.get(a)
    if b and b["asks"] and now_ms()-b["received_ms"]<=MAX_BOOK_AGE_MS:
        return now_ms()-b["received_ms"]
    await refresh_book(a); b=books.get(a)
    return now_ms()-b["received_ms"] if b else None

def simulate_buy(a,wanted,max_total=None):
    b=books.get(a)
    if not b or not b["asks"]: return [],0.0
    rem=wanted; fills=[]; spent=0.0
    for p in sorted(b["asks"]):
        q=b["asks"][p]
        take=min(q,rem)
        if max_total is not None:
            # Include estimated taker fee in affordability.
            per_share=p + fee_usdc(1,p)
            take=min(take,max(0.0,(max_total-spent)/per_share))
        if take>1e-9:
            fills.append((p,take)); spent += p*take + fee_usdc(take,p); rem-=take
        if rem<=1e-9 or (max_total is not None and spent>=max_total-1e-8): break
    return fills,wanted-rem

def parse_ws(raw):
    if isinstance(raw,bytes): raw=raw.decode("utf-8","ignore")
    if raw in ("","PING","PONG"): return []
    try:
        x=json.loads(raw); return x if isinstance(x,list) else [x]
    except Exception: return []

async def ws_sender(ws):
    while True:
        msg=await ws_send_queue.get()
        try: await ws.send(jd(msg))
        except Exception:
            await ws_send_queue.put(msg); return

async def ws_ping(ws):
    while True:
        try: await ws.send("PING")
        except Exception: return
        await asyncio.sleep(10)

async def poly_ws_loop():
    while True:
        try:
            if not subscribed_assets: await asyncio.sleep(1); continue
            async with websockets.connect(POLY_WS,ping_interval=None,close_timeout=5,max_size=20_000_000) as ws:
                await ws.send(jd({"assets_ids":list(subscribed_assets),"type":"market","custom_feature_enabled":True}))
                log.info("POLY WS connected | assets=%d",len(subscribed_assets))
                sender=asyncio.create_task(ws_sender(ws)); ping=asyncio.create_task(ws_ping(ws))
                try:
                    async for raw in ws:
                        for ev in parse_ws(raw):
                            if not isinstance(ev,dict): continue
                            et=str(ev.get("event_type") or ev.get("type") or "")
                            p=ev.get("payload") if isinstance(ev.get("payload"),dict) else ev
                            if et=="book":
                                a=str(p.get("asset_id") or p.get("token_id") or "")
                                if a: apply_book(a,p)
                            elif et=="price_change": apply_price_change(p)
                            elif et=="market_resolved": await settle_from_ws(p)
                finally: sender.cancel(); ping.cancel()
        except Exception as e:
            log.warning("POLY WS reconnect: %s",e); await asyncio.sleep(1)

# ============================================================
# Binance confidence engine - preserved from the old V2 research bot
# ============================================================

def _ema(values,period):
    if not values:return None
    alpha=2/(period+1); e=float(values[0])
    for v in values[1:]: e=alpha*float(v)+(1-alpha)*e
    return e

def _rsi(values,period=14):
    if len(values)<period+1:return None
    gains=[]; losses=[]
    for i in range(-period,0):
        d=float(values[i])-float(values[i-1]); gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains)/period; al=sum(losses)/period
    if al<=1e-12:return 100.0
    rs=ag/al; return 100-100/(1+rs)

def _latest_btc_price():
    return float(binance_tick_prices[-1][1]) if binance_tick_prices else None

def _price_ms_ago(msago):
    target=now_ms()-msago
    for ts,px in reversed(binance_tick_prices):
        if ts<=target:return float(px)
    return None

def _ret_ms(msago):
    a=_latest_btc_price(); b=_price_ms_ago(msago)
    return a/b-1 if a and b else 0.0

def _signed_flow(sec):
    cutoff=now_ms()-int(sec*1000); buy=sell=0.0
    for ts,p,q,s in reversed(binance_trades):
        if ts<cutoff:break
        if s>0:buy+=q
        else:sell+=q
    t=buy+sell; return (buy-sell)/t if t>1e-9 else 0.0

def _large_delta(sec):
    cutoff=now_ms()-int(sec*1000); buy=sell=0.0
    for ts,p,q,s in reversed(binance_trades):
        if ts<cutoff:break
        if q<BINANCE_LARGE_TRADE_USD:continue
        if s>0:buy+=q
        else:sell+=q
    t=buy+sell; return (buy-sell)/t if t>1e-9 else 0.0

def _book_imbalance():
    bid=sum(sf(x[1]) for x in binance_depth_bids[:10]); ask=sum(sf(x[1]) for x in binance_depth_asks[:10])
    t=bid+ask; return (bid-ask)/t if t>1e-9 else 0.0

def _regime_features(sec=30):
    cutoff=now_ms()-sec*1000; pts=[(t,float(p)) for t,p in binance_tick_prices if t>=cutoff]
    if len(pts)<4:return dict(path_efficiency=0,direction_changes=0,realized_move=0,regime="UNKNOWN")
    px=[p for _,p in pts]; net=px[-1]-px[0]; path=sum(abs(px[i]-px[i-1]) for i in range(1,len(px)))
    eff=abs(net)/path if path>1e-12 else 0
    signs=[]
    for i in range(1,len(px)):
        d=px[i]-px[i-1]
        if abs(d)>1e-12: signs.append(1 if d>0 else -1)
    changes=sum(1 for i in range(1,len(signs)) if signs[i]!=signs[i-1])
    move=px[-1]/px[0]-1 if px[0] else 0
    regime="TREND" if eff>=.55 and abs(move)>=.0005 else ("CHOP" if eff<=.25 and changes>=6 else "MIXED")
    return dict(path_efficiency=eff,direction_changes=changes,realized_move=move,regime=regime)

def _ensure_start_price(cid,m):
    if cid in market_binance_start_price:return market_binance_start_price[cid]
    target=m["start_ts"]*1000; best=None; bestdt=None
    for ts,px in binance_tick_prices:
        dt=abs(ts-target)
        if dt<=START_PRICE_CAPTURE_WINDOW_SEC*1000 and (bestdt is None or dt<bestdt): best=float(px); bestdt=dt
    if best is None:best=_latest_btc_price()
    if best is not None:market_binance_start_price[cid]=best
    return best

def confidence_from_features(outcome,poly_ask,f):
    direction=1.0 if outcome.lower()=="up" else -1.0
    impulse_raw=.35*(f["ret_250ms"]/.00020)+.30*(f["ret_500ms"]/.00030)+.20*(f["ret_1s"]/.00045)+.15*(f["ret_3s"]/.00080)
    impulse=max(-1,min(1,direction*impulse_raw))
    flow=max(-1,min(1,direction*(.45*f["flow_1s"]+.30*f["flow_3s"]+.25*f["flow_10s"])))
    book=max(-1,min(1,direction*f["book_imbalance"]))
    large=max(-1,min(1,direction*(.65*f["large_delta_10s"]+.35*f["large_delta_30s"])))
    trend=1.0 if direction*f["ema_bias"]>0 else -1.0
    if f["regime"]=="CHOP":trend*=.20
    elif f["regime"]=="MIXED":trend*=.55
    dist=max(-1,min(1,direction*(f["distance_from_start_pct"]/.0015)))
    poly=0 if poly_ask is None else max(-1,min(1,(.72-float(poly_ask))/.22))
    weighted=W_IMPULSE*impulse+W_FLOW*flow+W_BOOK*book+W_LARGE*large+W_TREND*trend+W_DISTANCE*dist+W_POLY_PRICE*poly
    return max(0,min(100,50+weighted/2))

def binance_snapshot(cid,m,outcome,poly_ask):
    btc=_latest_btc_price(); start=_ensure_start_price(cid,m); dist=btc/start-1 if btc and start else 0
    prices=[float(p) for _,p in binance_second_prices]
    e9=_ema(prices[-60:],9) if prices else None; e21=_ema(prices[-90:],21) if prices else None
    eb=e9/e21-1 if e9 and e21 else 0; reg=_regime_features(REGIME_WINDOW_SEC)
    f=dict(btc_price=btc,start_price=start,distance_from_start_pct=dist,
      ret_250ms=_ret_ms(250),ret_500ms=_ret_ms(500),ret_1s=_ret_ms(1000),ret_3s=_ret_ms(3000),ret_10s=_ret_ms(10000),
      flow_1s=_signed_flow(1),flow_3s=_signed_flow(3),flow_10s=_signed_flow(10),flow_30s=_signed_flow(30),
      book_imbalance=_book_imbalance(),large_delta_10s=_large_delta(10),large_delta_30s=_large_delta(30),
      ema9=e9,ema21=e21,ema_bias=eb,rsi14=_rsi(prices,14) if prices else None,
      data_age_ms=max(0,now_ms()-binance_last_trade_ms) if binance_last_trade_ms else 999999,**reg)
    f["confidence"]=confidence_from_features(outcome,poly_ask,f)
    return f

async def binance_ws_loop():
    """
    Exact old V2 combined Binance Futures feed: aggTrade + depth20@100ms.

    One intentional safety fix versus old V2: CONF freshness is measured from
    the last valid aggTrade only. Depth messages never refresh trade freshness.
    """
    global binance_depth_bids, binance_depth_asks
    global binance_last_event_ms, binance_last_trade_ms, binance_last_depth_ms

    while True:
        try:
            async with websockets.connect(
                BINANCE_WS,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=10_000_000,
            ) as ws:
                log.info("BINANCE V2 WS connected | %s", BINANCE_SYMBOL.upper())
                async for raw in ws:
                    recv_ms = now_ms()
                    data = json.loads(raw)
                    payload = data.get("data", data)
                    stream = str(data.get("stream", ""))
                    binance_last_event_ms = recv_ms

                    if "aggtrade" in stream.lower() or payload.get("e") == "aggTrade":
                        ts = si(payload.get("T") or payload.get("E") or recv_ms)
                        price = sf(payload.get("p"))
                        qty = sf(payload.get("q"))
                        if price <= 0 or qty <= 0:
                            continue
                        quote = price * qty
                        sign = -1 if bool(payload.get("m")) else 1
                        binance_trades.append((ts, price, quote, sign))
                        binance_tick_prices.append((ts, price))
                        binance_last_trade_ms = recv_ms
                        sec = ts // 1000
                        if binance_second_prices and binance_second_prices[-1][0] == sec:
                            binance_second_prices[-1] = (sec, price)
                        else:
                            binance_second_prices.append((sec, price))

                    elif "depth" in stream.lower():
                        binance_depth_bids = payload.get("b") or payload.get("bids") or []
                        binance_depth_asks = payload.get("a") or payload.get("asks") or []
                        binance_last_depth_ms = recv_ms

        except Exception as e:
            log.warning("BINANCE V2 WS reconnect: %s", e)
        await asyncio.sleep(1)


async def binance_watchdog_loop():
    """
    Diagnostic only. It never marks Binance fresh from depth.
    If aggTrade is missing, strategy entries remain blocked.
    """
    while True:
        try:
            await asyncio.sleep(10)
            age = now_ms() - binance_last_trade_ms if binance_last_trade_ms else None
            if age is None or age > BINANCE_NO_TRADE_RECONNECT_MS:
                log.warning(
                    "BINANCE WATCHDOG | aggTrade_age=%s | ticks=%d | trades=%d | depth_age=%s",
                    f"{age}ms" if age is not None else "NONE",
                    len(binance_tick_prices),
                    len(binance_trades),
                    (
                        f"{now_ms() - binance_last_depth_ms}ms"
                        if binance_last_depth_ms else "NONE"
                    ),
                )
        except Exception:
            log.exception("BINANCE watchdog failed")



# ============================================================
# Runtime cleanup
# ============================================================

def cleanup_old_runtime():
    cutoff = now_ts() - MEMORY_KEEP_RESOLVED_SEC
    with db() as c:
        rows = c.execute(
            "SELECT condition_id FROM markets WHERE resolved=1 AND end_ts<?",
            (cutoff,),
        ).fetchall()

    old_cids = {
        str(r["condition_id"])
        for r in rows
        if str(r["condition_id"]) in markets
    }
    if not old_cids:
        return 0

    for cid in old_cids:
        markets.pop(cid, None)
        price_history.pop(cid, None)
        market_binance_start_price.pop(cid, None)

    for key in list(strategy_state):
        if key[0] in old_cids:
            strategy_state.pop(key, None)

    for key in list(shadow_accepted_sides):
        if key[0] in old_cids:
            shadow_accepted_sides.pop(key, None)

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

async def cleanup_loop():
    while True:
        try:
            removed = cleanup_old_runtime()
            if removed:
                log.info(
                    "CLEANUP | removed_markets=%d | markets=%d | books=%d | assets=%d",
                    removed, len(markets), len(books), len(subscribed_assets)
                )
        except Exception:
            log.exception("Cleanup failed")
        await asyncio.sleep(60)

# ============================================================
# Three-strategy exact-shadow engine
# ============================================================

def get_st(cid, strategy_name):
    key = (cid, strategy_name)
    if key not in strategy_state:
        strategy_state[key] = {
            "buys": defaultdict(int),
            "last_buy": {},
            "started_sides": set(),
            "primary_asset": None,
        }
    return strategy_state[key]

def momentum_for(cid, asset, lookback):
    h = price_history[cid][asset]
    if len(h) <= lookback:
        return None, None
    return h[-1][1] - h[-1 - lookback][1], h[-1 - lookback][1]

def snapshot_book(asset, captured_ms):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return None
    return {
        "bids": dict(b.get("bids") or {}),
        "asks": dict(b.get("asks") or {}),
        "received_ms": int(b.get("received_ms") or captured_ms),
        "captured_ms": captured_ms,
    }

def best_ask_snapshot(book_snapshot):
    if not book_snapshot or not book_snapshot["asks"]:
        return None
    return min(book_snapshot["asks"])

def simulate_buy_snapshot(book_snapshot, wanted):
    if not book_snapshot or not book_snapshot["asks"]:
        return [], 0.0
    rem = float(wanted)
    fills = []
    for p in sorted(book_snapshot["asks"]):
        q = sf(book_snapshot["asks"][p])
        take = min(q, rem)
        if take > 1e-9:
            fills.append((float(p), take))
            rem -= take
        if rem <= 1e-9:
            break
    return fills, wanted - rem

def binance_core_snapshot(cid, market):
    # Capture Binance features once for this market decision tick.
    # confidence itself is recalculated per strategy signal because outcome and
    # Polymarket ask can differ.
    f = binance_snapshot(cid, market, "Up", None)
    f.pop("confidence", None)
    return f

def features_for_signal(core, outcome, poly_ask):
    f = dict(core)
    f["confidence"] = confidence_from_features(outcome, poly_ask, f)
    return f

def store_signal(cid, strategy_name, asset, outcome, typ, ask, ref, mom, elapsed, f, accepted, reason):
    with db() as c:
        c.execute(
            """INSERT INTO signals(
              signal_ms,condition_id,strategy,asset,outcome,signal_type,
              ask,reference_ask,momentum,elapsed_sec,confidence,binance_json,
              accepted,reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                now_ms(), cid, strategy_name, asset, outcome, typ,
                ask, ref, mom, elapsed, f.get("confidence"), jd(f),
                1 if accepted else 0, reason,
            ),
        )
        c.commit()

def execute_baseline_from_snapshot(cid, strategy, asset, outcome, typ, book_snapshot):
    """
    Execute the UNFILTERED base strategy only in the internal simulator.

    This advances the strategy's own buys/last_buy/started_sides even if
    CONF65 later blocks the trade, matching the old research shadow design.
    It never touches that strategy's $500 PAPER account.
    """
    fills, filled = simulate_buy_snapshot(book_snapshot, ORDER_SIZE)
    if filled <= 1e-8:
        return None

    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    avg = gross / filled
    total = gross + fee
    trade_ms = now_ms()
    age = max(0, int(book_snapshot["captured_ms"]) - int(book_snapshot["received_ms"]))

    with db() as c:
        c.execute(
            """INSERT INTO baseline_trades(
              trade_ms,condition_id,strategy,asset,outcome,signal_type,
              requested_shares,filled_shares,avg_price,gross_cost,fee,total_cost,
              book_age_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                trade_ms, cid, strategy["name"], asset, outcome, typ,
                ORDER_SIZE, filled, avg, gross, fee, total, age,
                jd([{"price": p, "shares": q} for p, q in fills]),
            ),
        )
        c.commit()

    st = get_st(cid, strategy["name"])
    st["buys"][asset] += 1
    st["last_buy"][asset] = avg
    st["started_sides"].add(asset)

    if typ == "ENTRY" and not bool(strategy.get("allow_switch", True)):
        st["primary_asset"] = asset

    log.info(
        "BASE %-16s | %-7s %-4s | %.2fsh @ %.4f | cost=%.4f",
        strategy["name"], typ, outcome, filled, avg, total,
    )

    return {
        "trade_ms": trade_ms,
        "age": age,
        "fills": fills,
        "filled": filled,
        "gross": gross,
        "fee": fee,
        "avg": avg,
        "total": total,
    }

def exact_shadow_decision(cid, strategy_name, asset, typ, f):
    """
    Exact old V2 shadow logic, now CONF65:
      ENTRY/SWITCH -> can start a shadow side only if fresh and conf>=65.
      PYRAMID      -> requires that side to have an accepted ENTRY/SWITCH
                      and fresh conf>=65.
    """
    fresh = f["data_age_ms"] <= BINANCE_SIGNAL_MAX_AGE_MS
    conf_ok = fresh and f["confidence"] >= CONF_MIN
    sides = shadow_accepted_sides[(cid, strategy_name)]

    if typ in {"ENTRY", "SWITCH"}:
        accepted = conf_ok
        if accepted:
            sides.add(asset)
        if accepted:
            reason = f"conf={f['confidence']:.1f};fresh=True"
        else:
            reason = f"blocked_conf={f['confidence']:.1f};fresh={fresh}"
        return accepted, reason

    if typ == "PYRAMID":
        if asset not in sides:
            return False, "no_shadow_position"
        accepted = conf_ok
        if accepted:
            reason = f"conf={f['confidence']:.1f};fresh=True"
        else:
            reason = f"blocked_conf={f['confidence']:.1f};fresh={fresh}"
        return accepted, reason

    return False, "unknown_signal"

def _trim_baseline_fills_to_budget(fills, max_total):
    if max_total <= 0:
        return [], 0.0
    out = []
    spent = 0.0
    shares = 0.0
    for p, q in fills:
        per_share = p + fee_usdc(1, p)
        affordable = max(0.0, (max_total - spent) / per_share)
        take = min(q, affordable)
        if take <= 1e-9:
            break
        out.append((p, take))
        spent += p * take + fee_usdc(take, p)
        shares += take
        if spent >= max_total - 1e-8:
            break
    return out, shares

def paper_has_asset_position(strategy_name, cid, asset):
    with db() as c:
        row = c.execute(
            """SELECT COALESCE(SUM(filled_shares),0) AS sh
               FROM trades
               WHERE mode='PAPER' AND strategy=? AND condition_id=? AND asset=?""",
            (strategy_name, cid, asset),
        ).fetchone()
    return sf(row["sh"] if row else 0) > 1e-8

def paper_execute_from_baseline(strategy, cid, asset, outcome, typ, base):
    name = strategy["name"]

    # The exact shadow state may accept a PYRAMID after a theoretical entry
    # which the $500 account itself could not afford. Do not create a real
    # PAPER pyramid on a side this account never actually bought.
    if typ == "PYRAMID" and not paper_has_asset_position(name, cid, asset):
        log.warning(
            "PAPER %-16s SKIP PYRAMID %-4s | no actual PAPER position",
            name, outcome,
        )
        return False

    cash = paper_cash(name)
    available = max(0.0, cash - MIN_FREE_CASH)
    fills = list(base["fills"])
    filled = base["filled"]
    theoretical_total = base["total"]

    cash_limited = theoretical_total > available + 1e-8
    if cash_limited:
        fills, filled = _trim_baseline_fills_to_budget(fills, available)

    if filled <= 1e-8:
        log.warning(
            "PAPER %-16s CASH BLOCK %s %s | cash=%.2f available=%.2f",
            name, typ, outcome, cash, available,
        )
        return False

    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    total = gross + fee
    avg = gross / filled
    after = cash - total

    with db() as c:
        c.execute(
            """INSERT INTO trades(
              trade_ms,mode,strategy,condition_id,asset,outcome,signal_type,
              requested_shares,filled_shares,avg_price,gross_cost,fee,total_cost,
              cash_before,cash_after,book_age_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                now_ms(), "PAPER", name, cid, asset, outcome, typ,
                ORDER_SIZE, filled, avg, gross, fee, total,
                cash, after, base["age"],
                jd([{"price": p, "shares": q} for p, q in fills]),
            ),
        )
        c.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (cash_key(name), str(after)),
        )
        c.commit()

    suffix = " CASH_LIMITED" if cash_limited else ""
    log.info(
        "PAPER %-16s%s | %-7s %-4s | %.2fsh @ %.4f | cost=%.4f | cash %.2f -> %.2f",
        name, suffix, typ, outcome, filled, avg, total, cash, after,
    )
    return True

def candidate_for_strategy(cid, strategy, elapsed, tick_books):
    """
    Pure signal-selection step. It mirrors the old research evaluate_variant:
    one strongest-momentum candidate per strategy per decision tick.
    """
    entry_cutoff_sec = strategy.get("entry_cutoff_sec")
    if entry_cutoff_sec is not None and elapsed > float(entry_cutoff_sec):
        return None

    st = get_st(cid, strategy["name"])
    allow_switch = bool(strategy.get("allow_switch", True))
    entry_price_min = strategy.get("entry_price_min")
    entry_price_max = strategy.get("entry_price_max")
    momentum_cap = strategy.get("momentum_cap")
    primary_asset = st.get("primary_asset")

    candidates = []

    for asset, outcome in tick_books["sides"]:
        snap = tick_books["books"].get(asset)
        ask = best_ask_snapshot(snap)
        if ask is None or ask < MIN_PRICE or ask > MAX_PRICE:
            continue

        mom, ref = momentum_for(cid, asset, strategy["lookback"])
        if mom is None:
            continue

        buys = st["buys"][asset]
        signal = None

        if buys == 0:
            if not st["started_sides"]:
                if entry_price_min is not None and ask < float(entry_price_min):
                    continue
                if entry_price_max is not None and ask > float(entry_price_max):
                    continue
                if momentum_cap is not None and mom > float(momentum_cap):
                    continue

                if mom >= strategy["entry_move"]:
                    signal = "ENTRY"

            else:
                if not allow_switch:
                    continue

                switch_price_max = strategy.get("switch_price_max")
                if switch_price_max is not None and ask > float(switch_price_max):
                    continue

                if strategy.get("dynamic_switch_v5"):
                    if elapsed <= 60.0 and ask > 0.45:
                        continue
                    if elapsed > 60.0:
                        if 0.45 < ask <= 0.50 and mom >= 0.10:
                            continue
                        if 0.50 < ask <= 0.70:
                            continue

                if mom >= strategy["switch_move"]:
                    signal = "SWITCH"

        else:
            if not allow_switch and primary_asset is not None and asset != primary_asset:
                continue

            if momentum_cap is not None and mom > float(momentum_cap):
                continue

            last_buy = st["last_buy"].get(asset)
            if (
                last_buy is not None
                and ask >= last_buy + strategy["pyramid_step"]
                and mom > 0
                and buys < strategy["max_buys_side"]
            ):
                signal = "PYRAMID"

        if signal:
            candidates.append((mom, asset, outcome, ask, ref, signal))

    if not candidates:
        return None

    candidates.sort(reverse=True, key=lambda x: x[0])
    mom, asset, outcome, ask, ref, signal = candidates[0]
    return {
        "mom": mom,
        "asset": asset,
        "outcome": outcome,
        "ask": ask,
        "ref": ref,
        "signal": signal,
    }

def evaluate_strategy(market, strategy, elapsed, tick_books, binance_core):
    cid = market["condition_id"]
    candidate = candidate_for_strategy(cid, strategy, elapsed, tick_books)
    if not candidate:
        return

    asset = candidate["asset"]
    outcome = candidate["outcome"]
    typ = candidate["signal"]

    # Base strategy advances first, independently of CONF65.
    base = execute_baseline_from_snapshot(
        cid,
        strategy,
        asset,
        outcome,
        typ,
        tick_books["books"][asset],
    )
    if not base:
        return

    # Every strategy sees the same Binance core snapshot for this market tick.
    f = features_for_signal(binance_core, outcome, candidate["ask"])
    accepted, shadow_reason = exact_shadow_decision(
        cid, strategy["name"], asset, typ, f
    )
    reason = (
        f"{shadow_reason};regime={f['regime']};age={f['data_age_ms']}ms;"
        f"base_avg={base['avg']:.4f}"
    )

    store_signal(
        cid,
        strategy["name"],
        asset,
        outcome,
        typ,
        candidate["ask"],
        candidate["ref"],
        candidate["mom"],
        elapsed,
        f,
        accepted,
        reason,
    )

    if not accepted:
        log.info(
            "BLOCK %-16s | %-7s %-4s | %s",
            strategy["name"], typ, outcome, reason,
        )
        return

    paper_execute_from_baseline(strategy, cid, asset, outcome, typ, base)

async def strategy_loop():
    """
    A/B/C fairness:
      * one 3-second scheduler;
      * one captured Polymarket order-book snapshot per market/tick;
      * one Binance core-feature snapshot per market/tick;
      * three independent base states;
      * three independent CONF65 shadow states;
      * three independent $500 PAPER accounts.
    """
    while True:
        started = time.monotonic()
        n = time.time()

        try:
            for cid, market in list(markets.items()):
                elapsed = n - market["start_ts"]

                if not (-30 <= elapsed <= 310):
                    continue

                # Refresh both books before capturing the common tick snapshot.
                for asset in (market["up_asset"], market["down_asset"]):
                    await ensure_book(asset)

                captured_ms = now_ms()
                up_snap = snapshot_book(market["up_asset"], captured_ms)
                down_snap = snapshot_book(market["down_asset"], captured_ms)
                if not up_snap or not down_snap:
                    continue

                sides = [
                    (market["up_asset"], "Up"),
                    (market["down_asset"], "Down"),
                ]
                tick_books = {
                    "captured_ms": captured_ms,
                    "sides": sides,
                    "books": {
                        market["up_asset"]: up_snap,
                        market["down_asset"]: down_snap,
                    },
                }

                # One price-history observation shared by all strategies.
                for asset, _outcome in sides:
                    ask = best_ask_snapshot(tick_books["books"][asset])
                    if ask is not None:
                        price_history[cid][asset].append((captured_ms, ask))

                if not trading_enabled() or elapsed < 0 or elapsed > TRADE_WINDOW_SECONDS:
                    continue

                # One Binance feature capture shared by all three candidates.
                core = binance_core_snapshot(cid, market)

                # No await inside evaluations: all three use this same tick snapshot.
                for strategy in STRATEGIES:
                    evaluate_strategy(market, strategy, elapsed, tick_books, core)

        except Exception:
            log.exception("Strategy loop failed")

        await asyncio.sleep(max(0.05, DECISION_INTERVAL - (time.monotonic() - started)))

# ============================================================
# Settlement / independent balances
# ============================================================

def resolve_winner(row):
    outcomes = [str(x) for x in parse_jsonish(row.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(row.get("clobTokenIds"))]
    prices = [sf(x, -1) for x in parse_jsonish(row.get("outcomePrices"))]
    if len(outcomes) >= 2 and len(tokens) >= 2 and len(prices) >= 2:
        i = max(range(len(prices)), key=lambda j: prices[j])
        others = [prices[j] for j in range(len(prices)) if j != i]
        if (
            prices[i] >= .999
            and max(others or [-1]) <= .001
            and bool(row.get("closed", False) or row.get("resolved", False) or prices[i] >= .9999)
        ):
            return tokens[i], outcomes[i]
    return None, None

async def settle_from_ws(ev):
    cid = str(ev.get("market") or ev.get("condition_id") or "")
    win = str(ev.get("winning_asset_id") or ev.get("winning_asset") or "")
    out = str(ev.get("winning_outcome") or "")
    if cid and win:
        await settle_market(cid, win, out)

async def settle_market(cid, win, out):
    async with settle_lock:
        settled = []

        with db() as c:
            for strategy in STRATEGIES:
                name = strategy["name"]

                existing = c.execute(
                    "SELECT 1 FROM results WHERE condition_id=? AND strategy=? AND mode='PAPER'",
                    (cid, name),
                ).fetchone()
                if existing:
                    continue

                rows = c.execute(
                    """SELECT * FROM trades
                       WHERE condition_id=? AND strategy=? AND mode='PAPER'""",
                    (cid, name),
                ).fetchall()

                cost = sum(sf(r["total_cost"]) for r in rows)
                payout = sum(
                    sf(r["filled_shares"])
                    for r in rows
                    if str(r["asset"]) == win
                )
                pnl = payout - cost
                cash = paper_cash(name)
                after = cash + payout

                c.execute(
                    """INSERT INTO results(
                      condition_id,strategy,mode,winning_asset,winning_outcome,
                      total_cost,payout,pnl,trades,settled_ms
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (
                        cid, name, "PAPER", win, out,
                        cost, payout, pnl, len(rows), now_ms(),
                    ),
                )
                c.execute(
                    "INSERT INTO state(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (cash_key(name), str(after)),
                )
                settled.append((strategy, pnl, after, len(rows)))

            c.execute(
                """UPDATE markets
                   SET resolved=1,winning_asset=?,winning_outcome=?
                   WHERE condition_id=?""",
                (win, out, cid),
            )
            c.commit()

        if settled:
            for strategy, pnl, after, trades_n in settled:
                log.info(
                    "SETTLED %-16s | winner=%s | trades=%d | pnl=%+.2f | cash=%.2f",
                    strategy["name"], out, trades_n, pnl, after,
                )

            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                lines = [f"✅ MARKET SETTLED | Winner: {out}", ""]
                for strategy, pnl, after, trades_n in settled:
                    lines.extend([
                        f"{strategy['short']}",
                        f"PnL: ${pnl:+.2f} | Cash: ${after:.2f} | Trades: {trades_n}",
                        "",
                    ])
                await tg_send("\n".join(lines).strip())

async def resolution_loop():
    while True:
        try:
            cutoff = now_ts() - 10
            with db() as c:
                rows = c.execute(
                    """SELECT * FROM markets
                       WHERE resolved=0 AND end_ts<?
                       ORDER BY end_ts LIMIT 50""",
                    (cutoff,),
                ).fetchall()

            for r in rows:
                ev = await fetch_event_by_slug(r["slug"])
                if not ev or not isinstance(ev.get("markets"), list):
                    continue
                raw = next(
                    (
                        x for x in ev["markets"]
                        if str(x.get("conditionId") or "") == r["condition_id"]
                    ),
                    None,
                )
                if raw is None and len(ev["markets"]) == 1:
                    raw = ev["markets"][0]
                if not raw:
                    continue
                win, out = resolve_winner(raw)
                if win:
                    await settle_market(r["condition_id"], win, out)

        except Exception:
            log.exception("Resolution fallback failed")

        await asyncio.sleep(10)

def account_stats(strategy_name):
    cash = paper_cash(strategy_name)
    initial = paper_initial(strategy_name)

    with db() as c:
        realized = sf(
            c.execute(
                """SELECT COALESCE(SUM(pnl),0) p
                   FROM results
                   WHERE mode='PAPER' AND strategy=?""",
                (strategy_name,),
            ).fetchone()["p"]
        )
        settled_markets = c.execute(
            """SELECT COUNT(*) c FROM results
               WHERE mode='PAPER' AND strategy=?""",
            (strategy_name,),
        ).fetchone()["c"]
        traded_markets = c.execute(
            """SELECT COUNT(*) c FROM results
               WHERE mode='PAPER' AND strategy=? AND trades>0""",
            (strategy_name,),
        ).fetchone()["c"]
        wins = c.execute(
            """SELECT COUNT(*) c FROM results
               WHERE mode='PAPER' AND strategy=? AND trades>0 AND pnl>0""",
            (strategy_name,),
        ).fetchone()["c"]
        losses = c.execute(
            """SELECT COUNT(*) c FROM results
               WHERE mode='PAPER' AND strategy=? AND trades>0 AND pnl<0""",
            (strategy_name,),
        ).fetchone()["c"]
        breakeven = c.execute(
            """SELECT COUNT(*) c FROM results
               WHERE mode='PAPER' AND strategy=? AND trades>0 AND ABS(pnl)<0.0000001""",
            (strategy_name,),
        ).fetchone()["c"]
        trades = c.execute(
            """SELECT COUNT(*) c FROM trades
               WHERE mode='PAPER' AND strategy=?""",
            (strategy_name,),
        ).fetchone()["c"]
        fees = sf(
            c.execute(
                """SELECT COALESCE(SUM(fee),0) f FROM trades
                   WHERE mode='PAPER' AND strategy=?""",
                (strategy_name,),
            ).fetchone()["f"]
        )
        open_cost = sf(
            c.execute(
                """SELECT COALESCE(SUM(t.total_cost),0) x
                   FROM trades t
                   LEFT JOIN results r
                     ON r.condition_id=t.condition_id
                    AND r.strategy=t.strategy
                    AND r.mode=t.mode
                   WHERE t.mode='PAPER'
                     AND t.strategy=?
                     AND r.condition_id IS NULL""",
                (strategy_name,),
            ).fetchone()["x"]
        )
        avg_win = sf(
            c.execute(
                """SELECT COALESCE(AVG(pnl),0) x FROM results
                   WHERE mode='PAPER' AND strategy=? AND trades>0 AND pnl>0""",
                (strategy_name,),
            ).fetchone()["x"]
        )
        avg_loss = sf(
            c.execute(
                """SELECT COALESCE(AVG(pnl),0) x FROM results
                   WHERE mode='PAPER' AND strategy=? AND trades>0 AND pnl<0""",
                (strategy_name,),
            ).fetchone()["x"]
        )

    equity_cost = cash + open_cost
    return {
        "strategy": strategy_name,
        "initial": initial,
        "cash": cash,
        "equity_cost": equity_cost,
        "realized": realized,
        "total_return": equity_cost - initial,
        "settled_markets": settled_markets,
        "traded_markets": traded_markets,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "trades": trades,
        "fees": fees,
        "open_cost": open_cost,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
    }

def all_account_stats():
    return {s["name"]: account_stats(s["name"]) for s in STRATEGIES}

# ============================================================
# Telegram control / separate reports
# ============================================================

def keyboard():
    return {
        "keyboard": [
            [{"text": "▶️ START"}, {"text": "⏹ STOP"}],
            [{"text": "💰 BALANCE"}, {"text": "📊 STATISTICS"}],
            [{"text": "📈 POSITIONS"}, {"text": "📜 TRADES"}],
            [{"text": "🟢 PAPER"}, {"text": "🔴 LIVE"}],
            [{"text": "🚨 EMERGENCY STOP"}],
        ],
        "resize_keyboard": True,
    }

async def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        await session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text[:4096],
                "reply_markup": keyboard(),
            },
            timeout=aiohttp.ClientTimeout(total=15),
        )
    except Exception:
        log.exception("Telegram send failed")

def format_balance_block(strategy, s):
    return (
        f"{strategy['short']}\n"
        f"Initial: ${s['initial']:.2f}\n"
        f"Cash: ${s['cash']:.2f}\n"
        f"Open positions: ${s['open_cost']:.2f}\n"
        f"Equity: ${s['equity_cost']:.2f}\n"
        f"Realized PnL: ${s['realized']:+.2f}"
    )

def format_stats_block(strategy, s):
    denom = s["wins"] + s["losses"]
    wr = (s["wins"] / denom * 100.0) if denom else 0.0
    return (
        f"{strategy['short']}\n"
        f"Traded markets: {s['traded_markets']}\n"
        f"W/L: {s['wins']}/{s['losses']} ({wr:.1f}% wins)\n"
        f"Trades: {s['trades']}\n"
        f"Fees: ${s['fees']:.2f}\n"
        f"Avg win/loss: ${s['avg_win']:+.2f} / ${s['avg_loss']:+.2f}\n"
        f"Realized PnL: ${s['realized']:+.2f}\n"
        f"Equity: ${s['equity_cost']:.2f}"
    )

async def send_balances():
    stats = all_account_stats()
    blocks = [
        format_balance_block(strategy, stats[strategy["name"]])
        for strategy in STRATEGIES
    ]
    total_equity = sum(stats[s["name"]]["equity_cost"] for s in STRATEGIES)
    total_initial = sum(stats[s["name"]]["initial"] for s in STRATEGIES)
    await tg_send(
        "💰 THREE INDEPENDENT PAPER ACCOUNTS\n\n"
        + "\n\n".join(blocks)
        + f"\n\nCombined test equity: ${total_equity:.2f} / ${total_initial:.2f}"
    )

async def send_statistics():
    stats = all_account_stats()
    blocks = [
        format_stats_block(strategy, stats[strategy["name"]])
        for strategy in STRATEGIES
    ]
    await tg_send("📊 A/B/C STATISTICS\n\n" + "\n\n".join(blocks))

async def send_positions():
    for strategy in STRATEGIES:
        name = strategy["name"]
        with db() as c:
            rows = c.execute(
                """SELECT t.condition_id,t.outcome,
                          SUM(t.filled_shares) shares,
                          SUM(t.total_cost) cost,
                          MAX(t.trade_ms) last_ms
                   FROM trades t
                   LEFT JOIN results r
                     ON r.condition_id=t.condition_id
                    AND r.strategy=t.strategy
                    AND r.mode=t.mode
                   WHERE t.mode='PAPER'
                     AND t.strategy=?
                     AND r.condition_id IS NULL
                   GROUP BY t.condition_id,t.outcome
                   ORDER BY last_ms DESC
                   LIMIT 15""",
                (name,),
            ).fetchall()

        if rows:
            body = "\n".join(
                f"{r['condition_id'][-6:]} {r['outcome']}: "
                f"{r['shares']:.2f} sh | ${r['cost']:.2f}"
                for r in rows
            )
        else:
            body = "None"
        await tg_send(f"📈 {strategy['short']} OPEN POSITIONS\n{body}")

async def send_trades():
    # Separate Telegram message for each strategy, as requested.
    for strategy in STRATEGIES:
        name = strategy["name"]
        with db() as c:
            rows = c.execute(
                """SELECT * FROM trades
                   WHERE mode='PAPER' AND strategy=?
                   ORDER BY id DESC LIMIT 10""",
                (name,),
            ).fetchall()

        if rows:
            lines = []
            for r in rows:
                dt = datetime.fromtimestamp(
                    sf(r["trade_ms"]) / 1000.0, tz=timezone.utc
                ).strftime("%H:%M:%S")
                lines.append(
                    f"{dt} {r['outcome']} {r['signal_type']} "
                    f"{r['filled_shares']:.2f}sh @ {r['avg_price']:.3f} | "
                    f"${r['total_cost']:.2f}"
                )
            body = "\n".join(lines)
        else:
            body = "No trades yet."

        await tg_send(f"📜 {strategy['short']} LAST TRADES\n{body}")

async def handle_tg(text):
    t = text.strip().upper()

    if t in {"/START", "▶️ START", "START"}:
        state_set("trading_enabled", "1")
        await tg_send(
            "▶️ A/B/C trading STARTED\n"
            "PAPER only | 3 independent accounts | CONF65"
        )

    elif t in {"⏹ STOP", "STOP", "/STOP"}:
        state_set("trading_enabled", "0")
        await tg_send(
            "⏹ New entries stopped for ALL 3 strategies.\n"
            "Existing PAPER positions remain until resolution."
        )

    elif t in {"🚨 EMERGENCY STOP", "EMERGENCY STOP"}:
        state_set("trading_enabled", "0")
        await tg_send("🚨 EMERGENCY STOP active. No new PAPER orders.")

    elif t in {"🟢 PAPER", "PAPER"}:
        await tg_send("🟢 Mode = PAPER\nAll three accounts are virtual and independent.")

    elif t in {"🔴 LIVE", "LIVE"}:
        await tg_send(
            "🔒 LIVE is disabled in this 3-way A/B/C build.\n"
            "The three independent $500 accounts are for PAPER comparison only."
        )

    elif t in {"💰 BALANCE", "BALANCE", "/BALANCE"}:
        await send_balances()

    elif t in {"📊 STATISTICS", "STATISTICS", "/STATS"}:
        await send_statistics()

    elif t in {"📈 POSITIONS", "POSITIONS"}:
        await send_positions()

    elif t in {"📜 TRADES", "TRADES"}:
        await send_trades()

    else:
        await tg_send(
            "M03 THREE-WAY + Binance CONF65\n"
            "A: M03_V3_NOSW90\n"
            "B: M03_V2_LOCK\n"
            "C: M03_V5_DYNAMIC\n"
            "Each has its own $500 PAPER balance."
        )

async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return

    offset = 0
    await tg_send(
        f"🤖 {VERSION} online\n"
        f"Trading: {'ON' if trading_enabled() else 'OFF'}\n"
        f"CONF >= {CONF_MIN:.1f}\n"
        "A/B/C each starts with an independent PAPER balance."
    )

    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35),
            ) as r:
                d = await r.json()

            for u in d.get("result", []):
                offset = max(offset, si(u.get("update_id")) + 1)
                msg = u.get("message") or {}
                chat = str((msg.get("chat") or {}).get("id", ""))
                if chat != str(TELEGRAM_CHAT_ID):
                    continue
                text = msg.get("text")
                if text:
                    await handle_tg(text)

        except Exception as e:
            log.warning("Telegram polling: %s", e)
            await asyncio.sleep(2)

# ============================================================
# Health
# ============================================================

async def health(request):
    stats = all_account_stats()
    return web.json_response({
        "ok": True,
        "version": VERSION,
        "strategy": "M03 V3 NOSW90 / V2 LOCK / V5 DYNAMIC + Binance CONF65",
        "mode": "PAPER",
        "trading_enabled": trading_enabled(),
        "live_enabled": False,
        "paper": stats,
        "markets_tracked": len(markets),
        "books": len(books),
        "binance_trade_age_ms": (
            max(0, now_ms() - binance_last_trade_ms)
            if binance_last_trade_ms else None
        ),
        "binance_ticks": len(binance_tick_prices),
        "binance_trades": len(binance_trades),
        "binance_depth_age_ms": (
            max(0, now_ms() - binance_last_depth_ms)
            if binance_last_depth_ms else None
        ),
        "binance_regime": _regime_features(REGIME_WINDOW_SEC).get("regime"),
        "base_states": len(strategy_state),
        "shadow_states": len(shadow_accepted_sides),
        "time_utc": utc_iso(),
    })

async def web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Health server :%d", PORT)

async def main():
    global session
    init_db()
    session = aiohttp.ClientSession(
        headers={
            "User-Agent": "M03ThreeWayCONF65/3.0",
            "Accept": "application/json",
        }
    )

    tasks = [
        asyncio.create_task(x())
        for x in (
            web_server,
            discovery_loop,
            poly_ws_loop,
            binance_ws_loop,
            binance_watchdog_loop,
            strategy_loop,
            resolution_loop,
            telegram_loop,
            cleanup_loop,
        )
    ]

    balances = ", ".join(
        f"{s['name']}=${paper_cash(s['name']):.2f}"
        for s in STRATEGIES
    )
    log.info(
        "%s started | PAPER ONLY | CONF>=%.1f | lot=%.1f | %s",
        VERSION, CONF_MIN, ORDER_SIZE, balances,
    )

    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
