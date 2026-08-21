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
# M03_V2_LOCK + BINANCE CONF60
# PAPER-first production-like simulator for Polymarket BTC 5m
# ============================================================

VERSION = "2.1-paper-conf60-feedfix"
HOST = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = 137

PORT = int(os.getenv("PORT", "8080"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    test = DATA_DIR / ".write_test"
    test.write_text("ok"); test.unlink()
except Exception:
    DATA_DIR = Path("./data"); DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "m03_conf60.db"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# PAPER account
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
ORDER_SIZE = float(os.getenv("ORDER_SIZE", "10"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1000"))
DECISION_INTERVAL = float(os.getenv("DECISION_INTERVAL", "3"))
TRADE_WINDOW_SECONDS = int(os.getenv("TRADE_WINDOW_SECONDS", "180"))
MIN_FREE_CASH = float(os.getenv("MIN_FREE_CASH", "5"))

# Exact M03_V2_LOCK parameters from our previous simulator.
ENTRY_MOVE = float(os.getenv("ENTRY_MOVE", "0.03"))
PYRAMID_STEP = float(os.getenv("PYRAMID_STEP", "0.08"))
LOOKBACK = int(os.getenv("LOOKBACK", "2"))
MAX_BUYS_SIDE = int(os.getenv("MAX_BUYS_SIDE", "6"))
ENTRY_PRICE_MIN = float(os.getenv("ENTRY_PRICE_MIN", "0.55"))
ENTRY_PRICE_MAX = float(os.getenv("ENTRY_PRICE_MAX", "0.75"))
MOMENTUM_CAP = float(os.getenv("MOMENTUM_CAP", "0.30"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.95"))

# Binance CONF60 - exact scoring weights/thresholds from the V4.2 research bot.
BINANCE_SYMBOL = os.getenv("BINANCE_SYMBOL", "btcusdt").lower()
BINANCE_WS = (
    "wss://fstream.binance.com/stream?streams="
    f"{BINANCE_SYMBOL}@aggTrade/{BINANCE_SYMBOL}@depth20@100ms"
)
BINANCE_LARGE_TRADE_USD = float(os.getenv("BINANCE_LARGE_TRADE_USD", "50000"))
BINANCE_SIGNAL_MAX_AGE_MS = int(os.getenv("BINANCE_SIGNAL_MAX_AGE_MS", "1500"))
REGIME_WINDOW_SEC = int(os.getenv("REGIME_WINDOW_SEC", "30"))
START_PRICE_CAPTURE_WINDOW_SEC = int(os.getenv("START_PRICE_CAPTURE_WINDOW_SEC", "3"))
CONF_MIN = float(os.getenv("CONF_MIN", "60"))
W_IMPULSE = float(os.getenv("W_IMPULSE", "22"))
W_FLOW = float(os.getenv("W_FLOW", "18"))
W_BOOK = float(os.getenv("W_BOOK", "14"))
W_LARGE = float(os.getenv("W_LARGE", "8"))
W_TREND = float(os.getenv("W_TREND", "14"))
W_DISTANCE = float(os.getenv("W_DISTANCE", "18"))
W_POLY_PRICE = float(os.getenv("W_POLY_PRICE", "6"))
# Our prior CONF60 comparison intentionally excluded book weight.
CONF_USE_BOOK = os.getenv("CONF_USE_BOOK", "0").strip() == "1"

# Live is deliberately OFF by default. UI cannot turn it on unless env allows it.
ENABLE_LIVE = os.getenv("ENABLE_LIVE", "0").strip() == "1"
PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()
DEPOSIT_WALLET_ADDRESS = os.getenv("POLY_DEPOSIT_WALLET_ADDRESS", "").strip()
POLY_API_KEY = os.getenv("POLY_API_KEY", "").strip()
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "").strip()
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "").strip()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("m03-conf60")

session: Optional[aiohttp.ClientSession] = None

# Runtime state
books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()
price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=120)))
strategy_state = {}
market_binance_start_price = {}

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

# ============================================================
# Helpers / DB
# ============================================================

def now_ts(): return int(time.time())
def now_ms(): return int(time.time() * 1000)
def utc_iso(ts=None):
    if ts is None: ts = time.time()
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
def sf(v, d=0.0):
    try: return float(v)
    except (TypeError, ValueError): return d
def si(v, d=0):
    try: return int(float(v))
    except (TypeError, ValueError): return d
def jd(v): return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
def parse_jsonish(v):
    if isinstance(v, list): return v
    if v is None: return []
    try:
        x = json.loads(v); return x if isinstance(x, list) else []
    except Exception: return []
def parse_iso(s):
    try: return datetime.fromisoformat(str(s).replace("Z", "+00:00")) if s else None
    except Exception: return None

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS markets(
          condition_id TEXT PRIMARY KEY, question TEXT, slug TEXT,
          start_ts INTEGER,end_ts INTEGER,up_asset TEXT,down_asset TEXT,
          resolved INTEGER DEFAULT 0,winning_asset TEXT,winning_outcome TEXT
        );
        CREATE TABLE IF NOT EXISTS signals(
          id INTEGER PRIMARY KEY AUTOINCREMENT, signal_ms INTEGER,
          condition_id TEXT, asset TEXT, outcome TEXT, signal_type TEXT,
          ask REAL, reference_ask REAL, momentum REAL, elapsed_sec REAL,
          confidence REAL, binance_json TEXT, accepted INTEGER, reason TEXT
        );
        CREATE TABLE IF NOT EXISTS trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT, trade_ms INTEGER,
          mode TEXT, condition_id TEXT, asset TEXT, outcome TEXT,
          signal_type TEXT, requested_shares REAL, filled_shares REAL,
          avg_price REAL,gross_cost REAL,fee REAL,total_cost REAL,
          cash_before REAL,cash_after REAL,book_age_ms INTEGER,fills_json TEXT,
          live_order_id TEXT
        );
        CREATE TABLE IF NOT EXISTS results(
          condition_id TEXT, mode TEXT, winning_asset TEXT,winning_outcome TEXT,
          total_cost REAL,payout REAL,pnl REAL,trades INTEGER,settled_ms INTEGER,
          PRIMARY KEY(condition_id,mode)
        );
        CREATE INDEX IF NOT EXISTS idx_trades_ms ON trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_signals_ms ON signals(signal_ms);
        """)
        if c.execute("SELECT 1 FROM state WHERE key='paper_cash'").fetchone() is None:
            c.execute("INSERT INTO state(key,value) VALUES('paper_cash',?)",(str(PAPER_START_BALANCE),))
        if c.execute("SELECT 1 FROM state WHERE key='paper_initial'").fetchone() is None:
            c.execute("INSERT INTO state(key,value) VALUES('paper_initial',?)",(str(PAPER_START_BALANCE),))
        if c.execute("SELECT 1 FROM state WHERE key='trading_enabled'").fetchone() is None:
            c.execute("INSERT INTO state(key,value) VALUES('trading_enabled','0')")
        if c.execute("SELECT 1 FROM state WHERE key='mode'").fetchone() is None:
            c.execute("INSERT INTO state(key,value) VALUES('mode','PAPER')")
        c.commit()

def state_get(k,d=None):
    with db() as c:
        r=c.execute("SELECT value FROM state WHERE key=?",(k,)).fetchone()
        return r["value"] if r else d
def state_set(k,v):
    with db() as c:
        c.execute("INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(k,str(v))); c.commit()
def paper_cash(): return sf(state_get("paper_cash", PAPER_START_BALANCE))
def paper_initial(): return sf(state_get("paper_initial", PAPER_START_BALANCE))
def trading_enabled(): return state_get("trading_enabled","0")=="1"
def current_mode(): return state_get("mode","PAPER").upper()

# Crypto 5m taker fee formula. Actual LIVE fees are protocol-determined.
def fee_usdc(shares, price):
    fee = shares * 0.07 * price * (1.0-price)
    return round(fee,5) if fee >= 0.000005 else 0.0

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
# Binance CONF60 - preserved from our research bot
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
    bw=W_BOOK if CONF_USE_BOOK else 0
    weighted=W_IMPULSE*impulse+W_FLOW*flow+bw*book+W_LARGE*large+W_TREND*trend+W_DISTANCE*dist+W_POLY_PRICE*poly
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
    global binance_depth_bids, binance_depth_asks
    global binance_last_event_ms, binance_last_trade_ms, binance_last_depth_ms

    while True:
        try:
            async with websockets.connect(
                BINANCE_WS,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=4_000_000,
            ) as ws:
                log.info("BINANCE WS connected | %s", BINANCE_SYMBOL.upper())
                connected_ms = now_ms()
                last_diag_ms = 0

                async for raw in ws:
                    d = json.loads(raw)
                    p = d.get("data", d)
                    stream = str(d.get("stream", ""))
                    recv_ms = now_ms()
                    binance_last_event_ms = recv_ms

                    if "aggtrade" in stream.lower() or p.get("e") == "aggTrade":
                        ts = si(p.get("T") or p.get("E") or recv_ms)
                        px = sf(p.get("p"))
                        qty = sf(p.get("q"))

                        if px <= 0 or qty <= 0:
                            continue

                        quote = px * qty
                        sign = -1 if bool(p.get("m")) else 1

                        binance_trades.append((ts, px, quote, sign))
                        binance_tick_prices.append((ts, px))
                        binance_last_trade_ms = recv_ms

                        sec = ts // 1000
                        if binance_second_prices and binance_second_prices[-1][0] == sec:
                            binance_second_prices[-1] = (sec, px)
                        else:
                            binance_second_prices.append((sec, px))

                    elif "depth" in stream.lower():
                        binance_depth_bids = p.get("b") or p.get("bids") or []
                        binance_depth_asks = p.get("a") or p.get("asks") or []
                        binance_last_depth_ms = recv_ms

                    # Diagnostic every 30 sec so Render proves that price history is alive.
                    if recv_ms - last_diag_ms >= 30000:
                        age = recv_ms - binance_last_trade_ms if binance_last_trade_ms else None
                        reg = _regime_features(REGIME_WINDOW_SEC)
                        log.info(
                            "BINANCE DATA | price=%s | ticks=%d | trades=%d | "
                            "trade_age=%sms | regime=%s | dirchg=%s | path=%.3f",
                            f"{_latest_btc_price():.2f}" if _latest_btc_price() else "NONE",
                            len(binance_tick_prices),
                            len(binance_trades),
                            age if age is not None else "NONE",
                            reg["regime"],
                            reg["direction_changes"],
                            reg["path_efficiency"],
                        )
                        last_diag_ms = recv_ms

                    # Depth may continue while aggTrade stream is dead.
                    # Do not let that falsely mark Binance as fresh.
                    if recv_ms - connected_ms > BINANCE_NO_TRADE_RECONNECT_MS:
                        if not binance_last_trade_ms or recv_ms - binance_last_trade_ms > BINANCE_NO_TRADE_RECONNECT_MS:
                            log.warning(
                                "BINANCE no aggTrade for %dms -> reconnect",
                                recv_ms - binance_last_trade_ms if binance_last_trade_ms else recv_ms - connected_ms,
                            )
                            break

        except Exception as e:
            log.warning("BINANCE reconnect: %s", e)

        await asyncio.sleep(1)



# ============================================================
# Runtime cleanup
# ============================================================

def cleanup_old_runtime():
    cutoff = now_ts() - MEMORY_KEEP_RESOLVED_SEC
    with db() as c:
        rows = c.execute(
            "SELECT condition_id,up_asset,down_asset FROM markets "
            "WHERE resolved=1 AND end_ts<?",
            (cutoff,),
        ).fetchall()

    old_cids = {str(r["condition_id"]) for r in rows}
    if not old_cids:
        return 0

    for cid in old_cids:
        markets.pop(cid, None)
        price_history.pop(cid, None)
        strategy_state.pop(cid, None)
        market_binance_start_price.pop(cid, None)

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
# Strategy / execution
# ============================================================

def get_st(cid):
    if cid not in strategy_state:
        strategy_state[cid]={"buys":defaultdict(int),"last_buy":{},"primary_asset":None}
    return strategy_state[cid]

def momentum_for(cid,a):
    h=price_history[cid][a]
    if len(h)<=LOOKBACK:return None,None
    return h[-1][1]-h[-1-LOOKBACK][1],h[-1-LOOKBACK][1]

def store_signal(cid,a,outcome,typ,ask,ref,mom,elapsed,f,accepted,reason):
    with db() as c:
        c.execute("""INSERT INTO signals(signal_ms,condition_id,asset,outcome,signal_type,ask,reference_ask,momentum,
          elapsed_sec,confidence,binance_json,accepted,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_ms(),cid,a,outcome,typ,ask,ref,mom,elapsed,f.get("confidence"),jd(f),1 if accepted else 0,reason))
        c.commit()

async def paper_execute(cid,a,outcome,typ):
    age=await ensure_book(a); cash=paper_cash()
    available=max(0,cash-MIN_FREE_CASH)
    fills,filled=simulate_buy(a,ORDER_SIZE,available)
    if filled<=1e-8:return False
    gross=sum(p*q for p,q in fills); fee=sum(fee_usdc(q,p) for p,q in fills); total=gross+fee
    if total>cash+1e-7:return False
    after=cash-total
    with db() as c:
        c.execute("""INSERT INTO trades(trade_ms,mode,condition_id,asset,outcome,signal_type,requested_shares,filled_shares,
          avg_price,gross_cost,fee,total_cost,cash_before,cash_after,book_age_ms,fills_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_ms(),"PAPER",cid,a,outcome,typ,ORDER_SIZE,filled,gross/filled,gross,fee,total,cash,after,age,
           jd([{"price":p,"shares":q} for p,q in fills])))
        c.execute("INSERT INTO state(key,value) VALUES('paper_cash',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(after),))
        c.commit()
    log.info("PAPER %s %s %.4fsh @ %.4f | cost=%.4f | cash %.2f -> %.2f",typ,outcome,filled,gross/filled,total,cash,after)
    return True

async def live_execute(cid,a,outcome,typ):
    # Safety gate: LIVE cannot be activated accidentally from Telegram alone.
    if not ENABLE_LIVE:
        log.error("LIVE blocked: ENABLE_LIVE=0"); return False
    if not all([PRIVATE_KEY,DEPOSIT_WALLET_ADDRESS,POLY_API_KEY,POLY_API_SECRET,POLY_API_PASSPHRASE]):
        log.error("LIVE blocked: credentials incomplete"); return False
    # Use official SDK only when live is explicitly enabled.
    try:
        from py_clob_client_v2 import ClobClient, ApiCreds
        from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType
        from py_clob_client_v2.constants import BUY
    except Exception as e:
        log.error("LIVE SDK import failed: %s",e); return False
    try:
        client=ClobClient(host=HOST,chain_id=CHAIN_ID,key=PRIVATE_KEY,
          creds=ApiCreds(api_key=POLY_API_KEY,api_secret=POLY_API_SECRET,api_passphrase=POLY_API_PASSPHRASE),
          signature_type=3,funder=DEPOSIT_WALLET_ADDRESS)
        # FOK market buy in dollars. Approximate 10-share intent using current ask.
        await ensure_book(a); ask=best_ask(a)
        if not ask:return False
        amount=round(ORDER_SIZE*ask,2)
        args=MarketOrderArgs(token_id=a,amount=amount,side=BUY,order_type=OrderType.FOK)
        signed=client.create_market_order(args)
        resp=client.post_order(signed,OrderType.FOK)
        log.info("LIVE order response: %s",resp)
        return bool(resp and resp.get("success",True))
    except Exception:
        log.exception("LIVE order failed"); return False

async def execute(cid,a,outcome,typ):
    return await (paper_execute(cid,a,outcome,typ) if current_mode()=="PAPER" else live_execute(cid,a,outcome,typ))

async def evaluate_market(m,elapsed):
    cid=m["condition_id"]; st=get_st(cid)
    sides=[(m["up_asset"],"Up"),(m["down_asset"],"Down")]
    candidates=[]
    for a,outcome in sides:
        ask=best_ask(a)
        if ask is None or ask<MIN_PRICE or ask>MAX_PRICE:continue
        mom,ref=momentum_for(cid,a)
        if mom is None:continue
        buys=st["buys"][a]; typ=None
        if buys==0:
            if st["primary_asset"] is not None: continue  # LOCK: never switch side
            if ask<ENTRY_PRICE_MIN or ask>ENTRY_PRICE_MAX or mom>MOMENTUM_CAP:continue
            if mom>=ENTRY_MOVE:typ="ENTRY"
        else:
            if a!=st["primary_asset"] or mom>MOMENTUM_CAP:continue
            last=st["last_buy"].get(a)
            if last is not None and ask>=last+PYRAMID_STEP and mom>0 and buys<MAX_BUYS_SIDE:typ="PYRAMID"
        if typ:
            f=binance_snapshot(cid,m,outcome,ask)
            fresh=f["data_age_ms"]<=BINANCE_SIGNAL_MAX_AGE_MS
            accepted=fresh and f["confidence"]>=CONF_MIN
            reason=f"conf={f['confidence']:.1f};fresh={fresh};regime={f['regime']};age={f['data_age_ms']}ms"
            store_signal(cid,a,outcome,typ,ask,ref,mom,elapsed,f,accepted,reason)
            if accepted:candidates.append((f["confidence"],mom,a,outcome,ask,typ))
            else:log.info("BLOCK %s %s | %s",typ,outcome,reason)
    if not candidates:return
    candidates.sort(reverse=True,key=lambda x:(x[0],x[1]))
    conf,mom,a,outcome,ask,typ=candidates[0]
    ok=await execute(cid,a,outcome,typ)
    if ok:
        st["buys"][a]+=1; st["last_buy"][a]=ask
        if typ=="ENTRY":st["primary_asset"]=a

async def strategy_loop():
    while True:
        started=time.monotonic(); n=time.time()
        try:
            for cid,m in list(markets.items()):
                elapsed=n-m["start_ts"]
                if -30<=elapsed<=310:
                    for a in (m["up_asset"],m["down_asset"]):
                        ask=best_ask(a)
                        if ask is not None:price_history[cid][a].append((now_ms(),ask))
                if not trading_enabled() or elapsed<0 or elapsed>TRADE_WINDOW_SECONDS:continue
                if best_ask(m["up_asset"]) is None or best_ask(m["down_asset"]) is None:continue
                await evaluate_market(m,elapsed)
        except Exception:log.exception("Strategy loop failed")
        await asyncio.sleep(max(.05,DECISION_INTERVAL-(time.monotonic()-started)))

# ============================================================
# Settlement / balance
# ============================================================

def resolve_winner(row):
    outcomes=[str(x) for x in parse_jsonish(row.get("outcomes"))]
    tokens=[str(x) for x in parse_jsonish(row.get("clobTokenIds"))]
    prices=[sf(x,-1) for x in parse_jsonish(row.get("outcomePrices"))]
    if len(outcomes)>=2 and len(tokens)>=2 and len(prices)>=2:
        i=max(range(len(prices)),key=lambda j:prices[j]); others=[prices[j] for j in range(len(prices)) if j!=i]
        if prices[i]>=.999 and max(others or [-1])<=.001 and bool(row.get("closed",False) or row.get("resolved",False) or prices[i]>=.9999):
            return tokens[i],outcomes[i]
    return None,None

async def settle_from_ws(ev):
    cid=str(ev.get("market") or ev.get("condition_id") or "")
    win=str(ev.get("winning_asset_id") or ev.get("winning_asset") or "")
    out=str(ev.get("winning_outcome") or "")
    if cid and win:await settle_market(cid,win,out)

async def settle_market(cid,win,out):
    with db() as c:
        if c.execute("SELECT 1 FROM results WHERE condition_id=? AND mode='PAPER'",(cid,)).fetchone():return
        rows=c.execute("SELECT * FROM trades WHERE condition_id=? AND mode='PAPER'",(cid,)).fetchall()
        cost=sum(sf(r["total_cost"]) for r in rows)
        payout=sum(sf(r["filled_shares"]) for r in rows if str(r["asset"])==win)
        pnl=payout-cost
        cash=paper_cash(); after=cash+payout
        c.execute("""INSERT INTO results(condition_id,mode,winning_asset,winning_outcome,total_cost,payout,pnl,trades,settled_ms)
          VALUES(?,?,?,?,?,?,?,?,?)""",(cid,"PAPER",win,out,cost,payout,pnl,len(rows),now_ms()))
        c.execute("UPDATE markets SET resolved=1,winning_asset=?,winning_outcome=? WHERE condition_id=?",(win,out,cid))
        c.execute("INSERT INTO state(key,value) VALUES('paper_cash',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(after),))
        c.commit()
    log.info("SETTLED %s | winner=%s | cost=%.2f payout=%.2f pnl=%+.2f | cash=%.2f",cid[-8:],out,cost,payout,pnl,after)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        await tg_send(f"✅ Market settled\nWinner: {out}\nPnL: ${pnl:+.2f}\nCash: ${after:.2f}")

async def resolution_loop():
    while True:
        try:
            cutoff=now_ts()-10
            with db() as c:
                rows=c.execute("SELECT * FROM markets WHERE resolved=0 AND end_ts<? ORDER BY end_ts LIMIT 50",(cutoff,)).fetchall()
            for r in rows:
                ev=await fetch_event_by_slug(r["slug"])
                if not ev or not isinstance(ev.get("markets"),list):continue
                raw=next((x for x in ev["markets"] if str(x.get("conditionId") or "")==r["condition_id"]),None)
                if raw is None and len(ev["markets"])==1:raw=ev["markets"][0]
                if not raw:continue
                win,out=resolve_winner(raw)
                if win:await settle_market(r["condition_id"],win,out)
        except Exception:log.exception("Resolution fallback failed")
        await asyncio.sleep(10)

def account_stats():
    cash=paper_cash(); initial=paper_initial()
    with db() as c:
        realized=sf(c.execute("SELECT COALESCE(SUM(pnl),0) p FROM results WHERE mode='PAPER'").fetchone()["p"])
        markets_n=c.execute("SELECT COUNT(*) c FROM results WHERE mode='PAPER'").fetchone()["c"]
        wins=c.execute("SELECT COUNT(*) c FROM results WHERE mode='PAPER' AND pnl>0").fetchone()["c"]
        losses=c.execute("SELECT COUNT(*) c FROM results WHERE mode='PAPER' AND pnl<0").fetchone()["c"]
        trades=c.execute("SELECT COUNT(*) c FROM trades WHERE mode='PAPER'").fetchone()["c"]
        fees=sf(c.execute("SELECT COALESCE(SUM(fee),0) f FROM trades WHERE mode='PAPER'").fetchone()["f"])
        open_cost=sf(c.execute("""SELECT COALESCE(SUM(t.total_cost),0) x FROM trades t
          LEFT JOIN results r ON r.condition_id=t.condition_id AND r.mode='PAPER'
          WHERE t.mode='PAPER' AND r.condition_id IS NULL""").fetchone()["x"])
    # Cash already has open positions deducted. Equity at cost = cash + open_cost.
    equity_cost=cash+open_cost
    return dict(initial=initial,cash=cash,equity_cost=equity_cost,realized=realized,
                total_return=cash+open_cost-initial,markets=markets_n,wins=wins,losses=losses,trades=trades,fees=fees,open_cost=open_cost)

# ============================================================
# Telegram control
# ============================================================

def keyboard():
    return {"keyboard":[
      [{"text":"▶️ START"},{"text":"⏹ STOP"}],
      [{"text":"💰 BALANCE"},{"text":"📊 STATISTICS"}],
      [{"text":"📈 POSITIONS"},{"text":"📜 TRADES"}],
      [{"text":"🟢 PAPER"},{"text":"🔴 LIVE"}],
      [{"text":"🚨 EMERGENCY STOP"}]
    ],"resize_keyboard":True}

async def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:return
    try:
        await session.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
          json={"chat_id":TELEGRAM_CHAT_ID,"text":text[:4096],"reply_markup":keyboard()},
          timeout=aiohttp.ClientTimeout(total=15))
    except Exception:log.exception("Telegram send failed")

async def handle_tg(text):
    t=text.strip().upper()
    if t in {"/START","▶️ START","START"}:
        state_set("trading_enabled","1"); await tg_send(f"▶️ Trading STARTED\nMode: {current_mode()}")
    elif t in {"⏹ STOP","STOP","/STOP"}:
        state_set("trading_enabled","0"); await tg_send("⏹ New entries stopped. Existing positions remain until resolution.")
    elif t in {"🚨 EMERGENCY STOP","EMERGENCY STOP"}:
        state_set("trading_enabled","0"); await tg_send("🚨 EMERGENCY STOP active. No new orders.")
    elif t in {"🟢 PAPER","PAPER"}:
        state_set("mode","PAPER"); await tg_send("🟢 Mode = PAPER")
    elif t in {"🔴 LIVE","LIVE"}:
        if not ENABLE_LIVE: await tg_send("🔒 LIVE is locked by ENABLE_LIVE=0. Telegram cannot bypass this safety lock.")
        elif not all([PRIVATE_KEY,DEPOSIT_WALLET_ADDRESS,POLY_API_KEY,POLY_API_SECRET,POLY_API_PASSPHRASE]):
            await tg_send("🔒 LIVE credentials are incomplete.")
        else:
            state_set("mode","LIVE"); state_set("trading_enabled","0")
            await tg_send("🔴 Mode = LIVE, but trading is STOPPED. Press START separately to arm live orders.")
    elif t in {"💰 BALANCE","BALANCE","/BALANCE"}:
        s=account_stats()
        await tg_send(f"💰 PAPER ACCOUNT\nInitial: ${s['initial']:.2f}\nCash: ${s['cash']:.2f}\nOpen positions at cost: ${s['open_cost']:.2f}\nEquity (cost basis): ${s['equity_cost']:.2f}\nRealized PnL: ${s['realized']:+.2f}")
    elif t in {"📊 STATISTICS","STATISTICS","/STATS"}:
        s=account_stats()
        wr=(s["wins"]/s["markets"]*100) if s["markets"] else 0
        await tg_send(f"📊 STATISTICS\nMarkets: {s['markets']}\nW/L: {s['wins']}/{s['losses']} ({wr:.1f}% wins)\nTrades: {s['trades']}\nFees: ${s['fees']:.2f}\nRealized PnL: ${s['realized']:+.2f}\nCash: ${s['cash']:.2f}")
    elif t in {"📈 POSITIONS","POSITIONS"}:
        with db() as c:
            rows=c.execute("""SELECT t.condition_id,t.outcome,SUM(t.filled_shares) shares,SUM(t.total_cost) cost
              FROM trades t LEFT JOIN results r ON r.condition_id=t.condition_id AND r.mode=t.mode
              WHERE t.mode='PAPER' AND r.condition_id IS NULL GROUP BY t.condition_id,t.outcome ORDER BY MAX(t.trade_ms) DESC LIMIT 15""").fetchall()
        msg="📈 OPEN PAPER POSITIONS\n"+("\n".join(f"{r['condition_id'][-6:]} {r['outcome']}: {r['shares']:.2f} sh | ${r['cost']:.2f}" for r in rows) if rows else "None")
        await tg_send(msg)
    elif t in {"📜 TRADES","TRADES"}:
        with db() as c:
            rows=c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 10").fetchall()
        msg="📜 LAST TRADES\n"+("\n".join(f"{r['outcome']} {r['signal_type']} {r['filled_shares']:.2f}sh @ {r['avg_price']:.3f} | ${r['total_cost']:.2f}" for r in rows) if rows else "None")
        await tg_send(msg)
    else:
        await tg_send("M03_V2_LOCK + Binance CONF60\nUse the buttons below.")

async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured"); return
    offset=0
    await tg_send(f"🤖 {VERSION} online\nMode: {current_mode()}\nTrading: {'ON' if trading_enabled() else 'OFF'}")
    while True:
        try:
            async with session.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
              params={"timeout":25,"offset":offset},timeout=aiohttp.ClientTimeout(total=35)) as r:
                d=await r.json()
            for u in d.get("result",[]):
                offset=max(offset,si(u.get("update_id"))+1)
                msg=u.get("message") or {}; chat=str((msg.get("chat") or {}).get("id",""))
                if chat!=str(TELEGRAM_CHAT_ID):continue
                text=msg.get("text")
                if text:await handle_tg(text)
        except Exception as e:
            log.warning("Telegram polling: %s",e); await asyncio.sleep(2)

# ============================================================
# Health
# ============================================================

async def health(request):
    s=account_stats()
    return web.json_response({"ok":True,"version":VERSION,"strategy":"M03_V2_LOCK + Binance CONF60",
      "mode":current_mode(),"trading_enabled":trading_enabled(),"live_env_enabled":ENABLE_LIVE,
      "paper":s,"markets_tracked":len(markets),"books":len(books),
      "binance_trade_age_ms":max(0,now_ms()-binance_last_trade_ms) if binance_last_trade_ms else None,
      "binance_ticks":len(binance_tick_prices),"binance_trades":len(binance_trades),
      "binance_regime":_regime_features(REGIME_WINDOW_SEC).get("regime"),
      "time_utc":utc_iso()})

async def web_server():
    app=web.Application(); app.router.add_get("/",health); app.router.add_get("/health",health)
    runner=web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner,"0.0.0.0",PORT).start()
    log.info("Health server :%d",PORT)

async def main():
    global session
    init_db()
    session=aiohttp.ClientSession(headers={"User-Agent":"M03CONF60Bot/2.0","Accept":"application/json"})
    tasks=[asyncio.create_task(x()) for x in (web_server,discovery_loop,poly_ws_loop,binance_ws_loop,strategy_loop,resolution_loop,telegram_loop,cleanup_loop)]
    log.info("%s started | PAPER=$%.2f | lot=%.1f | CONF>=%.1f",VERSION,paper_cash(),ORDER_SIZE,CONF_MIN)
    try: await asyncio.gather(*tasks)
    finally:
        for t in tasks:t.cancel()
        await session.close()

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
