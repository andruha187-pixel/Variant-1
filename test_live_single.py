import os
import time
import tempfile
import asyncio
import importlib.util
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

TMP = tempfile.mkdtemp(prefix="safe67_single_live_test_")
os.environ["DATA_DIR"] = TMP
os.environ["SYMBOLS"] = "XRP"
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["LIVE_MASTER_ENABLE"] = "0"
os.environ["PAPER_START_BALANCE"] = "500"

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", HERE / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

# Exact SAFE67 constants from the source ZIP.
assert bot.SAFE_ENTRY_PRICE_MIN == 0.67
assert bot.SAFE_ENTRY_PRICE_MAX == 0.75
assert bot.SAFE_ENTRY_MOM_MIN == 0.05
assert bot.SAFE_ENTRY_MOM_MAX == 0.10
assert bot.V2_ELIGIBLE_PRICE_MIN == 0.55
assert bot.V2_ELIGIBLE_PRICE_MAX == 0.75
assert bot.V2_ELIGIBLE_MOM_MIN == 0.03
assert bot.V2_ELIGIBLE_MOM_MAX == 0.30
assert bot.PYRAMID_STEP == 0.08
assert bot.LOOKBACK_TICKS == 2
assert bot.MAX_BUYS_SIDE == 2

# Exactly one strategy per token.
assert len(bot.STRATEGIES) == 1
V = bot.strategy_for_symbol("XRP")
assert V["name"] == "XRP_SAFE67"
assert bot.strategy_mode(V["name"]) == "PAPER"
assert bot.token_enabled("XRP")
assert bot.entry_shares("XRP") == 5
assert bot.pyramid_shares("XRP") == 10
assert bot.configured_stop_loss("XRP") is None


def fresh_book(asset, bid, ask, size=100):
    bot.books[asset] = {
        "bids": {float(bid): float(size)},
        "asks": {float(ask): float(size)},
        "received_ms": bot.now_ms(),
        "source": "test",
    }


def market(cid, up, down):
    now = int(time.time())
    return {
        "condition_id": cid,
        "symbol": "XRP",
        "question": "XRP Up or Down test",
        "slug": f"xrp-updown-5m-{(now//300)*300}",
        "start_ts": (now//300)*300,
        "end_ts": (now//300)*300 + 300,
        "up_asset": up,
        "down_asset": down,
    }


def seed_safe_entry(m):
    fresh_book(m["up_asset"], .68, .69)
    fresh_book(m["down_asset"], .30, .31)
    bot.price_history[m["condition_id"]][m["up_asset"]].extend([
        (bot.now_ms()-6000, .61),
        (bot.now_ms()-3000, .63),
        (bot.now_ms(), .69),
    ])
    bot.price_history[m["condition_id"]][m["down_asset"]].extend([
        (bot.now_ms()-6000, .39),
        (bot.now_ms()-3000, .37),
        (bot.now_ms(), .31),
    ])


# PAPER: ENTRY 5, PYRAMID 10.
m1 = market("paper-standard", "UP1", "DN1")
seed_safe_entry(m1)
asyncio.run(bot.evaluate_variant(m1, V, 30.0))
pos = bot.position_totals(m1["condition_id"], V["name"])
assert abs(pos["bought"] - 5) < 1e-9
assert not pos["has_pyramid"]

fresh_book("UP1", .76, .77)
bot.price_history[m1["condition_id"]]["UP1"].extend([
    (bot.now_ms()-3000, .70), (bot.now_ms(), .77)
])
asyncio.run(bot.evaluate_variant(m1, V, 60.0))
pos = bot.position_totals(m1["condition_id"], V["name"])
assert abs(pos["bought"] - 15) < 1e-9
assert pos["has_pyramid"]

# SL default OFF: even a low bid does nothing.
fresh_book("UP1", .20, .21)
assert asyncio.run(bot.process_stop_loss(m1, V)) is None
assert bot.position_totals(m1["condition_id"], V["name"])["remaining"] == 15

# User can set any valid stop. Existing PYRAMID position uses it immediately.
bot.state_set("stop_loss:XRP", "0.40")
fresh_book("UP1", .39, .40, size=100)
r = asyncio.run(bot.process_stop_loss(m1, V))
assert r and r["mode"] == "PAPER"
assert bot.stop_triggered(m1["condition_id"], V["name"])
assert bot.position_totals(m1["condition_id"], V["name"])["remaining"] < 1e-8

# Post-PYRAMID rule is preserved: a configured SL does not stop the first ENTRY.
m2 = market("paper-entry-only", "UP2", "DN2")
seed_safe_entry(m2)
asyncio.run(bot.evaluate_variant(m2, V, 30.0))
fresh_book("UP2", .10, .11, size=100)
assert asyncio.run(bot.process_stop_loss(m2, V)) is None
assert not bot.stop_triggered(m2["condition_id"], V["name"])
assert abs(bot.position_totals(m2["condition_id"], V["name"])["remaining"] - 5) < 1e-9

# SL can be turned off completely.
bot.state_set("stop_loss:XRP", "OFF")
assert bot.configured_stop_loss("XRP") is None

# Telegram SL command really changes the persisted per-token setting.
_messages = []
async def fake_tg_send(text):
    _messages.append(str(text))
    return True
bot.tg_send = fake_tg_send
asyncio.run(bot.handle_tg("SL XRP 0.35"))
assert abs(bot.configured_stop_loss("XRP") - 0.35) < 1e-9
asyncio.run(bot.handle_tg("SL XRP OFF"))
assert bot.configured_stop_loss("XRP") is None

# Per-token share sizes are configurable.
bot.state_set("entry_shares:XRP", "2.5")
bot.state_set("pyramid_shares:XRP", "4.5")
assert bot.requested_shares(V, "ENTRY") == 2.5
assert bot.requested_shares(V, "PYRAMID") == 4.5

# Mock official SDK LIVE execution preserves exact requested shares.
@dataclass(frozen=True, slots=True, kw_only=True)
class FakeSigned:
    order_type: str = "GTC"
    post_only: bool = False
    side: str = "BUY"
    price: str = "0"
    size: str = "0"

class FakeAccepted:
    ok = True
    status = "matched"
    order_id = "0xorder"
    trade_ids = ("t1",)
    transactions_hashes = ()
    def __init__(self, making, taking):
        self.making_amount = Decimal(str(making))
        self.taking_amount = Decimal(str(taking))
    def model_dump(self, mode="json"):
        return {
            "ok": True,
            "status": self.status,
            "order_id": self.order_id,
            "making_amount": str(self.making_amount),
            "taking_amount": str(self.taking_amount),
            "trade_ids": list(self.trade_ids),
        }

class FakeClient:
    async def create_limit_order(self, **kwargs):
        return FakeSigned(
            side=kwargs["side"],
            price=str(kwargs["price"]),
            size=str(kwargs["size"]),
        )
    async def post_order(self, order):
        size = Decimal(order.size)
        price = Decimal(order.price)
        assert order.order_type == "FAK"
        if order.side == "BUY":
            return FakeAccepted(size * price, size)
        return FakeAccepted(size, size * price)

bot.LIVE_MASTER_ENABLE = True
bot.live_client_ready = True
bot.live_client = FakeClient()
bot.sdk_post_order_with_allowance_recovery = None

m3 = market("live-buy", "UP3", "DN3")
fresh_book("UP3", .68, .69, size=100)
res = asyncio.run(bot.execute_live_fak(
    m3["condition_id"], V, "UP3", "Up", "ENTRY", "BUY", 2.5
))
assert res["ok"]
assert abs(res["filled"] - 2.5) < 1e-9
assert abs(res["avg"] - .69) < 1e-9
assert bot.position_totals(m3["condition_id"], V["name"])["execution_mode"] == "LIVE"

# Cannot cross an open LIVE position to PAPER.
bot.state_set(f"mode:{V['name']}", "OFF")
ok, msg = bot._set_mode_direct(V, "PAPER")
assert not ok and "open LIVE position" in msg

# No hourly report loop exists in this build.
assert not hasattr(bot, "report_loop")
assert not hasattr(bot, "make_report")

print("SAFE67 SINGLE PAPER/LIVE + CONFIGURABLE SL regression: OK")
