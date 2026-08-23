import os
import tempfile
import importlib.util
from pathlib import Path

tmp = tempfile.mkdtemp(prefix="m03_abc_test_")
os.environ["DATA_DIR"] = tmp
os.environ["PAPER_START_BALANCE"] = "500"
os.environ["CONF_MIN"] = "65"

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", HERE / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

bot.init_db()

names = [s["name"] for s in bot.STRATEGIES]
assert names == ["M03_V3_NOSW90", "M03_V2_LOCK", "M03_V5_DYNAMIC"]
for name in names:
    assert abs(bot.paper_cash(name) - 500.0) < 1e-9

# Exact parameters.
v3 = bot.STRATEGY_BY_NAME["M03_V3_NOSW90"]
v2 = bot.STRATEGY_BY_NAME["M03_V2_LOCK"]
v5 = bot.STRATEGY_BY_NAME["M03_V5_DYNAMIC"]
assert (v3["entry_move"], v3["pyramid_step"], v3["lookback"], v3["max_buys_side"]) == (0.03, 0.08, 2, 5)
assert v3["entry_cutoff_sec"] == 90 and v3["allow_switch"] is False
assert v2["entry_price_min"] == 0.55 and v2["entry_price_max"] == 0.75
assert v2["momentum_cap"] == 0.30 and v2["max_buys_side"] == 6 and v2["allow_switch"] is False
assert v5["switch_move"] == 0.03 and v5["max_buys_side"] == 5 and v5["dynamic_switch_v5"] is True

# Shadow state is independent by strategy and requires a shadow entry before pyramid.
cid = "test-market"
asset = "UP"
f64 = {"data_age_ms": 10, "confidence": 64.0}
f66 = {"data_age_ms": 10, "confidence": 66.0}

ok, _ = bot.exact_shadow_decision(cid, v3["name"], asset, "ENTRY", f64)
assert ok is False
ok, reason = bot.exact_shadow_decision(cid, v3["name"], asset, "PYRAMID", f66)
assert ok is False and reason == "no_shadow_position"

ok, _ = bot.exact_shadow_decision(cid, v2["name"], asset, "ENTRY", f66)
assert ok is True
assert asset in bot.shadow_accepted_sides[(cid, v2["name"])]
assert asset not in bot.shadow_accepted_sides[(cid, v3["name"])]

# A real accepted paper fill affects only that strategy's account.
snap = {
    "asks": {0.60: 100.0},
    "bids": {},
    "received_ms": 1000,
    "captured_ms": 1000,
}
base = bot.execute_baseline_from_snapshot("m1", v2, asset, "Up", "ENTRY", snap)
assert base and abs(base["filled"] - 10.0) < 1e-9
assert bot.paper_execute_from_baseline(v2, "m1", asset, "Up", "ENTRY", base) is True
assert bot.paper_cash(v2["name"]) < 500.0
assert abs(bot.paper_cash(v3["name"]) - 500.0) < 1e-9
assert abs(bot.paper_cash(v5["name"]) - 500.0) < 1e-9

# V3 stops all new buys after 90 sec, matching the research simulator.
bot.price_history["cutoff"]["UP"].extend([(1, 0.50), (2, 0.52), (3, 0.56)])
bot.price_history["cutoff"]["DOWN"].extend([(1, 0.50), (2, 0.48), (3, 0.44)])
tick = {
    "sides": [("UP", "Up"), ("DOWN", "Down")],
    "books": {
        "UP": {"asks": {0.56: 100}, "bids": {}, "received_ms": 1, "captured_ms": 1},
        "DOWN": {"asks": {0.44: 100}, "bids": {}, "received_ms": 1, "captured_ms": 1},
    },
}
assert bot.candidate_for_strategy("cutoff", v3, 91.0, tick) is None

# Settlement credits only the matching strategy's independent cash.
import asyncio
asyncio.run(bot.settle_market("m1", asset, "Up"))
assert abs(bot.paper_cash(v2["name"]) - (500.0 - base["total"] + 10.0)) < 1e-6
assert abs(bot.paper_cash(v3["name"]) - 500.0) < 1e-9
assert abs(bot.paper_cash(v5["name"]) - 500.0) < 1e-9
s2 = bot.account_stats(v2["name"])
s3 = bot.account_stats(v3["name"])
assert s2["traded_markets"] == 1 and s2["wins"] == 1
assert s3["traded_markets"] == 0

print("three-way CONF65 regression: OK")
