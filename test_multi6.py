import os
import time
import tempfile
import asyncio
import importlib.util
import zipfile
from pathlib import Path

TMP = tempfile.mkdtemp(prefix='multi6_safe67_test_')
os.environ['DATA_DIR'] = TMP
os.environ['TELEGRAM_BOT_TOKEN'] = ''
os.environ['TELEGRAM_CHAT_ID'] = ''
os.environ['PAPER_START_BALANCE'] = '500'
os.environ['SYMBOLS'] = 'XRP,BNB,SOL,ETH,DOGE,HYPE'

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('bot', HERE / 'main.py')
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.SYMBOLS == ['XRP','BNB','SOL','ETH','DOGE','HYPE']
assert 'BTC' not in bot.SYMBOLS
assert len(bot.STRATEGIES) == 12
assert set(bot.STRATEGIES_BY_SYMBOL) == set(bot.SYMBOLS)
for sym in bot.SYMBOLS:
    pair = bot.STRATEGIES_BY_SYMBOL[sym]
    assert len(pair) == 2
    a, b = pair
    assert a['symbol'] == b['symbol'] == sym
    assert a['stop_loss_price'] is None
    assert b['stop_loss_price'] == 0.40
    assert b['stop_after_pyramid'] is True
    assert a['safe_entry_price_min'] == b['safe_entry_price_min'] == 0.67
    assert a['safe_entry_price_max'] == b['safe_entry_price_max'] == 0.75
    assert a['safe_entry_mom_min'] == b['safe_entry_mom_min'] == 0.05
    assert a['safe_entry_mom_max'] == b['safe_entry_mom_max'] == 0.10
    assert abs(bot.paper_cash(a['name']) - 500.0) < 1e-9
    assert abs(bot.paper_cash(b['name']) - 500.0) < 1e-9

# Prefix/parser checks for all six series.
slot = (int(time.time()) // 300) * 300
for sym in bot.SYMBOLS:
    prefix = bot.ASSET_CONFIG[sym]['prefix']
    raw = {
        'conditionId': f'cid-parse-{sym}',
        'question': f'{bot.ASSET_CONFIG[sym]["label"]} Up or Down test',
        'slug': f'{prefix}-{slot}',
        'outcomes': '["Up","Down"]',
        'clobTokenIds': f'["UP_{sym}","DOWN_{sym}"]',
    }
    event = {'title': raw['question'], 'slug': raw['slug']}
    m = bot.parse_market_from_event(raw, event, sym)
    assert m and m['symbol'] == sym
    assert m['slug'] == f'{prefix}-{slot}'
    assert bot.market_symbol(m) == sym

# Full A/B flow on XRP.
sym = 'XRP'
a, b = bot.STRATEGIES_BY_SYMBOL[sym]
market = {
    'condition_id': 'cid-xrp-flow',
    'symbol': sym,
    'question': 'XRP Up or Down test',
    'slug': f'xrp-updown-5m-{slot}',
    'start_ts': slot,
    'end_ts': slot + 300,
    'up_asset': 'XRP_UP',
    'down_asset': 'XRP_DOWN',
}
bot.markets[market['condition_id']] = market
bot.persist_market(market)

nowms = bot.now_ms()
bot.books['XRP_UP'] = {
    'bids': {0.69: 100.0}, 'asks': {0.70: 100.0},
    'received_ms': nowms, 'source': 'test'
}
bot.books['XRP_DOWN'] = {
    'bids': {0.29: 100.0}, 'asks': {0.30: 100.0},
    'received_ms': nowms, 'source': 'test'
}
bot.price_history['cid-xrp-flow']['XRP_UP'].extend([
    (nowms-6000, 0.63), (nowms-3000, 0.65), (nowms, 0.70)
])
bot.price_history['cid-xrp-flow']['XRP_DOWN'].extend([
    (nowms-6000, 0.37), (nowms-3000, 0.35), (nowms, 0.30)
])

async def eval_pair(elapsed):
    for v in bot.STRATEGIES_BY_SYMBOL[sym]:
        await bot.evaluate_variant(market, v, elapsed)

asyncio.run(eval_pair(30.0))
for v in (a,b):
    pos = bot.position_totals('cid-xrp-flow', v['name'])
    assert len(pos['buys']) == 1
    assert abs(pos['bought'] - 5.0) < 1e-9
    assert pos['has_pyramid'] is False

# Stop must NOT be armed after entry only.
bot.books['XRP_UP']['bids'] = {0.35: 100.0}
bot.books['XRP_UP']['received_ms'] = bot.now_ms()
assert bot.process_stop_loss(market, b) is None
assert not bot.stop_triggered('cid-xrp-flow', b['name'])

# Restore/advance to pyramid at +0.08.
bot.books['XRP_UP']['bids'] = {0.77: 100.0}
bot.books['XRP_UP']['asks'] = {0.78: 100.0}
bot.books['XRP_UP']['received_ms'] = bot.now_ms()
ms2 = bot.now_ms()
bot.price_history['cid-xrp-flow']['XRP_UP'].extend([
    (ms2-3000, 0.74), (ms2, 0.78)
])
asyncio.run(eval_pair(60.0))
for v in (a,b):
    pos = bot.position_totals('cid-xrp-flow', v['name'])
    assert len(pos['buys']) == 2
    assert abs(pos['bought'] - 15.0) < 1e-8
    assert pos['has_pyramid'] is True

# B stop after pyramid at best bid <= .40. A remains untouched.
bot.books['XRP_UP']['bids'] = {0.39: 100.0}
bot.books['XRP_UP']['received_ms'] = bot.now_ms()
r = bot.process_stop_loss(market, b)
assert r and r['filled'] > 14.999
assert bot.stop_triggered('cid-xrp-flow', b['name'])
assert bot.position_totals('cid-xrp-flow', b['name'])['remaining'] < 1e-8
assert abs(bot.position_totals('cid-xrp-flow', a['name'])['remaining'] - 15.0) < 1e-8

# Settlement must touch only XRP's two accounts, not ETH etc.
eth_a, eth_b = bot.STRATEGIES_BY_SYMBOL['ETH']
eth_before = (bot.paper_cash(eth_a['name']), bot.paper_cash(eth_b['name']))
asyncio.run(bot.settle_market('cid-xrp-flow', 'XRP_UP', 'Up'))
with bot.db() as conn:
    rows = conn.execute("SELECT variant FROM market_results WHERE condition_id=? ORDER BY variant", ('cid-xrp-flow',)).fetchall()
assert {r['variant'] for r in rows} == {a['name'], b['name']}
assert (bot.paper_cash(eth_a['name']), bot.paper_cash(eth_b['name'])) == eth_before

# Build one empty-ish hourly report and verify all 12 folders exist.
hour_start = slot - (slot % 3600)
path, summaries = bot.make_report(hour_start, hour_start + 3600)
assert len(summaries) == 12
assert set(s['symbol'] for s in summaries) == set(bot.SYMBOLS)
with zipfile.ZipFile(path, 'r') as z:
    names = set(z.namelist())
    assert 'variants_summary.csv' in names
    assert 'markets.csv' in names
    assert 'report.txt' in names
    for s in bot.SYMBOLS:
        assert f'{s}/A_safe67_no_stop/summary.csv' in names
        assert f'{s}/B_safe67_postpyr_stop_040/summary.csv' in names
        assert f'{s}/A_safe67_no_stop/position_trajectory.csv' in names
        assert f'{s}/B_safe67_postpyr_stop_040/stop_events.csv' in names

print('MULTI6 SAFE67 A/B regression: OK')
