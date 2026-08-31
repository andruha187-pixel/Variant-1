import os
import time
import asyncio
import tempfile
import importlib.util
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

TMP = tempfile.mkdtemp(prefix='bnb_eth_gh_')
os.environ['DATA_DIR'] = TMP
os.environ['TELEGRAM_BOT_TOKEN'] = ''
os.environ['TELEGRAM_CHAT_ID'] = ''
os.environ['LIVE_MASTER_ENABLE'] = '0'
os.environ['PAPER_START_BALANCE'] = '500'

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('bot', HERE/'main.py')
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.SYMBOLS == ['BTC','XRP','BNB','SOL','ETH','DOGE','HYPE']
assert bot.TRADE_SYMBOLS == ['BNB','ETH']
assert [(v['symbol'],v['code']) for v in bot.STRATEGIES] == [('BNB','G'),('BNB','H'),('ETH','G'),('ETH','H')]
for v in bot.STRATEGIES:
    assert (v['safe_entry_price_min'],v['safe_entry_price_max']) == (0.67,0.70)
    assert (v['safe_entry_mom_min'],v['safe_entry_mom_max']) == (0.05,0.10)
    assert v['consensus_min_other_tokens'] == 2
    assert v['consensus_window_sec'] == 10
    assert bot.entry_shares(v) == 5
    assert bot.strategy_mode(v['name']) == 'PAPER'
assert bot.dca_shares(bot.strategy_for('BNB','H')) == 5
assert bot.dca_shares(bot.strategy_for('BNB','G')) == 0
H = bot.strategy_for('BNB','H')
assert H['dca_arm_price'] == .50
assert H['dca_min_buy_price'] == .30
assert H['dca_max_buy_price'] == .60
assert H['dca_rebound_mom'] == .05
assert H['dca_rebound_mom_max'] == .15
assert H['dca_deadline_sec'] == 120


def fresh_book(asset,bid,ask,size=100):
    bot.books[asset]={'bids':{float(bid):float(size)},'asks':{float(ask):float(size)},'received_ms':bot.now_ms(),'source':'test'}

def make_market(symbol,tag):
    slot=(int(time.time())//300)*300
    cfg=bot.ASSET_CONFIG[symbol]
    m={'condition_id':f'cid-{symbol}-{tag}','symbol':symbol,'question':f'{symbol} test','slug':f"{cfg['prefix']}-{slot}",
       'start_ts':slot,'end_ts':slot+300,'up_asset':f'{symbol}UP-{tag}','down_asset':f'{symbol}DN-{tag}'}
    bot.markets[m['condition_id']]=m; bot.persist_market(m); return m

def seed_up(m,ask,mom):
    ms=bot.now_ms(); ref=ask-mom; mid=ref+mom/2
    fresh_book(m['up_asset'],max(.01,ask-.01),ask)
    fresh_book(m['down_asset'],max(.01,1-ask-.01),max(.01,1-ask))
    bot.price_history[m['condition_id']][m['up_asset']].clear()
    bot.price_history[m['condition_id']][m['up_asset']].extend([(ms-6000,ref),(ms-3000,mid),(ms,ask)])
    bot.price_history[m['condition_id']][m['down_asset']].clear()
    bot.price_history[m['condition_id']][m['down_asset']].extend([(ms-6000,.45),(ms-3000,.40),(ms,.35)])

def set_path(m,ref,mid,ask):
    ms=bot.now_ms(); fresh_book(m['up_asset'],max(.01,ask-.01),ask)
    h=bot.price_history[m['condition_id']][m['up_asset']]; h.clear(); h.extend([(ms-6000,ref),(ms-3000,mid),(ms,ask)])

# Monitor-only BTC and SOL can confirm a BNB trade.
base=bot.now_ms()
mbtc=make_market('BTC','vote1'); seed_up(mbtc,.60,.04); bot.record_first_v2_vote(mbtc,30,base-5000)
msol=make_market('SOL','vote2'); seed_up(msol,.61,.04); bot.record_first_v2_vote(msol,30,base-2000)
mbnb=make_market('BNB','target'); seed_up(mbnb,.69,.07); bot.record_first_v2_vote(mbnb,30,base)
G=bot.strategy_for('BNB','G'); H=bot.strategy_for('BNB','H')
asyncio.run(bot.evaluate_consensus_variant(mbnb,G,35))
asyncio.run(bot.evaluate_consensus_variant(mbnb,H,35))
assert bot.position_totals(mbnb['condition_id'],G['name'])['bought'] == 5
assert bot.position_totals(mbnb['condition_id'],H['name'])['bought'] == 5
with bot.db() as conn:
    rows=conn.execute('SELECT variant,confirm_count,confirm_symbols_json,passed FROM consensus_events WHERE condition_id=?',(mbnb['condition_id'],)).fetchall()
assert len(rows)==2 and all(r['passed']==1 and r['confirm_count']==2 for r in rows)
assert all(set(bot.parse_jsonish(r['confirm_symbols_json']))=={'BTC','SOL'} for r in rows)

# H exact DCA behavior: arm <=.50, no arm-tick buy, then valid rebound once.
set_path(mbnb,.58,.54,.50)
asyncio.run(bot.evaluate_consensus_variant(mbnb,H,60))
assert bot.get_variant_state(mbnb['condition_id'],H)['dca_armed']
assert bot.position_totals(mbnb['condition_id'],H['name'])['bought']==5
set_path(mbnb,.19,.22,.25)  # below .30: reject
asyncio.run(bot.evaluate_consensus_variant(mbnb,H,70))
assert bot.position_totals(mbnb['condition_id'],H['name'])['bought']==5
set_path(mbnb,.15,.25,.35)  # +.20: reject
asyncio.run(bot.evaluate_consensus_variant(mbnb,H,80))
assert bot.position_totals(mbnb['condition_id'],H['name'])['bought']==5
set_path(mbnb,.25,.30,.35)  # +.10: accept
asyncio.run(bot.evaluate_consensus_variant(mbnb,H,90))
posh=bot.position_totals(mbnb['condition_id'],H['name'])
assert posh['bought']==10 and posh['dca_trades']==1
asyncio.run(bot.evaluate_consensus_variant(mbnb,H,95))
assert bot.position_totals(mbnb['condition_id'],H['name'])['bought']==10
assert bot.position_totals(mbnb['condition_id'],G['name'])['bought']==5

# User-configurable Telegram SIZE commands persist independently.
_msgs=[]
async def fake_tg_send(text):
    _msgs.append(str(text)); return True
bot.tg_send=fake_tg_send
asyncio.run(bot.handle_tg('SIZE ETH 7 6'))
assert bot.entry_shares(bot.strategy_for('ETH','G'))==7
assert bot.entry_shares(bot.strategy_for('ETH','H'))==7
assert bot.dca_shares(bot.strategy_for('ETH','H'))==6
asyncio.run(bot.handle_tg('SIZE ETH G 8'))
asyncio.run(bot.handle_tg('SIZE ETH H 9 4'))
assert bot.entry_shares(bot.strategy_for('ETH','G'))==8
assert bot.entry_shares(bot.strategy_for('ETH','H'))==9
assert bot.dca_shares(bot.strategy_for('ETH','H'))==4

# Mock LIVE FAK keeps exact requested shares and existing safety wrapper.
@dataclass(frozen=True, slots=True, kw_only=True)
class FakeSigned:
    order_type:str='GTC'; post_only:bool=False; side:str='BUY'; price:str='0'; size:str='0'
class FakeAccepted:
    ok=True; status='matched'; order_id='0xorder'; trade_ids=('t1',); transactions_hashes=()
    def __init__(self,making,taking): self.making_amount=Decimal(str(making)); self.taking_amount=Decimal(str(taking))
    def model_dump(self,mode='json'):
        return {'ok':True,'status':self.status,'order_id':self.order_id,'making_amount':str(self.making_amount),'taking_amount':str(self.taking_amount),'trade_ids':list(self.trade_ids)}
class FakeClient:
    async def create_limit_order(self,**kwargs):
        return FakeSigned(side=kwargs['side'],price=str(kwargs['price']),size=str(kwargs['size']))
    async def post_order(self,order):
        assert order.order_type=='FAK'
        size=Decimal(order.size); price=Decimal(order.price)
        return FakeAccepted(size*price,size) if order.side=='BUY' else FakeAccepted(size,size*price)

bot.LIVE_MASTER_ENABLE=True; bot.live_client_ready=True; bot.live_client=FakeClient(); bot.sdk_post_order_with_allowance_recovery=None
meth=make_market('ETH','live'); fresh_book(meth['up_asset'],.68,.69,100)
VE=bot.strategy_for('ETH','G')
res=asyncio.run(bot.execute_live_fak(meth['condition_id'],VE,meth['up_asset'],'Up','ENTRY','BUY',8))
assert res['ok'] and abs(res['filled']-8)<1e-9 and abs(res['avg']-.69)<1e-9
assert bot.position_totals(meth['condition_id'],VE['name'])['execution_mode']=='LIVE'

# Mode crossing is blocked while an open LIVE position exists.
bot.state_set(f"mode:{VE['name']}",'OFF')
ok,msg=bot._set_mode_direct(VE,'PAPER')
assert not ok and 'open LIVE position' in msg

# There are no tradable strategies for monitor-only tokens.
assert bot.strategies_for_market(mbtc)==[]
assert bot.strategies_for_market(msol)==[]

print('BNB/ETH G/H PAPER/LIVE regression: OK')
