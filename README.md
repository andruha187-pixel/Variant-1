# M03 Three-Way CONF65 PAPER Bot v3.0

One process runs three candidate strategies on the same BTC 5-minute Polymarket market data and the same Binance decision snapshot.

## Three independent strategies

### A — M03_V3_NOSW90 + CONF65
- entry move: 0.03
- pyramid step: 0.08
- lookback: 2
- no side switching
- max 5 buys on a side
- all new buys stop after second 90, exactly like the research implementation

### B — M03_V2_LOCK + CONF65
- entry move: 0.03
- pyramid step: 0.08
- lookback: 2
- no side switching
- max 6 buys
- first-entry price band: 0.55–0.75
- momentum cap: 0.30

### C — M03_V5_DYNAMIC + CONF65
- entry move: 0.03
- pyramid step: 0.08
- lookback: 2
- switch move: 0.03
- max 5 buys per side
- dynamic switch rules from the research simulator:
  - first 60 sec: switch price > 0.45 blocked
  - after 60 sec: 0.46–0.50 blocked when momentum >= 0.10
  - after 60 sec: 0.51–0.70 blocked
  - >0.70 remains allowed as in the research variant

## Exact-shadow architecture

For each strategy independently:

1. The base M03 strategy generates one strongest-momentum signal.
2. Its internal BASE fill is simulated first and advances `buys`, `last_buy`, and side state even if Binance later rejects the signal.
3. Binance CONF65 is applied as a shadow layer.
4. ENTRY/SWITCH can start a shadow side only with fresh Binance data and confidence >= 65.
5. PYRAMID also requires that the same side previously had an accepted shadow ENTRY/SWITCH.

This is the same shadow concept used by the old V2 research bot; the Binance filter does not rewrite the underlying base strategy state.

## Fair A/B/C timing

All three variants run inside one process:
- one ~3-second scheduler;
- one captured Polymarket order-book snapshot per market/tick;
- one shared Binance core-feature snapshot per market/tick;
- independent base states;
- independent shadow states;
- independent PAPER balances.

This avoids the cross-Render timing/phase problem that occurred when strategies were compared in separate services.

## PAPER balances and database

Each strategy starts from its own `$500` by default. Balances are not pooled.

Database:
`/var/data/m03_threeway_conf65_ab.db`

Tables include the strategy name:
- `signals`
- `baseline_trades`
- `trades`
- `results`

So transactions, fees, settlements, PnL and open positions remain separate for A, B and C.

## Telegram

The old control buttons remain.

- `BALANCE` shows three separate accounts.
- `STATISTICS` shows W/L, trade count, fees, average win/loss, PnL and equity separately.
- `POSITIONS` sends one separate position report per strategy.
- `TRADES` sends one separate Telegram message per strategy with its last 10 trades.
- Market settlement reports show each strategy's PnL and cash separately.

`START` and `STOP` control all three together.

## LIVE

This build is deliberately PAPER-only.

Three independent virtual `$500` accounts cannot be mapped safely to one real Polymarket wallet without first deciding position sizing and capital allocation. The LIVE button therefore stays locked during this comparison.

## Render

1. Replace the repository files with this package.
2. Build command: `pip install -r requirements.txt`
3. Start command: `python main.py`
4. Persistent disk: `/var/data`
5. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
6. Leave `CONF_MIN=65`.
7. Press START in Telegram after the service comes online.

On startup the log should contain:

`3.0-paper-abc-m03-conf65-exact-shadow started | PAPER ONLY | CONF>=65.0`

## Verification

Run:

`python test_threeway.py`

Expected:

`three-way CONF65 regression: OK`
