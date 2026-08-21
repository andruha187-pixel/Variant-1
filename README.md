# M03 CONF60 Bot V2.3

This build restores the exact old V2 research logic used for the selected `M03_V2_LOCK + CONF60` result, while keeping the newer PAPER/LIVE execution shell.

## V2.3 changes

- Exact `M03_V2_LOCK`: entry move 0.03, pyramid 0.08, lookback 2, max 6 buys/side, entry price 0.55-0.75, momentum cap 0.30, no switch.
- Exact old V2 CONF60 scoring: book imbalance is included with `W_BOOK=14`.
- Exact old V2 combined Binance Futures stream: `aggTrade + depth20@100ms` via `/market/stream?streams=...`.
- Safety correction: Binance freshness (`data_age_ms`) is updated only by a valid real `aggTrade`; depth cannot make stale trade data look fresh.
- PAPER account remains $500 by default.
- LIVE remains locked unless `ENABLE_LIVE=1` and credentials are configured. Telegram cannot bypass the environment lock.

# M03_V2_LOCK + Binance CONF60

Standalone PAPER-first Polymarket BTC 5-minute bot.

## What it does
- Only one strategy: M03_V2_LOCK + Binance CONF60.
- Starts with a persistent $500 PAPER account by default.
- Cash is debited when virtual orders fill and winning payout is credited only after resolution.
- Uses live Polymarket order-book depth for simulated fills, including partial fills.
- Uses the crypto taker-fee curve in PAPER mode.
- Binance is only a signal/filter source. Trades are on Polymarket.
- Telegram controls: START, STOP, BALANCE, STATISTICS, POSITIONS, TRADES, PAPER, LIVE, EMERGENCY STOP.
- LIVE is hard-locked by ENABLE_LIVE=0.

## Render
1. Create a new GitHub repository and upload `main.py` and `requirements.txt`.
2. Render -> New Web Service -> connect the repository.
3. Build command: `pip install -r requirements.txt`
4. Start command: `python main.py`
5. Add a persistent disk mounted at `/var/data`.
6. Add environment variables from `.env.example`.
7. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
8. Keep `ENABLE_LIVE=0`.

## PAPER accounting
`cash` is real simulated free cash. When a PAPER buy fills, cost + fee is deducted immediately.
At resolution, only winning shares are credited at $1/share. The next market therefore uses the updated balance.
The database persists under `/var/data/m03_conf60.db`.

## LIVE
Do not enable LIVE until PAPER results and accounting have been checked.
LIVE requires the Polymarket wallet/funder address and API credentials. The bot has a second safety step:
switching Telegram to LIVE automatically leaves trading STOPPED; START must be pressed separately.

Important: verify the installed `py-clob-client-v2` API against the current official Polymarket docs before first real-money order.


## v2.2 Binance feed fix
Futures aggTrade and depth run on separate WebSocket connections. CONF freshness uses only aggTrade.
