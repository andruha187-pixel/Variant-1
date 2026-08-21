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
