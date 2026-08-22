# M05_P08_L2 + Binance CONF60 Bot v2.4

PAPER-first Polymarket BTC 5-minute bot using the selected research strategy.

## Strategy
- M05_P08_L2: entry move 0.05, pyramid step 0.08, lookback 2, switch move 0.05, max 6 buys per side.
- Unlike the old M03_V2_LOCK build, M05_P08_L2 may start the opposite side when its own switch momentum reaches 0.05.
- Contract range remains 0.08-0.95, matching the research simulator.
- Binance V2 CONF60 remains the entry gate with aggTrade + depth20@100ms scoring.
- Binance freshness is based only on real aggTrade messages.

## PAPER
- Default starting balance: $500.
- Default lot: 10 shares.
- Trading window: first 180 seconds of each BTC 5-minute market.
- PAPER fills use current Polymarket book depth and estimated taker fees.
- Results persist in `/var/data/m05_p08_l2_conf60.db`. This is a new database so old M03 paper results do not mix with this test.

## Render
1. Replace `main.py`, `.env.example`, `README.md`, and `requirements.txt` in the repository.
2. Build: `pip install -r requirements.txt`
3. Start: `python main.py`
4. Persistent disk: `/var/data`
5. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
6. Keep `ENABLE_LIVE=0` while testing.

## Important
LIVE remains hard-locked unless `ENABLE_LIVE=1` and all credentials are configured. Telegram cannot bypass that lock.
