# SAFE67 Multi-Asset PAPER/LIVE Bot — single strategy + configurable stop

This version removes the old A/B duplication. There is now **one SAFE67 strategy per token**:

- XRP
- BNB
- SOL
- ETH
- DOGE
- HYPE (Hyperliquid)

BTC is intentionally excluded.

## Strategy logic

The signal logic remains the same as the source ZIP:

```text
V2-eligible:
price    0.55..0.75
momentum 0.03..0.30
lookback 2 ticks

SAFE67 PASS:
price    0.67..0.75
momentum 0.05..0.10

ENTRY
one PYRAMID after +0.08
PYRAMID momentum >0 and <=0.30
max 2 buys
no side switch
first 180 seconds only
3-second signal loop
no pre-decision REST refresh
```

The only execution wrapper change is that a signal is routed to PAPER or LIVE according to that token's mode.

## One mode per token

Each token can independently be:

```text
PAPER
LIVE
OFF
```

Examples:

```text
MODE XRP PAPER
MODE ETH LIVE
MODE DOGE OFF
```

LIVE requires a second confirmation:

```text
MODE ETH LIVE
CONFIRM LIVE ETH
```

The confirmation expires after 60 seconds.

`LIVE_MASTER_ENABLE=1` must also be set on Render before any real order is allowed.

## Turn tokens on/off

Independent of mode:

```text
TOKEN XRP ON
TOKEN XRP OFF
```

Global `STOP` prevents new ENTRY/PYRAMID actions on all tokens. It does not cancel a stop that has already triggered.

## Set share sizes yourself

Per token:

```text
SIZE XRP 5 10
SIZE ETH 3 6
SIZE SOL 2.5 5
```

The first number is ENTRY shares, the second is PYRAMID shares.

Sizes cannot be changed while that token has an open bot position.

LIVE orders are signed limit orders converted to **FAK**, so the bot attempts up to the requested share quantity against the visible book and does not intentionally leave an unfilled remainder resting in the order book. The exchange can still reject a size that violates that market's own minimum order size.

## Optional stop-loss — no B variant

Every token starts with:

```text
SL OFF
```

Set any level:

```text
SL XRP 0.40
SL ETH 0.35
SL SOL 0.30
```

Disable it:

```text
SL XRP OFF
```

To preserve the stop behavior from the last ZIP, an enabled SL is **armed only after the PYRAMID actually fills**. The first ENTRY alone is not stopped.

After it triggers, the bot keeps trying to liquidate the remaining tracked shares. Turning SL OFF after a trigger does not cancel an already-started liquidation.

## Telegram buttons

```text
START
STOP
TOKENS
MODES
SIZES
STOPLOSS
BALANCE
POSITIONS
STATISTICS
TRADES
WALLET
EMERGENCY STOP
```

There are **no hourly ZIP reports** in this build.

## Connect your Polymarket wallet

Do not send your private key in Telegram, chat, GitHub, screenshots, or logs.

On Render → service → Environment add secrets:

```text
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_WALLET_ADDRESS=0x...
```

`POLYMARKET_PRIVATE_KEY` is the signer private key.

`POLYMARKET_WALLET_ADDRESS` is the Polymarket wallet/deposit wallet that owns the collateral/positions. If your account setup does not require an explicit different wallet, the SDK may derive/use the appropriate wallet from the signer setup; use the WALLET command after deploy to verify what the SDK reports.

Keep:

```text
LIVE_MASTER_ENABLE=0
```

for the first deployment.

After deploy press:

```text
WALLET
```

Check that:

```text
SDK: READY
Wallet: expected address
Collateral: expected balance
```

Only then change Render to:

```text
LIVE_MASTER_ENABLE=1
```

and redeploy.

Even with master enabled, every token remains PAPER until you explicitly run `MODE TOKEN LIVE` and confirm it.

## Render

Build:

```text
pip install -r requirements.txt
```

Start:

```text
python main.py
```

Persistent disk:

```text
/var/data
```

The bot uses a new database:

```text
/var/data/safe67_multi6_single_paper_live.db
```

## Real-order safety

The LIVE path includes several guards:

- LIVE master switch on Render.
- Per-token PAPER/LIVE/OFF mode.
- 60-second second confirmation before switching a token to LIVE.
- Mode cannot cross PAPER↔LIVE while that bot token has an open position.
- Size cannot change while a token has an open bot position.
- Before order execution the book is freshness-checked.
- Real order uses FAK rather than a resting GTC order.
- If submission becomes ambiguous after a network/API error, the same market/action is fail-closed and not automatically retried, reducing duplicate-order risk.
- An already-triggered stop continues liquidation even if global trading or the token is turned OFF.

## First real-money test

Use a small size and one token only:

```text
TOKEN XRP ON
TOKEN BNB OFF
TOKEN SOL OFF
TOKEN ETH OFF
TOKEN DOGE OFF
TOKEN HYPE OFF

SIZE XRP 5 10
SL XRP OFF
MODE XRP LIVE
CONFIRM LIVE XRP
START
```

After you verify actual entries/settlement behavior, enable more tokens if desired.

## Tests

```text
python test_live_single.py
```

Expected:

```text
SAFE67 SINGLE PAPER/LIVE + CONFIGURABLE SL regression: OK
```

`strategy_parity_check.txt` verifies that the core SAFE67 signal functions match the source ZIP after normalizing only the execution dispatcher and optional stop guard.
