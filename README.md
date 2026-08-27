# SAFE67 Multi-6 A/B PAPER Bot

Точная мульти-токенная версия последнего BTC-бота SAFE67 A/B, но **без Bitcoin**.

## Токены

Бот одновременно отслеживает 6 Polymarket 5-minute Up/Down серий:

- XRP
- BNB
- SOL (Solana)
- ETH (Ethereum)
- DOGE (Dogecoin)
- HYPE (Hyperliquid)

Hyperliquid в slug/тикере Polymarket используется как `HYPE`.

## Логика не менялась

Для каждого токена работают две независимые PAPER-версии.

### A / SAFE67 NO STOP

- сначала ждём первый V2-eligible сигнал:
  - price `0.55–0.75`
  - momentum `0.03–0.30`
- SAFE PASS только если первый V2-eligible сигнал:
  - price `0.67–0.75`
  - momentum `0.05–0.10`
- ENTRY = `5 shares`
- PYRAMID = `10 shares` после роста ещё на `+0.08`
- максимум 2 покупки / 15 shares
- SWITCH = OFF
- stop-loss отсутствует

### B / SAFE67 POST-PYR STOP 0.40

Входы и PYRAMID полностью такие же, как A.

Стоп не существует, пока открыты только первые 5 shares.

После фактического PYRAMID стоп вооружается:

```text
best bid <= 0.40
```

После триггера PAPER-бот продаёт оставшиеся shares по реально видимым bid-уровням стакана. Если ликвидности для полного выхода нет, продолжает выходить на последующих новых стаканах.

## Сигнальный цикл

Сохранён тот же CLEAN LOOP, что в последней BTC-версии:

- примерно один decision tick каждые 3 секунды;
- перед сигнальным решением **нет** принудительного REST `ensure_book()`;
- signal history берётся из WebSocket-maintained стакана;
- `ensure_book()` остаётся непосредственно перед PAPER BUY;
- у B `ensure_book()` также используется перед проверкой/исполнением стопа.

То есть логика входа переносится на остальные токены без дополнительной модификации.

## 12 независимых PAPER-счетов

Каждый токен имеет собственные A и B счета по $500:

```text
XRP_A / XRP_B
BNB_A / BNB_B
SOL_A / SOL_B
ETH_A / ETH_B
DOGE_A / DOGE_B
HYPE_A / HYPE_B
```

Итого по умолчанию 12 независимых PAPER-экспериментов. Деньги между токенами не смешиваются, поэтому потом можно точно определить, на каком активе SAFE67 работает лучше.

## Market discovery

Бот использует 5-minute slug chains:

```text
xrp-updown-5m-{epoch}
bnb-updown-5m-{epoch}
sol-updown-5m-{epoch}
eth-updown-5m-{epoch}
doge-updown-5m-{epoch}
hype-updown-5m-{epoch}
```

Для каждого токена автоматически обнаруживаются текущий, предыдущий и следующий 5-минутный рынок.

## Telegram

Сохранены кнопки:

- START
- STOP
- BALANCE
- STATISTICS
- POSITIONS
- TRADES
- PAPER
- LIVE
- EMERGENCY STOP

`BALANCE` и `STATISTICS` присылают отдельный блок по каждому токену с A/B результатами.

`POSITIONS` и `TRADES` показывают все 12 PAPER-вариантов.

После первого запуска торговля стоит OFF — нажми START.

Версия PAPER-only. LIVE заблокирован.

## Часовой ZIP-отчёт

Каждый UTC-час, примерно через 5 минут после его окончания, Telegram получает один общий ZIP.

В корне:

```text
variants_summary.csv
markets.csv
report.txt
```

Далее отдельные папки по токенам и A/B:

```text
XRP/A_safe67_no_stop/
XRP/B_safe67_postpyr_stop_040/
BNB/A_safe67_no_stop/
BNB/B_safe67_postpyr_stop_040/
SOL/...
ETH/...
DOGE/...
HYPE/...
```

В каждой папке:

```text
summary.csv
gate_decisions.csv
paper_trades.csv
paper_exits.csv
stop_events.csv
signals.csv
market_results.csv
position_trajectory.csv
report.txt
```

Это позволит отдельно анализировать XRP, BNB, SOL, ETH, DOGE и HYPE и затем сравнить их с BTC.

## База

Новая база:

```text
/var/data/safe67_multi6_ab_postpyr_stop40.db
```

Она не смешивается с BTC-ботом.

## Render

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
python main.py
```

Persistent disk:

```text
/var/data
```

Существующие `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID` можно оставить те же.

## Тест

```text
python test_multi6.py
```

Ожидаемый результат:

```text
MULTI6 SAFE67 A/B regression: OK
```

## Важно

Параметры SAFE67 были перенесены с BTC **без оптимизации под другие монеты**. Именно так и задумано: сначала собираем честный out-of-sample результат на каждом токене, потом сравниваем, где стратегия реально имеет преимущество.
