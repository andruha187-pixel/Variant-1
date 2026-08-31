# BNB + ETH G/H FIRST‑V2 Consensus — PAPER/LIVE

Торговая версия стратегий **G и H**. Реальные и PAPER-сделки открываются **только на BNB и ETH**.

При этом бот продолжает наблюдать 7 пятиминутных рынков:

`BTC, XRP, BNB, SOL, ETH, DOGE, HYPE`

Это обязательно для исходной логики G/H: целевому BNB или ETH нужны **минимум 2 DISTINCT OTHER токена** с одинаковым FIRST‑V2 направлением за предыдущие 10 секунд. Остальные пять токенов являются только источниками голосов и никогда не открывают позиции.

## Стратегия G

FIRST‑V2 голос любого наблюдаемого токена:

- price `0.55..0.75`
- momentum `0.03..0.30`
- lookback `2` decision ticks

Для BNB/ETH G:

- target ask `0.67..0.70`
- target momentum `+0.05..+0.10`
- минимум `2` других токена
- то же направление
- FIRST‑V2 голоса в предыдущие `10 sec`
- один ENTRY
- DCA нет
- stop-loss нет
- side switching нет

Default ENTRY: **5 shares**.

## Стратегия H

ENTRY полностью совпадает с G.

После фактического ENTRY разрешён один safer reversal DCA:

1. held-side ask `<= 0.50` при elapsed `<=120 sec` → DCA ARMED;
2. на tick, где произошло ARM, покупки нет;
3. на более позднем tick ask должен быть `0.30..0.60`;
4. rebound momentum должен быть `+0.05..+0.15`;
5. elapsed `<=120 sec`;
6. один DCA максимум.

Default: **ENTRY 5 shares + DCA 5 shares**.

## Настройка shares из Telegram

Быстро на весь токен:

```text
SIZE BNB 7 7
```

Это означает:

- BNB G ENTRY = 7
- BNB H ENTRY = 7
- BNB H DCA = 7

Отдельно по стратегиям:

```text
SIZE BNB G 7
SIZE BNB H 7 5
SIZE ETH G 5
SIZE ETH H 5 5
```

Размер нельзя менять, пока у конкретной стратегии есть открытая bot-tracked позиция.

## PAPER / LIVE отдельно для G и H

Каждая из четырёх стратегий имеет независимый режим:

```text
BNB G
BNB H
ETH G
ETH H
```

Команды:

```text
MODE BNB G PAPER
MODE BNB G OFF
MODE BNB G LIVE
CONFIRM LIVE BNB G

MODE BNB H PAPER
MODE BNB H LIVE
CONFIRM LIVE BNB H
```

LIVE требует одновременно:

1. `LIVE_MASTER_ENABLE=1` в Environment;
2. готовый Polymarket wallet SDK;
3. `MODE <TOKEN> <G/H> LIVE`;
4. `CONFIRM LIVE <TOKEN> <G/H>` в течение 60 секунд.

**Важно:** если одновременно включить `BNB G = LIVE` и `BNB H = LIVE`, при одном и том же G/H entry-сигнале будут отправлены **два независимых реальных ENTRY**. Это ожидаемое поведение двух стратегий. Если нужна только одна реальная позиция, вторую стратегию оставь PAPER или OFF.

## Telegram buttons

```text
START
STOP
MODES
SIZES
BALANCE
POSITIONS
STATISTICS
TRADES
WALLET
EMERGENCY STOP
```

`STOP` / `EMERGENCY STOP` блокирует новые ENTRY/DCA. Стратегия stop-loss не используется.

## LIVE order execution

LIVE использует защитный wrapper из торгового бота:

- `LIVE_MASTER_ENABLE`;
- отдельный PAPER/LIVE/OFF mode;
- 60-секундное подтверждение LIVE;
- fresh-book check перед execution;
- signed limit order, конвертированный в `FAK`;
- requested shares — максимальный объём попытки исполнения;
- при неоднозначной ошибке submission этот market/action становится fail-closed и автоматически повторно не отправляется;
- режим PAPER↔LIVE нельзя пересечь при открытой позиции этой стратегии.

Частичный fill возможен, если в видимом стакане недостаточно ликвидности.

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

Новая база:

```text
/var/data/bnb_eth_gh_paper_live.db
```

Hourly ZIP reports в этой торговой сборке отключены.

## Первый запуск

Сначала оставь:

```text
LIVE_MASTER_ENABLE=0
```

После deploy нажми `WALLET`. Проверь SDK, wallet и collateral. Потом при необходимости включай master и redeploy.

Для первого реального теста лучше оставить только одну стратегию LIVE, например:

```text
MODE BNB H LIVE
CONFIRM LIVE BNB H
```

а остальные держать PAPER/OFF.

## Regression test

```text
python test_bnb_eth_gh.py
```

Expected:

```text
BNB/ETH G/H PAPER/LIVE regression: OK
```
