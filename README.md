# Парсер дивидендной истории российских акций

Скрипты для получения истории дивидендных выплат с сайта https://закрытияреестров.рф/

## Зачем два слоя данных

Сайт хранит дивиденд **как объявляли на дату решения** (на акцию того номинала).
После сплита / обратного сплита новые выплаты уже в новых акциях, старые — нет.
Без корректировки ряды GMKN/TRNFP/PLZL/VTBR/IRAO сравнивать нельзя.

Поэтому:

| Путь | Содержимое |
|------|------------|
| `data/raw/*.csv` | Сырые значения с сайта (не править руками) |
| `data/*.csv` | **Использовать эти** — приведены к текущей акции |
| `metadata/splits.json` | Реестр корпоративных действий |

## Описание проекта

Автосбор дивидендов российских компаний:

- даты закрытия реестра
- год, за который платят
- тип периода (`full year`, `half year`, `3|6|9 months`)
- размер дивиденда в рублях
- обыкновенные и привилегированные акции (отдельные CSV)

## Структура проекта

```
dividends/
├── data/                       # Скорректированные CSV (рабочие)
│   ├── raw/                    # Сырые значения с сайта
│   ├── SBER.csv
│   ├── PLZL.csv
│   └── ...
├── metadata/
│   ├── ticker_mappings.json    # Тикер → URL на сайте
│   ├── splits.json             # Реестр сплитов / reverse-сплитов
│   ├── split_anomalies.json    # Автодетектор скачков YoY
│   ├── anomaly_review.json     # Ручной разбор аномалий
│   └── parsing_summary.json    # Итог последнего полного прогона
├── ticker_mapper.py
├── dividend_parser.py          # Парсинг одного/тикера → data/raw/
├── adjust_splits.py            # raw → data/ по splits.json
├── parse_all_dividends.py      # Все тикеры + adjust
├── requirements.txt
└── README.md
```

## Пайплайн

```
ticker_mapper.py
       ↓
metadata/ticker_mappings.json
       ↓
parse_all_dividends.py  (или dividend_parser.py TICKER)
       ↓
data/raw/*.csv          ← as declared
       ↓
adjust_splits.py
       ↓
data/*.csv              ← current share basis
       +
metadata/split_anomalies.json
```

Задержка между HTTP-запросами: **2 секунды**.

## Скрипты

### 1. `ticker_mapper.py`

Парсит главную страницу сайта → `metadata/ticker_mappings.json` (`{тикер: url_путь}`).

```bash
python ticker_mapper.py
```

### 2. `dividend_parser.py`

Парсит страницу тикера → CSV в `data/raw/`.

- автоопределение таблицы (2 или 3 колонки)
- ordinary → `TICKER.csv`, preferred → `TICKERP.csv`
- российский формат чисел (пробелы тысяч, запятая десятичная)
- после парса одного тикера сам вызывает `adjust_all()`

```bash
python dividend_parser.py SNGS
```

### 3. `adjust_splits.py`

Читает `data/raw/`, пишет `data/` с учётом `metadata/splits.json`.

Правило для каждой строки с `closing_date < split.date` (или без даты):

| `kind` | Пример | Корректировка |
|--------|--------|---------------|
| `split` (по умолчанию) | 1→100 | `dividend /= ratio` |
| `reverse` | 5000→1 | `dividend *= ratio` |

Несколько событий на тикер перемножаются.

Также детектит скачки соседних ненулевых дивидендов ≥ **50×** (пары с одной `closing_date` пропускаются) → `metadata/split_anomalies.json`.

```bash
python adjust_splits.py
```

### 4. `parse_all_dividends.py`

Полный прогон: все тикеры из маппинга → raw → adjust → `parsing_summary.json`.

```bash
python parse_all_dividends.py
```

## Формат CSV

```csv
closing_date,year,period_type,dividend_value
2025-07-18,2024,full year,34.84
2024-07-11,2023,full year,33.3
```

- `closing_date` — ISO `YYYY-MM-DD` (может быть пустым, если на сайте нет даты)
- `year` — год, за который объявлен дивиденд
- `period_type` — `full year` | `half year` | `3 months` | `6 months` | `9 months`
- `dividend_value` — рубли на акцию (`0.0` = решение не выплачивать)

В `data/` значения уже на **текущей** акции; в `data/raw/` — как на сайте.

## Реестр сплитов (`metadata/splits.json`)

Актуальные записи:

| Тикер | Дата | kind | ratio | Событие |
|-------|------|------|-------|---------|
| GMKN | 2024-04-04 | split | 100 | 1:100 |
| TRNFP / TRNFPP | 2024-02-20 | split | 100 | 1:100 |
| PLZL | 2025-03-27 | split | 10 | 1:10 |
| VTBR | 2024-07-11 | reverse | 5000 | 5000:1 |
| IRAO | 2014-12-26 | reverse | 100 | 100:1 |

Пример записи:

```json
"PLZL": [
  {"date": "2025-03-27", "ratio": 10, "kind": "split", "note": "1:10"}
],
"VTBR": [
  {"date": "2024-07-11", "ratio": 5000, "kind": "reverse", "note": "5000:1 consolidation"}
]
```

### Как добавить новый сплит

1. Дописать тикер в `metadata/splits.json`
2. `python adjust_splits.py` (репарс не нужен, если raw уже актуален)
3. Проверить `data/TICKER.csv` и `metadata/split_anomalies.json`

## Аномалии и разбор

Автодетектор (`split_anomalies.json`) ловит кандидатов на пропущенный сплит.
Не каждый скачок — сплит: бывают уставные минимумы префов, спецдивиденды, кризисы.

Ручной разбор: **`metadata/anomaly_review.json`**.

Кратко по разобранным false positives:

| Тикер | Вердикт | Суть |
|-------|---------|------|
| BANEP | уставной минимум | `0.10₽` = 10% номинала 1₽ (2016, 2020) |
| MTLRP | номинал в кризис | реально платили `0.05₽` в 2012–15 |
| LNZLP | спецдив | `3699₽` после продажи ЗДК Полюсу |
| CNTL / CNTLP | спецдив | продажа недвижимости 2018–19 |
| MFGSP | спецдив | `202.5₽` H1'2005 — раздача накоплений |
| LPSB | возобновление | `16₽` после длинного нуля |
| TRCN | кризис 2009 | `19.29→0.16` при том же числе акций |
| TASBP | шум | почти пустая история |

Единственный пропущенный CA, найденный детектором и добавленный в реестр: **IRAO reverse 100:1 (2014)**.

## Использование

```bash
pip install -r requirements.txt

# обновить список тикеров
python ticker_mapper.py

# один тикер
python dividend_parser.py SBER

# всё
python parse_all_dividends.py

# только пересчёт сплитов
python adjust_splits.py
```

На Windows при кириллице в консоли полезно:

```powershell
$env:PYTHONIOENCODING='utf-8'
```

## Зависимости

```
requests==2.31.0
beautifulsoup4==4.12.2
pandas==2.1.4
```

## Особенности

- Сырые данные никогда не правятся руками — только через репарс
- Скорректированные данные не коммитить «ручной правкой»: правь `splits.json` и гоняй `adjust_splits.py`
- Pref-файлы создаются только если на странице есть 3-я колонка
- Тикеры, исчезнувшие с сайта, удаляются из `data/` при синхронизации с `data/raw/` (сироты)
