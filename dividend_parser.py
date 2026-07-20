"""
Скрипт для парсинга дивидендной истории акций с сайта https://закрытияреестров.рф/

Получает историю дивидендных выплат для заданного тикера, включая:
- Даты закрытия реестра
- Год, за который выплачивается дивиденд
- Тип периода (полный год или несколько месяцев)
- Размер дивиденда в рублях (0.0 если дивиденды не выплачиваются)

Сырые результаты (как на сайте) сохраняются в data/raw/.
Скорректированные на сплиты значения — см. adjust_splits.py → data/.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, Optional
import re
from datetime import datetime
import os
import json
from ticker_mapper import get_ticker_mappings

RAW_DIR = "data/raw"


def load_ticker_mappings() -> Dict[str, str]:
    """Загружает маппинг тикеров из JSON или получает с сайта."""
    json_path = "metadata/ticker_mappings.json"
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return get_ticker_mappings()


def parse_russian_date(date_str: str) -> Optional[datetime]:
    """Преобразует дату dd.mm.yyyy в datetime."""
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        return None


def _save_raw_csv(df: pd.DataFrame, filename: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, filename), index=False)


def _parse_dividend_amount(text: str) -> Optional[float]:
    if "РЕШЕНИЕ ДИВИДЕНДЫ НЕ ВЫПЛАЧИВАТЬ" in text:
        return 0.0
    match = re.search(r"(\d{1,3}(?:\s\d{3})*(?:,\d+)?)\s*руб\.", text)
    if not match:
        match = re.search(r"(\d{1,3}(?:\s\d{3})*(?:,\d+)?)", text)
    if not match:
        return None
    return float(match.group(1).replace(" ", "").replace(",", "."))


def _period_type(period: str) -> str:
    p = period.lower()
    if "i полугодие" in p or "1 полугодие" in p or "і полугодие" in p:
        return "half year"
    if "9 месяцев" in p or "9 мес" in p:
        return "9 months"
    if "6 месяцев" in p or "6 мес" in p:
        return "6 months"
    if "3 месяца" in p or "3 мес" in p:
        return "3 months"
    return "full year"


def get_dividend_history(
    ticker: str,
    save_csv: bool = True,
    output_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    Получает историю дивидендных выплат для заданного тикера (сырые значения с сайта).

    Args:
        ticker: Тикер акции
        save_csv: Сохранять ли результат в CSV
        output_dir: Папка для CSV (по умолчанию data/raw/)

    Returns:
        pd.DataFrame со столбцами closing_date, year, period_type, dividend_value
    """
    if output_dir is None:
        output_dir = RAW_DIR

    try:
        ticker_mappings = load_ticker_mappings()
        base_url = "https://xn--80aeiahhn9aobclif2kuc.xn--p1ai"
        url_path = ticker_mappings.get(ticker.upper())
        if not url_path:
            raise ValueError(f"Не найден URL для тикера {ticker}")

        url = f"{base_url}/{url_path}/"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = "utf-8"

        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")

        target_table = None
        for table in tables:
            rows = table.find_all("tr")
            if not rows:
                continue
            table_text = " ".join(row.text.strip().lower() for row in rows)
            if any(k in table_text for k in ["период", "дивиденд", "выплат", "акци"]):
                target_table = table
                break

        if not target_table:
            raise ValueError(f"Не найдена таблица с дивидендами для тикера {ticker}")

        rows = []
        preferred_rows = []
        has_preferred_shares = False

        for tr in target_table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            period = cells[0].text.strip()
            dividend = cells[1].text.strip()

            if len(cells) >= 3:
                has_preferred_shares = True
                preferred_dividend = cells[2].text.strip()

            if period == "Период выплаты" or "ИТОГО" in period:
                continue

            period = period.replace("\n", " ").strip()

            date_match = re.search(r"закрытие реестра (\d{1,2}\.\d{2}\.\d{4})", period)
            if not date_match:
                date_match = re.search(r"(\d{1,2}\.\d{2}\.\d{4})", period)

            if date_match:
                day, month, year_part = date_match.group(1).split(".")
                closing_date = f"{day.zfill(2)}.{month}.{year_part}"
            else:
                closing_date = None

            period_for_year = period
            if closing_date:
                period_for_year = period.replace(closing_date.split(".")[-1], "")

            year_match = re.search(r"(?:^|\s)(\d{4})(?:\s|$)", period_for_year)
            if not year_match:
                year_match = re.search(r"(\d{4})", period_for_year)
            if not year_match:
                year_match = re.search(r"(\d{4})", cells[0].text.strip())
            if not year_match:
                continue

            year = int(year_match.group(1))
            period_type = _period_type(period)

            dividend_value = _parse_dividend_amount(dividend)
            if dividend_value is None:
                continue

            closing_ts = (
                pd.to_datetime(closing_date, format="%d.%m.%Y") if closing_date else pd.NaT
            )
            rows.append({
                "closing_date": closing_ts,
                "year": year,
                "period_type": period_type,
                "dividend_value": float(dividend_value),
            })

            if has_preferred_shares and len(cells) >= 3:
                preferred_value = _parse_dividend_amount(preferred_dividend)
                if preferred_value is None:
                    continue
                preferred_rows.append({
                    "closing_date": closing_ts,
                    "year": year,
                    "period_type": period_type,
                    "dividend_value": float(preferred_value),
                })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(
                ["closing_date", "year"], ascending=False, na_position="last"
            )
            if save_csv:
                _save_raw_csv(df, f"{ticker.upper()}.csv", output_dir)
        else:
            print(f"Для тикера {ticker} не найдено данных по обыкновенным акциям")

        df_preferred = pd.DataFrame(preferred_rows)
        if not df_preferred.empty:
            df_preferred = df_preferred.sort_values(
                ["closing_date", "year"], ascending=False, na_position="last"
            )
            if save_csv:
                _save_raw_csv(df_preferred, f"{ticker.upper()}P.csv", output_dir)

        return df

    except Exception as e:
        print(f"Ошибка при обработке тикера {ticker}: {str(e)}")
        return pd.DataFrame()


if __name__ == "__main__":
    import sys
    from adjust_splits import adjust_all

    ticker = sys.argv[1] if len(sys.argv) > 1 else "FIVE"
    print(f"Парсинг дивидендной истории для тикера {ticker}")

    df_ordinary = get_dividend_history(ticker)
    if not df_ordinary.empty:
        print(f"\nСырые данные {ticker}:")
        print(df_ordinary.to_string())
        adjust_all()
        print(f"\nСкорректированные данные записаны в data/{ticker.upper()}.csv")
    else:
        print(f"Для тикера {ticker} не найдено данных по обыкновенным акциям")
