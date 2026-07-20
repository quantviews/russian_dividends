"""
Скрипт для парсинга дивидендной истории всех тикеров с сайта https://закрытияреестров.рф/

Пайплайн:
  1. Парсит каждый тикер → data/raw/*.csv (сырые значения с сайта)
  2. adjust_splits → data/*.csv (с учётом сплитов из metadata/splits.json)
"""

import os
import time
import json
from dividend_parser import get_dividend_history, RAW_DIR
from adjust_splits import adjust_all


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs("data", exist_ok=True)
    os.makedirs("metadata", exist_ok=True)

    with open("metadata/ticker_mappings.json", "r", encoding="utf-8") as f:
        ticker_mappings = json.load(f)

    parsing_summary = {
        "success": [],
        "failed": [],
        "total_processed": 0,
        "success_count": 0,
        "failed_count": 0,
    }

    for ticker in ticker_mappings:
        print(f"\nОбработка тикера {ticker}...")
        try:
            df = get_dividend_history(ticker, save_csv=True, output_dir=RAW_DIR)

            if not df.empty:
                print(f"Сырые данные сохранены в {RAW_DIR}/{ticker}.csv")
                parsing_summary["success"].append(ticker)
                parsing_summary["success_count"] += 1
            else:
                print(f"Для тикера {ticker} не найдено данных")
                parsing_summary["failed"].append({"ticker": ticker, "error": "Нет данных"})
                parsing_summary["failed_count"] += 1

        except Exception as e:
            print(f"Ошибка при обработке тикера {ticker}: {str(e)}")
            parsing_summary["failed"].append({"ticker": ticker, "error": str(e)})
            parsing_summary["failed_count"] += 1

        time.sleep(2)
        parsing_summary["total_processed"] += 1

    print("\nПрименение корректировок на сплиты...")
    adjust_summary = adjust_all()
    parsing_summary["split_adjust"] = {
        "adjusted_tickers": adjust_summary["adjusted_tickers"],
        "raw_anomalies": len(adjust_summary["raw_anomalies"]),
        "remaining_anomalies": len(adjust_summary["remaining_anomalies"]),
    }

    with open("metadata/parsing_summary.json", "w", encoding="utf-8") as f:
        json.dump(parsing_summary, f, ensure_ascii=False, indent=2)

    print("\nИтоговая статистика:")
    print(f"Всего обработано: {parsing_summary['total_processed']}")
    print(f"Успешно: {parsing_summary['success_count']}")
    print(f"Ошибок: {parsing_summary['failed_count']}")
    if parsing_summary["failed"]:
        print(
            "Тикеры с ошибками: "
            + ", ".join(f["ticker"] for f in parsing_summary["failed"])
        )
    print(f"Split-adjusted: {', '.join(adjust_summary['adjusted_tickers'])}")
    if adjust_summary["remaining_anomalies"]:
        print(
            f"Внимание: {len(adjust_summary['remaining_anomalies'])} "
            "возможных пропущенных сплитов — см. metadata/split_anomalies.json"
        )


if __name__ == "__main__":
    main()
