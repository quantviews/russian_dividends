"""
Скрипт для парсинга дивидендной истории акций с сайта https://закрытияреестров.рф/

Получает историю дивидендных выплат для заданного тикера, включая:
- Даты закрытия реестра
- Год, за который выплачивается дивиденд
- Тип периода (полный год или несколько месяцев)
- Размер дивиденда в рублях (0.0 если дивиденды не выплачиваются)

Результаты сохраняются в CSV-файл в папке data/
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict
import re
from datetime import datetime
import os
import json
from ticker_mapper import get_ticker_mappings

def load_ticker_mappings() -> Dict[str, str]:
    """
    Загружает маппинг тикеров из JSON-файла или получает их с сайта
    
    Returns:
        Dict[str, str]: Словарь вида {тикер: url_путь}
    """
    json_path = 'metadata/ticker_mappings.json'
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return get_ticker_mappings()

def parse_russian_date(date_str: str) -> datetime:
    """
    Преобразует дату из строки в формате dd.mm.yyyy в объект datetime
    
    Args:
        date_str: Дата в формате dd.mm.yyyy
    Returns:
        datetime: Объект datetime или None в случае ошибки
    """
    try:
        return datetime.strptime(date_str, '%d.%m.%Y')
    except:
        return None

def get_dividend_history(ticker: str, save_csv: bool = True) -> pd.DataFrame:
    """
    Получает историю дивидендных выплат для заданного тикера
    
    Args:
        ticker: Тикер акции
        save_csv: Сохранять ли результат в CSV-файл
        
    Returns:
        pd.DataFrame: DataFrame с историей дивидендов со столбцами:
            - closing_date: Дата закрытия реестра
            - year: Год выплаты
            - period_type: Тип периода
            - dividend_value: Размер дивиденда (0.0 если дивиденды не выплачиваются)
    """
    try:
        # Загрузка маппинга тикеров
        ticker_mappings = load_ticker_mappings()
        
        base_url = "https://xn--80aeiahhn9aobclif2kuc.xn--p1ai"
        url_path = ticker_mappings.get(ticker.upper())
        if not url_path:
            raise ValueError(f"Не найден URL для тикера {ticker}")
            
        url = f"{base_url}/{url_path}/"
        print(f"Загрузка данных с {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Поиск таблицы с дивидендами
        tables = soup.find_all('table')
        print(f"Найдено таблиц: {len(tables)}")
        
        # Таблица с дивидендами должна быть первой с 2 колонками
        target_table = None
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            if rows and len(rows[0].find_all(['td', 'th'])) == 2:
                target_table = table
                print(f"Найдена таблица с дивидендами (таблица #{i+1})")
                print("\nHTML таблицы:")
                print(table.prettify())
                break
        
        if not target_table:
            raise ValueError(f"Не найдена таблица с дивидендами для тикера {ticker}")
            
        # Извлечение данных
        rows = []
        for tr in target_table.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            if len(cells) == 2:
                period = cells[0].text.strip()
                dividend = cells[1].text.strip()
                print(f"\nОбработка строки:")
                print(f"HTML ячейки периода: {cells[0]}")
                print(f"Период: {period}")
                print(f"Дивиденд: {dividend}")
                
                if period != "Период выплаты":
                    # Пропуск строк с "ИТОГО"
                    if "ИТОГО" in period:
                        print("Пропуск строки с ИТОГО")
                        continue
                        
                    # Извлечение даты закрытия реестра и года
                    # Заменяем переносы строк на пробелы для упрощения парсинга
                    period = period.replace('\n', ' ').strip()
                    
                    # Ищем дату в формате "закрытие реестра 9.07.2025" или "закрытие реестра 09.07.2025"
                    date_match = re.search(r'закрытие реестра (\d{1,2}\.\d{2}\.\d{4})', period)
                    if date_match:
                        closing_date = date_match.group(1)
                        # Добавляем ведущий ноль к дню, если нужно
                        day, month, year = closing_date.split('.')
                        closing_date = f"{day.zfill(2)}.{month}.{year}"
                    else:
                        # Попытка найти дату в другом формате
                        date_match = re.search(r'(\d{1,2}\.\d{2}\.\d{4})', period)
                        if date_match:
                            closing_date = date_match.group(1)
                            # Добавляем ведущий ноль к дню, если нужно
                            day, month, year = closing_date.split('.')
                            closing_date = f"{day.zfill(2)}.{month}.{year}"
                        else:
                            closing_date = None
                    
                    # Ищем год выплаты (должен быть отдельным числом, не частью даты)
                    # Исключаем год из даты закрытия реестра
                    if closing_date:
                        period = period.replace(closing_date.split('.')[-1], '')
                    
                    year_match = re.search(r'(?:^|\s)(\d{4})(?:\s|$)', period)
                    year = year_match.group(1) if year_match else None
                    
                    print(f"Найдены - дата: {closing_date}, год: {year}")
                    
                    # Пропуск если нет года
                    if not year:
                        print("Пропуск строки - не найден год")
                        continue
                    
                    # Тип периода - всегда полный год для X5
                    period_type = 'full year'
                    
                    # Извлечение значения дивиденда
                    if "РЕШЕНИЕ ДИВИДЕНДЫ НЕ ВЫПЛАЧИВАТЬ" in dividend:
                        dividend_value = 0.0
                        print("Дивиденды не выплачиваются")
                    else:
                        dividend_match = re.search(r'(\d+(?:,\d+)?(?:\.\d+)?)\s*руб\.', dividend)
                        if not dividend_match:
                            # Попытка найти значение без указания "руб."
                            dividend_match = re.search(r'(\d+(?:,\d+)?(?:\.\d+)?)', dividend)
                        
                        dividend_value = dividend_match.group(1) if dividend_match else None
                        if dividend_value:
                            dividend_value = dividend_value.replace(',', '.')
                            print(f"Найдено значение дивиденда: {dividend_value}")
                        else:
                            print("Пропуск строки - не найдено значение дивиденда")
                            continue
                    
                    parsed_date = parse_russian_date(closing_date) if closing_date else None
                    if parsed_date:
                        rows.append({
                            'closing_date': parsed_date,
                            'year': int(year),
                            'period_type': period_type,
                            'dividend_value': float(dividend_value)
                        })
                        print("Строка успешно добавлена")
                    else:
                        # Если нет даты закрытия реестра, но есть год и значение дивиденда
                        rows.append({
                            'closing_date': None,
                            'year': int(year),
                            'period_type': period_type,
                            'dividend_value': float(dividend_value)
                        })
                        print("Строка успешно добавлена (без даты закрытия реестра)")
        
        # Преобразование в DataFrame
        df = pd.DataFrame(rows)
        
        # Сортировка по дате закрытия реестра
        if not df.empty:
            df = df.sort_values('year', ascending=False)
            print("\nИтоговый DataFrame:")
            print(df)
        else:
            print("\nDataFrame пустой - данные не найдены")
        
        # Сохранение в CSV если запрошено
        if save_csv and not df.empty:
            os.makedirs('data', exist_ok=True)
            csv_path = os.path.join('data', f'{ticker.upper()}.csv')
            df.to_csv(csv_path, index=False)
            print(f"Данные сохранены в {csv_path}")
        
        return df
        
    except Exception as e:
        print(f"Ошибка при обработке тикера {ticker}: {str(e)}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Пример использования
    ticker = "FIVE"  # X5 Group
    df = get_dividend_history(ticker)
    print(f"\nДивидендная история для {ticker}:")
    print(df.to_string()) 