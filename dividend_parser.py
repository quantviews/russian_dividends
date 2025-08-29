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
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Поиск таблицы с дивидендами
        tables = soup.find_all('table')
        
        # Поиск таблицы с дивидендами
        target_table = None
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            if not rows:
                continue
                
            # Проверяем все строки на наличие ключевых слов
            table_text = ' '.join(row.text.strip().lower() for row in rows)
            if any(keyword in table_text for keyword in ['период', 'дивиденд', 'выплат', 'акци']):
                target_table = table
                break
        
        if not target_table:
            raise ValueError(f"Не найдена таблица с дивидендами для тикера {ticker}")
            
        # Извлечение данных
        rows = []
        preferred_rows = []
        has_preferred_shares = False
        
        for tr in target_table.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            
            if len(cells) >= 2:  # Минимум 2 колонки (период и обыкновенные акции)
                period = cells[0].text.strip()
                dividend = cells[1].text.strip()  # Дивиденд по обыкновенным акциям
                
                # Проверяем наличие колонки с привилегированными акциями
                if len(cells) >= 3:
                    has_preferred_shares = True
                    preferred_dividend = cells[2].text.strip()  # Дивиденд по привилегированным акциям
                
                if period != "Период выплаты" and not "ИТОГО" in period:
                    # Извлечение даты закрытия реестра и года
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
                    
                    # Ищем год выплаты
                    if closing_date:
                        period = period.replace(closing_date.split('.')[-1], '')
                    
                    # Исправленный regex для извлечения года - ищем 4-значное число в начале строки или после пробела
                    year_match = re.search(r'(?:^|\s)(\d{4})(?:\s|$)', period)
                    if not year_match:
                        # Альтернативный поиск - ищем 4-значное число в любом месте
                        year_match = re.search(r'(\d{4})', period)
                    
                    year = year_match.group(1) if year_match else None
                    
                    # Дополнительная проверка - если год не найден, попробуем найти его в исходном тексте
                    if not year:
                        # Ищем год в исходном тексте периода до замены даты
                        original_period = cells[0].text.strip()
                        year_match = re.search(r'(\d{4})', original_period)
                        year = year_match.group(1) if year_match else None
                    
                    if not year:
                        continue
                    
                    # Определяем тип периода
                    period_type = 'full year'
                    if 'i полугодие' in period.lower() or '1 полугодие' in period.lower() or 'і полугодие' in period.lower():
                        period_type = 'half year'
                    elif '9 месяцев' in period.lower() or '9 мес' in period.lower():
                        period_type = '9 months'
                    elif '6 месяцев' in period.lower() or '6 мес' in period.lower():
                        period_type = '6 months'
                    elif '3 месяца' in period.lower() or '3 мес' in period.lower():
                        period_type = '3 months'
                    
                    # Извлечение значения дивиденда по обыкновенным акциям
                    if "РЕШЕНИЕ ДИВИДЕНДЫ НЕ ВЫПЛАЧИВАТЬ" in dividend:
                        dividend_value = 0.0
                    else:
                        # Исправленный regex для корректного извлечения чисел с разделителями тысяч (пробелы) и десятичными (запятые)
                        dividend_match = re.search(r'(\d{1,3}(?:\s\d{3})*(?:,\d+)?)\s*руб\.', dividend)
                        if not dividend_match:
                            # Попытка найти значение без указания "руб."
                            dividend_match = re.search(r'(\d{1,3}(?:\s\d{3})*(?:,\d+)?)', dividend)
                        
                        dividend_value = dividend_match.group(1) if dividend_match else None
                        if dividend_value:
                            # Убираем все пробелы (разделители тысяч) и заменяем запятую на точку для десятичных
                            dividend_value = dividend_value.replace(' ', '').replace(',', '.')
                        else:
                            continue
                    
                    # Добавляем строку в результат для обыкновенных акций
                    row_data = {
                        'closing_date': pd.to_datetime(closing_date, format='%d.%m.%Y') if closing_date else pd.NaT,
                        'year': int(year),
                        'period_type': period_type,
                        'dividend_value': float(dividend_value)
                    }
                    rows.append(row_data)
                    
                    # Если есть привилегированные акции, обрабатываем их
                    if has_preferred_shares and len(cells) >= 3:
                        if "РЕШЕНИЕ ДИВИДЕНДЫ НЕ ВЫПЛАЧИВАТЬ" in preferred_dividend:
                            preferred_dividend_value = 0.0
                        else:
                            # Извлечение значения дивиденда по привилегированным акциям
                            preferred_dividend_match = re.search(r'(\d{1,3}(?:\s\d{3})*(?:,\d+)?)\s*руб\.', preferred_dividend)
                            if not preferred_dividend_match:
                                # Попытка найти значение без указания "руб."
                                preferred_dividend_match = re.search(r'(\d{1,3}(?:\s\d{3})*(?:,\d+)?)', preferred_dividend)
                            
                            preferred_dividend_value = preferred_dividend_match.group(1) if preferred_dividend_match else None
                            if preferred_dividend_value:
                                # Убираем все пробелы (разделители тысяч) и заменяем запятую на точку для десятичных
                                preferred_dividend_value = preferred_dividend_value.replace(' ', '').replace(',', '.')
                            else:
                                continue
                        
                        # Добавляем строку в результат для привилегированных акций
                        preferred_row_data = {
                            'closing_date': pd.to_datetime(closing_date, format='%d.%m.%Y') if closing_date else pd.NaT,
                            'year': int(year),
                            'period_type': period_type,
                            'dividend_value': float(preferred_dividend_value)
                        }
                        preferred_rows.append(preferred_row_data)
        
        # Преобразование в DataFrame
        df = pd.DataFrame(rows)
        
        # Сортировка по дате закрытия реестра
        if not df.empty:
            df = df.sort_values('year', ascending=False)
        else:
            print("\nDataFrame пустой - данные не найдены")

        # Сохранение в CSV если запрошено
        if save_csv and not df.empty:
            os.makedirs('data', exist_ok=True)
            csv_path = os.path.join('data', f'{ticker.upper()}.csv')
            df.to_csv(csv_path, index=False)
        else:
            print(f"Для тикера {ticker} не найдено данных по обыкновенным акциям")

        # Преобразование в DataFrame для привилегированных акций
        df_preferred = pd.DataFrame(preferred_rows)

        # Сортировка по дате закрытия реестра
        if not df_preferred.empty:
            df_preferred = df_preferred.sort_values('year', ascending=False)
        else:
            print("\nDataFrame пустой - данные привилегированных акций не найдены")

        # Сохранение в CSV если запрошено
        if save_csv and not df_preferred.empty:
            csv_path = os.path.join('data', f'{ticker.upper()}P.csv')
            df_preferred.to_csv(csv_path, index=False)
        
        return df
        
    except Exception as e:
        print(f"Ошибка при обработке тикера {ticker}: {str(e)}")
        return pd.DataFrame()

if __name__ == "__main__":
    import sys
    
    # Получаем тикер из аргументов командной строки или используем значение по умолчанию
    ticker = sys.argv[1] if len(sys.argv) > 1 else "FIVE"
    
    print(f"Парсинг дивидендной истории для тикера {ticker}")
    
    try:
        # Получаем историю дивидендов
        df_ordinary = get_dividend_history(ticker)
        
        if not df_ordinary.empty:
            print(f"\nДивидендная история для {ticker} (обыкновенные акции):")
            print(df_ordinary.to_string())
        else:
            print(f"Для тикера {ticker} не найдено данных по обыкновенным акциям")
            
    except Exception as e:
        print(f"Ошибка при обработке тикера {ticker}: {str(e)}") 