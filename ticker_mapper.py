"""
Скрипт для получения списка тикеров с сайта https://закрытияреестров.рф/

Парсит главную страницу сайта и создает JSON-файл с соответствием
тикеров акций и их URL на сайте.

Результат сохраняется в файл metadata/ticker_mappings.json
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import os

def get_ticker_mappings():
    """
    Получает список тикеров и соответствующих им URL с главной страницы сайта.
    
    Returns:
        Dict[str, str]: Словарь вида {тикер: url_путь}
    """
    base_url = "https://xn--80aeiahhn9aobclif2kuc.xn--p1ai"
    url = f"{base_url}/_/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Поиск всех ссылок с информацией о тикерах
        ticker_mappings = {}
        
        # Шаблон "Название компании (ТИКЕР)"
        pattern = re.compile(r'(.*?)\s*\(([A-Z0-9\.]+)\)')
        
        # Получение всех ссылок
        links = soup.find_all('a')
        for link in links:
            text = link.text.strip()
            href = link.get('href', '')
            
            # Пропуск пустых и навигационных ссылок
            if not text or text in ['>', '2025', '2024', '2023', '2022', '2021', '2020', 
                                  '2019', '2018', '2017', '2016', '2015', 
                                  'Дивидендные истории А-Я', 'Страница Донатов']:
                continue
            
            # Поиск по шаблону
            match = pattern.search(text)
            if match and href:
                company_name = match.group(1).strip()
                ticker = match.group(2).strip()
                # Удаление начального слеша если есть
                url_path = href.strip('/')
                ticker_mappings[ticker] = url_path
        
        # Сохранение маппинга в JSON
        if ticker_mappings:
            os.makedirs('metadata', exist_ok=True)
            with open('metadata/ticker_mappings.json', 'w', encoding='utf-8') as f:
                json.dump(ticker_mappings, f, ensure_ascii=False, indent=2)
            print(f"Сохранено {len(ticker_mappings)} тикеров в metadata/ticker_mappings.json")
        
        return ticker_mappings
        
    except Exception as e:
        print(f"Ошибка получения списка тикеров: {str(e)}")
        return {}

if __name__ == "__main__":
    mappings = get_ticker_mappings()
    print("\nПримеры маппинга:")
    # Вывод первых 10 тикеров для примера
    for i, (ticker, url_path) in enumerate(list(mappings.items())[:10]):
        print(f"{ticker}: {url_path}") 