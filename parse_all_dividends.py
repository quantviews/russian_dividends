"""
Скрипт для парсинга дивидендной истории всех тикеров с сайта https://закрытияреестров.рф/
"""

import os
import time
import json
from dividend_parser import get_dividend_history

def main():
    # Создаем директории если их нет
    os.makedirs('data', exist_ok=True)
    os.makedirs('metadata', exist_ok=True)
    
    # Загружаем маппинг тикеров
    with open('metadata/ticker_mappings.json', 'r', encoding='utf-8') as f:
        ticker_mappings = json.load(f)
    
    # Список тикеров для повторной обработки
    failed_tickers = [
        'VZRZ', 'BSPB', 'BISV', 'BANE', 'VGSB', 'GAZA', 'RLMN', 'POSI', 'DZRD', 'KZOS',
        'KOGK', 'KTSB', 'KRSB', 'KAZT', 'KGKC', 'LNZL', 'MAGE', 'MTLR', 'MGTS', 'MRKS',
        'NKNC', 'OMZZ', 'PMSB', 'CHGZ', 'AGRO', 'RGSS', 'RDRB', 'RSTI', 'LSNG', 'RTKM',
        'RUAL', 'SAGO', 'KRKN', 'SARE', 'SBER', 'MFGS', 'JNOS', 'STSB', 'SNGS', 'TASB',
        'TANL', 'TATN', 'VRSB', 'MISB', 'NNSB', 'RTSB', 'YRSB', 'TORS', 'TRNFP', 'HIMC',
        'WTCM', 'CNTL', 'CHKZ', 'UKUZ', 'EVR', 'HHRU', 'NORD', 'POLY', 'QIWI', 'FIVE'
    ]
    
    # Создаем словарь для хранения результатов парсинга
    parsing_summary = {
        'success': [],
        'failed': [],
        'total_processed': 0,
        'success_count': 0,
        'failed_count': 0
    }
    
    # Обрабатываем каждый тикер
    for ticker in failed_tickers:
        if ticker not in ticker_mappings:
            print(f"Тикер {ticker} не найден в маппинге")
            parsing_summary['failed'].append({
                'ticker': ticker,
                'error': 'Тикер не найден в маппинге'
            })
            parsing_summary['failed_count'] += 1
            continue
            
        print(f"\nОбработка тикера {ticker}...")
        try:
            # Получаем историю дивидендов
            df = get_dividend_history(ticker)
            
            if not df.empty:
                # Сохраняем в CSV
                df.to_csv(f'data/{ticker}.csv', index=False)
                print(f"Данные сохранены в data/{ticker}.csv")
                parsing_summary['success'].append(ticker)
                parsing_summary['success_count'] += 1
            else:
                print(f"Для тикера {ticker} не найдено данных")
                parsing_summary['failed'].append({
                    'ticker': ticker,
                    'error': 'Нет данных'
                })
                parsing_summary['failed_count'] += 1
                
        except Exception as e:
            print(f"Ошибка при обработке тикера {ticker}: {str(e)}")
            parsing_summary['failed'].append({
                'ticker': ticker,
                'error': str(e)
            })
            parsing_summary['failed_count'] += 1
            
        # Задержка между запросами
        time.sleep(2)
        
        parsing_summary['total_processed'] += 1
    
    # Сохраняем результаты парсинга
    with open('metadata/parsing_summary.json', 'w', encoding='utf-8') as f:
        json.dump(parsing_summary, f, ensure_ascii=False, indent=2)
    
    # Выводим итоговую статистику
    print("\nИтоговая статистика:")
    print(f"Всего обработано: {parsing_summary['total_processed']}")
    print(f"Успешно: {parsing_summary['success_count']}")
    print(f"Ошибок: {parsing_summary['failed_count']}")
    print(f"Успешные тикеры: {', '.join(parsing_summary['success'])}")
    print(f"Тикеры с ошибками: {', '.join([f['ticker'] for f in parsing_summary['failed']])}")

if __name__ == "__main__":
    main() 