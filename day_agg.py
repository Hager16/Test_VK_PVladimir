import pandas as pd
import os
import sys
from datetime import datetime


# Функция для подсчета агрегатов за один день
def aggregate_daily_data(intermediate_dir, input_dir, date_str):
    log_file = os.path.join(input_dir, f'{date_str}.csv')  # путь к файлу логов

    if os.path.exists(log_file):
        # Чтение данных
        logs = pd.read_csv(log_file, header=None, names=['email', 'action', 'dt'])

        # Группировка данных по email и подсчет количества действий каждого типа
        aggregated_data = logs.groupby(['email', 'action']).size().unstack(fill_value=0).reset_index()

        # Убедимся, что все действия (CREATE, READ, UPDATE, DELETE) есть в результатах
        for action in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
            if action not in aggregated_data.columns:
                aggregated_data[action] = 0

        # Переименуем колонки для удобства
        aggregated_data.columns = ['email', 'create_count', 'read_count', 'update_count', 'delete_count']

        # Сохраняем промежуточные результаты в файл intermediate/YYYY-MM-DD.csv
        output_file = os.path.join(intermediate_dir, f'{date_str}.csv')
        aggregated_data.to_csv(output_file, index=False)
        print(f"Aggregated data for {date_str} saved to {output_file}.")
    else:
        print(f"Log file for {date_str} not found.")


# Основной запуск скрипта
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <intermediate_dir> <input_dir> <YYYY-MM-DD>")
        sys.exit(1)

    intermediate_dir = sys.argv[1]
    input_dir = sys.argv[2]
    date_str = sys.argv[3]

    # Проверка правильности формата даты
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)

    # Запускаем агрегирование данных
    aggregate_daily_data(intermediate_dir, input_dir, date_str)
