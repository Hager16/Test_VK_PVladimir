import pandas as pd
import os
import sys
from datetime import datetime, timedelta


# Функция для чтения и объединения данных за последние 7 дней 
def aggregate_weekly_data(intermediate_dir, target_date_str):
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    logs = []
    missing_files = []
    processed_days = []

    # Чтение промежуточных данных за последние 7 дней 
    for i in range(1, 8):
        log_date = target_date - timedelta(days=i)
        log_file = os.path.join(intermediate_dir, f'{log_date.strftime("%Y-%m-%d")}.csv')

        if os.path.exists(log_file):
            daily_log = pd.read_csv(log_file)
            logs.append(daily_log)
            processed_days.append(log_date.strftime("%Y-%m-%d"))  # Добавляем день, за который есть данные
        else:
            missing_files.append(log_date.strftime("%Y-%m-%d"))  # Добавляем отсутствующие дни

    # Предупреждение о недостающих днях
    if missing_files:
        print(f"Warning: Missing data for the following dates: {', '.join(missing_files)}")

    if processed_days:
        print(f"Aggregation will be done for the following days: {', '.join(processed_days)}")

    # Агрегация данных за неделю, если есть хотя бы один день с данными
    if logs:
        combined_logs = pd.concat(logs)
        weekly_aggregate = combined_logs.groupby('email').sum().reset_index()
        return weekly_aggregate
    else:
        print("No data available for aggregation.")
        return pd.DataFrame()


# Агрегации за неделю и сохранение результата
def save_weekly_aggregate(intermediate_dir, output_dir, target_date_str):
    weekly_aggregate = aggregate_weekly_data(intermediate_dir, target_date_str)

    if not weekly_aggregate.empty:
        output_file = os.path.join(output_dir, f'{target_date_str}.csv')
        weekly_aggregate.to_csv(output_file, index=False)
        print(f"Weekly aggregate saved to {output_file}.")
    else:
        print("No weekly data to save.")


# Основной запуск скрипта
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <intermediate_dir> <output_dir> <YYYY-MM-DD>")
        sys.exit(1)

    intermediate_dir = sys.argv[1]
    output_dir = sys.argv[2]
    target_date_str = sys.argv[3]

    # Проверка правильности формата даты
    try:
        datetime.strptime(target_date_str, '%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)

    # Запуск недельной агрегации
    save_weekly_aggregate(intermediate_dir, output_dir, target_date_str)
