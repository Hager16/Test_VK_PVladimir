import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from datetime import datetime, timedelta

# Функция для чтения и объединения данных за последние 7 дней
def aggregate_weekly_data(intermediate_dir, target_date_str):
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    missing_files = []  # Список отсутствующих файлов
    processed_days = []  # Список дней, для которых есть данные

    # Инициализация Spark сессии
    spark = SparkSession.builder.appName('WeeklyAggregation').getOrCreate()

    # Список для хранения DataFrame'ов за каждый день
    logs = []

    # Чтение промежуточных данных за последние 7 дней
    for i in range(1, 8):
        log_date = target_date - timedelta(days=i)
        log_file = os.path.join(intermediate_dir, f'{log_date.strftime("%Y-%m-%d")}.csv')

        if os.path.exists(log_file):
            # Чтение файла с помощью Spark
            daily_log = spark.read.csv(log_file, header=True, inferSchema=True)
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
        # Объединяем все DataFrame'ы
        combined_logs = logs[0]
        for log in logs[1:]:
            combined_logs = combined_logs.union(log)

        # Агрегация данных по email
        weekly_aggregate = combined_logs.groupBy('email') \
            .agg(
            spark_sum(col("create_count")).alias("create_count"),
            spark_sum(col("read_count")).alias("read_count"),
            spark_sum(col("update_count")).alias("update_count"),
            spark_sum(col("delete_count")).alias("delete_count")
        )
        return weekly_aggregate, spark
    else:
        print("No data available for aggregation.")
        return None, spark


# Агрегации за неделю и сохранение результата
def save_weekly_aggregate(intermediate_dir, output_dir, target_date_str):
    weekly_aggregate, spark = aggregate_weekly_data(intermediate_dir, target_date_str)

    if weekly_aggregate is not None:
        # Создаем временную директорию для сохранения данных
        temp_output_dir = os.path.join(output_dir, f'{target_date_str}_temp')
        weekly_aggregate.coalesce(1).write.csv(temp_output_dir, header=True, mode='overwrite')

        # Переименование файла "part-0000..." в "YYYY-MM-DD.csv"
        for file_name in os.listdir(temp_output_dir):
            if file_name.startswith('part-') and file_name.endswith('.csv'):
                final_output_file = os.path.join(output_dir, f'{target_date_str}.csv')
                os.rename(os.path.join(temp_output_dir, file_name), final_output_file)
                print(f"Weekly aggregated data for {target_date_str} saved to {final_output_file}.")

        # Удаляем временную директорию после перемещения файла
        shutil.rmtree(temp_output_dir)

        # Останавливаем Spark сессию
        spark.stop()
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