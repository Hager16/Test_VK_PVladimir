import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from datetime import datetime


# Функция для подсчета агрегатов за один день
def aggregate_daily_data(intermediate_dir, input_dir, date_str):
    log_file = os.path.join(input_dir, f'{date_str}.csv')  # путь к файлу логов

    # Проверяем, существует ли файл
    if os.path.exists(log_file):
        # Инициализируем Spark сессию
        spark = SparkSession.builder.appName('DailyAggregation').getOrCreate()

        # Чтение данных с помощью Spark
        logs = spark.read.csv(log_file, header=False, inferSchema=True).toDF("email", "action", "dt")

        # Группировка данных по email и подсчет количества действий каждого типа
        aggregated_data = logs.groupBy("email").pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"]) \
            .agg(count("action"))

        # Заполняем отсутствующие значения нулями
        aggregated_data = aggregated_data.fillna(0)

        # Переименуем колонки для удобства
        aggregated_data = aggregated_data.withColumnRenamed("CREATE", "create_count") \
            .withColumnRenamed("READ", "read_count") \
            .withColumnRenamed("UPDATE", "update_count") \
            .withColumnRenamed("DELETE", "delete_count")

        # Сохраняем результаты во временную директорию
        temp_output_dir = os.path.join(intermediate_dir, f'{date_str}_temp')
        aggregated_data.coalesce(1).write.csv(temp_output_dir, header=True, mode='overwrite')

        # Переименование файла "part-0000..." в "YYYY-MM-DD.csv"
        for file_name in os.listdir(temp_output_dir):
            if file_name.startswith('part-') and file_name.endswith('.csv'):
                final_output_file = os.path.join(intermediate_dir, f'{date_str}.csv')
                os.rename(os.path.join(temp_output_dir, file_name), final_output_file)
                print(f"Aggregated data for {date_str} saved to {final_output_file}.")

        # Удаление временной директории после перемещения файла
        shutil.rmtree(temp_output_dir)

        # Останавливаем Spark сессию
        spark.stop()
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