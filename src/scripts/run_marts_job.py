import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Добавляем текущую директорию в path
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, '.')

print("="*80)
print("ЗАПУСК run_marts_job.py")
print(f"Python path: {sys.path}")
print("="*80)

# Импортируем geo_classes
try:
    import geo_classes as gc
    print("✅ geo_classes импортирован")
except ImportError as e:
    print(f"❌ Ошибка импорта geo_classes: {e}")
    gc = None

# Импортируем функции расчета витрин
try:
    from user_mart import calculate_users_mart
    print("✅ user_mart импортирован")
except ImportError as e:
    print(f"❌ Ошибка импорта user_mart: {e}")
    calculate_users_mart = None

try:
    from zones_mart import calculate_zones_mart
    print("✅ zones_mart импортирован")
except ImportError as e:
    print(f"❌ Ошибка импорта zones_mart: {e}")
    calculate_zones_mart = None

try:
    from friends_mart import calculate_friends_mart
    print("✅ friends_mart импортирован")
except ImportError as e:
    print(f"❌ Ошибка импорта friends_mart: {e}")
    calculate_friends_mart = None


def main():
    if len(sys.argv) < 4:
        print("Использование: run_marts_job.py <task> <date> <sample_rate>")
        print(f"Получено аргументов: {len(sys.argv)}")
        sys.exit(1)
    
    task = sys.argv[1]
    date = sys.argv[2]
    sample_rate = float(sys.argv[3])
    
    print(f"\nЗадача: {task}")
    print(f"Дата: {date}")
    print(f"Сэмплирование: {sample_rate}")
    
    # Создаем Spark сессию
    spark = SparkSession.builder \
        .appName(f"{task}-{date}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark сессия создана")
    
    try:
        if task == "users_mart":
            if calculate_users_mart:
                result = calculate_users_mart(spark, date, sample_rate)
                result.desc()
            else:
                print("❌ calculate_users_mart не импортирован")
                sys.exit(1)
        elif task == "zones_mart":
            if calculate_zones_mart:
                result = calculate_zones_mart(spark, date, sample_rate)
                result.desc()
            else:
                print("❌ calculate_zones_mart не импортирован")
                sys.exit(1)
        elif task == "friends_mart":
            if calculate_friends_mart:
                result = calculate_friends_mart(spark, date, sample_rate)
                result.desc()
            else:
                print("❌ calculate_friends_mart не импортирован")
                sys.exit(1)
        else:
            print(f"❌ Неизвестная задача: {task}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("✅ Spark сессия остановлена")


if __name__ == "__main__":
    main()