"""
users_mart.py
Модуль для расчета витрины пользователей (Шаг 1)

Зависимости:
- geo_classes.py должен находиться в той же директории или в PYTHONPATH
"""

from geo_classes import (
    EventsRaw, EventsWithUserAndCoords,
    CitiesRaw, Cities, EventsWithCitiesPartial, EventsWithCitiesAll,
    Users, DataLoader, Config
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_users_mart(spark, date, sample_rate=1.0):
    """
    Шаг 1. Витрина в разрезе пользователей
    
    Parameters:
    -----------
    spark : SparkSession
        Spark сессия
    date : str
        Дата для фильтрации событий (формат: 'YYYY-MM-DD')
    sample_rate : float, optional
        Коэффициент сэмплирования (0.0 - 1.0)
        По умолчанию 1.0 (100% данных)
        Для отладки рекомендуется 0.01-0.1 (1-10% данных)
    """
    print("\n" + "="*80)
    print(f"ШАГ 1: РАСЧЕТ ВИТРИНЫ ПОЛЬЗОВАТЕЛЕЙ")
    print(f"Дата расчета: {date}")
    print(f"Сэмплирование: {sample_rate*100:.1f}% данных")
    print("="*80)
    
    # 1. Загружаем исходные события напрямую через DataLoader
    print("\n[1] Загрузка событий через DataLoader...")
    events_df = DataLoader.load(spark, 'source', 'events', filter_date=date)
    original_count = events_df.count()
    print(f"Загружено событий до сэмплирования: {original_count}")
    
    # Применяем сэмплирование если нужно
    if sample_rate < 1.0:
        print(f"\n[1a] Применение сэмплирования {sample_rate*100:.1f}%...")
        # Используем seed для воспроизводимости результатов
        events_df = events_df.sample(withReplacement=False, fraction=sample_rate, seed=42)
        sampled_count = events_df.count()
        print(f"После сэмплирования: {sampled_count} событий")
        print(f"Сэмпл составил {sampled_count/original_count*100:.1f}% от исходных данных")
    
    print("\nСхема загруженных данных:")
    events_df.printSchema()
    
    # 2. Извлекаем нужные поля из структуры event
    print("\n[2] Извлечение полей из структуры event...")
    
    events_df = events_df.select(
        'event_type',
        'lat',
        'lon',
        'date',
        F.col('event.message_id').alias('message_id'),
        F.col('event.message_from').alias('message_from'),
        F.col('event.message_to').alias('message_to'),
        F.col('event.reaction_from').alias('reaction_from'),
        F.col('event.subscription_channel').alias('subscription_channel'),
        F.col('event.user').alias('user'),
        F.col('event.datetime').alias('datetime')
    )
    
    print("Схема после извлечения полей:")
    events_df.printSchema()
    print("Пример данных:")
    events_df.show(5, truncate=False)
    
    # 3. Создаем класс-обертку для совместимости с EventsRaw
    class SimpleEventsSource:
        def __init__(self, df):
            self.df = df
        def read(self, date):
            pass  # Данные уже загружены
    
    # 4. Создаем EventsRaw с нашими данными
    print("\n[3] Создание EventsRaw...")
    events_source_wrapper = SimpleEventsSource(events_df)
    events_raw = EventsRaw(spark, events_source_wrapper)
    events_raw.df = events_df
    events_raw._cache_df()
    print(f"EventsRaw создан, записей: {events_raw.df.count()}")
    
    # 5. Добавляем user_id и координаты через EventsWithUserAndCoords
    print("\n[4] Обогащение данными пользователей...")
    events_with_user = EventsWithUserAndCoords(spark, events_raw)
    events_with_user.calc()
    
    # 6. Загружаем и обрабатываем города
    print("\n[5] Загрузка и обработка городов...")
    cities_raw = CitiesRaw(spark)
    cities_raw.calc()
    
    cities = Cities(spark, cities_raw)
    cities.calc()
    print(f"Загружено городов: {cities.df.count()}")
    
    # 7. Определяем город для каждого события
    print("\n[6] Определение ближайшего города для каждого события...")
    events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
    events_partial.calc()
    
    events_all = EventsWithCitiesAll(spark, events_partial)
    events_all.calc()
    
    # 8. Формируем профили пользователей
    print("\n[7] Формирование профилей пользователей...")
    users = Users(spark, events_all)
    users.calc()
    
    # 9. Создаем витрину пользователей с городами
    print("\n[8] Создание финальной витрины пользователей...")

    # Для отладки выведем доступные колонки
    print("Доступные колонки в events_all.df:")
    print(events_all.df.columns)

    window_spec = Window.partitionBy("user_id").orderBy(F.desc("date"))

    users_mart_df = events_all.df.withColumn(
        "rn", F.row_number().over(window_spec)
    ).filter(F.col("rn") == 1).select(
        "user_id",
        "city_id",
        F.col("city").alias("city_name"),
        "date",
        "lat",
        "lon"
    )

    users_mart_df = users_mart_df.cache()
    final_count = users_mart_df.count()
    
    print("\n✅ Результаты витрины пользователей:")
    print(f"Количество записей в финальной витрине: {final_count}")
    print(f"Процент от исходных данных: {final_count/original_count*100:.2f}%")
    print("Схема данных:")
    users_mart_df.printSchema()
    print("\nПример данных:")
    users_mart_df.show(10, truncate=False)
    
    # Создаем класс-обертку
    class UsersMart:
        def __init__(self, df, users, events_all, sample_rate):
            self.df = df
            self.users = users
            self.events_all = events_all
            self.sample_rate = sample_rate
        
        def desc(self):
            print(f"\n📊 Витрина пользователей:")
            print(f"   Записей: {self.df.count()}")
            print(f"   Сэмплирование: {self.sample_rate*100:.1f}%")
            print("   Статистика по городам:")
            self.df.groupBy("city_name").count()\
                .orderBy(F.desc("count")).show(10)
        
        def save(self, path=None):
        #Сохранить витрину в слой DML
        # Используем путь из конфигурации по умолчанию
            if path is None:
                # Базовое имя для слоя DML
                base_name = "UsersMart"
                if self.sample_rate < 1.0:
                    base_name = f"UsersMart_sample_{int(self.sample_rate*100)}pct"
                
                # Получаем полный путь через Config
                path = Config.get_path('dml', base_name)
            
            # Сохраняем
            self.df.write.mode("overwrite").parquet(path)
            print(f"✅ Витрина сохранена в {path}")
        
    return UsersMart(users_mart_df, users, events_all, sample_rate)


# Для тестирования файла
if __name__ == "__main__":
    print("Этот файл содержит функцию calculate_users_mart()")
    print("Используйте её в Jupyter Notebook следующим образом:")
    print("""
    from geo_classes import create_spark_session
    from users_mart import calculate_users_mart
    
    spark = create_spark_session(app_name="UsersMart")
    users_mart = calculate_users_mart(spark, 'ХХХХ-ХХ-ХХ', sample_rate=0.001)
    users_mart.desc()
    users_mart.df.show(5)
    users_mart.save() # Сохранит в DML слой
    spark.stop()
    """)