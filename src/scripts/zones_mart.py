"""
zones_mart.py
Модуль для расчета витрины зон (Шаг 2)

Зависимости:
- geo_classes.py должен находиться в той же директории или в PYTHONPATH
"""

from geo_classes import (
    EventsSource, EventsRaw, EventsWithUserAndCoords,
    CitiesRaw, Cities, EventsWithCitiesPartial, EventsWithCitiesAll,
    RegistrationsWithCities, EventsWithRegsWithCities,
    WeeklyMonthlyCityStats, DataLoader, Config
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


def calculate_zones_mart(spark, date, sample_rate=1.0):
    """
    Шаг 2. Витрина в разрезе зон (городов)
    
    Считает количество событий в конкретном городе за неделю и месяц.
    Витрина содержит следующие поля:
    - month — месяц расчёта
    - week — неделя расчёта
    - zone_id — идентификатор зоны (города)
    - week_message — количество сообщений за неделю
    - week_reaction — количество реакций за неделю
    - week_subscription — количество подписок за неделю
    - week_user — количество регистраций за неделю
    - month_message — количество сообщений за месяц
    - month_reaction — количество реакций за месяц
    - month_subscription — количество подписок за месяц
    - month_user — количество регистраций за месяц
    
    Parameters:
    -----------
    spark : SparkSession
        Spark сессия
    date : str
        Дата для фильтрации событий (формат: 'YYYY-MM-DD')
        Обязательный параметр!
    sample_rate : float, optional
        Коэффициент сэмплирования (0.0 - 1.0)
        По умолчанию 1.0 (100% данных)
        Для отладки рекомендуется 0.01-0.1 (1-10% данных)
    
    Returns:
    --------
    ZonesMart : объект с атрибутами:
        - df: DataFrame с витриной зон
        - city_stats: объект WeeklyMonthlyCityStats
    """
    print("\n" + "="*80)
    print(f"ШАГ 2: РАСЧЕТ ВИТРИНЫ ЗОН")
    print(f"Дата расчета: {date}")
    print(f"Сэмплирование: {sample_rate*100:.1f}% данных")
    print("="*80)
    
    # 1. Загружаем события напрямую через DataLoader (обход EventsSource)
    print("\n[1] Загрузка событий через DataLoader...")
    events_df = DataLoader.load(spark, 'source', 'events', filter_date=date)
    original_count = events_df.count()
    print(f"Загружено событий до сэмплирования: {original_count}")
    
    # Применяем сэмплирование если нужно
    if sample_rate < 1.0:
        print(f"\n[1a] Применение сэмплирования {sample_rate*100:.1f}%...")
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
    print("\n[4] Добавление user_id и координат...")
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
    print("\n[6] Определение города для событий...")
    events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
    events_partial.calc()
    
    events_all = EventsWithCitiesAll(spark, events_partial)
    events_all.calc()
    
    # 8. Добавляем регистрации
    print("\n[7] Добавление данных о регистрациях...")
    registrations = RegistrationsWithCities(spark, events_all)
    registrations.calc()
    
    events_with_regs = EventsWithRegsWithCities(spark, events_all, registrations)
    events_with_regs.calc()
    
    # 9. Используем готовый класс WeeklyMonthlyCityStats для расчета витрины зон
    print("\n[8] Расчет статистики по городам за неделю и месяц...")
    city_stats = WeeklyMonthlyCityStats(spark, events_with_regs)
    city_stats.calc()
    
    # Кэшируем результат
    city_stats.df = city_stats.df.cache()
    city_stats.df.count()
    
    print("\n✅ Результаты витрины зон:")
    print(f"Количество записей: {city_stats.df.count()}")
    print("Схема данных:")
    city_stats.df.printSchema()
    print("\nПример данных:")
    city_stats.df.show(15, truncate=False)
    
    # Создаем класс-обертку
    class ZonesMart:
        def __init__(self, df, city_stats, sample_rate):
            self.df = df
            self.city_stats = city_stats
            self.sample_rate = sample_rate
        
        def desc(self):
            print(f"\n📊 Витрина зон:")
            print(f"   Записей: {self.df.count()}")
            print(f"   Сэмплирование: {self.sample_rate*100:.1f}%")
            print("   Поля:", ", ".join(self.df.columns))
            print("\n   Сводная статистика:")
            self.df.select(
                F.sum("week_message").alias("total_messages"),
                F.sum("week_reaction").alias("total_reactions"),
                F.sum("week_subscription").alias("total_subscriptions"),
                F.sum("week_user").alias("total_registrations")
            ).show()
        
        def save(self, path=None):
        #Сохранить витрину в слой DML
        # Используем путь из конфигурации по умолчанию
            if path is None:
                # Базовое имя для слоя DML
                base_name = "ZonesMart"
                if self.sample_rate < 1.0:
                    base_name = f"ZonesMart_sample_{int(self.sample_rate*100)}pct"
                
                # Получаем полный путь через Config
                path = Config.get_path('dml', base_name)
            # Сохраняем
            self.df.write.mode("overwrite").parquet(path)
            print(f"✅ Витрина сохранена в {path}")
    
    return ZonesMart(city_stats.df, city_stats, sample_rate)


# Для самостоятельного тестирования файла
if __name__ == "__main__":
    print("Этот файл содержит функцию calculate_zones_mart()")
    print("Используйте её в Jupyter Notebook следующим образом:")
    print("""
    from geo_classes import create_spark_session
    from zones_mart import calculate_zones_mart
    
    spark = create_spark_session(app_name="ZonesMart")
    zones_mart = calculate_zones_mart(spark, '2022-12-20', sample_rate=0.01)
    zones_mart.desc()
    zones_mart.save("output/test_zones")
    spark.stop()
    """)