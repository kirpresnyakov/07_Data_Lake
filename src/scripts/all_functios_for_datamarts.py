"""
marts_calculation.py
Модуль для расчета витрин данных гео-проекта

Содержит функции для расчета трех витрин:
1. calculate_users_mart - витрина в разрезе пользователей
2. calculate_zones_mart - витрина в разрезе зон (городов)
3. calculate_friends_mart - витрина для рекомендации друзей
"""

from geo_classes import (
    EventsSource, EventsRaw, EventsWithUserAndCoords,
    CitiesRaw, Cities, EventsWithCitiesPartial, EventsWithCitiesAll,
    Users, UserChannelSubscriptions, UserCommonChannels,
    UsersCorresponded, UsersNear, UserTravelCities, UserTravels,
    UserGeoProfile, WeeklyMonthlyCityStats, ProximityBasedFriends,
    RegistrationsWithCities, EventsWithRegsWithCities,
    DataLoader  # ← Добавьте эту строку
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
from datetime import datetime


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
    from pyspark.sql import functions as F
    
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
    
    print("\n[8] Создание финальной витрины пользователей...")

    # Для отладки выведем доступные колонки
    print("Доступные колонки в events_all.df:")
    print(events_all.df.columns)

    # Проверим наличие колонки city
    if 'city' not in events_all.df.columns:
        print("ВНИМАНИЕ: колонка 'city' не найдена!")
        print("Доступные колонки с city:")
        city_cols = [col for col in events_all.df.columns if 'city' in col]
        print(city_cols)

    window_spec = Window.partitionBy("user_id").orderBy(F.desc("date"))

    users_mart_df = events_all.df.withColumn(
        "rn", F.row_number().over(window_spec)
    ).filter(F.col("rn") == 1).select(
        "user_id",
        "city_id",
        F.col("city").alias("city_name"),  # Переименовываем city в city_name
        "date",
        "lat",
        "lon"
    )

    # Если city нет, но есть другая колонка с названием города, используем её
    if 'city' not in events_all.df.columns and 'city_name' in events_all.df.columns:
        users_mart_df = events_all.df.withColumn(
            "rn", F.row_number().over(window_spec)
        ).filter(F.col("rn") == 1).select(
            "user_id",
            "city_id",
            "city_name",  # Используем существующую city_name
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
        
        def save(self, path="output/users_mart"):
            """Сохранить витрину в parquet"""
            # Добавляем информацию о сэмпле в название
            if self.sample_rate < 1.0:
                path = f"{path}_sample_{int(self.sample_rate*100)}pct"
            self.df.write.mode("overwrite").parquet(path)
            print(f"✅ Витрина сохранена в {path}")
    
    return UsersMart(users_mart_df, users, events_all, sample_rate)

def calculate_zones_mart(spark, date):
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
    
    Returns:
    --------
    ZonesMart : объект с атрибутами:
        - df: DataFrame с витриной зон
        - city_stats: объект WeeklyMonthlyCityStats
    """
    print("\n" + "="*80)
    print(f"ШАГ 2: РАСЧЕТ ВИТРИНЫ ЗОН")
    print(f"Дата расчета: {date}")
    print("="*80)
    
    # 1. Загружаем события
    print("\n[1] Загрузка событий...")
    events_source = EventsSource(spark)
    events_source.read(date)
    
    events_raw = EventsRaw(spark, events_source)
    events_raw.calc()
    print(f"Загружено событий: {events_raw.df.count()}")
    
    # 2. Добавляем user_id
    print("\n[2] Добавление user_id...")
    events_with_user = EventsWithUserAndCoords(spark, events_raw)
    events_with_user.calc()
    
    # 3. Загружаем города
    print("\n[3] Загрузка городов...")
    cities_raw = CitiesRaw(spark)
    cities_raw.calc()
    
    cities = Cities(spark, cities_raw)
    cities.calc()
    print(f"Загружено городов: {cities.df.count()}")
    
    # 4. Определяем город для каждого события
    print("\n[4] Определение города для событий...")
    events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
    events_partial.calc()
    
    events_all = EventsWithCitiesAll(spark, events_partial)
    events_all.calc()
    
    # 5. Добавляем регистрации
    print("\n[5] Добавление данных о регистрациях...")
    registrations = RegistrationsWithCities(spark, events_all)
    registrations.calc()
    
    events_with_regs = EventsWithRegsWithCities(spark, events_all, registrations)
    events_with_regs.calc()
    
    # 6. Используем готовый класс WeeklyMonthlyCityStats для расчета витрины зон
    print("\n[6] Расчет статистики по городам за неделю и месяц...")
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
        def __init__(self, df, city_stats):
            self.df = df
            self.city_stats = city_stats
        
        def desc(self):
            print(f"\n📊 Витрина зон: {self.df.count()} записей")
            print("Поля:", ", ".join(self.df.columns))
            print("\nСводная статистика:")
            self.df.select(
                F.sum("week_message").alias("total_messages"),
                F.sum("week_reaction").alias("total_reactions"),
                F.sum("week_subscription").alias("total_subscriptions"),
                F.sum("week_user").alias("total_registrations")
            ).show()
        
        def save(self, path="output/zones_mart"):
            """Сохранить витрину в parquet"""
            self.df.write.mode("overwrite").parquet(path)
            print(f"✅ Витрина сохранена в {path}")
    
    return ZonesMart(city_stats.df, city_stats)


def calculate_friends_mart(spark, date, sample_rate=0.1):
    """
    Шаг 4. Витрина для рекомендации друзей
    
    Рекомендация друзей работает по принципу:
    если пользователи подписаны на один канал, ранее никогда не переписывались 
    и расстояние между ними не превышает один километр, то им обоим будет 
    предложено добавить другого в друзья.
    
    Витрина содержит следующие атрибуты:
    - user_left — первый пользователь
    - user_right — второй пользователь
    - processed_dttm — дата расчёта витрины
    - zone_id — идентификатор зоны (города)
    - local_time — локальное время
    
    Parameters:
    -----------
    spark : SparkSession
        Spark сессия
    date : str
        Дата для фильтрации событий (формат: 'YYYY-MM-DD')
        Обязательный параметр!
    sample_rate : float, optional
        Коэффициент сэмплирования (0.0 - 1.0)
    
    Returns:
    --------
    FriendsMart : объект с атрибутами:
        - df: DataFrame с витриной рекомендаций друзей
        - friend_recommendations: объект ProximityBasedFriends
    """
    print("\n" + "="*80)
    print(f"ШАГ 4: РАСЧЕТ ВИТРИНЫ РЕКОМЕНДАЦИЙ ДРУЗЕЙ")
    print(f"Дата расчета: {date}")
    print(f"Коэффициент сэмплирования: {sample_rate}")
    print("="*80)
    
    start_time = datetime.now()
    
    # 1. Загружаем события
    print("\n[1] Загрузка событий...")
    events_source = EventsSource(spark)
    events_source.read(date)
    
    events_raw = EventsRaw(spark, events_source)
    events_raw.calc()
    original_count = events_raw.df.count()
    print(f"Загружено событий: {original_count}")
    
    # 2. Добавляем user_id
    print("\n[2] Добавление user_id...")
    events_with_user = EventsWithUserAndCoords(spark, events_raw)
    events_with_user.calc()
    
    # Применяем сэмплирование если нужно
    if sample_rate < 1.0:
        print(f"\n[2a] Сэмплирование данных ({sample_rate*100:.1f}%)...")
        unique_users = events_with_user.df.select("user_id").distinct()
        sampled_users = unique_users.sample(withReplacement=False, fraction=sample_rate, seed=42)
        events_with_user.df = events_with_user.df.join(
            sampled_users.hint("broadcast"),
            on="user_id",
            how="inner"
        )
        sampled_count = events_with_user.df.count()
        print(f"После сэмплирования: {sampled_count} событий ({sampled_count/original_count*100:.1f}%)")
    
    # 3. Рассчитываем подписки на каналы
    print("\n[3] Расчет подписок на каналы...")
    subscriptions = UserChannelSubscriptions(spark, events_with_user)
    subscriptions.calc(save=True, sample_rate=sample_rate)
    
    # Оптимизация
    subscriptions.df = subscriptions.df.repartition(200).cache()
    subs_count = subscriptions.df.count()
    print(f"Найдено подписок: {subs_count}")
    
    # 4. Находим общие каналы между пользователями
    print("\n[4] Поиск общих каналов...")
    common_channels = UserCommonChannels(spark, subscriptions, subscriptions)
    common_channels.calc()
    common_count = common_channels.df.count() if common_channels.df else 0
    print(f"Найдено пар с общими каналами: {common_count}")
    
    # 5. Находим пользователей, которые переписывались
    print("\n[5] Поиск переписывавшихся пользователей...")
    corresponded = UsersCorresponded(spark, events_with_user)
    corresponded.calc()
    corr_count = corresponded.df.count() if corresponded.df else 0
    print(f"Найдено переписывавшихся пар: {corr_count}")
    
    # 6. Загружаем города и определяем местоположение пользователей
    print("\n[6] Определение местоположения пользователей...")
    cities_raw = CitiesRaw(spark)
    cities_raw.calc()
    
    cities = Cities(spark, cities_raw)
    cities.calc()
    
    events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
    events_partial.calc()
    
    events_all = EventsWithCitiesAll(spark, events_partial)
    events_all.calc()
    
    # 7. Формируем профили пользователей
    print("\n[7] Формирование профилей пользователей...")
    users = Users(spark, events_all)
    users.calc()
    users_count = users.df.count() if users.df else 0
    print(f"Сформировано профилей пользователей: {users_count}")
    
    # 8. Находим географически близких пользователей
    print("\n[8] Поиск географически близких пользователей...")
    users_near = UsersNear(spark, users, users)
    users_near.calc()
    near_count = users_near.df.count() if users_near.df else 0
    print(f"Найдено географически близких пар: {near_count}")
    
    # 9. Используем готовый класс ProximityBasedFriends для финальных рекомендаций
    print("\n[9] Формирование рекомендаций друзей...")
    friend_recommendations = ProximityBasedFriends(spark, common_channels, corresponded, users_near)
    friend_recommendations.calc()
    
    # Кэшируем результат
    if friend_recommendations.df:
        friend_recommendations.df = friend_recommendations.df.cache()
        recommendations_count = friend_recommendations.df.count()
    else:
        recommendations_count = 0
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n✅ Результаты витрины рекомендаций друзей:")
    print(f"Количество рекомендаций: {recommendations_count}")
    print(f"Время выполнения: {duration:.2f} сек")
    print("Схема данных:")
    if friend_recommendations.df:
        friend_recommendations.df.printSchema()
        print("\nПример данных:")
        friend_recommendations.df.show(10, truncate=False)
    else:
        print("Нет данных для отображения")
    
    # Создаем класс-обертку
    class FriendsMart:
        def __init__(self, df, friend_recommendations):
            self.df = df
            self.friend_recommendations = friend_recommendations
        
        def desc(self):
            if self.df is None:
                print("\n📊 Витрина рекомендаций друзей: нет данных")
                return
            
            print(f"\n📊 Витрина рекомендаций друзей: {self.df.count()} записей")
            print("Поля:", ", ".join(self.df.columns))
            print("\nСтатистика по зонам:")
            self.df.groupBy("zone_id").count()\
                .orderBy(F.desc("count")).show(10)
        
        def save(self, path="output/friends_mart"):
            """Сохранить витрину в parquet"""
            if self.df is not None:
                self.df.write.mode("overwrite").parquet(path)
                print(f"✅ Витрина сохранена в {path}")
            else:
                print("❌ Нет данных для сохранения")
    
    return FriendsMart(friend_recommendations.df, friend_recommendations)


def run_selected_mart(spark, mart_name, **kwargs):
    """
    Удобная функция для запуска выбранной витрины
    
    Parameters:
    -----------
    spark : SparkSession
        Spark сессия
    mart_name : str
        Название витрины ('users', 'zones', 'friends')
    **kwargs : dict
        Дополнительные параметры:
        - date: str (обязательный для всех витрин!)
        - sample_rate: float (только для friends)
    
    Returns:
    --------
    result : объект витрины (UsersMart, ZonesMart или FriendsMart)
    """
    print(f"\n{'🚀'*40}")
    print(f"ЗАПУСК ВИТРИНЫ: {mart_name.upper()}")
    print(f"{'🚀'*40}")
    
    if 'date' not in kwargs:
        raise ValueError("Параметр 'date' обязателен для всех витрин!")
    
    if mart_name == 'users':
        return calculate_users_mart(spark, kwargs['date'])
    elif mart_name == 'zones':
        return calculate_zones_mart(spark, kwargs['date'])
    elif mart_name == 'friends':
        sample_rate = kwargs.get('sample_rate', 0.1)
        return calculate_friends_mart(spark, kwargs['date'], sample_rate)
    else:
        print(f"❌ Неизвестная витрина: {mart_name}")
        print("Доступные витрины: 'users', 'zones', 'friends'")
        return None


# Пример использования в Jupyter Notebook:
"""
# В Jupyter Notebook:

import sys
sys.path.append('/path/to/your/project')

from geo_classes import create_spark_session
from marts_calculation import calculate_users_mart, calculate_zones_mart, calculate_friends_mart

# Создаем Spark сессию
spark = create_spark_session(app_name="MyProject")

# ОБЯЗАТЕЛЬНО указываем дату при вызове!
users_mart = calculate_users_mart(spark, date='2022-12-20')
# или
# zones_mart = calculate_zones_mart(spark, date='2022-12-20')
# или
# friends_mart = calculate_friends_mart(spark, date='2022-12-20', sample_rate=0.05)

# Анализ результатов
if users_mart:
    users_mart.desc()
    users_mart.save("output/my_users_mart")
    
    # Дополнительные запросы
    users_mart.df.createOrReplaceTempView("users")
    spark.sql("SELECT city_name, COUNT(*) as cnt FROM users GROUP BY city_name ORDER BY cnt DESC").show()

# Закрываем сессию
# spark.stop()
"""