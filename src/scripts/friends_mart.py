from geo_classes import (
    EventsRaw, EventsWithUserAndCoords,  # Убрали EventsSource
    CitiesRaw, Cities, EventsWithCitiesPartial, EventsWithCitiesAll,
    Users, UserChannelSubscriptions, UserCommonChannels,
    UsersCorresponded, UsersNear, ProximityBasedFriends,
    RegistrationsWithCities, EventsWithRegsWithCities,
    WeeklyMonthlyCityStats, DataLoader, Config
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from datetime import datetime


def calculate_friends_mart(spark, date, sample_rate=0.1):
    """
    Шаг 4. Витрина для рекомендации друзей
    """
    print("\n" + "="*80)
    print(f"ШАГ 4: РАСЧЕТ ВИТРИНЫ РЕКОМЕНДАЦИЙ ДРУЗЕЙ")
    print(f"Дата расчета: {date}")
    print(f"Коэффициент сэмплирования: {sample_rate}")
    print("="*80)
    
    start_time = datetime.now()
    
    # 1. Загружаем события через DataLoader (как в zones_mart)
    print("\n[1] Загрузка событий через DataLoader...")
    events_df = DataLoader.load(spark, 'source', 'events', filter_date=date)
    original_count = events_df.count()
    print(f"Загружено событий до сэмплирования: {original_count}")
    
    # Применяем сэмплирование если нужно
    if sample_rate < 1.0:
        print(f"\n[1a] Сэмплирование данных ({sample_rate*100:.1f}%)...")
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
    
    # Применяем сэмплирование по пользователям если нужно
    if sample_rate < 1.0:
        print(f"\n[4a] Сэмплирование пользователей...")
        unique_users = events_with_user.df.select("user_id").distinct()
        sampled_users = unique_users.sample(withReplacement=False, fraction=sample_rate, seed=42)
        events_with_user.df = events_with_user.df.join(
            sampled_users.hint("broadcast"),
            on="user_id",
            how="inner"
        )
        sampled_count = events_with_user.df.count()
        print(f"После сэмплирования: {sampled_count} событий")
    
    # 6. Рассчитываем подписки на каналы
    print("\n[5] Расчет подписок на каналы...")
    subscriptions = UserChannelSubscriptions(spark, events_with_user)
    subscriptions.calc(save=True, sample_rate=sample_rate)
    
    # Оптимизация
    subscriptions.df = subscriptions.df.repartition(200).cache()
    subs_count = subscriptions.df.count()
    print(f"Найдено подписок: {subs_count}")
    
    # 7. Находим общие каналы между пользователями
    print("\n[6] Поиск общих каналов...")
    common_channels = UserCommonChannels(spark, subscriptions, subscriptions)
    common_channels.calc()
    common_count = common_channels.df.count() if common_channels.df else 0
    print(f"Найдено пар с общими каналами: {common_count}")
    
    # 8. Находим пользователей, которые переписывались
    print("\n[7] Поиск переписывавшихся пользователей...")
    corresponded = UsersCorresponded(spark, events_with_user)
    corresponded.calc()
    corr_count = corresponded.df.count() if corresponded.df else 0
    print(f"Найдено переписывавшихся пар: {corr_count}")
    
    # 9. Загружаем города и определяем местоположение пользователей
    print("\n[8] Определение местоположения пользователей...")
    cities_raw = CitiesRaw(spark)
    cities_raw.calc()
    
    cities = Cities(spark, cities_raw)
    cities.calc()
    
    events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
    events_partial.calc()
    
    events_all = EventsWithCitiesAll(spark, events_partial)
    events_all.calc()
    
    # 10. Формируем профили пользователей
    print("\n[9] Формирование профилей пользователей...")
    users = Users(spark, events_all)
    users.calc()
    users_count = users.df.count() if users.df else 0
    print(f"Сформировано профилей пользователей: {users_count}")
    
    # 11. Находим географически близких пользователей
    print("\n[10] Поиск географически близких пользователей...")
    users_near = UsersNear(spark, users, users)
    users_near.calc()
    near_count = users_near.df.count() if users_near.df else 0
    print(f"Найдено географически близких пар: {near_count}")
    
    # 12. Используем готовый класс ProximityBasedFriends для финальных рекомендаций
    print("\n[11] Формирование рекомендаций друзей...")
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
    
    # Создаем класс-обертку
    class FriendsMart:
        def __init__(self, df, friend_recommendations, sample_rate=1.0):
            self.df = df
            self.friend_recommendations = friend_recommendations
            self.sample_rate = sample_rate
        
        def desc(self):
            if self.df is None:
                print("\n📊 Витрина рекомендаций друзей: нет данных")
                return
            
            print(f"\n📊 Витрина рекомендаций друзей: {self.df.count()} записей")
            print(f"   Сэмплирование: {self.sample_rate*100:.1f}%")
            print("Поля:", ", ".join(self.df.columns))
            print("\nСтатистика по зонам:")
            self.df.groupBy("zone_id").count()\
                .orderBy(F.desc("count")).show(10)
        
        def save(self, path=None):
            if self.df is None:
                print("❌ Нет данных для сохранения")
                return
            
            if path is None:
                base_name = "FriendsMart"
                if self.sample_rate < 1.0:
                    # Форматируем процент с 2 знаками
                    percent = self.sample_rate * 100
                    if percent >= 0.1:
                        percent_str = f"{percent:.1f}".rstrip('0').rstrip('.')
                    else:
                        percent_str = f"{percent:.2f}".rstrip('0').rstrip('.')
                    
                    percent_str = percent_str.replace('.', '_')
                    base_name = f"FriendsMart_sample_{percent_str}pct"
                
                path = Config.get_path('dml', base_name)
            
            self.df.write.mode("overwrite").parquet(path)
            print(f"✅ Витрина сохранена в {path}")
    
    return FriendsMart(friend_recommendations.df, friend_recommendations, sample_rate)

# Для самостоятельного тестирования файла
if __name__ == "__main__":
    print("Этот файл содержит функцию calculate_friends_mart()")  # Исправлено имя функции
    print("Используйте её в Jupyter Notebook следующим образом:")
    print("""
    from geo_classes import create_spark_session
    from friends_mart import calculate_friends_mart  # Исправлен импорт
    
    spark = create_spark_session(app_name="FriendsMart")
    friends_mart = calculate_friends_mart(spark, '2022-12-20', sample_rate=0.1)
    friends_mart.desc()
    friends_mart.df.show(5)
    friends_mart.save()  # Сохранит в DML слой
    spark.stop()
    """)