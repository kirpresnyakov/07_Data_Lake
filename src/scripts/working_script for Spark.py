def run_full_pipeline(spark):
    """Запуск полного пайплайна обработки данных"""
    
    print("="*80)
    print("ЗАПУСК ПОЛНОГО ПАЙПЛАЙНА ОБРАБОТКИ ДАННЫХ")
    print("="*80)
    
    # ========== 1. ЗАГРУЗКА ИСХОДНЫХ ДАННЫХ ==========
    print("\n[1] ЗАГРУЗКА ИСХОДНЫХ ДАННЫХ")
    print("-" * 40)
    
    # Загружаем данные через DataLoader
    print("Загрузка данных событий через DataLoader...")
    events_df = DataLoader.load(spark, 'source', 'events', filter_date='2022-12-20')
    
    print("Схема загруженных данных:")
    events_df.printSchema()
    print("Пример данных:")
    events_df.show(5, truncate=False)
    
    # Создаем класс-обертку для EventsRaw
    class SimpleEventsSource:
        def __init__(self, df):
            self.df = df
        def read(self, date):
            pass  # Данные уже загружены
    
    # Создаем EventsRaw с нашими данными
    events_raw = EventsRaw(spark, SimpleEventsSource(events_df))
    events_raw.df = events_df
    events_raw._cache_df()
    events_raw.desc()
    
    # ========== 2. ОБРАБОТКА ГОРОДОВ ==========
    print("\n[2] ОБРАБОТКА ГОРОДОВ")
    print("-" * 40)
    
    cities_raw = CitiesRaw(spark)
    cities_raw.calc()
    cities_raw.desc()
    
    cities = Cities(spark, cities_raw)
    cities.calc()
    cities.desc()
    
    # ========== 3. ОБРАБОТКА СОБЫТИЙ ==========
    print("\n[3] ОБРАБОТКА СОБЫТИЙ")
    print("-" * 40)

    # Извлекаем user_id из структуры event (без UDF)
    from pyspark.sql import functions as F

    events_raw.df = events_raw.df.withColumn(
        'user_id', 
        F.coalesce(
            F.col("event.message_from").cast("int"),
            F.col("event.reaction_from").cast("int"),
            F.col("event.user").cast("int"),
            F.lit(None)
        )
    )

    print("Данные после добавления user_id:")
    events_raw.df.select('event', 'user_id', 'event_type', 'lat', 'lon', 'date').show(10, truncate=50)

    # Создаем временный DataFrame с нужными полями на верхнем уровне
    events_with_ids_df = events_raw.df.select(
        'event_type',
        'lat',
        'lon',
        'date',
        F.col('event.message_id').alias('message_id'),
        F.col('event.message_to').alias('message_to'),
        F.col('event.subscription_channel').alias('subscription_channel'),
        F.col('event.datetime').alias('event_datetime'),  # Переименовываем в event_datetime
        'user_id'
    )

    # Обновляем events_raw.df
    events_raw.df = events_with_ids_df

    events_with_user = EventsWithUserAndCoords(spark, events_raw)
    events_with_user.df = events_raw.df  # Используем уже обогащенный DataFrame
    events_with_user._cache_df()
    events_with_user.desc()

    events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
    events_partial.calc()
    events_partial.desc()

    events_all = EventsWithCitiesAll(spark, events_partial)
    events_all.calc()
    events_all.desc()

    # ========== 4. РЕГИСТРАЦИИ ==========
    print("\n[4] ОБРАБОТКА РЕГИСТРАЦИЙ")
    print("-" * 40)

    registrations = RegistrationsWithCities(spark, events_all)
    registrations.calc()
    registrations.desc()

    events_with_regs = EventsWithRegsWithCities(spark, events_all, registrations)
    events_with_regs.calc()
    events_with_regs.desc()

    # ========== 5. ПРОФИЛИ ПОЛЬЗОВАТЕЛЕЙ ==========
    print("\n[5] ФОРМИРОВАНИЕ ПРОФИЛЕЙ ПОЛЬЗОВАТЕЛЕЙ")
    print("-" * 40)

    # Проверяем наличие нужных колонок перед созданием Users
    print("Колонки в events_all.df перед созданием Users:")
    print(events_all.df.columns)

    users = Users(spark, events_all)
    users.calc()
    users.desc()
    
    
    
    # ========== 6. ПОДПИСКИ И ОБЩЕНИЕ ==========
    print("\n[6] АНАЛИЗ ПОДПИСОК И ОБЩЕНИЯ")
    print("-" * 40)
    
    subscriptions = UserChannelSubscriptions(spark, events_with_user)
    subscriptions.calc(save=True, sample_rate=0.1)  # 10% сэмпл для тестирования
    subscriptions.desc()
    
    # Оптимизация
    subscriptions.df = subscriptions.df.repartition(600).cache()
    subscriptions.df.count()
    
    common_channels = UserCommonChannels(spark, subscriptions, subscriptions)
    common_channels.calc()
    common_channels.desc()
    
    corresponded = UsersCorresponded(spark, events_with_user)
    corresponded.calc()
    corresponded.desc()
    
    # ========== 7. ГЕОГРАФИЧЕСКАЯ БЛИЗОСТЬ ==========
    print("\n[7] ПОИСК ГЕОГРАФИЧЕСКИ БЛИЗКИХ ПОЛЬЗОВАТЕЛЕЙ")
    print("-" * 40)
    
    users_near = UsersNear(spark, users, users)
    users_near.calc()
    users_near.desc()
    
    # ========== 8. ПУТЕШЕСТВИЯ ПОЛЬЗОВАТЕЛЕЙ ==========
    print("\n[8] АНАЛИЗ ПУТЕШЕСТВИЙ")
    print("-" * 40)
    
    travel_cities = UserTravelCities(spark, events_all)
    travel_cities.calc()
    travel_cities.desc()
    
    travels = UserTravels(spark, travel_cities)
    travels.calc()
    travels.desc()
    
    # ========== 9. ФИНАЛЬНЫЕ ОТЧЕТЫ ==========
    print("\n[9] ГЕНЕРАЦИЯ ФИНАЛЬНЫХ ОТЧЕТОВ")
    print("-" * 40)
    
    print("\n--- Отчет 1: Гео-профили пользователей ---")
    user_profile = UserGeoProfile(spark, users, travels)
    user_profile.calc()
    user_profile.desc()
    
    print("\n--- Отчет 2: Статистика по городам ---")
    city_stats = WeeklyMonthlyCityStats(spark, events_with_regs)
    city_stats.calc()
    city_stats.desc()
    
    print("\n--- Отчет 3: Рекомендации друзей на основе близости ---")
    friend_recommendations = ProximityBasedFriends(spark, common_channels, corresponded, users_near)
    friend_recommendations.calc()
    friend_recommendations.desc()
    
    print("\n" + "="*80)
    print("ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН")
    print("="*80)
    
    return {
        'users': users,
        'events_all': events_all,
        'friend_recommendations': friend_recommendations,
        'user_profile': user_profile,
        'city_stats': city_stats
    }
def run_specific_report(spark, report_name, date='2022-12-20', sample_rate=0.1):
    """Запуск конкретного отчета"""
    
    print(f"\nЗАПУСК ОТЧЕТА: {report_name}")
    
    if report_name == 'Cities':
        cities_raw = CitiesRaw(spark)
        cities_raw.calc()
        cities_raw.desc()
        
        cities = Cities(spark, cities_raw)
        cities.calc()
        cities.desc()
        return cities
    
    elif report_name == 'EventsWithCitiesAll':
        events_source = EventsSource(spark)
        events_source.read(date)
        
        events_raw = EventsRaw(spark, events_source)
        events_raw.calc()
        
        events_with_user = EventsWithUserAndCoords(spark, events_raw)
        events_with_user.calc()
        
        cities_raw = CitiesRaw(spark)
        cities_raw.calc()
        cities = Cities(spark, cities_raw)
        cities.calc()
        
        events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
        events_partial.calc()
        
        events_all = EventsWithCitiesAll(spark, events_partial)
        events_all.calc()
        events_all.desc()
        return events_all
    
    elif report_name == 'ProximityBasedFriends':
        # Загружаем необходимые данные
        events_source = EventsSource(spark)
        events_source.read(date)
        
        events_raw = EventsRaw(spark, events_source)
        events_raw.calc()
        
        events_with_user = EventsWithUserAndCoords(spark, events_raw)
        events_with_user.calc()
        
        subscriptions = UserChannelSubscriptions(spark, events_with_user)
        subscriptions.calc(save=True, sample_rate=sample_rate)
        
        common_channels = UserCommonChannels(spark, subscriptions, subscriptions)
        common_channels.calc()
        
        corresponded = UsersCorresponded(spark, events_with_user)
        corresponded.calc()
        
        cities_raw = CitiesRaw(spark)
        cities_raw.calc()
        cities = Cities(spark, cities_raw)
        cities.calc()
        
        events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
        events_partial.calc()
        events_all = EventsWithCitiesAll(spark, events_partial)
        events_all.calc()
        
        users = Users(spark, events_all)
        users.calc()
        
        users_near = UsersNear(spark, users, users)
        users_near.calc()
        
        friend_recommendations = ProximityBasedFriends(spark, common_channels, corresponded, users_near)
        friend_recommendations.calc()
        friend_recommendations.desc()
        
        # Показываем пример результатов
        print("\nПример рекомендаций:")
        friend_recommendations.df.show(10, truncate=False)
        
        return friend_recommendations
    
    else:
        print(f"Неизвестный отчет: {report_name}")
        return None

def main():
    """Основная функция"""
    
    # Создаем Spark сессию
    spark = create_spark_session(app_name="GeoProject", executor_instances=4)
    
    # Выводим информацию о сессии
    print("Spark сессия создана")
    print("app_id:", spark.sparkContext._jsc.sc().applicationId())
    
    # Выбор режима работы
    mode = "full"  # "full" - полный пайплайн, или название конкретного отчета
    
    if mode == "full":
        results = run_full_pipeline(spark)
    else:
        results = run_specific_report(spark, mode)
    
    print("\nРабота завершена")
    
    # Останавливаем сессию
    spark.stop()


if __name__ == "__main__":
    main()