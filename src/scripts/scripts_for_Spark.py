import os
import sys

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

# Импорт всех классов из geo_classes.py
from geo_classes import (
    create_spark_session, Config, DataLoader, DataExporter,
    EventsSource, CitiesRaw, Cities, EventsRaw, EventsWithUserAndCoords,
    EventsWithCitiesPartial, EventsWithCitiesAll, RegistrationsWithCities,
    EventsWithRegsWithCities, Users, UserChannelSubscriptions,
    UserCommonChannels, UsersCorresponded, UsersNear, UserTravelCities,
    UserTravels, UserGeoProfile, WeeklyMonthlyCityStats, ProximityBasedFriends
)


def run_full_pipeline(spark):
    """Запуск полного пайплайна обработки данных"""
    
    print("="*80)
    print("ЗАПУСК ПОЛНОГО ПАЙПЛАЙНА ОБРАБОТКИ ДАННЫХ")
    print("="*80)
    
    # ========== 1. ЗАГРУЗКА ИСХОДНЫХ ДАННЫХ ==========
    print("\n[1] ЗАГРУЗКА ИСХОДНЫХ ДАННЫХ")
    print("-" * 40)
    
    events_source = EventsSource(spark)
    events_source.read('2022-12-20')
    events_source.desc()
    
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
    
    events_raw = EventsRaw(spark, events_source)
    events_raw.calc()
    events_raw.desc()
    
    events_with_user = EventsWithUserAndCoords(spark, events_raw)
    events_with_user.calc()
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
        
        cities = Cities(spark, None)
        cities.read()
        
        events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
        events_partial.calc()
        
        events_all = EventsWithCitiesAll(spark, events_partial)
        events_all.calc()
        events_all.desc()
        return events_all
    
    elif report_name == 'ProximityBasedFriends':
        # Загружаем необходимые данные
        events_with_user = EventsWithUserAndCoords(spark, None)
        events_with_user.read()
        
        subscriptions = UserChannelSubscriptions(spark, events_with_user)
        subscriptions.calc(save=True, sample_rate=sample_rate)
        
        common_channels = UserCommonChannels(spark, subscriptions, subscriptions)
        common_channels.calc()
        
        corresponded = UsersCorresponded(spark, events_with_user)
        corresponded.calc()
        
        events_all = EventsWithCitiesAll(spark, None)
        events_all.read()
        
        users = Users(spark, events_all)
        users.read()
        
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
    spark = create_spark_session(app_name="GeoProject", executor_instances=2)
    
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