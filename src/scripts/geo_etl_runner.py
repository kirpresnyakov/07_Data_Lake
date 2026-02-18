import os, sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import geo_classes as gc


def main():
    task = sys.argv[1]
    
    print("--------===== Start =====--------")
    
    try:
        spark = gc.get_session()
        print("--------===== Started Spark =====--------")
        gc.print_conf(spark)
        print("Passed task name:", task)

        # Словарь с конфигурациями задач
        task_configs = {
            'Cities': lambda: run_cities(spark),
            'EventsWithUserAndCoords': lambda: run_events_user_coords(spark),
            'EventsWithCitiesAll': lambda: run_events_cities_all(spark),
            'EventsWithRegsWithCities': lambda: run_events_regs_cities(spark),
            'Users': lambda: run_users(spark),
            'UserTravels': lambda: run_user_travels(spark),
            'UserGeoProfile': lambda: run_user_geo_profile(spark),
            'WeeklyMonthlyCityStats': lambda: run_weekly_monthly_stats(spark),
            'ProximityBasedFriends': lambda: run_proximity_friends(spark)
        }
        
        if task in task_configs:
            print(f"Working with {task}")
            task_configs[task]()
        else:
            raise Exception(f"Unknown task: {task}")
        
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise
    finally:
        print("--------=== End =====--------")


def run_cities(spark):
    """Расчет городов"""
    cities_raw = gc.CitiesRaw(spark)
    cities_raw.calc(True)
    cities_raw.desc()
    
    cities = gc.Cities(spark, cities_raw)
    cities.calc(True)
    cities.desc()


def run_events_user_coords(spark):
    """События с пользователями и координатами"""
    events_source = gc.EventsSource(spark)
    events_source.read('2022-04-01')
    events_source.desc()

    events_raw = gc.EventsRaw(spark, events_source)
    events_raw.calc(True)
    events_raw.desc()

    events_user_coords = gc.EventsWithUserAndCoords(spark, events_raw)
    events_user_coords.calc(True)
    events_user_coords.desc()


def run_events_cities_all(spark):
    """События со всеми городами"""
    events_user_coords = gc.EventsWithUserAndCoords(spark, None)
    events_user_coords.read()
    events_user_coords.desc()

    cities = gc.Cities(spark, None)
    cities.read()
    cities.desc()

    events_partial = gc.EventsWithCitiesPartial(spark, events_user_coords, cities)
    events_partial.calc(True)
    events_partial.desc()

    events_all = gc.EventsWithCitiesAll(spark, events_partial)
    events_all.calc(True)
    events_all.desc()


def run_events_regs_cities(spark):
    """События и регистрации с городами"""
    events_all = gc.EventsWithCitiesAll(spark, None)
    events_all.read()
    events_all.desc()

    registrations = gc.RegistrationsWithCities(spark, events_all)
    registrations.calc(True)
    registrations.desc()

    events_regs = gc.EventsWithRegsWithCities(spark, events_all, registrations)
    events_regs.calc(True)
    events_regs.desc()


def run_users(spark):
    """Расчет пользователей"""
    events_all = gc.EventsWithCitiesAll(spark, None)
    events_all.read()
    events_all.desc()
    
    users = gc.Users(spark, events_all)
    users.calc(True)
    users.desc()


def run_user_travels(spark):
    """Расчет путешествий пользователей"""
    events_all = gc.EventsWithCitiesAll(spark, None)
    events_all.read()
    events_all.desc()

    travel_cities = gc.UserTravelCities(spark, events_all)
    travel_cities.calc(True)
    travel_cities.desc()

    travels = gc.UserTravels(spark, travel_cities)
    travels.calc(True)
    travels.desc()


def run_user_geo_profile(spark):
    """Гео-профиль пользователя"""
    users = gc.Users(spark, None)
    users.read()
    users.desc()
    
    travels = gc.UserTravels(spark, None)
    travels.read()
    travels.desc()
    
    geo_profile = gc.UserGeoProfile(spark, users, travels)
    geo_profile.calc(True)
    geo_profile.desc()


def run_weekly_monthly_stats(spark):
    """Недельная/месячная статистика по городам"""
    events_regs = gc.EventsWithRegsWithCities(spark, None, None)
    events_regs.read()
    events_regs.desc()

    stats = gc.WeeklyMonthlyCityStats(spark, events_regs)
    stats.calc(True)
    stats.desc()


def run_proximity_friends(spark):
    """Рекомендации друзей на основе близости"""
    events_user_coords = gc.EventsWithUserAndCoords(spark, None)
    events_user_coords.read()
    events_user_coords.desc()

    # Подписки
    subscriptions = gc.UserChannelSubscriptions(spark, events_user_coords)
    subscriptions.calc(True, 0.001)
    subscriptions.desc()

    # Общие каналы
    common_channels = gc.UserCommonChannels(spark, subscriptions, subscriptions)
    common_channels.calc(True)
    common_channels.desc()

    # Переписывающиеся пользователи
    corresponded = gc.UsersCorresponded(spark, events_user_coords)
    corresponded.calc(True)
    corresponded.desc()

    # Близкие пользователи
    users = gc.Users(spark, None)
    users.read()
    users.desc()
    
    users_near = gc.UsersNear(spark, users, users)
    users_near.calc(True)
    users_near.desc()

    # Финальный отчет
    friends = gc.ProximityBasedFriends(spark, common_channels, corresponded, users_near)
    friends.calc(True)
    friends.desc()


if __name__ == "__main__":
    main()