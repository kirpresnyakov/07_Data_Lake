#!/usr/bin/env python3
"""
run_marts_job.py
Универсальная джоба для расчета витрин данных

Использование:
    python run_marts_job.py <task_name> <date> [sample_rate]

    task_name: cities, events_user_coords, events_cities_all, events_regs_cities,
               users, user_travels, user_geo_profile, weekly_monthly_stats,
               proximity_friends, users_mart, zones_mart, friends_mart, all_marts
    
    date: дата расчета в формате YYYY-MM-DD (обязательный параметр)
    sample_rate: коэффициент сэмплирования 0.0-1.0 (по умолчанию: 0.1)

Примеры:
    python run_marts_job.py users_mart 2022-12-20 0.1
    python run_marts_job.py zones_mart 2022-12-20 0.05
    python run_marts_job.py friends_mart 2022-12-20 0.01
    python run_marts_job.py all_marts 2022-12-20 0.1
"""

import os
import sys
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

import sys
sys.path.append('/home/kirillprsv')

# Импортируем geo_classes
import geo_classes as gc

# Импортируем функции расчета витрин из отдельных модулей
try:
    from users_mart import calculate_users_mart
    from zones_mart import calculate_zones_mart
    from friends_mart import calculate_friends_mart
except ImportError as e:
    print(f"Предупреждение: не удалось импортировать модули витрин: {e}")
    print("Будут доступны только базовые задачи из geo_classes")


def main():
    # Парсинг аргументов
    if len(sys.argv) < 3:
        print(__doc__)
        print("\n❌ Ошибка: необходимо указать задачу и дату!")
        sys.exit(1)
    
    task = sys.argv[1]
    date = sys.argv[2]  # Дата теперь обязательная
    
    # Проверяем формат даты
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        print(f"❌ Ошибка: неверный формат даты '{date}'. Используйте YYYY-MM-DD")
        sys.exit(1)
    
    # sample_rate опциональный
    sample_rate = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1
    
    # Проверяем sample_rate
    if not (0 < sample_rate <= 1.0):
        print(f"❌ Ошибка: sample_rate должен быть от 0.0 до 1.0, получено {sample_rate}")
        sys.exit(1)
    
    print("\n" + "="*80)
    print("ЗАПУСК ДЖОБЫ РАСЧЕТА ВИТРИН")
    print(f"Задача: {task}")
    print(f"Дата: {date}")
    print(f"Сэмплирование: {sample_rate*100:.1f}%")
    print("="*80)
    
    start_time = datetime.now()
    
    try:
        # Создаем Spark сессию
        spark = SparkSession.builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark сессия создана")
        #gc.print_conf(spark)
        
        # Словарь с конфигурациями задач
        task_configs = {
            # Базовые задачи из geo_classes
            'cities': lambda: run_cities(spark),
            'events_user_coords': lambda: run_events_user_coords(spark, date),
            'events_cities_all': lambda: run_events_cities_all(spark),
            'events_regs_cities': lambda: run_events_regs_cities(spark),
            'users': lambda: run_users(spark),
            'user_travels': lambda: run_user_travels(spark),
            'user_geo_profile': lambda: run_user_geo_profile(spark),
            'weekly_monthly_stats': lambda: run_weekly_monthly_stats(spark),
            'proximity_friends': lambda: run_proximity_friends(spark, sample_rate),
            
            # Витрины из отдельных модулей
            'users_mart': lambda: run_users_mart(spark, date, sample_rate),
            'zones_mart': lambda: run_zones_mart(spark, date, sample_rate),
            'friends_mart': lambda: run_friends_mart(spark, date, sample_rate),
            'all_marts': lambda: run_all_marts(spark, date, sample_rate)
        }
        
        if task in task_configs:
            print(f"\n▶️ Выполнение задачи: {task}")
            result = task_configs[task]()
            
            # Если результат - витрина, показываем статистику
            if hasattr(result, 'desc'):
                result.desc()
            
            # Сохраняем результат если есть метод save
            if hasattr(result, 'save'):
                save_mart(result, task, date, sample_rate)
                
        else:
            print(f"❌ Неизвестная задача: {task}")
            print("Доступные задачи:", ", ".join(task_configs.keys()))
            sys.exit(1)
        
        # Выводим время выполнения
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"\n⏱️ Время выполнения: {duration:.2f} сек ({duration/60:.2f} мин)")
        
    except Exception as err:
        print(f"\n❌ Ошибка выполнения: {err}")
        print(f"Тип ошибки: {type(err)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        print("\n🔚 Завершение работы")


def save_mart(mart, task_name, date, sample_rate):
    """Сохранить витрину в HDFS/локальную файловую систему"""
    # Определяем базовый путь из переменной окружения или используем локальный
    base_path = os.environ.get('MART_OUTPUT_PATH', '/data/marts')
    
    # Формируем имя файла
    if sample_rate < 1.0:
        percent = int(sample_rate * 100)
        output_path = f"{base_path}/{task_name}/date={date}/sample_{percent}pct"
    else:
        output_path = f"{base_path}/{task_name}/date={date}"
    
    # Сохраняем
    mart.save(output_path)
    print(f"✅ Витрина сохранена: {output_path}")


# ============== Базовые задачи из geo_classes ==============

def run_cities(spark):
    """Расчет городов"""
    cities_raw = gc.CitiesRaw(spark)
    cities_raw.calc(True)
    cities_raw.desc()
    
    cities = gc.Cities(spark, cities_raw)
    cities.calc(True)
    cities.desc()
    return cities


def run_events_user_coords(spark, date):
    """События с пользователями и координатами"""
    events_source = gc.EventsSource(spark)
    events_source.read(date)
    events_source.desc()

    events_raw = gc.EventsRaw(spark, events_source)
    events_raw.calc(True)
    events_raw.desc()

    events_user_coords = gc.EventsWithUserAndCoords(spark, events_raw)
    events_user_coords.calc(True)
    events_user_coords.desc()
    return events_user_coords


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
    return events_all


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
    return events_regs


def run_users(spark):
    """Расчет пользователей"""
    events_all = gc.EventsWithCitiesAll(spark, None)
    events_all.read()
    events_all.desc()
    
    users = gc.Users(spark, events_all)
    users.calc(True)
    users.desc()
    return users


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
    return travels


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
    return geo_profile


def run_weekly_monthly_stats(spark):
    """Недельная/месячная статистика по городам"""
    events_regs = gc.EventsWithRegsWithCities(spark, None, None)
    events_regs.read()
    events_regs.desc()

    stats = gc.WeeklyMonthlyCityStats(spark, events_regs)
    stats.calc(True)
    stats.desc()
    return stats


def run_proximity_friends(spark, sample_rate):
    """Рекомендации друзей на основе близости"""
    events_user_coords = gc.EventsWithUserAndCoords(spark, None)
    events_user_coords.read()
    events_user_coords.desc()

    # Подписки
    subscriptions = gc.UserChannelSubscriptions(spark, events_user_coords)
    subscriptions.calc(True, sample_rate)
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
    return friends


# ============== Витрины из отдельных модулей ==============

def run_users_mart(spark, date, sample_rate):
    """Расчет витрины пользователей"""
    try:
        users_mart = calculate_users_mart(spark, date, sample_rate)
        return users_mart
    except NameError as e:
        print("❌ Модуль users_mart не импортирован")
        raise


def run_zones_mart(spark, date, sample_rate):
    """Расчет витрины зон"""
    try:
        zones_mart = calculate_zones_mart(spark, date, sample_rate)
        return zones_mart
    except NameError as e:
        print("❌ Модуль zones_mart не импортирован")
        raise


def run_friends_mart(spark, date, sample_rate):
    """Расчет витрины друзей"""
    try:
        friends_mart = calculate_friends_mart(spark, date, sample_rate)
        return friends_mart
    except NameError as e:
        print("❌ Модуль friends_mart не импортирован")
        raise


def run_all_marts(spark, date, sample_rate):
    """Расчет всех витрин последовательно"""
    results = {}
    
    print("\n" + "="*60)
    print("ЗАПУСК ВСЕХ ВИТРИН")
    print("="*60)
    
    # Витрина пользователей
    print("\n📊 1. Витрина пользователей")
    results['users'] = run_users_mart(spark, date, sample_rate)
    
    # Витрина зон
    print("\n📊 2. Витрина зон")
    results['zones'] = run_zones_mart(spark, date, sample_rate)
    
    # Витрина друзей
    print("\n📊 3. Витрина друзей")
    results['friends'] = run_friends_mart(spark, date, sample_rate)
    
    print("\n" + "="*60)
    print("✅ ВСЕ ВИТРИНЫ РАССЧИТАНЫ")
    print("="*60)
    
    return results


if __name__ == "__main__":
    main()