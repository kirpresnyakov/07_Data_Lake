import os, sys
from abc import ABC, abstractmethod
from functools import wraps

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType

# ==================== КОНФИГУРАЦИЯ ====================

class Config:
    BASE_PATH = '/user/kirillprsv/project7/'
    SOURCE_PATH = '/user/master/data/geo/'
    CITIES_SOURCE = '/user/kirillprsv/data/actual/geo.csv'
    
    LAYERS = {
        'raw': BASE_PATH + 'raw/',
        'ods': BASE_PATH + 'ods/',
        'dds': BASE_PATH + 'dds/',
        'dml': BASE_PATH + 'dml/'
    }
    
    @classmethod
    def get_path(cls, layer, name):
        return cls.LAYERS[layer] + name

# ==================== ДЕКОРАТОРЫ ====================

def require_dfs(*dfs_names):
    """Декоратор для проверки наличия зависимых датафреймов"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for df_name in dfs_names:
                attr = getattr(self, df_name, None)
                if attr is None:
                    raise Exception(f"Required {df_name} is None in {self.__class__.__name__}")
                if hasattr(attr, 'df') and attr.df is None:
                    raise Exception(f"{df_name}.df is None in {self.__class__.__name__}")
            return func(self, *args, **kwargs)
        return wrapper
    return decorator

# ==================== БАЗОВЫЕ КЛАССЫ ====================

class BaseEntity(ABC):
    """Базовый класс для всех сущностей"""
    def __init__(self, session, name, layer):
        self.session = session
        self.path = name
        self.layer = layer
        self.df = None
        self.count = -1
    
    def read(self):
        """Чтение данных из parquet"""
        if self.df is None:
            self.df = DataLoader.load(self.session, self.layer, self.path)
            self._cache_df()
    
    def _cache_df(self):
        """Кеширование датафрейма"""
        if self.df is not None:
            self.df = self.df.cache()
            self.df.rdd.setName(self.path)
            self.count = self.df.count()
    
    @abstractmethod
    def calc(self, save=True):
        """Абстрактный метод для расчета"""
        pass
    
    def desc(self):
        """Описание датафрейма"""
        print(f'Count: {self.count}, Layer: {self.layer}, Name: {self.path}')
        if self.df:
            print(f'RDD Name: {self.df.rdd.name()}')
            print(f'Partitions: {self.df.rdd.getNumPartitions()}')
            self.df.printSchema()
        else:
            print('self.df is None')

class BaseSource(BaseEntity):
    """Базовый класс для источников данных"""
    def __init__(self, session, name, source_path=None):
        super().__init__(session, name, 'source')
        self.source_path = source_path or Config.SOURCE_PATH
    
    @abstractmethod
    def read(self, filter_date=None):
        pass

# ==================== ЗАГРУЗКА ДАННЫХ ====================

class DataLoader:
    """Загрузка данных из всех источников (raw, source, ods, dds, dml)"""
    
    EXPORT_CONFIGS = {
        'EventsRaw': {'partitionBy': ['date'], 'coalesce': False},
        'EventsWithUserAndCoords': {'partitionBy': ['date'], 'coalesce': False},
        'EventsWithCitiesPartial': {'partitionBy': ['date'], 'coalesce': False, 'repartition': 'date'},
        'EventsWithCitiesAll': {'partitionBy': ['date'], 'coalesce': False, 'repartition': 'date'},
        'RegistrationsWithCities': {'partitionBy': ['date'], 'coalesce': False, 'repartition': 'date'},
        'EventsWithRegsWithCities': {'partitionBy': ['date'], 'coalesce': False, 'repartition': 'date'},
        'Cities': {'coalesce': True, 'partitionBy': []},
        'CitiesRaw': {'coalesce': True, 'partitionBy': []},
        'Users': {'coalesce': False, 'partitionBy': []},
        'UsersNear': {'coalesce': False, 'partitionBy': [], 'repartition': 1},
        'UserChannelSubscriptions': {'coalesce': False, 'partitionBy': []},
        'UserCommonChannels': {'coalesce': False, 'partitionBy': []},
        'UsersCorresponded': {'coalesce': False, 'partitionBy': []},
        'UserTravelCities': {'coalesce': False, 'partitionBy': [], 'repartition': 5},
        'UserTravels': {'coalesce': False, 'partitionBy': []},
        'UserGeoProfile': {'format': 'csv', 'delimiter': ';', 'repartition': 1},
        'WeeklyMonthlyCityStats': {'format': 'csv', 'delimiter': ';', 'repartition': 1},
        'ProximityBasedFriends': {'format': 'csv', 'delimiter': ';', 'repartition': 1}
    }
    
    @classmethod
    def load(cls, session, layer, name, format='parquet', filter_date=None):
        """Загрузка данных с указанного слоя"""
        try:
            if layer == 'raw' and name == 'EventsRaw':
                path = Config.get_path(layer, name)
                df = session.read.parquet(path)
                if filter_date:
                    df = df.filter(f"date < '{filter_date}'")
                return df
            elif layer == 'source':
                if name == 'events':
                    df = session.read.parquet(Config.SOURCE_PATH + 'events')
                    if filter_date:
                        df = df.filter(f"date < '{filter_date}'")
                    return df
                elif name == 'cities':
                    return session.read.csv(Config.CITIES_SOURCE, sep=";", header=True)
            else:
                return session.read.parquet(Config.get_path(layer, name))
        except Exception as E:
            print(f'DataLoader error: layer={layer}, name={name}', E)
            raise E

# ==================== ЭКСПОРТ ДАННЫХ ====================

class DataExporter:
    """Экспорт данных в целевые форматы с оптимизациями"""
    
    @classmethod
    def export(cls, df, layer, name):
        """Экспорт данных с учетом конфигурации"""
        config = DataLoader.EXPORT_CONFIGS.get(name, {})
        
        # Применяем трансформации перед записью
        if config.get('repartition'):
            if isinstance(config['repartition'], str):
                df = df.repartition(F.col(config['repartition']))
            else:
                df = df.repartition(config['repartition'])
        elif config.get('coalesce'):
            df = df.coalesce(1)
        
        # Настраиваем запись
        writer = df.write.mode('overwrite')
        
        if config.get('partitionBy'):
            writer = writer.partitionBy(config['partitionBy'])
        
        if config.get('format') == 'csv':
            writer = writer.format('csv')\
                .option("header", "true")\
                .option("delimiter", config.get('delimiter', ';'))
        else:
            writer = writer.format('parquet')
        
        writer.save(Config.get_path(layer, name))

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def create_spark_session(app_name="GeoProject", executor_instances=2):
    """Создание Spark сессии"""
    return SparkSession.builder.master("yarn")\
        .appName(app_name)\
        .config("spark.executor.instances", str(executor_instances))\
        .config("spark.executor.cores", "2")\
        .config("spark.driver.memory", "1g")\
        .config("spark.sql.shuffle.partitions", "170")\
        .config("spark.eventLog.logBlockUpdates.enabled", "true")\
        .getOrCreate()

def radians(col_name):
    """Преобразование координат в радианы"""
    return F.round(F.toRadians(F.regexp_replace(col_name, ',', '.')), 5)

def haversine_distance(lat1, lon1, lat2, lon2):
    """Вычисление расстояния по формуле гаверсинуса"""
    return F.acos(
        F.sin(F.col(lat1)) * F.sin(F.col(lat2)) + 
        F.cos(F.col(lat1)) * F.cos(F.col(lat2)) * 
        F.cos(F.col(lon1) - F.col(lon2))
    ) * F.lit(6371)

# ==================== КЛАССЫ-СУЩНОСТИ ====================

class EventsSource(BaseSource):
    """Исходные события"""
    def __init__(self, session):
        super().__init__(session, 'events')
    
    def read(self, filter_date=None):
        if self.df is None:
            self.df = DataLoader.load(self.session, 'source', 'events', filter_date=filter_date)\
                .select('event_type', 'message_from', 'message_to', 'reaction_from',
                       'user', 'subscription_channel', 'date', 'lat', 'lon', 
                       'message_id', 'datetime')
            self._cache_df()
        return self.df

class CitiesRaw(BaseEntity):
    """Сырые данные о городах"""
    def __init__(self, session):
        super().__init__(session, 'CitiesRaw', 'raw')
    
    def calc(self, save=True):
        self.df = DataLoader.load(self.session, 'source', 'cities')
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class Cities(BaseEntity):
    """Города с координатами в радианах"""
    def __init__(self, session, cities_raw):
        super().__init__(session, 'Cities', 'ods')
        self.cities_raw = cities_raw
    
    @require_dfs('cities_raw')
    def calc(self, save=True):
        self.df = self.cities_raw.df\
            .withColumn('city_lat', radians('lat'))\
            .withColumn('city_lon', radians('lng'))\
            .drop('lat', 'lng')\
            .withColumnRenamed('id', 'city_id')
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class EventsRaw(BaseEntity):
    """Сырые события"""
    def __init__(self, session, events_source):
        super().__init__(session, 'EventsRaw', 'raw')
        self.events_source = events_source
    
    @require_dfs('events_source')
    def calc(self, save=True):
        self.df = self.events_source.df
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class EventsWithUserAndCoords(BaseEntity):
    """События с идентифицированными пользователями и координатами в радианах"""
    def __init__(self, session, events_raw):
        super().__init__(session, 'EventsWithUserAndCoords', 'ods')
        self.events_raw = events_raw
    
    @require_dfs('events_raw')
    def calc(self, save=True):
        self.df = self.events_raw.df\
            .withColumn('lat', radians('lat'))\
            .withColumn('lon', radians('lon'))\
            .withColumn('user_id', 
                F.when(F.col('event_type') == 'subscription', F.col('user'))
                 .when(F.col('event_type') == 'reaction', F.col('reaction_from'))
                 .when(F.col('event_type') == 'message', F.col('message_from')))\
            .selectExpr('event_type', 'user_id', 'date', 'message_id',
                       'message_to', 'subscription_channel', 'lat', 'lon', 
                       'datetime as event_datetime')
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class EventsWithCitiesPartial(BaseEntity):
    """События с привязкой к ближайшим городам"""
    def __init__(self, session, events_with_user, cities):
        super().__init__(session, 'EventsWithCitiesPartial', 'dds')
        self.events_with_user = events_with_user
        self.cities = cities
    
    @require_dfs('events_with_user', 'cities')
    def calc(self, save=True):
        events_df = self.events_with_user.df.withColumn('evt_id', F.monotonically_increasing_id())
        cities_df = F.broadcast(self.cities.df)
        
        # Cross join для поиска ближайшего города
        self.df = events_df.crossJoin(cities_df)\
            .withColumn('dist', haversine_distance('lat', 'lon', 'city_lat', 'city_lon'))
        
        # Выбираем ближайший город для каждого события
        w = Window.partitionBy('evt_id').orderBy('dist')
        self.df = self.df.withColumn('row', F.row_number().over(w))\
            .filter('row = 1')\
            .drop('row', 'city_lat', 'city_lon', 'dist')\
            .withColumn('city_id', F.when(~F.isnull('city_id'), F.col('city_id')))\
            .withColumn('city', F.when(~F.isnull('city'), F.col('city')))
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class EventsWithCitiesAll(BaseEntity):
    """События с доопределенными городами (по последнему сообщению)"""
    def __init__(self, session, events_partial):
        super().__init__(session, 'EventsWithCitiesAll', 'dds')
        self.events_partial = events_partial
    
    @require_dfs('events_partial')
    def calc(self, save=True):
        w = Window().partitionBy('user_id').orderBy(
            F.when(F.col('event_type') == 'message', 1).otherwise(0).desc(),
            F.desc('date')
        )
        
        # Один раз вычисляем first значения
        first_values = {
            'event_type': F.first('event_type', ignorenulls=True).over(w),
            'city_id': F.first('city_id', ignorenulls=True).over(w),
            'city': F.first('city', ignorenulls=True).over(w)
        }
        
        self.df = self.events_partial.df\
            .withColumn('first_event_type', first_values['event_type'])\
            .withColumn('first_city_id', first_values['city_id'])\
            .withColumn('first_city', first_values['city'])\
            .withColumn('added', 
                F.when(F.isnull('city_id') & 
                      (F.col('first_event_type') == 'message') & 
                      ~F.isnull('first_city_id'), 1)
                 .otherwise(0))\
            .withColumn('city_id',
                F.when(F.isnull('city_id') & 
                      (F.col('first_event_type') == 'message'),
                      F.col('first_city_id'))
                 .otherwise(F.col('city_id')))\
            .withColumn('city',
                F.when(F.isnull('city') & 
                      (F.col('first_event_type') == 'message'),
                      F.col('first_city'))
                 .otherwise(F.col('city')))\
            .drop('first_event_type', 'first_city_id', 'first_city')
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class RegistrationsWithCities(BaseEntity):
    """Регистрации пользователей"""
    def __init__(self, session, events_all):
        super().__init__(session, 'RegistrationsWithCities', 'dds')
        self.events_all = events_all
    
    @require_dfs('events_all')
    def calc(self, save=True):
        w = Window().partitionBy('user_id').orderBy('date')
        
        self.df = self.events_all.df\
            .filter('city is not null')\
            .withColumn('row', F.row_number().over(w))\
            .filter('row = 1')\
            .withColumn('event_type', F.lit('user_registration'))\
            .drop('row')
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class EventsWithRegsWithCities(BaseEntity):
    """Объединение событий и регистраций"""
    def __init__(self, session, events_all, registrations):
        super().__init__(session, 'EventsWithRegsWithCities', 'dds')
        self.events_all = events_all
        self.registrations = registrations
    
    @require_dfs('events_all', 'registrations')
    def calc(self, save=True):
        self.df = self.events_all.df.unionByName(self.registrations.df)
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class Users(BaseEntity):
    """Профили пользователей"""
    def __init__(self, session, events_all):
        super().__init__(session, 'Users', 'dds')
        self.events_all = events_all
    
    @require_dfs('events_all')
    def calc(self, save=True):
        # Окно для определения актуального города (по последнему сообщению)
        w1 = Window().partitionBy('user_id').orderBy(
            F.when(F.col('event_type') == 'message', 1).otherwise(0).desc(),
            F.desc('date')
        )
        
        # Окно для последнего события
        w2 = Window().partitionBy('user_id').orderBy(F.desc('date'))
        
        first_vals = {
            'event_type': F.first('event_type', ignorenulls=True).over(w1),
            'city_id': F.first('city_id', ignorenulls=True).over(w1),
            'city': F.first('city', ignorenulls=True).over(w1),
            'lat': F.first('lat', ignorenulls=True).over(w1),
            'lon': F.first('lon', ignorenulls=True).over(w1),
            'message_id': F.first('message_id', ignorenulls=True).over(w1)
        }
        
        last_vals = {
            'city_id': F.first('city_id', ignorenulls=True).over(w2),
            'city': F.first('city', ignorenulls=True).over(w2),
            'datetime': F.first('event_datetime', ignorenulls=True).over(w2)
        }
        
        self.df = self.events_all.df\
            .withColumn('first_event_type', first_vals['event_type'])\
            .withColumn('first_city_id', first_vals['city_id'])\
            .withColumn('first_city', first_vals['city'])\
            .withColumn('first_lat', first_vals['lat'])\
            .withColumn('first_lon', first_vals['lon'])\
            .withColumn('first_message_id', first_vals['message_id'])\
            .withColumn('last_city_id', last_vals['city_id'])\
            .withColumn('last_city', last_vals['city'])\
            .withColumn('last_datetime', last_vals['datetime'])\
            .withColumn('actual_city_id',
                F.when(F.col('first_event_type') == 'message', F.col('first_city_id')))\
            .withColumn('actual_city',
                F.when(F.col('first_event_type') == 'message', F.col('first_city')))\
            .withColumn('actual_lat',
                F.when(F.col('first_event_type') == 'message', F.col('first_lat')))\
            .withColumn('actual_lon',
                F.when(F.col('first_event_type') == 'message', F.col('first_lon')))\
            .withColumn('last_message_id',
                F.when(F.col('first_event_type') == 'message', F.col('first_message_id')))\
            .select('user_id', 'actual_city_id', 'actual_city', 'last_city_id', 
                   'last_city', 'last_message_id', 'actual_lat', 'actual_lon', 
                   'last_datetime')\
            .distinct()
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class UserChannelSubscriptions(BaseEntity):
    """Подписки пользователей на каналы"""
    def __init__(self, session, events_with_user):
        super().__init__(session, 'UserChannelSubscriptions', 'dds')
        self.events_with_user = events_with_user
    
    @require_dfs('events_with_user')
    def calc(self, save=True, sample_rate=1.0):
        self.df = self.events_with_user.df\
            .filter("event_type = 'subscription' AND subscription_channel IS NOT NULL")\
            .select('user_id', 'subscription_channel')\
            .distinct()\
            .sample(sample_rate)
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class UserCommonChannels(BaseEntity):
    """Общие каналы у пар пользователей"""
    def __init__(self, session, subs_left, subs_right):
        super().__init__(session, 'UserCommonChannels', 'dds')
        self.subs_left = subs_left
        self.subs_right = subs_right
    
    @require_dfs('subs_left', 'subs_right')
    def calc(self, save=True):
        self.df = self.subs_left.df.alias('left')\
            .join(
                self.subs_right.df.alias('right'),
                F.col('left.subscription_channel') == F.col('right.subscription_channel'),
                'inner'
            )\
            .select(
                F.col('left.user_id').alias('user_left'),
                F.col('right.user_id').alias('user_right'),
                F.col('left.subscription_channel')
            )\
            .distinct()
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class UsersCorresponded(BaseEntity):
    """Пары переписывающихся пользователей"""
    def __init__(self, session, events_with_user):
        super().__init__(session, 'UsersCorresponded', 'dds')
        self.events_with_user = events_with_user
    
    @require_dfs('events_with_user')
    def calc(self, save=True):
        self.df = self.events_with_user.df\
            .filter("event_type = 'message' AND message_to IS NOT NULL")\
            .select(
                F.col('user_id').alias('user_left'),
                F.col('message_to').cast(StringType()).alias('user_right')
            )\
            .distinct()\
            .withColumn('temp_left', F.col('user_left'))\
            .withColumn('user_left',
                F.when(F.col('user_left') > F.col('user_right'), F.col('user_right'))
                 .otherwise(F.col('user_left')))\
            .withColumn('user_right',
                F.when(F.col('temp_left') > F.col('user_right'), F.col('temp_left'))
                 .otherwise(F.col('user_right')))\
            .drop('temp_left')\
            .distinct()
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class UsersNear(BaseEntity):
    """Пользователи в радиусе 1 км"""
    def __init__(self, session, users_left, users_right):
        super().__init__(session, 'UsersNear', 'dds')
        self.users_left = users_left
        self.users_right = users_right
    
    @require_dfs('users_left', 'users_right')
    def calc(self, save=True):
        # Подготавливаем левый датафрейм
        left_df = self.users_left.df\
            .filter('actual_lat IS NOT NULL')\
            .select(
                F.col('user_id').alias('user_left'),
                F.col('actual_city_id').alias('city_id_left'),
                F.col('actual_city').alias('city_left'),
                F.col('actual_lat').alias('left_lat'),
                F.col('actual_lon').alias('left_lon')
            )
        
        # Подготавливаем правый датафрейм
        right_df = self.users_right.df\
            .filter('actual_lat IS NOT NULL')\
            .select(
                F.col('user_id').alias('user_right'),
                F.col('actual_city_id').alias('city_id_right'),
                F.col('actual_city').alias('city_right'),
                F.col('actual_lat').alias('actual_lat'),
                F.col('actual_lon').alias('actual_lon')
            )
        
        # Репартиционируем для лучшего распределения
        left_df = left_df.repartition(100)
        right_df = right_df.repartition(100)
        
        # Cross join с фильтрацией уникальных пар
        self.df = left_df.crossJoin(right_df)\
            .filter('user_left < user_right')\
            .withColumn('dist', haversine_distance('left_lat', 'left_lon', 'actual_lat', 'actual_lon'))\
            .filter('dist <= 1')
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class UserTravelCities(BaseEntity):
    """Периоды пребывания в городах"""
    def __init__(self, session, events_all):
        super().__init__(session, 'UserTravelCities', 'dds')
        self.events_all = events_all
    
    @require_dfs('events_all')
    def calc(self, save=True):
        df = self.events_all.df.filter('city IS NOT NULL')
        
        w = Window().partitionBy('user_id').orderBy('date')
        
        # Определяем начала и концы пребываний
        df = df.withColumn('start_flag', 
            F.when((F.col('city') != F.lag('city').over(w)) | F.isnull(F.lag('city').over(w)), 1))
        df = df.withColumn('end_flag',
            F.when((F.col('city') != F.lead('city').over(w)) | F.isnull(F.lead('city').over(w)), 1))
        
        # Группируем пребывания
        df = df.withColumn('stay_id', F.sum('start_flag').over(w))
        
        w2 = Window().partitionBy('user_id', 'stay_id')
        
        self.df = df\
            .withColumn('city_stay_start', F.min('date').over(w2))\
            .withColumn('city_stay_end', F.max('date').over(w2))\
            .withColumn('city_stay_len', F.datediff('city_stay_end', 'city_stay_start') + 1)\
            .select('user_id', 'city_id', 'city', 'city_stay_start', 'city_stay_end', 'city_stay_len')\
            .distinct()
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class UserTravels(BaseEntity):
    """Маршруты путешествий"""
    def __init__(self, session, travel_cities):
        super().__init__(session, 'UserTravels', 'dds')
        self.travel_cities = travel_cities
    
    @require_dfs('travel_cities')
    def calc(self, save=True):
        df = self.travel_cities.df\
            .withColumn('long_stay_flag', F.when(F.col('city_stay_len') > 27, 1).otherwise(0))
        
        w = Window().partitionBy('user_id').orderBy(F.desc('long_stay_flag'), F.desc('city_stay_start'))
        
        self.df = df\
            .withColumn('home_city',
                F.when(F.first('long_stay_flag').over(w) == 1, F.first('city').over(w)))\
            .withColumn('home_city_id',
                F.when(F.first('long_stay_flag').over(w) == 1, F.first('city_id').over(w)))\
            .groupBy('user_id')\
            .agg(
                F.sort_array(F.collect_list(F.struct('city_stay_start', 'city'))).alias('tuples_array'),
                F.count('*').alias('travel_count'),
                F.min('home_city').alias('home_city'),
                F.min('home_city_id').alias('home_city_id')
            )\
            .withColumn('travel_array', F.col('tuples_array.city'))
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

# ==================== ОТЧЕТЫ (самодокументируемые названия) ====================

class UserGeoProfile(BaseEntity):
    """
    Гео-профиль пользователя:
    - act_city: где пользователь активен сейчас
    - home_city: где живет (>27 дней)
    - travel_history: история перемещений
    - local_time: местное время пользователя
    """
    def __init__(self, session, users, travels):
        super().__init__(session, 'UserGeoProfile', 'dml')
        self.users = users
        self.travels = travels
    
    @require_dfs('users', 'travels')
    def calc(self, save=True):
        self.df = self.users.df.join(self.travels.df, 'user_id', 'outer')\
            .withColumn('TIME', F.col('last_datetime').cast('Timestamp'))\
            .withColumn('local_time',
                F.when(
                    F.col('last_city').isin('Sydney', 'Melbourne', 'Brisbane', 'Perth',
                                           'Adelaide', 'Canberra', 'Hobart', 'Darwin'),
                    F.from_utc_timestamp(F.col('TIME'), 
                                         F.concat(F.lit('Australia/'), F.col('last_city')))
                ).otherwise(None))\
            .withColumn('travel_array', F.concat_ws(',', F.col('travel_array')))\
            .select('user_id', 
                    F.col('actual_city').alias('act_city'),
                    'home_city', 'travel_count', 'travel_array', 'local_time')
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class WeeklyMonthlyCityStats(BaseEntity):
    """
    Статистика по городам:
    - Недельные и месячные метрики
    - Детализация по типам событий
    - Накопительные итоги за месяц
    """
    def __init__(self, session, events_with_regs):
        super().__init__(session, 'WeeklyMonthlyCityStats', 'dml')
        self.events_with_regs = events_with_regs
    
    @require_dfs('events_with_regs')
    def calc(self, save=True):
        df = self.events_with_regs.df\
            .withColumn('month', F.trunc('date', 'Month'))\
            .withColumn('week', F.trunc('date', 'Week'))
        
        w = Window().partitionBy('month', 'city_id')
        
        self.df = df.groupBy('month', 'week', 'city_id')\
            .agg(
                F.sum(F.when(F.col('event_type') == 'message', 1)).alias('week_message'),
                F.sum(F.when(F.col('event_type') == 'reaction', 1)).alias('week_reaction'),
                F.sum(F.when(F.col('event_type') == 'subscription', 1)).alias('week_subscription'),
                F.sum(F.when(F.col('event_type') == 'user_registration', 1)).alias('week_user')
            )\
            .withColumn('month_message', F.sum('week_message').over(w))\
            .withColumn('month_reaction', F.sum('week_reaction').over(w))\
            .withColumn('month_subscription', F.sum('week_subscription').over(w))\
            .withColumn('month_user', F.sum('week_user').over(w))\
            .withColumnRenamed('city_id', 'zone_id')\
            .orderBy('month', 'week', 'zone_id')
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

class ProximityBasedFriends(BaseEntity):
    """
    Рекомендации друзей на основе:
    - Географическая близость (<1km)
    - Общие каналы подписок
    - Отсутствие истории переписки
    """
    def __init__(self, session, common_channels, corresponded, near):
        super().__init__(session, 'ProximityBasedFriends', 'dml')
        self.common_channels = common_channels
        self.corresponded = corresponded
        self.near = near
    
    @require_dfs('common_channels', 'corresponded', 'near')
    def calc(self, save=True):
        # Подготавливаем данные
        near_df = self.near.df.selectExpr('user_left', 'user_right', 'city_left', 'city_id_left as zone_id')
        common_df = self.common_channels.df\
            .withColumnRenamed('user_left', 'ucc_user_left')\
            .withColumnRenamed('user_right', 'ucc_user_right')
        
        # Создаем алиас для near_df
        near_df = near_df.alias('near')
        
        # Создаем алиас для corresponded.df
        corresponded_df = self.corresponded.df.alias('corr')
        
        # Соединяем и фильтруем
        self.df = near_df.join(
            common_df,
            (F.col('near.user_left') == F.col('ucc_user_left')) & 
            (F.col('near.user_right') == F.col('ucc_user_right')),
            'inner'
        ).join(
            corresponded_df,
            (F.col('near.user_left') == F.col('corr.user_left')) & 
            (F.col('near.user_right') == F.col('corr.user_right')),
            'leftanti'
        ).withColumn('processed_dttm', F.current_timestamp())\
         .withColumn('local_time',
            F.when(
                F.col('near.city_left').isin('Sydney', 'Melbourne', 'Brisbane', 'Perth',
                                           'Adelaide', 'Canberra', 'Hobart', 'Darwin'),
                F.from_utc_timestamp(F.col('processed_dttm'),
                                     F.concat(F.lit('Australia/'), F.col('near.city_left')))
            ).otherwise(None))\
         .drop('near.city_left', 'subscription_channel', 'ucc_user_left', 'ucc_user_right')
        
        self._cache_df()
        if save:
            DataExporter.export(self.df, self.layer, self.path)
        return self.df

# ==================== ОСНОВНОЙ КОД ====================

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Инициализация и расчет всех сущностей
    events_source = EventsSource(spark)
    events_source.read('2022-12-20')
    
    cities_raw = CitiesRaw(spark)
    cities_raw.calc()
    
    cities = Cities(spark, cities_raw)
    cities.calc()
    
    events_raw = EventsRaw(spark, events_source)
    events_raw.calc()
    
    events_with_user = EventsWithUserAndCoords(spark, events_raw)
    events_with_user.calc()
    
    events_partial = EventsWithCitiesPartial(spark, events_with_user, cities)
    events_partial.calc()
    
    events_all = EventsWithCitiesAll(spark, events_partial)
    events_all.calc()
    
    registrations = RegistrationsWithCities(spark, events_all)
    registrations.calc()
    
    events_with_regs = EventsWithRegsWithCities(spark, events_all, registrations)
    events_with_regs.calc()
    
    users = Users(spark, events_all)
    users.calc()
    
    subscriptions = UserChannelSubscriptions(spark, events_with_user)
    subscriptions.calc(save=True, sample_rate=1.0)
    
    common_channels = UserCommonChannels(spark, subscriptions, subscriptions)
    common_channels.calc()
    
    corresponded = UsersCorresponded(spark, events_with_user)
    corresponded.calc()
    
    users_near = UsersNear(spark, users, users)
    users_near.calc()
    
    travel_cities = UserTravelCities(spark, events_all)
    travel_cities.calc()
    
    travels = UserTravels(spark, travel_cities)
    travels.calc()
    
    # Отчеты
    user_profile = UserGeoProfile(spark, users, travels)
    user_profile.calc()
    
    city_stats = WeeklyMonthlyCityStats(spark, events_with_regs)
    city_stats.calc()
    
    friend_recommendations = ProximityBasedFriends(spark, common_channels, corresponded, users_near)
    friend_recommendations.calc()