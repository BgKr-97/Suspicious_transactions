from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2

from Database.database import DBExtractor
from models.risk_model import RiskScoringModel
import time


# Вспомогательные функции
def compute_age(df: pd.DataFrame, is_read: bool = False) -> pd.DataFrame:
    '''
    Функция добавляет к DF столбец 'client_age': возраст клиента.

    Параметры:
    - is_read: True — вывод информации для проверки работы функции,
    False - для использования в рабочих целях (добавление столбца 'client_age' в DF).
    '''
    df = df.copy()
    df['birth_date'] = pd.to_datetime(df['birth_date'])

    today = datetime.today()
    age = today.year - df['birth_date'].dt.year

    # Если ДР ещё не наступил в этом году - вычитаем 1
    before_birthday = (today.month < df['birth_date'].dt.month) | \
                      ((today.month == df['birth_date'].dt.month) & (today.day < df['birth_date'].dt.day))
    age -= before_birthday.astype(int)
    df.insert(4, 'client_age', age)

    if is_read:
        df = df.groupby('client_id')[['birth_date', 'client_age']].first().reset_index()

    return df


# Приоритетные признаки: detect_large_amounts, detect_night_transactions, detect_geolocation
def detect_large_amounts(df: pd.DataFrame, threshold: float = 100_000, is_read: bool = False) -> pd.DataFrame:
    """
    Признак: 'Большая сумма операции (Сумма > threshold)'
    Функция добавляет к DF булев столбец 'risk_big_sum': TRUE если сумма транзакции превышает threshold.

    Параметры:
    - threshold: пороговая сумма транзакции;
    - is_read: True — вывод информации для проверки работы функции,
    False - для использования в рабочих целях (добавление столбца 'risk_big_sum' в DF).
    """
    df = df.copy()

    df['risk_big_sum'] = (df['amount'] > threshold).astype(int)

    if is_read:
        df = df[['client_id', 'amount', 'risk_big_sum']]
    return df


def detect_night_transactions(df: pd.DataFrame, start_hour: int = 0, end_hour: int = 5, is_read: bool = False) -> pd.DataFrame:
    """
    Признак: 'Операции в ночное время (start_hour – end_hour)'
    Функция добавляет к DF булев столбец 'risk_night_time': TRUE если транзакция проводилась в период [start_hour; end_hour]

    Параметры:
    - start_hour, end_hour: время рассматриваемого промежутка;
    - is_read: True — вывод информации для проверки работы функции,
    False - для использования в рабочих целях (добавление столбца 'risk_night_time' в DF).
    """
    df = df.copy()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df['risk_night_time'] = df['date_time'].dt.hour.between(start_hour, end_hour).astype(int)

    if is_read:
        df = df[['client_id', 'date_time', 'risk_night_time']]

    return df


def detect_geolocation(df: pd.DataFrame, distance_km: float = 500, max_hours: float = 1, is_read=False) -> pd.DataFrame:
    """
    Признак: 'Резкое изменение геолокации (изменение > distance_km (км.))'
    Функция добавляет к DF булев столбец 'risk_geolocation_change': TRUE если геопозиция совершенной транзакции
    по сравнению с предыдущей отличается не менее чем на distance_km (км.) в течение max_hours (ч.)

    Параметры:
    - distance_km, max_hours: ограничения дистанции и времени;
    - is_read: True — вывод информации для проверки работы функции,
    False - для использования в рабочих целях (добавление столбца 'risk_geolocation_change' в DF).
    """

    def haversine(lat1: int, lon1: int, lat2: int, lon2: int) -> float:
        """
        Вычисляет расстояние между двумя точками по координатам (в км).

        Параметры:
        - lat1, lat2: широты геолокации,
        - lon1, lon2: долготы геолокации
        """
        R = 6371
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c

    df = df.copy()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df = df.sort_values(['client_id', 'date_time'])

    df['prev_lat'] = df.groupby('client_id')['sender_latitude'].shift()
    df['prev_lon'] = df.groupby('client_id')['sender_longitude'].shift()
    df['prev_time'] = df.groupby('client_id')['date_time'].shift()

    df['distance_km'] = df.apply(
        lambda row: haversine(row.prev_lat, row.prev_lon, row.sender_latitude, row.sender_longitude)
        if pd.notnull(row.prev_lat) else 0,
        axis=1
    )
    df['hours_diff'] = (df['date_time'] - df['prev_time']).dt.total_seconds() / 3600
    df['risk_geolocation_change'] = ((df['distance_km'] > distance_km) & (df['hours_diff'] <= max_hours)).astype(int)

    if not is_read:
        df.drop(columns=['prev_lat', 'prev_lon', 'prev_time', 'distance_km', 'hours_diff'], axis=1, inplace=True)
    else:
        df.drop(columns=['birth_date', 't_type', 'amount', 'blacklist'], axis=1, inplace=True)

    return df


# Вторичные признаки: detect_operation_rate, detect_small_sums, detect_none_type
def detect_operation_rate(df: pd.DataFrame, n_threshold: int = 7, time_window: int = 120, is_read: bool = False) -> pd.DataFrame:
    """
    Признак: 'Увеличение числа операций за короткое время'
    Функция добавляет к DF булев столбец 'oper_rate':
    TRUE, если за последние time_window (мин.) до текущей транзакции (включая её)
    клиент совершил более n_threshold транзакций.

    Параметры:
    - n_threshold: количество транзакций, при превышении которых срабатывает флаг;
    - time_window: окно времени (мин.);
    - is_read: True — вывод информации для проверки работы функции,
    False - для использования в рабочих целях (добавление столбца 'oper_rate' в DF).
    """
    df = df.copy()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df = df.sort_values(['client_id', 'date_time'])
    df['oper_rate'] = 0

    for client_id, group_df in df.groupby('client_id', sort=False):
        times = group_df['date_time']
        idxs = group_df.index

        for i, current_time in enumerate(times):
            window_start = current_time - timedelta(minutes=time_window)
            window_mask = (times >= window_start) & (times <= current_time)
            if window_mask.sum() > n_threshold:
                df.loc[idxs[i], 'oper_rate'] = 1

    if is_read:
        df = df[['client_id', 'date_time', 'oper_rate']]

    return df


def detect_small_sums(df: pd.DataFrame, min_amt: float = 0, max_amt: float = 10000,
              total_threshold: float = 20000, time_window: int = 60, is_read: bool = False) -> pd.DataFrame:
    """
    Признак: 'Несколько маленьких сумм вместо одной большой'
    Добавляет булев столбец 'small_sum':
    TRUE, если за последние time_window (мин.) сумма мелких транзакций превысила total_threshold.

    Параметры:
    - min_amt, max_amt: границы 'мелкой' транзакции;
    - total_threshold: пороговая сумма для окна time_window;
    - time_window: размер окна (мин.);
    - is_read: True — вывод информации для проверки работы функции,
    False - для использования в рабочих целях (добавление столбца 'oper_rate' в DF).
    """
    df = df.copy()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df = df.sort_values(['client_id', 'date_time'])
    df['small_transactions'] = df['amount'].between(min_amt, max_amt)
    df['small_sum'] = 0

    for client_id, group_df in df.groupby('client_id', sort=False):
        small_df = group_df[group_df['small_transactions']]
        times = small_df['date_time']
        idxs = small_df.index

        for i, current_time in enumerate(times):
            window_start = current_time - timedelta(minutes=time_window)
            mask = (times >= window_start) & (times <= current_time)
            window_sum = small_df.loc[mask, 'amount'].sum()

            if window_sum > total_threshold:
                df.loc[idxs[i], 'small_sum'] = 1

    if is_read:
        df = df[['client_id', 'date_time', 'amount', 'small_transactions', 'small_sum']]
    else:
        df.drop(['small_transactions'], axis=1, inplace=True)

    return df


def detect_none_type(df: pd.DataFrame, is_read: bool = False) -> pd.DataFrame:
    """
    Признак: 'Категория перевода (Неизвестная)'
    Функция добавляет к DF булев столбец 'none_type': TRUE, если тип транзакции - неизвестный.

    Параметры:
    - is_read: True — вывод информации для проверки работы функции,
    False - для использования в рабочих целях (добавление столбца 'oper_rate' в DF).
    """
    df = df.copy()

    df['none_type'] = df['t_type'].apply(lambda x: x in ['Неизвестно', 'Unknown', 'Other', ''] or not x).astype(int)

    if is_read:
        df = df[['client_id', 't_type', 'none_type']]

    return df


def main():
    # 1. Подгружаем параметры из окружения
    load_dotenv()
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    RISK_JSON = os.getenv('RISK_JSON')
    DM_SHEMA = os.getenv('DM_SHEMA')
    DM_TABLE = os.getenv('DM_TABLE')

    extractor = DBExtractor(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)

    # 2. Загружаем данные из схемы core и дополняем DataFrame булевыми столбцами: True, если признак выполняется
    df_transactions = extractor.fetch_merged_transactions()
    df_main = (
        df_transactions

        # Добавляем возраст клиентов
        .pipe(compute_age)

        # Добавляем колонки с приоритетными признаками
        .pipe(detect_large_amounts)
        .pipe(detect_night_transactions)
        .pipe(detect_geolocation)

        # Добавляем колонки с вторичными признаками
        .pipe(detect_operation_rate)
        .pipe(detect_small_sums)
        .pipe(detect_none_type)
    )

    # 3. Инициализируем модель риска и рассчитываем: оценку риска, статус риска, причины риска
    start_time = time.perf_counter()
    risk_model = RiskScoringModel(RISK_JSON)
    df_calculated_risks = risk_model.calculate_scores(df_main)
    duration = time.perf_counter() - start_time
    print(f'✅ Данные успешно обработаны за {duration:.2f} секунд., риск оценен.')

    # 4. Забираем с БД справочную информацию, необходимую для витрины, и объединяем с основным DataFrame
    df_info = extractor.fetch_merged_info()
    df_data_mart = df_calculated_risks.merge(df_info, on='transaction_id', how='inner')

    # 5. Заливаем трансформированные данные (df_data_mart) в витрину
    # Создание витрины
    extractor.create_datamart()

    # Убираем лишние поля, не нужные в витрине
    df_data_mart.drop([
       'birth_date', 'risk_geolocation_change', 'small_sum', 'none_type',
       'blacklist', 'risk_big_sum', 'risk_night_time', 'oper_rate'], axis=1, inplace=True)
    extractor.load_datamart(df_data_mart, DM_SHEMA, DM_TABLE)

if __name__ == '__main__':
    main()
