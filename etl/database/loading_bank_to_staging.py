import argparse
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_countries() -> None:
    """Добавляет данные стран с банковской БД со слоя bank в БД транзакции на слой staging."""
    df_bank_countries = pd.read_sql_query(
        "SELECT * FROM bank.countries",
        con=engine_bank,
        index_col=None
    )
    df_staging_ids = pd.read_sql_query(
        "SELECT country_id FROM staging.countries",
        con=engine_transactions
    )

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['country_id'].to_numpy()
        max_staging_id = id_rows_staging.max()
        df_bank_countries = df_bank_countries[df_bank_countries['country_id'] > max_staging_id].drop_duplicates()

        if not df_bank_countries.empty:
            df_bank_countries['created_at'] = datetime.now()
            df_bank_countries.to_sql(
                name='countries',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
        else:
            logger.info("Нет новых записей для добавления в таблицу countries")
    else:
        logger.info("Таблица staging.countries пуста, добавляем все записи")
        df_bank_countries['created_at'] = datetime.now()
        df_bank_countries.to_sql(
            name='countries',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi'
        )
 

def add_cities() -> None:
    """Добавляет данные городов с банковской БД со слоя bank в БД транзакции на слой staging."""

    # Выборка данных из таблицы bank.cities
    df_bank_cities = pd.read_sql_query(f"""
        SELECT *
        FROM bank.cities
    """, con=engine_bank, index_col=None)

    # Выборка id из таблицы staging.cities
    df_staging_ids = pd.read_sql_query(f"""
        SELECT city_id
        FROM staging.cities
    """, con=engine_transactions)

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['city_id'].to_numpy()
        max_staging_id = id_rows_staging.max()
        df_bank_cities = df_bank_cities[df_bank_cities['city_id'] > max_staging_id].drop_duplicates()

        if not df_bank_cities.empty:
            df_bank_cities['created_at'] = datetime.now()
            df_bank_cities.to_sql(
                name='cities',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
        else:
            logger.info("Нет новых записей для добавления в таблицу cities")
    else:
        logger.info("Таблица staging.cities пуста, добавляем все записи")
        df_bank_cities['created_at'] = datetime.now()
        df_bank_cities.to_sql(
            name='cities',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi'
        )


def add_clients() -> None:
    """Добавляет данные клиентов с банковской БД со слоя bank в БД транзакции на слой staging."""

    # Выборка данных из таблицы bank.clients с конкатенацией full_name
    df_bank_clients = pd.read_sql_query(
        """
        SELECT 
            client_id,
            CASE 
                WHEN client_middle_name IS NULL THEN client_surname || ' ' || client_name
                ELSE client_surname || ' ' || client_name || ' ' || client_middle_name
            END AS full_name,
            birth_date,
            email,
            phone,
            geolocation_id
        FROM bank.clients
        """,
        con=engine_bank,
        index_col=None
    )

    # Выборка id из таблицы staging.clients
    df_staging_ids = pd.read_sql_query(
        """
        SELECT client_id
        FROM staging.clients
        """,
        con=engine_transactions
    )

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['client_id'].to_numpy()
        max_staging_id = id_rows_staging.max()

        # Фильтруем записи, где client_id больше максимального в staging
        df_new_clients = df_bank_clients[df_bank_clients['client_id'] > max_staging_id].drop_duplicates()

        if not df_new_clients.empty:
            df_bank_clients['created_at'] = datetime.now()
            df_new_clients.to_sql(
                name='clients',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
            logger.info(f"Добавлено {len(df_new_clients)} новых записей в таблицу staging.clients")
        else:
            logger.info("Нет новых записей для добавления в таблицу clients")
    else:
        # Если staging.clients пуста, добавляем все записи
        logger.info("Таблица staging.clients пуста, добавляем все записи")
        df_bank_clients['created_at'] = datetime.now()
        df_bank_clients.to_sql(
            name='clients',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi'
        )
        logger.info(f"Добавлено {len(df_bank_clients)} записей в таблицу staging.clients")


def add_accounts() -> None:
    """Добавляет данные аккаунтов с банковской БД со слоя bank в БД транзакции на слой staging."""

    # Выборка данных из таблицы bank.accounts, которых нет в таблице staging.accounts, заодно убираем дубли
    df_bank_accounts = pd.read_sql_query(f"""
        SELECT *
        FROM bank.accounts
    """, con=engine_bank,index_col=None)
    
    # Выборка id из таблицы staging.accounts
    df_staging_ids = pd.read_sql_query(f"""
        SELECT account_id
        FROM staging.accounts
    """, con=engine_transactions)

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['account_id'].to_numpy()
        max_staging_id = np.max(id_rows_staging)

        # Фильтруем записи, где client_id больше максимального в staging
        df_bank_accounts = df_bank_accounts[df_bank_accounts['account_id'] > max_staging_id].drop_duplicates()

        if not df_bank_accounts.empty:
            df_bank_accounts['created_at'] = datetime.now()
            df_bank_accounts.to_sql(
                name='accounts',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
            logger.info(f"Добавлено {len(df_bank_accounts)} новых записей в таблицу staging.accounts")
        else:
            logger.info("Нет новых записей для добавления в таблицу accounts")
    else:
        # Если staging.accounts пуста, добавляем все записи
        logger.info("Таблица staging.accounts пуста, добавляем все записи")
        df_bank_accounts['created_at'] = datetime.now()
        df_bank_accounts.to_sql(
            name='accounts',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi'
        )
        logger.info(f"Добавлено {len(df_bank_accounts)} записей в таблицу staging.accounts")


def add_cards() -> None:
    """Добавляет данные карт с банковской БД со слоя bank в БД транзакции на слой staging."""

    # Выборка данных из таблицы bank.clients
    df_bank_cards = pd.read_sql_query(f"""
        SELECT *
        FROM bank.cards
    """, con=engine_bank,index_col=None)
    
    # Выборка id из таблицы staging.cards
    df_staging_ids = pd.read_sql_query(f"""
        SELECT card_id
        FROM staging.cards
    """, con=engine_transactions)

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['card_id'].to_numpy()
        max_staging_id = np.max(id_rows_staging)

        # Фильтруем записи, где client_id больше максимального в staging
        df_bank_cards = df_bank_cards[df_bank_cards['card_id'] > max_staging_id].drop_duplicates()

        if not df_bank_cards.empty:
            df_bank_cards['created_at'] = datetime.now()
            df_bank_cards.to_sql(
                name='cards',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
            logger.info(f"Добавлено {len(df_bank_cards)} новых записей в таблицу staging.cards")
        else:
            logger.info("Нет новых записей для добавления в таблицу cards")
    else:
        # Если staging.cards пуста, добавляем все записи
        logger.info("Таблица staging.cards пуста, добавляем все записи")
        df_bank_cards['created_at'] = datetime.now()
        df_bank_cards.to_sql(
            name='cards',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi'
        )
        logger.info(f"Добавлено {len(df_bank_cards)} записей в таблицу staging.cards")


def add_regions() -> None:
    """Добавляет данные регионов с банковской БД со слоя bank в БД транзакции на слой staging."""

    # Выборка данных из таблицы bank.clients
    df_bank_regions = pd.read_sql_query(f"""
        SELECT *
        FROM bank.regions
    """, con=engine_bank, index_col=None)

    # Выборка id из таблицы staging.regions
    df_staging_ids = pd.read_sql_query(f"""
        SELECT region_id
        FROM staging.regions
    """, con=engine_transactions)

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['region_id'].to_numpy()
        max_staging_id = np.max(id_rows_staging)

        # Фильтруем записи, где region_id больше максимального в staging
        df_bank_regions = df_bank_regions[df_bank_regions['region_id'] > max_staging_id].drop_duplicates()

        if not df_bank_regions.empty:
            df_bank_regions['created_at'] = datetime.now()
            df_bank_regions.to_sql(
                name='regions',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
            logger.info(f"Добавлено {len(df_bank_regions)} новых записей в таблицу staging.regions")
        else:
            logger.info("Нет новых записей для добавления в таблицу regions")
    else:
        # Если staging.regions пуста, добавляем все записи
        logger.info("Таблица staging.regions пуста, добавляем все записи")
        df_bank_regions['created_at'] = datetime.now()
        df_bank_regions.to_sql(
            name='regions',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi'
        )
        logger.info(f"Добавлено {len(df_bank_regions)} записей в таблицу staging.regions")


def add_transaction_types() -> None:
    """Добавляет типы транзакций с банковской БД со слоя bank в БД транзакции на слой staging."""

    # Выборка данных из таблицы bank.transaction_types
    df_bank_transaction_types = pd.read_sql_query(f"""
        SELECT *
        FROM bank.transaction_types
    """, con=engine_bank, index_col=None)

    # Выборка id из таблицы staging.transaction_types
    df_staging_ids = pd.read_sql_query(f"""
        SELECT id
        FROM staging.transaction_types
    """, con=engine_transactions)

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['id'].to_numpy()
        max_staging_id = np.max(id_rows_staging)

        # Фильтруем записи, где client_id больше максимального в staging
        df_bank_transaction_types = df_bank_transaction_types[df_bank_transaction_types['id'] > max_staging_id].drop_duplicates()

        if not df_bank_transaction_types.empty:
            df_bank_transaction_types['created_at'] = datetime.now()
            df_bank_transaction_types.to_sql(
                name='transaction_types',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
            logger.info(f"Добавлено {len(df_bank_transaction_types)} новых записей в таблицу staging.transaction_types")
        else:
            logger.info("Нет новых записей для добавления в таблицу transaction_types")
    else:
        # Если staging.transaction_types пуста, добавляем все записи
        logger.info("Таблица staging.transaction_types пуста, добавляем все записи")
        df_bank_transaction_types['created_at'] = datetime.now()
        df_bank_transaction_types.to_sql(
            name='transaction_types',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi'
        )
        logger.info(f"Добавлено {len(df_bank_transaction_types)} записей в таблицу staging.transaction_types")


def add_transactions() -> None:
    """Добавляет транзакции с банковской БД со слоя bank в БД транзакции на слой staging."""

    # Выборка данных из таблицы bank.transactions
    df_bank_transactions = pd.read_sql_query(f"""
        SELECT *
        FROM bank.transactions
    """, con=engine_bank,index_col=None)
    
    # Выборка id из таблицы staging.transactions
    df_staging_ids = pd.read_sql_query(f"""
        SELECT id
        FROM staging.transactions
    """, con=engine_transactions)

    if not df_staging_ids.empty:
        id_rows_staging = df_staging_ids['id'].to_numpy()
        max_staging_id = np.max(id_rows_staging)

        # Фильтруем записи, где client_id больше максимального в staging
        df_bank_transactions = df_bank_transactions[df_bank_transactions['id'] > max_staging_id].drop_duplicates()

        if not df_bank_transactions.empty:
            df_bank_transactions['created_at'] = datetime.now()
            df_bank_transactions.to_sql(
                name='transactions',
                schema='staging',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi',
                chunksize=10000
            )
            logger.info(f"Добавлено {len(df_bank_transactions)} новых записей в таблицу staging.transactions")
        else:
            logger.info("Нет новых записей для добавления в таблицу transactions")
    else:
        # Если staging.transaction пуста, добавляем все записи
        logger.info("Таблица staging.transactions пуста, добавляем все записи")
        df_bank_transactions['created_at'] = datetime.now()
        df_bank_transactions.to_sql(
            name='transactions',
            schema='staging',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi',
            chunksize=10000
        )
        logger.info(f"Добавлено {len(df_bank_transactions)} записей в таблицу staging.transactions")


if __name__ == '__main__':
    # Запись логов в файл
    file_handler = RotatingFileHandler(os.path.join('logs', 'loading_bank_to_staging.log'),
                                       maxBytes=2*1024*1024,
                                       backupCount=1)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    # Вывод логов в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # Использование переменных окружения
    load_dotenv()

    db_name_bank = os.getenv("DB_NAME_BANK")
    db_host_bank = os.getenv("DB_HOST_BANK")
    db_user_bank = os.getenv("DB_USER_BANK")
    db_pass_bank = os.getenv("DB_PASS_BANK")
    db_port_bank = os.getenv("DB_PORT_BANK", "5432")  # По умолчанию порт 5432

    db_name_transactions = os.getenv("DB_NAME_TRANSACTIONS")
    db_host_transactions = os.getenv("DB_HOST_TRANSACTIONS")
    db_user_transactions = os.getenv("DB_USER_TRANSACTIONS")
    db_pass_transactions = os.getenv("DB_PASS_TRANSACTIONS")
    db_port_transactions = os.getenv("DB_PORT_TRANSACTIONS", "5433")  # По умолчанию порт 5433

    # Использование парсера
    parser = argparse.ArgumentParser()
    parser.add_argument('loading',
                        nargs='?',
                        help='Загрузка отдельных файлов '
                             'countries - загрузка данных о странах'
                             'cities - загрузка данных о городах.'
                             'clients - загрузка клиентов.'
                             'accounts - загрузка аккаунтов.'
                             'cards - загрузка данных о картах клиентов.'
                             'regions - загрузка регионов.'
                             'transaction_types - загрузка типов транзакций.'
                             'transactions - загрузка транзакций.'
                             'all - загрузка всех данных.',
                        default='all')

    args = parser.parse_args()

    # Подключение к базе данных
    try:
        url_bank = URL.create(
            "postgresql+psycopg2",
            username=db_user_bank, 
            password=db_pass_bank,
            host=db_host_bank,
            port=db_port_bank,
            database=db_name_bank 
        )
        engine_bank = create_engine(url_bank)
        
        with engine_bank.connect() as conn_bank:
            with conn_bank.begin():
        
                logger.info(f"Успешно подключились к базе данных {db_name_bank} на порту {db_port_bank}")

    except Exception as e:
        logger.exception(f"Ошибка подключения к базе данных {db_name_bank}: {e}")

    try:
        url_transactions = URL.create(
            "postgresql+psycopg2",
            username=db_user_transactions, 
            password=db_pass_transactions,
            host=db_host_transactions,
            port=db_port_transactions,
            database=db_name_transactions 
        )
        engine_transactions = create_engine(url_transactions)
        
        with engine_transactions.connect() as conn_transactions:
            with conn_transactions.begin():
        
        
         
                logger.info(f"Успешно подключились к базе данных {db_name_transactions} на порту {db_port_transactions}")

    except Exception as e:
        logger.exception(f"Ошибка подключения к базе данных {db_name_transactions}: {e}")

    try:
        if args.loading == 'countries':
            add_countries()
            logger.info("Данные о странах - загружены.")
        elif args.loading == 'cities':
            add_cities()
            logger.info("Данные о городах - загружены.")
        elif args.loading == 'clients':
            add_clients()
            logger.info("Данные о клиентах - загружены.")
        elif args.loading == 'accounts':
            add_accounts()
            logger.info("Данные об аккаунтах - загружены.")
        elif args.loading == 'transaction_types':
            add_transaction_types()
            logger.info("Данные о типах транзакций- загружены.")
        elif args.loading == 'regions':
            add_regions()
            logger.info("Данные о регионах - загружены.")
        elif args.loading == 'cards':
            add_cards()
            logger.info("Данные о картах- загружены.")
        elif args.loading == 'transactions':
            add_transactions()
            logger.info("Данные о транзакциях- загружены.")

        elif args.loading == 'all':
            add_countries()
            logger.info("Данные о странах - загружены.")
            add_cities()
            logger.info("Данные о городах - загружены.")
            add_clients()
            logger.info("Данные о клиентах - загружены.")
            add_accounts()
            logger.info("Данные об аккаунтах - загружены.")
            add_cards()
            logger.info("Данные об аккаунтах - загружены.")
            add_regions()
            logger.info("Данные о регионах- загружены.")
            add_transaction_types()
            logger.info("Данные о типах транзакций- загружены.")
            conn_bank.commit()
            add_transactions()
            logger.info("Все данные - загружены.")

        else:
            logger.info("""Вы ввели неправильный аргумент:
                             countries - загрузка данных о странах
                             cities - загрузка данных о городах.
                             clients - загрузка клиентов.
                             accounts - загрузка аккаунтов.
                             cards - загрузка данных о картах клиентов.
                             transaction_types - загрузка типов транзакций.
                             transactions - загрузка транзакций.
                             all - загрузка всех данных.""")

    except Exception as e:
        logger.exception("Произошла ошибка во время выполнения файла: %s", e)

   