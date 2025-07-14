import argparse
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_tables(table_name, table_prime_key) -> None:
    """Добавляет данные со слоя staging в БД транзакции на слой core."""
    df_staging = pd.read_sql_query(
        f"SELECT * FROM staging.{table_name}",
        con=engine_transactions,
        index_col=None
    )
    df_core_ids = pd.read_sql_query(
        f"SELECT {table_prime_key} FROM core.{table_name}",
        con=engine_transactions
    )

    if not df_core_ids.empty:
        id_rows_staging = df_core_ids[table_prime_key].to_numpy()
        max_staging_id = id_rows_staging.max()
        df_staging = df_staging[df_staging[table_prime_key] > max_staging_id].drop_duplicates()

        if not df_staging.empty:
            df_staging['created_at'] = datetime.now()
            df_staging.to_sql(
                name=table_name,
                schema='core',
                con=engine_transactions,
                index=False,
                if_exists='append',
                method='multi'
            )
        else:
            logger.info(f"Нет новых записей для добавления в таблицу {table_name}")
    else:
        logger.info(f"Таблица core.{table_name} пуста, добавляем все записи")
        df_staging['created_at'] = datetime.now()
        df_staging.to_sql(
            name=table_name,
            schema='core',
            con=engine_transactions,
            index=False,
            if_exists='append',
            method='multi',
            chunksize=10000
        )
        logger.info(f"Добавлено {len(df_staging)} записей в таблицу core.{table_name}")


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
                             'transaction_types - загрузка типов транзакций.'
                             'regions - загрузка регионов.'
                             'transactions - загрузка транзакций.'
                             'all - загрузка всех данных.',
                        default='all')

    args = parser.parse_args()

    # Подключение к базе данных
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
                logger.info(
                    f"Успешно подключились к базе данных {db_name_transactions} на порту {db_port_transactions}")

    except Exception as e:
        logger.exception(f"Ошибка подключения к базе данных {db_name_transactions}: {e}")

    try:
        if args.loading == 'countries':
            add_tables(table_prime_key='country_id', table_name='countries')
            logger.info("Данные о странах - загружены.")
        elif args.loading == 'cities':
            add_tables(table_prime_key='city_id', table_name='cities')
            logger.info("Данные о городах - загружены.")
        elif args.loading == 'clients':
            add_tables(table_prime_key='client_id', table_name='clients')
            logger.info("Данные о клиентах - загружены.")
        elif args.loading == 'accounts':
            add_tables(table_prime_key='account_id', table_name='accounts')
            logger.info("Данные об аккаунтах - загружены.")
        elif args.loading == 'transaction_types':
            add_tables(table_prime_key='id', table_name='transaction_types')
            logger.info("Данные о типах транзакций- загружены.")
        elif args.loading == 'cards':
            add_tables(table_prime_key='card_id', table_name='cards')
            logger.info("Данные о картах- загружены.")
        elif args.loading == 'regions':
            add_tables(table_prime_key='region_id', table_name='regions')
            logger.info("Данные о регионах- загружены.")
        elif args.loading == 'transactions':
            add_tables(table_prime_key='id', table_name='transactions')
            logger.info("Данные о транзакциях- загружены.")

        elif args.loading == 'all':
            add_tables(table_prime_key='country_id', table_name='countries')
            logger.info("Данные о странах - загружены.")
            add_tables(table_prime_key='city_id', table_name='cities')
            logger.info("Данные о городах - загружены.")
            add_tables(table_prime_key='client_id', table_name='clients')
            logger.info("Данные о клиентах - загружены.")
            add_tables(table_prime_key='account_id', table_name='accounts')
            logger.info("Данные об аккаунтах - загружены.")
            add_tables(table_prime_key='card_id', table_name='cards')
            logger.info("Данные о картах- загружены.")
            add_tables(table_prime_key='region_id', table_name='regions')
            logger.info("Данные о регионах- загружены.")
            add_tables(table_prime_key='id', table_name='transaction_types')
            logger.info("Данные о типах транзакций- загружены.")
            add_tables(table_prime_key='id', table_name='transactions')
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
