from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from etl.config.logger_config import setup_logger
import pandas as pd
import os

logger = setup_logger('config/etl.log')


class DBExtractor:
    def __init__(self, dbname, user, password, host, port):
        try:
            conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
            self.engine = create_engine(conn_str)

            # Проверка соединения (выполняется простой запрос)
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))

            logger.info(f"Успешное подключение к базе данных '{dbname}' на {host}:{port} как пользователь '{user}'")
        except SQLAlchemyError as e:
            logger.error("Не удалось подключиться к базе данных: %s", str(e))
            raise

    @staticmethod
    def _load_sql(path: str) -> str:
        """Читает SQL из файла."""
        full_path = os.path.join(os.path.dirname(__file__), path)
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()
                if not content.strip():
                    raise ValueError(f"Файл {full_path} пуст.")
                return content
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {full_path}: {str(e)}")
            raise

    def _fetch_df(self, path: str) -> pd.DataFrame:
        """Выполняет SQL и возвращает результат в DataFrame."""
        sql = self._load_sql(path)
        try:
            df = pd.read_sql(sql, self.engine)
            logger.info(f"Успешно извлечено {len(df)} строк из таблиц схемы 'staging'")
            return df
        except Exception as e:
            logger.error("Ошибка при извлечении данных: %s", str(e))
            raise

    def fetch_merged_transactions(self) -> pd.DataFrame:
        return self._fetch_df('sql/fetch_merged_transactions.sql')

    def fetch_merged_info(self) -> pd.DataFrame:
        return self._fetch_df('sql/fetch_merged_info.sql')

    def create_datamart(self):
        """Функция выполняет DDL-скрипт по созданию таблицы витрины"""
        sql = self._load_sql('sql/sql_data_mart.sql')
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info(f"DDL-скрипт успешно выполнен: 'sql_data_mart.sql'")
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при выполнении DDL-скрипта 'sql_data_mart.sql': %s", str(e))
            raise

    def load_datamart(self, df, schema, table):
        """Функция загружает финальные данные в витрину"""
        if df.empty:
            logger.info(f"DataFrame пуст — загрузка в витрину пропущена.")
            return

        # Ожидаемый порядок и набор столбцов
        expected_columns = [
            'transaction_id', 'client_id', 'client_name', 'client_age', 'account_id',
            'date_time', 'amount', 't_type', 'is_receipt',
            'sender_country', 'sender_city', 'sender_latitude', 'sender_longitude',
            'recipient_country', 'recipient_city', 'recipient_latitude', 'recipient_longitude',
            'is_suspicious', 'risk_score', 'reason_flags', 'risk_status'
        ]

        # Проверка структуры
        if set(df.columns) != set(expected_columns):
            logger.error("Столбцы в DataFrame не соответствуют ожидаемой структуре таблицы витрины")

        df = df[expected_columns]

        try:
            df.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists='append',
                index=False,
                method='multi'
            )

            logger.info(f"Загружено {len(df)} строк в таблицу {schema}.{table}")
        except Exception as e:
            logger.error("Ошибка при загрузке данных в витрину: %s", str(e))
            raise