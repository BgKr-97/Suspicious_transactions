import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

fake = Faker()
Faker.seed(0)
np.random.seed(0)
random.seed(0)

# Конфигурация
num_clients = 100
transactions_per_client = (30, 100)  # диапазон числа транзакций на клиента
max_tx_per_hour = 3                 # максимум транзакций в один час
days_range = 15                     # активность в пределах последних 30 дней

# Шаг 1: Сгенерировать клиентов
clients = []
for client_id in range(1, num_clients + 1):
    clients.append({
        "client_id": client_id,
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70)
    })

# Шаг 2: Сгенерировать транзакции
transaction_data = []

for client in clients:
    total_tx = 0
    max_tx = random.randint(*transactions_per_client)

    # Дата начала активности
    start_date = datetime.now() - timedelta(days=days_range)

    for day_offset in range(days_range):
        if total_tx >= max_tx:
            break

        current_day = start_date + timedelta(days=day_offset)

        if random.random() < 0.7:  # 70% вероятность активности в этот день
            for hour in range(24):
                if total_tx >= max_tx:
                    break

                tx_in_hour = random.randint(0, max_tx_per_hour)
                tx_in_hour = min(tx_in_hour, max_tx - total_tx)

                hour_start = current_day.replace(hour=hour, minute=0, second=0, microsecond=0)
                for _ in range(tx_in_hour):
                    minutes = random.randint(0, 59)
                    seconds = random.randint(0, 59)
                    tx_time = hour_start + timedelta(minutes=minutes, seconds=seconds)

                    t_type = random.choice(["purchase", "withdrawal", "transfer", "payment", "Неизвестно"])
                    amount = round(random.uniform(100, 7000), 2)
                    long_src, lat_src = fake.longitude(), fake.latitude()
                    long_dst, lat_dst = fake.longitude(), fake.latitude()

                    blacklist = bool(random.choice([0, 1]))

                    transaction_data.append({
                        "client_id": client["client_id"],
                        "birth_date": client["birth_date"],
                        "date_time": tx_time,
                        "t_type": t_type,
                        "amount": amount,
                        "sender_longitude": float(long_src),
                        "sender_latitude": float(lat_src),
                        "blacklist": blacklist
                    })
                    total_tx += 1

# Сборка DataFrame и хронологическая сортировка
df = pd.DataFrame(transaction_data)
df.sort_values(by=['client_id', 'date_time'], inplace=True)
df.reset_index(drop=True, inplace=True)
df['id'] = df.index + 1

desired_order = [
    'id',
    'client_id',
    'birth_date',
    'date_time',
    'amount',
    't_type',
    "sender_longitude",
    "sender_latitude",
    "blacklist"
]
df = df[desired_order]
df.set_index('id', inplace=True)
# df.to_csv('raw_data')
df_main = pd.read_csv('raw_data').head(200)




print(df_main)











