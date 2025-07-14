import pandas as pd
from sqlalchemy import create_engine
from faker import Faker 
fake = Faker('ru_RU')
import random 
from datetime import datetime, timedelta
from sqlalchemy import types

# Директория для загрузки данных 
 
 

path_df_clientss = "C:/Users/User/Desktop/League/Synthetic data/clients.csv"
 

first_name = []

middle_name = []

last_name = []


# Словарь для транслитерации имен и фамилий

dict_transliterate = {'а':'a', 'б':'b', 'в':'v', 'г':'g',
         'д':'d', 'е':'e', 'ж':'ž', 'з':'z',
         'и':'i', 'к':'k', 'л':'l', 'м':'m',
         'н':'n', 'о':'o', 'п':'p', 'р':'r',
         'с':'s', 'т':'t', 'у':'u', 'ф':'f', 
         'х':'h', 'ц':'c', 'ш':'sh','ь':'',
         'ч':'ch','ы':'y','й':'y','я':'ya',
         'э':'eh','ю':'yu','-':'-', 'щ':'sch',
         '.':'.', 'ё':'yo'}

 
# Функция для транслитерации имен и фамилий

def transliterate(string):
    return ''.join([dict_transliterate[char] for char in string])


 
  

for t in range(500):
       
       fio = fake.name()
     
       full_name = fio.split(' ')
       
       # Удалим обращение  (Господин, тов-щ) из имени
       
       if len(full_name)>3:
           
           full_name = full_name[1:]
           
       middle_name_pos = [i for i,v in enumerate(full_name) if v.endswith(('вна', '.вич'))]
       
       if len(middle_name_pos) > 1:
           
           
       
           first_name.append(full_name[0])
           
           last_name.append(full_name[2])
           
           middle_name.append(full_name[1])
           
           
       else:
          
          first_name.append(full_name[0])
          
          last_name.append(full_name[1])
          
          middle_name.append(full_name[2])

df_clients = pd.DataFrame(
    [
        {   "client_id": r+1,
            "client_name": first_name[r],
            "client_surname": last_name[r],
            "client_middlename": middle_name[r],
            "birth_date": fake.date_of_birth(minimum_age=16,maximum_age=100),
             "email": str.join("",(transliterate(first_name[r].lower()),'_',
                               transliterate(last_name[r].lower()),
                               str(random.randint(1924, 2008)),
                               random.choice(["@mail.ru","@gmail.com",
                                              "@ya.ru", "@potato.by",
                                              "@yandex.ru"]))),
             "phone": fake.phone_number(),
             "geolocation_id": random.randint(1, 500)
        }
        for r in range(500) 
    ]
)

# Генерируем данные для таблицы счетов
 

# Создадим 700 id счетов

acc_ids = list(range(1,701))

# Создадим 700 id клиентов

client_ids = list(range(1,501)) + [random.randint(1, 500) for x in list(range(1,201))]

# Перемешаем их

random.shuffle(client_ids)

# Создадим номера счетов

accounts = [fake.iban() for x in range(700)]       

# Создадим датафрейм счетов

df_accounts = pd.DataFrame(
     
        {   "account_id": acc_ids,
            "account_number": accounts,
            "client_id": client_ids
             
        }
        
)

# Генерируем данные для таблицы банковских карт

# Создадим 1000 id карт

card_ids = list(range(1,1001))

card_nums = [fake.credit_card_number() for x in range(1000)]

# Создадим 1000 id счетов и клиентов связанных с картами

card_accounts = accounts.copy()

card_client_ids = client_ids.copy()

for n in range(300):
    
    i = random.randint(0, 699) 
    
    card_client_ids.append(client_ids[i])
    
    card_accounts.append(accounts[i])
    
    
    
# Создадим датафрейм карт

df_cards = pd.DataFrame(
     
        {   "card_id": card_ids,
            "card_number": card_nums,
            "account_id": card_accounts,
            "client_id": card_client_ids
             
        }
        
)
 

# Генерируем данные для таблицы типов транзакций

tr_cat_1 = [0] * 30 # Список типов транзакций категории расходов

tr_cat_2 = [1] * 16 # Список типов транзакций категории пополнений

tr_categories = tr_cat_1 + tr_cat_2 # Общий список типов транзакций
 

tr_types_ids = list(range(1,47))

tr_types = ["Покупки товаров и услуг",
"Супермаркеты и продукты",
"Одежда и обувь",
"Электроника и техника",
"Аптеки и здоровье",
"Кафе, рестораны, фастфуд",
"Развлечения", 
"Онлайн-подписки и сервисы",
"Дом и ремонт",
"Спорт и фитнес",
"Авто и транспорт",
"Туризм и путешествия",
"Образование",
"Благотворительность и пожертвования",
"Платежи и переводы",
"Переводы физическим лицам", 
"Переводы на другие свои счета",
"Погашение кредитов и займов",
"Оплата налогов, штрафов, госпошлин",
"Коммунальные услуги (ЖКХ, интернет, телефон)",
"Оплата услуг (страхование, обслуживание, сервисы)",
"Снятие наличных",
"Снятие в банкомате своего банка",
"Снятие в банкомате другого банка",
"Снятие в отделении банка",
"Прочее",
"Возврат средств (отмена покупки)",
"Комиссии банка (за обслуживание, переводы, снятие)",
"Прочие расходы (не классифицированные)",
"Неизвестно",
"Пополнение счета",
"Пополнение через банкомат",
"Пополнение через кассу банка",
"Пополнение с другой карты (P2P)",
"Пополнение через электронные кошельки",
"Переводы и поступления",
"Зарплата",
"Социальные выплаты (пенсии, пособия)",
"Переводы от других лиц (P2P)",
"Доходы от инвестиций (проценты, дивиденды)",
"Возврат средств (отмена покупки, возврат товара)",
"Кэшбэк и бонусы банка",
"Прочее",
"Прочие поступления (подарки, случайные зачисления)",
"Компенсации, страховые выплаты",
"Неизвестно "]



df_tr_types = pd.DataFrame(
     
        {  "id": tr_types_ids,
            "t_type": tr_types,
            "is_receipt": tr_categories  
        }
)

# Сгенерируем 5000 транзакций
   
id = list(range(1,5001))
  
 
# Выберем диапазон дат
 
start_date, end_date = datetime(2025, 5, 1, hour=0, minute=0, second=0), datetime(2025, 6, 1, hour=0, minute=0, second=0)
 
# Выберем количество дат - m для генерации 
 
m = 0
n = 4999

# Начало списка
transaction_datetimes = [start_date]
  
# Сгенерируем список дат между начальной и конечной
while m != n:
    rand_date = start_date + timedelta(minutes=random.randint(1, 44630))
    transaction_datetimes.append(rand_date)
    m = m + 1
    rand_date = start_date
 
    
# Сгенерируем список id клиентов
 
client_id = [random.randint(1, 500) for x in range(5000)]


# Сгенерируем списки типов транзакций и принадлежности типа к пополнению счета

 
t_type = [tr_types_ids[random.randint(0, 44)] for x in range(5000)]

is_receipt = [tr_categories[t_type[x]] for x in range(5000)]

# Сгенерируем список сумм транзакций

amount = [random.randint(500, 120000) for x in range(5000)]   

# Сгенерируем списки геолокаций источника и получателя транзакций


df_transactions = pd.DataFrame(
     
        {   "id": id,
            "date_time": transaction_datetimes,
            "client_id":client_id,
            "transaction_type_id": t_type,
            "amount": amount
        }
)


# Загрузим базу данных по населенным пунктам  мира 

df_cities_long = pd.read_csv('allCountries.txt',delimiter='\t')


df_cities = df_cities_long.iloc[:,[1,4,5,8]].reset_index()

 

df_cities.columns = ['city_id','city_name','latitude','longitude','country_id']
 
 # Загрузим базу данных по странам мира 
 
 
df_countries = pd.read_csv('countryInfo.txt',delimiter='\t')


df_countries = df_countries.iloc[:,[0,4]]

df_countries = df_countries.reset_index()


df_countries.columns = ['country_id','country_iso_code','country_name',]

iso_list = list(df_countries['country_iso_code'])

black_list_iso = ['IR','MM','BG','BF','CM','HR','KE','CD','HT','JM','ML','MZ',
                  'NA','NG','PH','SN','ZA','SS','SY','TZ','TR','VN','YE']


black_list = ['1' if iso_list[x] in black_list_iso else '0' for x in range(len(iso_list))]

df_countries['blacklist'] = black_list
 

df_cities['country_id'] = df_cities['country_id'].map(df_countries.set_index('country_iso_code')['country_id'])

# Получим списки долготы и широты для городов в России (id = 191) и за рубежом
  
rus_cities = df_cities[df_cities['country_id'] == 191]

int_cities = df_cities[df_cities['country_id'] != 191]


# Выберем рандомно 500 городов в России как локаций для наших клиентов 
 # и 50 иностранных городов для таблицы транзакций


rus_cities_500 = rus_cities.sample(500)

int_cities_50 = int_cities.sample(50)

# Назначим геолокацию клиентам
 
df_clients['geolocation_id'] = list(rus_cities_500['city_id'])


# Назначим долготу и широту источника транзакций согласно геолокации клиента

 

df_transactions_source = df_clients[['client_id','geolocation_id']].merge(rus_cities_500[['city_id']], how='left', left_on='geolocation_id',right_on='city_id')

 


# Уберем лишние колонки

df_transactions_source = df_transactions_source[['client_id','city_id']]

  
df_transactions_dest = df_transactions_source.copy()

 

# Перенесем эти координаты в таблицу с транзакциями как источники и назначение

 
df_transactions = df_transactions.merge(df_transactions_source, how='left', left_on='client_id',right_on='client_id')

df_transactions = df_transactions.merge(df_transactions_dest, how='left', left_on='client_id',right_on='client_id')


# Заменим 10 % координат источника и назначения транзакций на рандомные
# из  полного списка выбранных городов  

df_replacement_cities = pd.concat([rus_cities_500,int_cities_50])

# Сгенерируем сначала 500  id транзакций в которых поменяем источник, а потом
# столько же для назначения

source_replace = [random.randint(0, 4999) for x in range(550)]

dest_replace = [random.randint(0, 4999) for x in range(550)]

# Заменим координаты в выбранных транзакциях 

l = len(df_replacement_cities)

for n in range(l):

    df_transactions['city_id_x'].iloc[source_replace[n]] =  df_replacement_cities['city_id'].iloc[n]
    
    df_transactions['city_id_y'].iloc[dest_replace[n]] =  df_replacement_cities['city_id'].iloc[n]
    
  

 
df_transactions.columns = ['id','date_time','client_id','transaction_type_id',
                           'amount','source_city_id','destination_city_id']
 
# Заполним целевые таблицы в слое внешних источников хранилища данных


# Сначала заполним таблицы стран и городов, а потом Карт, Клиентов,
# Типов транзакций и Транзакций

 
 
# Установим подключение к базе данных
 
engine = create_engine("postgresql+psycopg2://***:********.***.**.*:****/******")
try:
     
     # Попытаемся подключиться к базе данных
 
     # Вставим данные в таблицу countries 
    
    with engine.connect() as conn:
        with conn.begin():
            # В случае успеха будет выведено сообщение
            
            
            
            df_countries.to_sql(name='countries',schema='bank',con=engine,index=False,
                           if_exists='append'
                           # , 
                           # dtype={'country_id': types.Integer(), 
                           #   'country_iso_code':  types.Text(),
                           #   'country_name': types.Text(),
                           #   'blacklist': types.Text()})
                           )
            print('Connected to database')
              
except:
    
    # В случае сбоя подключения будет выведено сообщение 
    print('Can`t establish connection to database')
    
    
    
# Вставим данные в таблицу Cities 
 

 
 
try:
         
         # Попытаемся подключиться к базе данных 
         
    with engine.connect() as conn:
        with conn.begin():
         # В случае успеха будет выведено сообщение
                
            
                
            df_replacement_cities.to_sql(name='cities',schema='bank',con=engine,
                if_exists='append', method=None,index=False)
                 
            print('Connected to database')
except:
    
    # В случае сбоя подключения будет выведено сообщение 
    print('Can`t establish connection to database') 
        
  
  # Остальные датафреймы
    with engine.connect() as conn:
        with conn.begin(): 
            conn.execute("CREATE TABLE bank.temp_table")
  
    df_dict = {'clients':df_clients,'accounts':df_accounts,'cards':df_cards,'transaction_types':df_tr_types,'transactions':df_transactions}   
  

    for name in df_dict:
        
        try:
             
            # Попытаемся подключиться к базе данных
            
            with engine.connect() as conn:
                with conn.begin():
                    # В случае успеха будет выведено сообщение
                    
                    
                    
                    df_dict[name].to_sql(name=name,schema='bank',con=engine,index=False,
                                  if_exists='append')
                    print('Connected to database')
        except:
        
            # В случае сбоя подключения будет выведено сообщение 
            print('Can`t establish connection to database')
                 
                  
try:
     
     # Попытаемся подключиться к базе данных
 
     # Вставим данные в таблицу countries 
    
    with engine.connect() as conn:
        with conn.begin():
            # В случае успеха будет выведено сообщение
            
            
            
            df_clients.to_sql(name='clients',schema='bank',con=engine,index=False,
                           if_exists='append')
                           # , 
                           # dtype={'country_id': types.Integer(), 
                           #   'country_iso_code':  types.Text(),
                           #   'country_name': types.Text(),
                           #   'blacklist': types.Text()})
                           
            print('Connected to database')
              
except:
    
    # В случае сбоя подключения будет выведено сообщение 
    print('Can`t establish connection to database')                
       