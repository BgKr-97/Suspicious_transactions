CREATE SCHEMA IF NOT EXISTS bank;
create table bank.countries (
country_id int primary key,
country_iso_code char(2),
country_name varchar(100),
blacklist boolean
);
 

create table bank.cities (
city_id int primary key,
city_name varchar(100),
latitude decimal(9,6),
longitude decimal(9,6),
country_id int references bank.countries(country_id)
);



create table bank.clients (
client_id int primary key,
client_name text,
client_surname text,
client_middle_name text,
birth_date date,
email text,
phone text,
geolocation_id int references bank.cities(city_id)
);


create table bank.accounts (
account_id int primary key,
account_number varchar(50),
client_id int references bank.clients(client_id)
);


create table bank.cards (
card_id int primary key,
card_number varchar(20),
account_id int references bank.accounts(account_id),
client_id int references bank.clients(client_id)
);


create table bank.transaction_types (
id int primary key,
t_type varchar(50),
is_receipt boolean
);
 

create table bank.transactions(
id int primary key,
date_time timestamp,
client_id int references bank.clients(client_id),
transaction_type_id int references bank.transaction_types(id),
amount decimal(15,2),
source_city_id int references bank.clients(client_id),
destination_city_id int references bank.clients(client_id)
);
 