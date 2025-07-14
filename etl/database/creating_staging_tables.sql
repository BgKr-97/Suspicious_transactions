CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE staging.countries (
	country_id int4 NOT NULL,
	country_iso_code varchar NULL,
	country_name text not NULL,
	blacklist bool not NULL,
	created_at timestamp NOT NULL,
	CONSTRAINT countries_pkey PRIMARY KEY (country_id)
);


CREATE TABLE staging.cities (
	city_id int8 NOT NULL,
	city_name text not NULL,
	latitude numeric not NULL,
	longitude numeric not NULL,
	country_id int4 not NULL,
	region_id int4 NULL,
	created_at timestamp not NULL,
	foreign KEY (country_id) references staging.countries(country_id),
	CONSTRAINT cities_pkey PRIMARY KEY (city_id)
);


CREATE TABLE staging.clients (
	client_id int8 NOT NULL,
	full_name varchar not NULL,
	birth_date date not NULL,
	email varchar NULL,
	phone varchar not NULL,
	geolocation_id int8 not NULL,
	created_at timestamp not NULL,
	foreign KEY (geolocation_id) references staging.cities(city_id),
	CONSTRAINT clients_pkey PRIMARY KEY (client_id)
);


CREATE TABLE staging.accounts (
	account_id int8 NOT NULL,
	account_number text not NULL,
	client_id int8 not NULL,
	created_at timestamp not NULL,
	foreign KEY (client_id) references staging.clients(client_id),
	CONSTRAINT accounts_pkey PRIMARY KEY (account_id)
);


CREATE TABLE staging.cards (
	card_id int8 NOT NULL,
	card_number int8 NOT NULL,
	account_id int8 NOT NULL,
	client_id int8 NOT NULL,
	created_at timestamp NOT NULL,
	foreign KEY (client_id) references staging.clients(client_id),
	foreign KEY (account_id) references staging.accounts(account_id),
	CONSTRAINT cards_pkey PRIMARY KEY (card_id)
);

CREATE TABLE staging.regions (
	region_id int4 NOT NULL,
	region_name varchar(100) NOT NULL,
	country_id int4 NULL,
	region_code varchar(50) NOT NULL,
	created_at timestamp NOT NULL,
	foreign KEY (country_id) references staging.countries(country_id),
	CONSTRAINT regions_pkey PRIMARY KEY (region_id)
);

CREATE TABLE staging.transaction_types (
	id int4 NOT NULL,
	t_type text NOT NULL,
	is_receipt bool NOT NULL,
	created_at timestamp NOT NULL,
	CONSTRAINT transaction_types_pkey PRIMARY KEY (id)
);


CREATE TABLE staging.transactions (
	id int4 NOT NULL,
	date_time timestamp NOT NULL,
	client_id int4 NOT NULL,
	account_id int8 not NULL,
	card_id int8 not NULL,
	transaction_type_id int4 NOT NULL,
	amount numeric(15, 2) NOT NULL,
	source_city_id int4 NOT NULL,
	source_region_id int4 NULL,
	destination_city_id int4 NULL,
	destination_region_id int4 NULL,
	created_at timestamp NOT NULL,
	foreign KEY (client_id) references staging.clients(client_id),
	foreign KEY (destination_city_id) references staging.cities(city_id),
	foreign KEY (destination_region_id) references staging.regions(region_id),
	foreign KEY (source_city_id) references staging.cities(city_id),
	foreign KEY (source_region_id) references staging.regions(region_id),
	foreign KEY (transaction_type_id) references staging.transaction_types(id),
	CONSTRAINT transactions_pkey PRIMARY KEY (id)
);