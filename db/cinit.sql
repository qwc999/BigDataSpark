CREATE TABLE mock_data (
	id INT,
	customer_first_name varchar(50) NULL, -- покупатель
	customer_last_name varchar(50) NULL,
	customer_age integer NULL,
	customer_email varchar(50) NULL,
	customer_country varchar(50) NULL,
	customer_postal_code varchar(50) NULL,
	customer_pet_type varchar(50) NULL,
	customer_pet_name varchar(50) NULL,
	customer_pet_breed varchar(50) NULL,
	seller_first_name varchar(50) NULL, -- продавец
	seller_last_name varchar(50) NULL,
	seller_email varchar(50) NULL,
	seller_country varchar(50) NULL,
	seller_postal_code varchar(50) NULL,
	product_name varchar(50) NULL, -- товар
	product_category varchar(50) NULL,
	product_price real NULL,
	product_quantity integer NULL,
	sale_date date NULL, -- покупка
	sale_customer_id integer NULL,
	sale_seller_id integer NULL,
	sale_product_id integer NULL,
	sale_quantity integer NULL,
	sale_total_price real NULL,
	store_name varchar(50) NULL, -- магазин
	store_location varchar(50) NULL,
	store_city varchar(50) NULL,
	store_state varchar(50) NULL,
	store_country varchar(50) NULL,
	store_phone varchar(50) NULL,
	store_email varchar(50) NULL,
	pet_category varchar(50) NULL, -- вид животного
	product_weight real NULL, -- хар-ки продукта
	product_color varchar(50) NULL,
	product_size varchar(50) NULL,
	product_brand varchar(50) NULL,
	product_material varchar(50) NULL,
	product_description varchar(1024) NULL,
	product_rating real NULL,
	product_reviews integer NULL,
	product_release_date date NULL,
	product_expiry_date date NULL,
	supplier_name varchar(50) NULL, -- хар-ки поставщика
	supplier_contact varchar(50) NULL,
	supplier_email varchar(50) NULL,
	supplier_phone varchar(50) NULL,
	supplier_address varchar(50) NULL,
	supplier_city varchar(50) NULL,
	supplier_country varchar(50) NULL
);

COPY mock_data FROM '/data/MOCK_DATA (1).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (2).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (3).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (4).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (5).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (6).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (7).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (8).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (9).csv' DELIMITER ',' CSV HEADER;

COPY mock_data FROM '/data/MOCK_DATA (10).csv' DELIMITER ',' CSV HEADER;

