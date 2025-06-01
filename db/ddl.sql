DROP TABLE IF EXISTS d_customer_pet_type CASCADE;
DROP TABLE IF EXISTS d_customer_pet_breed CASCADE;
DROP TABLE IF EXISTS d_pet_category CASCADE;
DROP TABLE IF EXISTS d_customer_country CASCADE;
DROP TABLE IF EXISTS d_customer_postal_code CASCADE;
DROP TABLE IF EXISTS d_customers CASCADE;

DROP TABLE IF EXISTS d_seller_country CASCADE;
DROP TABLE IF EXISTS d_seller_postal_code CASCADE;
DROP TABLE IF EXISTS d_sellers CASCADE;

DROP TABLE IF EXISTS d_store_city CASCADE;
DROP TABLE IF EXISTS d_store_state CASCADE;
DROP TABLE IF EXISTS d_store_country CASCADE;
DROP TABLE IF EXISTS d_stores CASCADE;

DROP TABLE IF EXISTS d_supplier_city CASCADE;
DROP TABLE IF EXISTS d_supplier_country CASCADE;
DROP TABLE IF EXISTS d_suppliers CASCADE;

DROP TABLE IF EXISTS d_product_name CASCADE;
DROP TABLE IF EXISTS d_product_category CASCADE;
DROP TABLE IF EXISTS d_product_color CASCADE;
DROP TABLE IF EXISTS d_product_size CASCADE;
DROP TABLE IF EXISTS d_product_brand CASCADE;
DROP TABLE IF EXISTS d_product_material CASCADE;
DROP TABLE IF EXISTS d_date CASCADE;
DROP TABLE IF EXISTS d_products CASCADE;

DROP TABLE IF EXISTS f_sales CASCADE;

--------------покупатель--------------------
CREATE TABLE d_customer_pet_type (
    customer_pet_type_id SERIAL PRIMARY KEY,
    pet_type_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_customer_pet_breed (
    customer_pet_breed_id SERIAL PRIMARY KEY,
    breed_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_customer_country (
    customer_country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_customer_postal_code (
    customer_postal_code_id SERIAL PRIMARY KEY,
    postal_code VARCHAR(50) UNIQUE
);

CREATE TABLE d_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_first_name VARCHAR(50),
    customer_last_name VARCHAR(50),
    customer_age INTEGER,
    customer_email VARCHAR(50),
    customer_country_id INT REFERENCES d_customer_country(customer_country_id),
    customer_postal_code_id INT REFERENCES d_customer_postal_code(customer_postal_code_id),
    customer_pet_type_id INT REFERENCES d_customer_pet_type(customer_pet_type_id),
    customer_pet_breed_id INT REFERENCES d_customer_pet_breed(customer_pet_breed_id),
    pet_category_id INT REFERENCES d_pet_category(pet_category_id)
);

--------------продавец--------------------

CREATE TABLE d_seller_country (
    seller_country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_seller_postal_code (
    seller_postal_code_id SERIAL PRIMARY KEY,
    postal_code VARCHAR(50) UNIQUE
);

CREATE TABLE d_sellers (
    seller_id SERIAL PRIMARY KEY,
    seller_first_name VARCHAR(50),
    seller_last_name VARCHAR(50),
    seller_email VARCHAR(50),
    seller_country_id INT REFERENCES d_seller_country(seller_country_id),
    seller_postal_code_id INT REFERENCES d_seller_postal_code(seller_postal_code_id)
);

--------------магазин--------------------

CREATE TABLE d_store_city (
    store_city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_store_state (
    store_state_id SERIAL PRIMARY KEY,
    state VARCHAR(50) UNIQUE
);

CREATE TABLE d_store_country (
    store_country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(50),
	store_location varchar(50) NULL,
    store_city_id INT REFERENCES d_store_city(store_city_id),
    store_state_id INT REFERENCES d_store_state(store_state_id),
    store_country_id INT REFERENCES d_store_country(store_country_id),
	store_phone varchar(50),
    store_email VARCHAR(50)
);

--------------поставщик--------------------

CREATE TABLE d_supplier_city (
    supplier_city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_supplier_country (
    supplier_country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(50),
	supplier_contact varchar(50),
    supplier_email VARCHAR(50),
	supplier_phone varchar(50),
	supplier_address varchar(50),
    supplier_city_id INT REFERENCES d_supplier_city(supplier_city_id),
    supplier_country_id INT REFERENCES d_supplier_country(supplier_country_id)
);

--------------товар--------------------

CREATE TABLE d_product_name (
    product_name_id SERIAL PRIMARY KEY,
    product_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_product_category (
    product_category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_product_color (
    product_color_id SERIAL PRIMARY KEY,
    color_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_product_size (
    product_size_id SERIAL PRIMARY KEY,
    size VARCHAR(50) UNIQUE
);

CREATE TABLE d_product_brand (
    product_brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_product_material (
    product_material_id SERIAL PRIMARY KEY,
    material_name VARCHAR(50) UNIQUE
);

CREATE TABLE d_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    UNIQUE(day, month, year)
);

CREATE TABLE d_products (
    product_id SERIAL PRIMARY KEY,
    product_name_id INT REFERENCES d_product_name(product_name_id),    
    product_category_id INT REFERENCES d_product_category(product_category_id), 
	product_price real,
	product_quantity integer,
	product_weight real, 
    product_color_id INT REFERENCES d_product_color(product_color_id),
    product_size_id INT REFERENCES d_product_size(product_size_id),
    product_brand_id INT REFERENCES d_product_brand(product_brand_id),
    product_material_id INT REFERENCES d_product_material(product_material_id),
	product_description varchar(1024),
	product_rating real,
	product_reviews integer,
    product_release_date_id INT REFERENCES d_date(date_id),
    product_expiry_date_id INT REFERENCES d_date(date_id)
);

--------------покупка--------------------

CREATE TABLE f_sales (
    sale_id SERIAL PRIMARY KEY,    
	sale_date_id INT REFERENCES d_date(date_id),
	sale_customer_id INT REFERENCES d_customers(customer_id),
	sale_store_id INT REFERENCES d_stores(store_id),
    sale_supplier_id INT REFERENCES d_suppliers(supplier_id),
	sale_seller_id INT REFERENCES d_sellers(seller_id),
	sale_product_id INT REFERENCES d_products(product_id),
	sale_quantity integer,
	sale_total_price real
);