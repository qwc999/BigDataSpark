--------------покупатель--------------------
INSERT INTO d_customer_pet_type (pet_type_name)
SELECT DISTINCT customer_pet_type
FROM mock_data
WHERE customer_pet_type IS NOT NULL;

INSERT INTO d_customer_pet_breed (breed_name)
SELECT DISTINCT customer_pet_breed
FROM mock_data
WHERE customer_pet_breed IS NOT NULL;

INSERT INTO d_pet_category (category_name)
SELECT DISTINCT pet_category
FROM mock_data
WHERE pet_category IS NOT NULL;

INSERT INTO d_customer_country (country_name)
SELECT DISTINCT customer_country
FROM mock_data
WHERE customer_country IS NOT NULL;

INSERT INTO d_customer_postal_code (postal_code)
SELECT DISTINCT customer_postal_code
FROM mock_data
WHERE customer_postal_code IS NOT NULL;

INSERT INTO d_customers (
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country_id,
    customer_postal_code_id,
    customer_pet_type_id,
    customer_pet_breed_id,
    pet_category_id
)
SELECT
    md.customer_first_name,
    md.customer_last_name,
    md.customer_age,
    md.customer_email,
    dcc.customer_country_id,
    dpc.customer_postal_code_id,
    dpt.customer_pet_type_id,
    dpb.customer_pet_breed_id,
    dcat.pet_category_id
FROM mock_data md
JOIN d_customer_country dcc ON md.customer_country = dcc.country_name
JOIN d_customer_postal_code dpc ON md.customer_postal_code = dpc.postal_code
JOIN d_customer_pet_type dpt ON md.customer_pet_type = dpt.pet_type_name
JOIN d_customer_pet_breed dpb ON md.customer_pet_breed = dpb.breed_name
JOIN d_pet_category dcat ON md.pet_category = dcat.category_name;

--------------продавец--------------------

INSERT INTO d_seller_country (country_name)
SELECT DISTINCT seller_country
FROM mock_data
WHERE seller_country IS NOT NULL;

INSERT INTO d_seller_postal_code (postal_code)
SELECT DISTINCT seller_postal_code
FROM mock_data
WHERE seller_postal_code IS NOT NULL;

INSERT INTO d_sellers (
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country_id,
    seller_postal_code_id
)
SELECT
    md.seller_first_name,
    md.seller_last_name,
    md.seller_email,
    dsc.seller_country_id,
    dspc.seller_postal_code_id
FROM mock_data md
JOIN d_seller_country dsc ON md.seller_country = dsc.country_name
JOIN d_seller_postal_code dspc ON md.seller_postal_code = dspc.postal_code;

--------------магазин--------------------

INSERT INTO d_store_city (city_name)
SELECT DISTINCT store_city
FROM mock_data
WHERE store_city IS NOT NULL;

INSERT INTO d_store_state (state)
SELECT DISTINCT store_state
FROM mock_data
WHERE store_state IS NOT NULL;

INSERT INTO d_store_country (country_name)
SELECT DISTINCT store_country
FROM mock_data
WHERE store_country IS NOT NULL;

INSERT INTO d_stores (
    store_name,
    store_location,
    store_city_id,
    store_state_id,
    store_country_id,
    store_phone,
    store_email
)
SELECT
    md.store_name,
    md.store_location,
    dsc.store_city_id,
    dss.store_state_id,
    dsco.store_country_id,
    md.store_phone,
    md.store_email
FROM mock_data md
JOIN d_store_city dsc ON md.store_city = dsc.city_name
JOIN d_store_state dss ON md.store_state = dss.state
JOIN d_store_country dsco ON md.store_country = dsco.country_name;

--------------поставщик--------------------

INSERT INTO d_supplier_city (city_name)
SELECT DISTINCT supplier_city
FROM mock_data
WHERE supplier_city IS NOT NULL;

INSERT INTO d_supplier_country (country_name)
SELECT DISTINCT supplier_country
FROM mock_data
WHERE supplier_country IS NOT NULL;

INSERT INTO d_suppliers (
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city_id,
    supplier_country_id
)
SELECT
    md.supplier_name,
    md.supplier_contact,
    md.supplier_email,
    md.supplier_phone,
    md.supplier_address,
    dsc.supplier_city_id,
    dscn.supplier_country_id
FROM mock_data md
JOIN d_supplier_city dsc ON md.supplier_city = dsc.city_name
JOIN d_supplier_country dscn ON md.supplier_country = dscn.country_name;

--------------товар--------------------

INSERT INTO d_product_name (product_name)
SELECT DISTINCT product_name
FROM mock_data
WHERE product_name IS NOT NULL;

INSERT INTO d_product_category (category_name)
SELECT DISTINCT product_category
FROM mock_data
WHERE product_category IS NOT NULL;

INSERT INTO d_product_color (color_name)
SELECT DISTINCT product_color
FROM mock_data
WHERE product_color IS NOT NULL;

INSERT INTO d_product_size (size)
SELECT DISTINCT product_size
FROM mock_data
WHERE product_size IS NOT NULL;

INSERT INTO d_product_brand (brand_name)
SELECT DISTINCT product_brand
FROM mock_data
WHERE product_brand IS NOT NULL;

INSERT INTO d_product_material (material_name)
SELECT DISTINCT product_material
FROM mock_data
WHERE product_material IS NOT NULL;

INSERT INTO d_date (full_date, day, month, year)
SELECT DISTINCT
    product_release_date::DATE AS full_date,
    EXTRACT(DAY FROM product_release_date)::INT,
    EXTRACT(MONTH FROM product_release_date)::INT,
    EXTRACT(YEAR FROM product_release_date)::INT
FROM mock_data
WHERE product_release_date IS NOT NULL

UNION

SELECT DISTINCT
    product_expiry_date::DATE AS full_date,
    EXTRACT(DAY FROM product_expiry_date)::INT,
    EXTRACT(MONTH FROM product_expiry_date)::INT,
    EXTRACT(YEAR FROM product_expiry_date)::INT
FROM mock_data
WHERE product_expiry_date IS NOT NULL

UNION

SELECT DISTINCT
    sale_date::DATE AS full_date,
    EXTRACT(DAY FROM sale_date)::INT,
    EXTRACT(MONTH FROM sale_date)::INT,
    EXTRACT(YEAR FROM sale_date)::INT
FROM mock_data
WHERE sale_date IS NOT NULL;


INSERT INTO d_products (
    product_name_id,
    product_category_id,
    product_price,
    product_quantity,
    product_weight,
    product_color_id,
    product_size_id,
    product_brand_id,
    product_material_id,
    product_description,
    product_rating,
    product_reviews,
    product_release_date_id,
    product_expiry_date_id
)
SELECT
    dpn.product_name_id,
    dpc.product_category_id,
    md.product_price,
    md.product_quantity,
    md.product_weight,
    dcol.product_color_id,
    dsz.product_size_id,
    dbr.product_brand_id,
    dmat.product_material_id,
    md.product_description,
    md.product_rating,
    md.product_reviews,
    drelease.date_id,
    dexpiry.date_id
FROM mock_data md
JOIN d_product_name dpn ON md.product_name = dpn.product_name
JOIN d_product_category dpc ON md.product_category = dpc.category_name
JOIN d_product_color dcol ON md.product_color = dcol.color_name
JOIN d_product_size dsz ON md.product_size = dsz.size
JOIN d_product_brand dbr ON md.product_brand = dbr.brand_name
JOIN d_product_material dmat ON md.product_material = dmat.material_name
JOIN d_date drelease ON md.product_release_date::DATE = drelease.full_date
JOIN d_date dexpiry ON md.product_expiry_date::DATE = dexpiry.full_date;

--------------покупка--------------------

INSERT INTO f_sales (
    sale_date_id,
    sale_customer_id,
    sale_store_id,
    sale_supplier_id,
    sale_seller_id,
    sale_product_id,
    sale_quantity,
    sale_total_price
)
SELECT
    dd.date_id,
    dc.customer_id,  
    ds.store_id,  
    dsu.supplier_id, 
    dsel.seller_id, 
    dp.product_id, 
    md.sale_quantity,
    md.sale_total_price
FROM mock_data md
JOIN d_customers dc ON md.customer_email = dc.customer_email
JOIN d_sellers dsel ON md.seller_email = dsel.seller_email
JOIN d_products dp ON dp.product_name_id = (
    SELECT product_name_id FROM d_product_name WHERE product_name = md.product_name
)
JOIN d_stores ds ON md.store_name = ds.store_name
JOIN d_date dd ON md.sale_date::DATE = dd.full_date
JOIN d_suppliers dsu ON md.supplier_name = dsu.supplier_name;