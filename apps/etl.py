from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, expr, year, quarter, month, dayofmonth,
    dayofweek, weekofyear, row_number
)

# --- Application Configuration ---
APP_NAME = "PostgresToStarSchemaETL"
POSTGRES_JDBC_JAR_PATH = "/opt/spark/jars/postgresql-42.6.0.jar"

# --- Database Connection Details ---
DB_CONNECTION_URL = "jdbc:postgresql://postgres_db:5432/bigdata"
DB_PROPERTIES = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}
RAW_DATA_TABLE = "mock_data"
JDBC_WRITE_MODE = "append"


def initialize_spark_session(app_name, jar_path):
    """Initializes and returns a SparkSession."""
    print(f"Initializing Spark session: {app_name} with JAR: {jar_path}")
    session = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jar_path)
        .getOrCreate()
    )
    print("Spark session initialized.")
    return session


def load_dataframe_from_postgres(spark, url, table, properties):
    """Loads a DataFrame from a PostgreSQL table."""
    print(f"Loading data from PostgreSQL table: {table}")
    df = (
        spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", properties["user"])
        .option("password", properties["password"])
        .option("driver", properties["driver"])
        .load()
    )
    print(f"Loaded {df.count()} rows from {table}.")
    return df


def save_dataframe_to_postgres(df, url, table, mode, properties):
    """Saves a DataFrame to a PostgreSQL table."""
    print(f"Saving {df.count()} rows to PostgreSQL table: {table} (mode: {mode})")
    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", properties["user"])
        .option("password", properties["password"])
        .option("driver", properties["driver"])
        .mode(mode)
        .save()
    )
    print(f"Successfully saved data to {table}.")


def process_dimension_table(source_df, id_col, order_col, attributes_map, target_table_name):
    """
    Extracts, transforms, and loads a dimension table.
    Uses a window function to pick the most recent attributes based on order_col (SCD Type 1).
    """
    print(f"Processing dimension table: {target_table_name}")

    cols_to_select = [id_col, order_col] + list(attributes_map.keys())
    distinct_cols_to_select = sorted(list(set(cols_to_select)))
    dim_df = source_df.select(*distinct_cols_to_select)
    window_spec = Window.partitionBy(col(id_col)).orderBy(col(order_col).desc())

    processed_dim_df = (
        dim_df
        .withColumn("rn", row_number().over(window_spec))
        .where(col("rn") == 1)
        .drop("rn", order_col)
    )

    for source_name, target_name in attributes_map.items():
        processed_dim_df = processed_dim_df.withColumnRenamed(source_name, target_name)

    save_dataframe_to_postgres(processed_dim_df, DB_CONNECTION_URL, target_table_name, JDBC_WRITE_MODE, DB_PROPERTIES)
    return processed_dim_df  # Возвращаем длѝ возможного иѝпользованиѝ (хотѝ длѝ SK мы перечитываем)


def run_etl():
    """Main ETL process."""
    spark = initialize_spark_session(APP_NAME, POSTGRES_JDBC_JAR_PATH)

    # 1. Load source data from mock_data table
    source_dataframe = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, RAW_DATA_TABLE, DB_PROPERTIES)
    source_dataframe.cache()  # Cache for multiple uses

    # 2. Process and load Dimension Tables
    process_dimension_table(
        source_df=source_dataframe,
        id_col="sale_customer_id",
        order_col="sale_date",
        attributes_map={
            "sale_customer_id": "customer_id",
            "customer_first_name": "customer_first_name",
            "customer_last_name": "customer_last_name",
            "customer_age": "customer_age",
            "customer_email": "customer_email",
            "customer_country": "customer_country",
            "customer_postal_code": "customer_postal_code",
            "customer_pet_type": "customer_pet_type",
            "customer_pet_breed": "customer_pet_breed",
            "pet_category": "pet_category"
        },
        target_table_name="d_customers"
    )

    process_dimension_table(
        source_df=source_dataframe,
        id_col="sale_seller_id",
        order_col="sale_date",
        attributes_map={
            "sale_seller_id": "seller_id",
            "seller_first_name": "seller_first_name",
            "seller_last_name": "seller_last_name",
            "seller_email": "seller_email",
            "seller_country": "seller_country",
            "seller_postal_code": "seller_postal_code"
        },
        target_table_name="d_sellers"
    )

    process_dimension_table(
        source_df=source_dataframe,
        id_col="sale_product_id",
        order_col="sale_date",
        attributes_map={
            "sale_product_id": "product_id",
            "product_name": "product_name",
            "product_category": "product_category",
            "product_weight": "product_weight",
            "product_color": "product_color",
            "product_size": "product_size",
            "product_brand": "product_brand",
            "product_material": "product_material",
            "product_description": "product_description",
            "product_rating": "product_rating",
            "product_reviews": "product_reviews",
            "product_release_date": "product_release_date",
            "product_expiry_date": "product_expiry_date",
            "product_price": "product_price",
            "product_quantity": "product_quantity"
        },
        target_table_name="d_products"
    )

    process_dimension_table(
        source_df=source_dataframe,
        id_col="store_name",
        order_col="sale_date",
        attributes_map={
            "store_name": "store_name",
            "store_location": "store_location",
            "store_city": "store_city",
            "store_state": "store_state",
            "store_country": "store_country",
            "store_phone": "store_phone",
            "store_email": "store_email"
        },
        target_table_name="d_stores"
    )

    process_dimension_table(
        source_df=source_dataframe,
        id_col="supplier_name",
        order_col="sale_date",
        attributes_map={
            "supplier_name": "supplier_name",
            "supplier_contact": "supplier_contact",
            "supplier_email": "supplier_email",
            "supplier_phone": "supplier_phone",
            "supplier_address": "supplier_address",
            "supplier_city": "supplier_city",
            "supplier_country": "supplier_country"
        },
        target_table_name="d_suppliers"
    )

    # dim_date
    print("Processing dimension table: d_date")
    dim_date_df = (
        source_dataframe
        .select(col("sale_date"))
        .filter(col("sale_date").isNotNull())
        .distinct()
        .withColumn("day", dayofmonth(col("sale_date")))
        .withColumn("month", month(col("sale_date")))
        .withColumn("year", year(col("sale_date")))
    )
    save_dataframe_to_postgres(dim_date_df, DB_CONNECTION_URL, "d_date", JDBC_WRITE_MODE, DB_PROPERTIES)

    # 3. Load Dimension tables back to get their generated surrogate keys (_id)
    print("Reloading dimension tables to get surrogate keys.")
    customer_dim_id = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "d_customers", DB_PROPERTIES).select("customer_id")
    seller_dim_id = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "d_sellers", DB_PROPERTIES).select("seller_id")
    product_dim_id = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "d_products", DB_PROPERTIES).select("product_id")
    store_dim_id = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "d_stores", DB_PROPERTIES).select("store_id")
    supplier_dim_id = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "d_suppliers", DB_PROPERTIES).select("supplier_id")
    date_dim_id = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "d_date", DB_PROPERTIES).select("date_id", "full_date")

    # 4. Process and load Fact Table
    print("Processing fact table: f_sales")
    fact_sales_df = (
        source_dataframe
        .join(date_dim_id, source_dataframe.sale_date == date_dim_id.full_date, "inner")
        .join(customer_dim_id, source_dataframe.sale_customer_id == customer_dim_id.customer_id, "inner")
        .join(seller_dim_id, source_dataframe.sale_seller_id == seller_dim_id.seller_id, "inner")
        .join(product_dim_id, source_dataframe.sale_product_id == product_dim_id.product_id, "inner")
        .join(store_dim_id, source_dataframe.store_name == store_dim_id.store_name, "inner")
        .join(supplier_dim_id, source_dataframe.supplier_name == supplier_dim_id.supplier_name, "inner")
        .select(
            date_dim_id.date_id,
            customer_dim_id.customer_id,
            seller_dim_id.seller_id,
            product_dim_id.product_id,
            store_dim_id.store_id,
            supplier_dim_id.supplier_id,
            source_dataframe.sale_quantity,
            source_dataframe.sale_total_price
        )
    )
    save_dataframe_to_postgres(fact_sales_df, DB_CONNECTION_URL, "f_sales", JDBC_WRITE_MODE, DB_PROPERTIES)

    source_dataframe.unpersist()  # Release cache
    print("ETL process to Star Schema in PostgreSQL finished successfully.")
    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    run_etl()