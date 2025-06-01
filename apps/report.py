from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

# --- Application Configuration ---
APP_NAME = "DWHtoClickHouseReports"
POSTGRES_JDBC_JAR_PATH = "/opt/spark/jars/postgresql-42.6.0.jar"
CLICKHOUSE_JDBC_JAR_PATH = "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar"

# --- PostgreSQL Connection Details (Иѝточник DWH) ---
PG_DB_URL = "jdbc:postgresql://postgres_db:5432/bigdata"
PG_DB_PROPERTIES = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# --- ClickHouse Connection Details (Целеваѝ БД длѝ отчетов) ---
CH_DB_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_DB_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

CH_WRITE_MODE = "overwrite"
CH_TABLE_ENGINE_OPTIONS = "ENGINE = MergeTree() ORDER BY tuple()"

def initialize_spark_session(app_name, pg_jar, ch_jar):
    """Initializes and returns a SparkSession."""
    print(f"Initializing Spark session: {app_name} with JARs: {pg_jar}, {ch_jar}")
    jars_path = f"{pg_jar},{ch_jar}"
    session = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jars_path)
        .getOrCreate()
    )
    print("Spark session initialized.")
    return session


def load_dwh_table(spark, table_name):
    """Loads a table from PostgreSQL DWH."""
    print(f"Loading DWH table from PostgreSQL: {table_name}")
    df = (
        spark.read
        .format("jdbc")
        .option("url", PG_DB_URL)
        .option("dbtable", table_name)
        .options(**PG_DB_PROPERTIES)
        .load()
    )
    print(f"Loaded {df.count()} rows from {table_name}.")
    return df


def save_report_to_clickhouse(report_df, report_table_name, order_by_cols=None):
    """Saves a report DataFrame to a ClickHouse table."""
    print(f"Saving report to ClickHouse table: {report_table_name}")

    current_ch_table_engine = CH_TABLE_ENGINE_OPTIONS
    if "MergeTree" in CH_TABLE_ENGINE_OPTIONS and order_by_cols:
        order_by_clause = ", ".join(order_by_cols)
        current_ch_table_engine = f"ENGINE = MergeTree() ORDER BY ({order_by_clause})"
    elif "MergeTree" in CH_TABLE_ENGINE_OPTIONS and not order_by_cols and "ORDER BY tuple()" not in CH_TABLE_ENGINE_OPTIONS:
        current_ch_table_engine = f"{CH_TABLE_ENGINE_OPTIONS.replace('ORDER BY tuple()', '')} ORDER BY tuple()"

    (
        report_df.write
        .format("jdbc")
        .mode(CH_WRITE_MODE)
        .option("url", CH_DB_URL)
        .option("dbtable", report_table_name)
        .option("createTableOptions", current_ch_table_engine)
        .options(**CH_DB_PROPERTIES)
        .save()
    )
    print(f"Successfully saved report {report_table_name} with {report_df.count()} rows.")


def generate_reports(spark):
    """Generates and saves all required reports to ClickHouse."""

    # 1. Load data from DWH (PostgreSQL)
    print("Loading base DWH tables...")
    fact_sales_df = load_dwh_table(spark, "f_sales").cache()
    d_product_df = load_dwh_table(spark, "d_products").cache()
    d_customer_df = load_dwh_table(spark, "d_customers").cache()
    d_date_df = load_dwh_table(spark, "d_date").cache()
    d_store_df = load_dwh_table(spark, "d_stores").cache()
    d_supplier_df = load_dwh_table(spark, "d_suppliers").cache()
    print("Base DWH tables loaded and cached.")

    # --- Витрина продаж по продуктам ---
    print("Generating Product Sales Mart...")
    # 1.1 Топ-10 ѝамых продаваемых продуктов (по количеѝтву)
    report_top_10_products = (
        fact_sales_df.groupBy("product_id")
        .agg(
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.sum("sale_total_price").alias("total_revenue_generated")
        )
        .join(d_product_df, "product_id")
        .select(
            d_product_df.product_id.alias("business_product_id"),
            d_product_df.name.alias("product_name"),
            d_product_df.category.alias("product_category"),
            "total_quantity_sold",
            "total_revenue_generated"
        )
        .orderBy(F.desc("total_quantity_sold"))
        .limit(10)
    )
    save_report_to_clickhouse(report_top_10_products, "mart_top_10_selling_products",
                              order_by_cols=["total_quantity_sold"])

    # 1.2 Общаѝ выручка по категориѝм продуктов
    report_revenue_by_category = (
        fact_sales_df.join(d_product_df, "product_id")
        .groupBy(d_product_df.category.alias("product_category"))
        .agg(F.sum("sale_total_price").alias("category_total_revenue"))
        .orderBy(F.desc("category_total_revenue"))
    )
    save_report_to_clickhouse(report_revenue_by_category, "mart_revenue_by_product_category",
                              order_by_cols=["product_category"])

    # 1.3 Средний рейтинг и количеѝтво отзывов длѝ каждого продукта
    report_product_feedback_summary = d_product_df.select(
        d_product_df.product_id.alias("business_product_id"),
        d_product_df.name.alias("product_name"),
        d_product_df.category.alias("product_category"),
        d_product_df.rating.alias("average_rating"),
        d_product_df.reviews.alias("number_of_reviews")
    ).orderBy(F.desc("number_of_reviews"))
    save_report_to_clickhouse(report_product_feedback_summary, "mart_product_feedback_summary",
                              order_by_cols=["business_product_id"])

    # --- Витрина продаж по клиентам ---
    print("Generating Customer Sales Mart...")
    # 2.1 Топ-10 клиентов ѝ наибольшей общей ѝуммой покупок
    report_top_10_customers_by_purchase = (
        fact_sales_df.groupBy("customer_id")
        .agg(F.sum("sale_total_price").alias("customer_total_purchases"))
        .join(d_customer_df, "customer_id")
        .select(
            d_customer_df.customer_id.alias("business_customer_id"),
            d_customer_df.first_name,
            d_customer_df.last_name,
            d_customer_df.country.alias("customer_country"),
            "customer_total_purchases"
        )
        .orderBy(F.desc("customer_total_purchases"))
        .limit(10)
    )
    save_report_to_clickhouse(report_top_10_customers_by_purchase, "mart_top_10_customers_by_purchase",
                              order_by_cols=["customer_total_purchases"])

    # 2.2 Раѝпределение клиентов по ѝтранам
    report_customer_distribution_by_country = (
        d_customer_df.groupBy(d_customer_df.country.alias("customer_country"))
        .agg(F.countDistinct(d_customer_df.customer_id).alias("distinct_customer_count"))
        .orderBy(F.desc("distinct_customer_count"))
    )
    save_report_to_clickhouse(report_customer_distribution_by_country, "mart_customer_distribution_by_country",
                              order_by_cols=["customer_country"])

    # 2.3 Средний чек длѝ каждого клиента
    # Считаем количеѝтво уникальных транзакций (продаж) на клиента, чтобы получить ѝредний чек
    # Предположим, что каждаѝ ѝтрока в fact_sales - ѝто отдельнаѝ транзакциѝ (чек)
    # Еѝли неѝколько ѝтрок в fact_sales могут отноѝитьѝѝ к одному чеку, то нужна группировка по ID чека.
    # В нашей модели fact_sales каждаѝ ѝтрока - ѝто запиѝь о продаже товара, возможно, в рамках одного чека.
    # Длѝ "ѝреднего чека" лучше иметь отдельный идентификатор транзакции (чека).
    # Еѝли его нет, то можно ѝчитать ѝреднюю ѝумму *одной запиѝи о продаже* на клиента.
    # Или, еѝли sale_total_price - ѝто уже ѝумма вѝего чека (что маловероѝтно, еѝли еѝть sale_quantity),
    # то нужно агрегировать иначе.
    # Пример ниже ѝчитает ѝреднюю ѝтоимоѝть ОДНОЙ СТРОКИ в fact_sales длѝ клиента.
    # Длѝ более точного ѝреднего чека, еѝли неѝколько ѝтрок в fact_sales ѝто один чек,
    # нам нужен был бы ID чека.
    # (sum(total_price) / count(quantity)) - ѝто ѝкорее ѝреднѝѝ цена *за единицу товара* по клиенту, еѝли quantity > 1.
    # sum(total_price) / count(distinct transaction_id) еѝли бы он был.
    # Так как его нет, "sale_quantity" как "количеѝтво чеков/транзакций".
    # НО! sale_quantity в fact_sales - ѝто количеѝтво товара в транзакции.
    # Еѝли мы хотим ѝредний чек, то нужно количеѝтво УНИКНЛЬНЫХ транзакций.
    # Еѝли каждаѝ ѝтрока в fact_sales - ѝто уникальнаѝ транзакциѝ, то count(*) или count("sale_sk")
    report_avg_customer_order_value = (
        fact_sales_df.groupBy("customer_id")
        .agg(
            (F.sum("sale_total_price") / F.countDistinct("sale_sk")).alias("avg_transaction_value_per_customer")
        )
        .join(d_customer_df, "customer_id")
        .select(
            d_customer_df.customer_id.alias("business_customer_id"),
            d_customer_df.first_name,
            d_customer_df.last_name,
            "avg_transaction_value_per_customer"
        )
        .orderBy(F.desc("avg_transaction_value_per_customer"))
    )
    save_report_to_clickhouse(report_avg_customer_order_value, "mart_avg_customer_order_value",
                              order_by_cols=["business_customer_id"])

    # --- Витрина продаж по времени ---
    print("Generating Time-based Sales Mart...")
    # 3.1 Меѝѝчные и годовые тренды продаж
    sales_with_date_details_df = fact_sales_df.join(d_date_df, "date_id")

    report_monthly_sales_trends = (
        sales_with_date_details_df
        .groupBy(d_date_df.year, d_date_df.month, d_date_df.month_name)
        .agg(
            F.sum("sale_total_price").alias("monthly_total_revenue"),
            F.sum("sale_quantity").alias("monthly_total_quantity_sold")
        )
        .orderBy(d_date_df.year, d_date_df.month)
    )
    save_report_to_clickhouse(report_monthly_sales_trends, "mart_monthly_sales_trends", order_by_cols=["year", "month"])

    report_yearly_sales_trends = (
        sales_with_date_details_df
        .groupBy(d_date_df.year)
        .agg(
            F.sum("sale_total_price").alias("yearly_total_revenue"),
            F.sum("sale_quantity").alias("yearly_total_quantity_sold")
        )
        .orderBy(d_date_df.year)
    )
    save_report_to_clickhouse(report_yearly_sales_trends, "mart_yearly_sales_trends", order_by_cols=["year"])

    # 3.2 Сравнение выручки за разные периоды (MoM - Month over Month)
    window_spec_mom = Window.orderBy("year", "month")
    report_mom_revenue_comparison = (
        report_monthly_sales_trends
        .withColumn("previous_month_revenue", F.lag("monthly_total_revenue", 1, 0).over(window_spec_mom))
        .withColumn(
            "mom_revenue_change",
            (F.col("monthly_total_revenue") - F.col("previous_month_revenue"))
        )
        .withColumn(
            "mom_revenue_change_percent",
            F.when(F.col("previous_month_revenue") != 0,
                   F.round((F.col("mom_revenue_change") / F.col("previous_month_revenue")) * 100, 2)
                   ).otherwise(0)
        )
        .select(
            "year", "month", "month_name",
            "monthly_total_revenue",
            "previous_month_revenue",
            "mom_revenue_change",
            "mom_revenue_change_percent"
        )
    )
    save_report_to_clickhouse(report_mom_revenue_comparison, "mart_mom_revenue_comparison",
                              order_by_cols=["year", "month"])

    # 3.3 Средний размер заказа по меѝѝцам
    # Средний размер заказа = Общаѝ выручка за меѝѝц / Количеѝтво транзакций за меѝѝц
    report_avg_order_size_by_month = (
        sales_with_date_details_df
        .groupBy(d_date_df.year, d_date_df.month, d_date_df.month_name)
        .agg(
            (F.sum("sale_total_price") / F.countDistinct("sale_sk")).alias("avg_monthly_order_size")
        )
        .orderBy(d_date_df.year, d_date_df.month)
    )
    save_report_to_clickhouse(report_avg_order_size_by_month, "mart_avg_order_size_by_month",
                              order_by_cols=["year", "month"])

    # --- Витрина продаж по магазинам ---
    print("Generating Store Sales Mart...")
    sales_with_store_details_df = fact_sales_df.join(d_store_df, "store_id")

    # 4.1 Топ-5 магазинов � наибольшей выручкой
    report_top_5_stores_by_revenue = (
        sales_with_store_details_df.groupBy(d_store_df.name.alias("store_name"), d_store_df.city,
                                            d_store_df.country)
        .agg(F.sum("sale_total_price").alias("store_total_revenue"))
        .orderBy(F.desc("store_total_revenue"))
        .limit(5)
    )
    save_report_to_clickhouse(report_top_5_stores_by_revenue, "mart_top_5_stores_by_revenue",
                              order_by_cols=["store_total_revenue"])

    # 4.2 Раѝпределение продаж по городам и ѝтранам (магазинов)
    report_sales_by_store_location = (
        sales_with_store_details_df
        .groupBy(d_store_df.city.alias("store_city"), d_store_df.country.alias("store_country"))
        .agg(
            F.sum("sale_total_price").alias("location_total_revenue"),
            F.sum("sale_quantity").alias("location_total_quantity_sold")
        )
        .orderBy(F.desc("location_total_revenue"))
    )
    save_report_to_clickhouse(report_sales_by_store_location, "mart_sales_by_store_location",
                              order_by_cols=["store_country", "store_city"])

    # 4.3 Средний чек длѝ каждого магазина
    report_avg_store_order_value = (
        sales_with_store_details_df
        .groupBy(d_store_df.name.alias("store_name"))
        .agg(
            (F.sum("sale_total_price") / F.countDistinct("sale_sk")).alias("avg_transaction_value_per_store")
        )
        .orderBy(F.desc("avg_transaction_value_per_store"))
    )
    save_report_to_clickhouse(report_avg_store_order_value, "mart_avg_store_order_value", order_by_cols=["store_name"])

    print("Generating Supplier Sales Mart...")
    sales_with_supplier_details_df = fact_sales_df.join(d_supplier_df, "supplier_id")

    # 5.1 Топ-5 по�тавщиков � наибольшей выручкой (от продаж товаров �тих по�тавщиков)
    report_top_5_suppliers_by_revenue = (
        sales_with_supplier_details_df
        .groupBy(d_supplier_df.name.alias("supplier_name"), d_supplier_df.country.alias("supplier_country"))
        .agg(F.sum("sale_total_price").alias("supplier_generated_revenue"))
        .orderBy(F.desc("supplier_generated_revenue"))
        .limit(5)
    )
    save_report_to_clickhouse(report_top_5_suppliers_by_revenue, "mart_top_5_suppliers_by_revenue",
                              order_by_cols=["supplier_generated_revenue"])

    # 5.2 Среднѝѝ цена товаров (transaction_unit_price из fact_sales) от каждого поѝтавщика
    report_avg_product_price_by_supplier = (
        fact_sales_df
        .join(d_product_df,
              "product_id")
        .join(d_supplier_df, "supplier_id")
        .groupBy(d_supplier_df.name.alias("supplier_name"))
        .agg(F.avg("transaction_unit_price").alias("avg_sold_unit_price_from_supplier"))
        .orderBy(F.desc("avg_sold_unit_price_from_supplier"))
    )
    save_report_to_clickhouse(report_avg_product_price_by_supplier, "mart_avg_product_price_by_supplier",
                              order_by_cols=["supplier_name"])

    # 5.3 Раѝпределение продаж по ѝтранам поѝтавщиков
    report_sales_by_supplier_country = (
        sales_with_supplier_details_df
        .groupBy(d_supplier_df.country.alias("supplier_country"))
        .agg(
            F.sum("sale_total_price").alias("country_total_revenue"),
            F.sum("sale_quantity").alias("country_total_quantity_sold")
        )
        .orderBy(F.desc("country_total_revenue"))
    )
    save_report_to_clickhouse(report_sales_by_supplier_country, "mart_sales_by_supplier_country",
                              order_by_cols=["supplier_country"])

    # --- Витрина качеѝтва продукции ---
    print("Generating Product Quality Mart...")
    # 6.1 Продукты ѝ наивыѝшим и наименьшим рейтингом (Топ-10 длѝ каждого)
    report_highest_rated_products = (
        d_product_df.select(
            d_product_df.product_id.alias("business_product_id"),
            d_product_df.name.alias("product_name"),
            d_product_df.rating
        )
        .orderBy(F.desc_nulls_last("rating"))
        .limit(10)
    )
    save_report_to_clickhouse(report_highest_rated_products, "mart_highest_rated_products", order_by_cols=["rating"])

    report_lowest_rated_products = (
        d_product_df.select(
            d_product_df.product_id.alias("business_product_id"),
            d_product_df.name.alias("product_name"),
            d_product_df.rating
        )
        .filter(F.col("rating").isNotNull())
        .orderBy(F.asc_nulls_first("rating"))
        .limit(10)
    )
    save_report_to_clickhouse(report_lowest_rated_products, "mart_lowest_rated_products", order_by_cols=["rating"])

    # 6.2 Коррел�ци� между рейтингом и объемом продаж
    # Объем продаж - sum(sale_quantity) длѝ каждого продукта
    product_sales_and_rating_df = (
        fact_sales_df.join(d_product_df, "product_id")
        .groupBy(
            d_product_df.product_id.alias("business_product_id"),
            d_product_df.name.alias("product_name")
        )
        .agg(
            F.avg(d_product_df.rating).alias("avg_product_rating"),
            F.sum(fact_sales_df.sale_quantity).alias("total_product_quantity_sold")
        )
        .filter(F.col("avg_product_rating").isNotNull() & (F.col("total_product_quantity_sold").isNotNull()))
    )

    if product_sales_and_rating_df.count() > 1:
        correlation_value = product_sales_and_rating_df.stat.corr("avg_product_rating", "total_product_quantity_sold")
        print(f"Correlation between product rating and sales volume: {correlation_value}")
    else:
        correlation_value = None  # или 0.0
        print("Not enough data points to calculate correlation between rating and sales volume.")

    correlation_df = spark.createDataFrame(
        [(correlation_value if correlation_value is not None else float('nan'),)],
        ["rating_sales_volume_correlation_coeff"]
    )
    save_report_to_clickhouse(correlation_df, "mart_rating_sales_correlation",
                              order_by_cols=[])

    # 6.3 Продукты ѝ наибольшим количеѝтвом отзывов (Топ-10)
    report_top_reviewed_products = (
        d_product_df.select(
            d_product_df.product_id.alias("business_product_id"),
            d_product_df.name.alias("product_name"),
            d_product_df.reviews.alias("number_of_reviews")
        )
        .orderBy(F.desc_nulls_last("number_of_reviews"))
        .limit(10)
    )
    save_report_to_clickhouse(report_top_reviewed_products, "mart_top_reviewed_products",
                              order_by_cols=["number_of_reviews"])

    fact_sales_df.unpersist()
    d_product_df.unpersist()
    d_customer_df.unpersist()
    d_date_df.unpersist()
    d_store_df.unpersist()
    d_supplier_df.unpersist()
    print("All reports generated and saved to ClickHouse. Cached DataFrames unpersisted.")


if __name__ == "__main__":
    spark_session = initialize_spark_session(
        APP_NAME,
        POSTGRES_JDBC_JAR_PATH,
        CLICKHOUSE_JDBC_JAR_PATH
    )
    try:
        generate_reports(spark_session)
    finally:
        print("Stopping Spark session.")
        spark_session.stop()