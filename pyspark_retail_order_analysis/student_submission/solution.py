from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Load CSV with header
def load_orders_data(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.options("header",True).option("inferSchema",True).csv("file:/C:\Users\shrey\Desktop\pyspark_retail_order_analysis\data\orders.csv")

# 2. Load with schema
def load_orders_data_with_schema(spark: SparkSession, path: str) -> DataFrame:
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("category", StringType(), True),
        StructField("city", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("final_price", DoubleType(), True),
        StructField("payment_mode", StringType(), True),
        StructField("is_returned", StringType(), True)
    ])
    return spark.read.option("header",True).schema(schema).csv(path)

# 3. Create DataFrame from list
def create_sample_orders(spark: SparkSession) -> DataFrame:
    data = [
        ("O1", "C1", "2023-01-10", "2023-01-15", "Furniture", "New York", 2, 100.0, None, 0.1, 180.0, "Card", "No"),
        ("O2", "C2", "2023-03-05", "2023-03-10", "Electronics", "Boston", 1, 300.0, None, 0.05, 285.0, "UPI", "No")
    ]
    schema = ["order_id", "customer_id", "order_date", "ship_date", "category", "city", "quantity", "unit_price", "total_price", "discount", "final_price", "payment_mode", "is_returned"]
    return spark.createDataFrame(data, schema)


# 4. Add computed column (total_price)
def add_total_price(df: DataFrame) -> DataFrame:
    return df.withColumn("total_price", col("quantity") * col("unit_price"))

# 5. Filter by status = Delivered
def filter_delivered_orders(df: DataFrame) -> DataFrame:
    return df.filter(col("is_returned") == "No")

# 6. Drop unnecessary column
def drop_status_column(df: DataFrame) -> DataFrame:
    return df.dropna("is_returned")

# 7. Rename a column
def rename_customer_id(df: DataFrame) -> DataFrame:
    return withColumnRenamed("customer_id", "cust_id")

# 8. Cast quantity to double
def cast_quantity_to_double(df: DataFrame) -> DataFrame:
    return df.withColumn("quantity",col("quantity").cast("double"))

# 9. Get unique categories as list
def list_unique_categories(df: DataFrame) -> list:
    return [x[0] for x in df.select("category").distinct()]

# 10. Total quantity per region
def total_quantity_by_region(df: DataFrame) -> DataFrame:
    return df.groupBy("city").agg(sum("quantity").alias("new col"))

# 11. Max unit price by category
def max_price_by_category(df: DataFrame) -> DataFrame:
    return df.groupBy("category").agg(max("unit_price").alias("new col"))

# 12. Top 3 categories by revenue
def top_categories_by_revenue(df: DataFrame) -> DataFrame:
    return df.groupBy("category").agg(sum("final_price").alias("revenue")).orderBy(desc("revenue")).limit(3)    

# 13. Orders placed after specific date
def filter_orders_after_date(df: DataFrame, date_str: str) -> DataFrame:
    return df.filter(col("order_date") > lit(date_str))

# 14. Year-wise revenue
def yearly_revenue(df: DataFrame) -> DataFrame:
    return df.withColumn("year", year("order_date")).groupBy("year").agg(sum("final_price").alias("total_revenue"))

# 15. Drop duplicates
def remove_duplicate_orders(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()

# 16. Orders per category per region
def category_region_count(df: DataFrame) -> DataFrame:
    return df.groupBy("category", "city").count()

# 17. Join with returns
def join_with_returns(df: DataFrame, returns_df: DataFrame) -> DataFrame:
    

# 18. Filter orders with quantity > 10
def filter_large_orders(df: DataFrame) -> DataFrame:
    pass

# 19. Replace null prices
def replace_null_prices(df: DataFrame) -> DataFrame:
    pass

# 20. Calculate average unit price per category
def average_price_per_category(df: DataFrame) -> DataFrame:
    pass

# 21. Filter by multiple conditions
def filter_north_electronics(df: DataFrame) -> DataFrame:
    pass

# 22. Count delivered vs returned
def count_status_types(df: DataFrame) -> DataFrame:
    pass

# 23. Find most frequent sub_category
def most_common_sub_category(df: DataFrame) -> str:
    pass

# 24. Revenue per sub_category in tuple
def revenue_by_sub_category(df: DataFrame, target: str) -> tuple:
    pass

# 25. Find null counts
def null_count(df: DataFrame) -> DataFrame:
    pass

# 26. Get earliest order
def earliest_order(df: DataFrame) -> str:
    pass

# 27. Remove orders before year
def remove_orders_before_year(df: DataFrame, year_threshold: int) -> DataFrame:
    pass

# 28. Total revenue by customer
def revenue_by_customer(df: DataFrame) -> DataFrame:
    pass

# 29. Group by multiple columns with aggregation
def quantity_price_by_category_region(df: DataFrame) -> DataFrame:
    pass

# 30. Most valuable order
def highest_value_order(df: DataFrame) -> str:
    pass
