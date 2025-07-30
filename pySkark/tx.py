from pyspark.sql import SparkSession, DataFrame 

def fun1(spark, path):
    return spark.read.option("header", True).option("inferSchema",True).csv("path.csv")

def fun2(spark):
    return spark.read.option("header",True).option("inferSchema",True).csv("path.csv")

def fun3(df):
    return df.filter( (col("username").isNotNull()) & (trim (col("username")) != ""))

def fun4(df):
    return df.filter( (col("username").isNotNull()) & trim((col("username")) != "") )

def count_compute(df):
    cleaned_df = df.filter(col("rating").isNotNull()).withColumn("rating", col("rating").cast("int"))
    agg_row = cleaned_df.agg(
        spark_min("rating").alias("min_rating"),
        spark_max("rating").alias("max_rating"),
        avg("rating").alias("avg_rating")
    ).collect()[0]
    return{
        "min_rating" : agg_row["min_rating"],
        "max_rating" : agg_row["max_rating"]
    }

def fun5(df):
    cleandf = df.filter(col("rating").isNotNull()).withColumn("rating", col("rating").cast("int"))
    ans = cleandf.agg(
        spark_min("rating").alias("min_rating"),
        spark_max("rating").alias("max_rating"),
        avg("rating").alias("avg_rating")
    ).collect()[0]

    return{
        "min_rating" : ans["min_rating"],
        "max_rating" : ans["max_rating"],
        "avg_rating" : ans["avg_rating"]
    }


def count_val(df):
    return df.filter(col("rating").isNotNull()).count()

def fun10(f):
    f.filter(col("rating").isNotNull()).count()