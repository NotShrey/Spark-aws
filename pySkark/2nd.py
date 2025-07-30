from pyspark.sql import SparkSession, DataFrame

def load_wallet(spark,df):
    return spark.read.option("header",True).option("inferSchema",True).csv(df)

def load_wallle(s,f):
    return spark.read.option("header",True).option("inferSchema",True).csv(f)

def highVal(df,th):
    return df.withColumn("newName", col("amount") > th)

def flag_unusal(df):
    return df.withColumn("newName",col("from_city") != col("to_city"))

def summarize(df):
    return df.groupby("user_id").agg(spark_count("*")).alias("total_txns")


def summary(df):
    return df.groupBy("user_id").agg(spark_count("*")).alias("total_trnx")