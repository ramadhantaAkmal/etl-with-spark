from pyspark.sql import SparkSession

from config.setting import PRODUCTS_PATH, USERS_PATH, TRANSACTIONS_PATH, JDBC_POSTGRES_PATH,JDBC_POSTGRES_URL,POSTGRES_USER,POSTGRES_PASSWORD,TABLE_NAME
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_to_dwh

spark = SparkSession.builder \
    .appName('retail-pipeline') \
    .config("spark.jars", JDBC_POSTGRES_PATH) \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Extract
product_df = extract_data(PRODUCTS_PATH, spark, "csv")
users_df = extract_data(USERS_PATH, spark, "json")
transactions_df = extract_data(TRANSACTIONS_PATH, spark, "csv")

#Transform
final_df = transform_data(users_df, product_df, transactions_df)

#Load
load_to_dwh(final_df, JDBC_POSTGRES_URL,POSTGRES_USER,POSTGRES_PASSWORD,TABLE_NAME)
print("✅ Finished ETL pipeline load to database")
spark.stop()

