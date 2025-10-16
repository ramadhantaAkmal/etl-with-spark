from pyspark.sql import SparkSession

from config.setting import PRODUCTS_PATH, USERS_PATH, TRANSACTIONS_PATH

spark = SparkSession.builder \
    .appName('retail-pipeline') \
    .master()
    


