from pyspark.sql import functions as F

def transform_data(users_df, products_df, transaction_df):
    products_df = products_df.withColumn("currency", F.substring("price", 1, 2))
    products_df = products_df.withColumn(
        "price_clean",
        F.regexp_replace("price", "[^0-9]", "")
    )
    products_df = products_df.withColumn(
        "price",
        F.col("price_clean").cast("int")
    )
    
    transaction_df = transaction_df.withColumn(
        "transaction_date",
        F.date_format(F.to_date("transaction_date", "d/M/yyyy"), "yyyy-MM-dd")
    )
    
    users_df = users_df.withColumn(
        "email",
        F.when(F.col("email") == "", None).otherwise(F.col("email"))
    )
    
    merge_df = transaction_df.join(users_df, on="user_id", how="left") \
                            .join(products_df, on="product_id", how="left")

    return merge_df