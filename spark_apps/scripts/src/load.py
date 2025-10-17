def load_to_dwh(df, jdbc_postgres_url, postgres_user, postgres_password, table_name):
    print(f"🔗 JDBC URL: {jdbc_postgres_url}")
    properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"
    }
    
    df.write.jdbc(url=jdbc_postgres_url,
                  table=table_name,
                  mode="overwrite",
                  properties=properties
                  )
    print(f"✅ Data loaded to {table_name}")