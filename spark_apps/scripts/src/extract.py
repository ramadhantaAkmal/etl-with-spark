def extract_data(path, spark, type):
    if type == "csv":
        return spark.read.csv(path, header=True, inferSchema=True)
    if type == "json":
        return spark.read.option("multiline","true").json(path)
    print("ERROR: extract type doesn't exist")
    return
    