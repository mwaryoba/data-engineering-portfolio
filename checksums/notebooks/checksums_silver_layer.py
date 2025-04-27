# MAGIC %run checksums/utilities/ChecksumUtil

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Silver Layer Checksum").getOrCreate()

catalog = "silver_prod_aspire"
schema = "guidecx"
delta_table_path = f"{catalog}.{schema}.checksums_silver"

spark.sql(f"DROP TABLE IF EXISTS {delta_table_path}")
tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

for row in tables:
    table_name = row['tableName']
    print(f"Processing: {table_name}")
    df = spark.table(f"{catalog}.{schema}.{table_name}")
    checksum_df = generate_checksums(df, table_name)
    write_checksums(checksum_df, None, delta_table_path)