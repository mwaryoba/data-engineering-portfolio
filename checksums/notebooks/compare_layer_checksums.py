from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Checksum Comparison").getOrCreate()

# Define paths to the checksum tables
bronze_table = "silver_prod_aspire.guidecx.checksums_bronze"
silver_table = "silver_prod_aspire.guidecx.checksums_silver"

# Load the checksum data
bronze_df = spark.table(bronze_table).withColumnRenamed("checksum", "bronze_checksum") \
                                     .withColumnRenamed("row_count", "bronze_row_count")
silver_df = spark.table(silver_table).withColumnRenamed("checksum", "silver_checksum") \
                                     .withColumnRenamed("row_count", "silver_row_count")

# Join on object_name
comparison_df = bronze_df.join(silver_df, on="object_name", how="full_outer")

# Compare checksums and row counts
result_df = comparison_df.withColumn(
    "status",
    when(col("bronze_checksum") != col("silver_checksum"), "CHECKSUM_MISMATCH")
    .when(col("bronze_row_count") != col("silver_row_count"), "ROW_COUNT_MISMATCH")
    .when(col("bronze_checksum").isNull() | col("silver_checksum").isNull(), "MISSING")
    .otherwise("MATCH")
)

# Show results
result_df.select("object_name", "bronze_row_count", "silver_row_count", "status").show(truncate=False)