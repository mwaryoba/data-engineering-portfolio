import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, lit

def detect_data_format(folder_path):
    try:
        dbutils.fs.ls(os.path.join(folder_path, "_delta_log"))
        return "delta"
    except:
        pass

    files = dbutils.fs.ls(folder_path)
    for file in files:
        if file.name.endswith(".xlsx") or file.name.endswith(".xls"):
            return "excel"
        elif file.name.endswith(".csv"):
            return "csv"
        elif file.name.endswith(".json"):
            return "json"

    return None

def load_data_with_format(input_path, data_format):
    spark = get_spark()
    if data_format == "json":
        return spark.read.json(input_path)
    elif data_format == "delta":
        return spark.read.format("delta").load(input_path)
    elif data_format == "csv":
        return spark.read.csv(input_path, header=True, inferSchema=True)
    elif data_format == "excel":
        return spark.read.format("com.crealytics.spark.excel") \
            .option("useHeader", "true") \
            .option("inferSchema", "true") \
            .load(input_path)
    return None

def generate_checksums(df, object_name):
    sorted_columns = sorted(df.columns)
    return df.withColumn("checksum", sha2(concat_ws("|", *sorted_columns), 256)) \
             .withColumn("object_name", lit(object_name))

def write_checksums(df, adls_path, delta_table_path, mode="append"):
    df.write.option("mergeSchema", "true").mode(mode).json(adls_path)
    df.write.format("delta").option("mergeSchema", "true").mode(mode).saveAsTable(delta_table_path)

def is_df_empty(df):
    return df is None or len(df.head(1)) == 0