from spark_session import create_spark_session
from pyspark.sql.types import *


spark = create_spark_session("io")


# Use the DataFrameReader interface to read a CSV file
def read_dataset_into_df(path, format="parquet", schema=None):
    if schema is None:
        df = (
            spark.read.format(format)
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "FAILFAST")
            .load(path)
        )
    else:
        df = (
            spark.read.format(format)
            .option("header", True)
            .option("mode", "FAILFAST")
            .schema(schema)
            .load(path)
        )
    return df


def write_dataset(df, mode, path, table_name=None):
    if mode == "file":
        df.write.format("parquet").mode("overwrite").option(
            "compression", "snappy"
        ).save(path)

    elif mode == "table":
        if table_name is None:
            raise ValueError("table name must be specified for saving as sql table")
        (df.write.mode("overwrite").option("path", path).saveAsTable(table_name))


# Main program
if __name__ == "__main__":

    # parquet
    file = "datasets/flights/summary-data/parquet/2010-summary.parquet/"
    df = read_dataset_into_df(file)
    # create temp table in memory
    df.createOrReplaceTempView("Data")
    spark.sql(
        "SELECT * FROM data WHERE ORIGIN_COUNTRY_NAME LIKE '%States%' AND  DEST_COUNTRY_NAME='France'"
    ).show()
    df.unpersist()

    # csv
    file = "datasets/flights/summary-data/csv/*"
    df = read_dataset_into_df(file, "csv")
    df.show(10)

    # json
    file = "datasets/flights/summary-data/json/*"
    df = read_dataset_into_df(file, "json")
    df.show(10)

    # read with schema specified and inferred schema set to false

    schema = StructType(
        [
                StructField("DEST_COUNTRY_NAME", StringType(), True),
                StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
                StructField("count", LongType(), True),
        ]
    )
    df = read_dataset_into_df(file, "json", schema=schema)
    df.show(10)

    # use DataFrameWriter interface to save dataframe as parquet file
    write_dataset(df, mode="file", path="/tmp/data/parquet/df_parquet")
    # or sql table
    write_dataset(
        df,
        mode="table",
        path="datasets/flights/spark-warehouse/parquet",
        table_name="us_delay_flights_tbl",
    )
