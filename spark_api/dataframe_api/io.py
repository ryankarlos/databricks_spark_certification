from spark_session import create_spark_session
from pyspark.sql.types import *

spark = create_spark_session("io")

schema = StructType(
    [
        StructField("CallNumber", IntegerType(), True),
        StructField("UnitID", StringType(), True),
        StructField("IncidentNumber", IntegerType(), True),
        StructField("CallType", StringType(), True),
        StructField("CallDate", StringType(), True),
        StructField("WatchDate", StringType(), True),
        StructField("CallFinalDisposition", StringType(), True),
        StructField("AvailableDtTm", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("ZipcodeofIncident", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("StationArea", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("OrigPriority", StringType(), True),
        StructField("Priority", StringType(), True),
        StructField("FinalPriority", IntegerType(), True),
        StructField("ALSUnit", BooleanType(), True),
        StructField("CallTypeGroup", StringType(), True),
        StructField("NumAlarms", IntegerType(), True),
        StructField("UnitType", StringType(), True),
        StructField("UnitSequenceInCallDispatch", IntegerType(), True),
        StructField("FirePreventionDistrict", StringType(), True),
        StructField("SupervisorDistrict", StringType(), True),
        StructField("Neighborhood", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("RowID", StringType(), True),
        StructField("Delay", FloatType(), True),
    ]
)


# Use the DataFrameReader interface to read a CSV file
def read_dataset_into_df(path, format="parquet", infer_schema=True, schema=None):
    if infer_schema:
        df = (
            spark.read.format(format)
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "FAILFAST")
            .load(path)
        )
    else:
        assert schema is not None
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
        df.write.format("parquet")\
            .mode("overwrite")\
            .option("compression", "snappy")\
            .save(path)

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
    spark.sql("SELECT * FROM data WHERE ORIGIN_COUNTRY_NAME LIKE '%States%' AND  DEST_COUNTRY_NAME='France'").show()
    df.unpersist()

    # csv
    file = "datasets/flights/summary-data/csv/*"
    df = read_dataset_into_df(file, "csv")
    df.show(10)

    # json
    file = "datasets/flights/summary-data/json/*"
    df = read_dataset_into_df(file, "json")
    df.show(10)

    # use DataFrameWriter interface to save dataframe as parquet file
    write_dataset(df, mode="file", path="/tmp/data/parquet/df_parquet")
    # or sql table
    write_dataset(df, mode="table", path="datasets/flights/spark-warehouse/parquet", table_name="us_delay_flights_tbl")
