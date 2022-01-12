import findspark

findspark.init()
findspark.find()

# In Python, define a schema
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Programmatic way to define a schema

# Create a SparkSession
spark = SparkSession.builder.appName("io").getOrCreate()

fire_schema = StructType(
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
sf_fire_file = "e:/datasets/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
fire_df.show(10)


# use DataFrameWriter interface to save dataframe as parquet file or sql table

parquet_path = "e:/datasets/sf-fire/fire-calls"
fire_df.write.format("parquet").save(parquet_path)

parquet_table = "fire_calls"
fire_df.write.format("parquet").saveAsTable(parquet_table)
