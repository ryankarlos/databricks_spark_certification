from spark_session import create_spark_session
from pyspark.sql import Row
from pyspark.sql.types import *

# Create a SparkSession
spark = create_spark_session("rows")

schema = StructType(
    [
        StructField("Author", StringType(), False),
        StructField("State", StringType(), False),
    ]
)

rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, schema)
authors_df.show()
