from spark_session import create_spark_session
from pyspark.sql.functions import udf, pandas_udf, col
from pyspark.sql.types import LongType
import pandas as pd
from timer import Timer
from dataframe_api.io import read_dataset_into_df

spark = create_spark_session("optimiser")


@udf(LongType())
def multiply(v: pd.Series, s: pd.Series) -> pd.Series:
    return v * s


@pandas_udf(LongType())
def pandas_multiply(v: pd.Series, s: pd.Series) -> pd.Series:
    return v * s


if __name__ == "__main__":
    file = "datasets/iot-devices/iot_devices.json"
    df = read_dataset_into_df(file, "json")

    print("\n Using pandas udf:")
    with Timer():
        df.select(
            "temp",
            "humidity",
            pandas_multiply(col("temp"), col("humidity")).alias("multiply"),
        ).show()

    print("\n Using normal udf:")
    with Timer():
        df.select(
            "temp", "humidity", multiply(col("temp"), col("humidity")).alias("multiply")
        ).show()
