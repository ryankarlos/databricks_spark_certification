from spark_session import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = create_spark_session("complex_types")

schema = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("email", StringType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("items", ArrayType(StringType(), True), True),
    ]
)

data = [
    [
        257437,
        "mra@random.com",
        1995,
        [None, "M_PREM_K", "Premium King Mattress", 1995, 1995, 1],
    ],
    [
        282611,
        "mrb@random.com",
        940,
        ["NEWBED10", "M_STAN_F", "Standard Foam Pillow", 940, 1045, 1],
    ],
    [
        257490,
        "mrb@random.com",
        1500,
        [None, "M_STAN_K", "Standard King Mattress", 1500, 1500, 1],
    ],
    [
        257448,
        "mrc@random.com",
        945,
        [None, "M_STAN_F", "Standard Full Mattress", 945, 945, 1],
    ],
    [
        257470,
        "mrc@random.com",
        500,
        [None, "M_STAN_D", "Standard Down Pillow", 500, 500, 1],
    ],
    [
        257449,
        "mrd@random.com",
        945,
        [None, "M_STAN_F", "Standard Foam Pillow", 945, 945, 1],
    ],
    [
        257450,
        "mrd@random.com",
        2000,
        [None, "M_PREM_T", "Premium Twin Mattress", 2000, 2000, 1],
    ],
]

df = spark.createDataFrame(data, schema=schema)

df.show(2)


# explode (opposite of this is collect_set)
df.withColumn("items", explode("items")).select("email", "items").show()

# split string
detailsDF = df.withColumn("item_name", col("items").getItem(2)).drop("items")
detailsDF = detailsDF.withColumn("details", split(col("item_name"), " "))
detailsDF.show()

# array contains and element at - filter standard size
standardDF = (
    detailsDF.filter(array_contains(col("details"), "Standard"))
    .withColumn("type", element_at(col("details"), 3))
    .withColumn("quality", element_at(col("details"), 2))
)
standardDF.show()

# collect_set - group by user and collect items type and quality in array for user
optionsDF = standardDF.groupBy("email").agg(
    collect_set("type").alias("type options"),
    collect_set("quality").alias("quality options"),
)
optionsDF.show()
