from spark_session import create_spark_session
from pyspark.sql.functions import *
from timer import Timer

spark = create_spark_session("join")

emp = [
    (1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "M", 4000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1),
]
empColumns = [
    "emp_id",
    "name",
    "superior_emp_id",
    "year_joined",
    "emp_dept_id",
    "gender",
    "salary",
]

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
deptColumns = ["dept_name", "dept_id"]


def spark_join(df1, df2, how, on, join_type):
    """
    Joins small table with big table by avoiding costly data shuffling.
    The table which is less than ~10MB(default threshold value) is broadcasted
    across all the nodes in cluster, such that this table becomes lookup to that
    local node in the cluster which avoids shuffling.
    It has two phases-
    1. Broadcast – smaller dataset is cached across the executors in the cluster.
    2. Hash Join– Where a standard hash join performed on each executor.

    Shuffle sort merge is Spark’s default join strategy. Spark performs this join
    when you are joining two BIG tables,Sort Merge Joins minimize data movements in the cluster,
    highly scalable approach
    Three phases of sort Merge Join –
    1. Shuffle Phase : The 2 big tables are repartitioned as per the join keys across the
    partitions in the cluster.
    2. Sort Phase: Sort the data within each partition in parallel.
    3. Merge Phase: Join the 2 Sorted and partitioned data. This is basically merging of dataset
    by iterating over the elements and joining the rows having the same value for the join key.
    """
    if join_type == "broadcast_hash":
        joinedDF = df1.join(broadcast(df2), how=how, on=on)
    elif join_type == "sort_merge":
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        joinedDF = df1.join(df2, how=how, on=on)

    return joinedDF


if __name__ == "__main__":
    empDF = spark.createDataFrame(data=emp, schema=empColumns)
    empDF.printSchema()
    empDF.show(truncate=False)
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    deptDF.printSchema()
    deptDF.show(truncate=False)

    with Timer():
        joined_df = spark_join(
            empDF,
            deptDF,
            how="inner",
            on=empDF.emp_dept_id == deptDF.dept_id,
            join_type="broadcast_hash",
        )
    joined_df.explain("formatted")
    joined_df.show(10)

    with Timer():
        joined_df = spark_join(
            empDF,
            deptDF,
            how="inner",
            on=empDF.emp_dept_id == deptDF.dept_id,
            join_type="sort_merge",
        )
    joined_df.explain("formatted")
    joined_df.show(10)
