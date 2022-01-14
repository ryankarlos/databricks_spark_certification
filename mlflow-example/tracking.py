import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow
import mlflow.spark
import pandas as pd
import sys

# increase spark driver memory (default 1gb) to avoid heap space warnings
spark = (
    SparkSession.builder.appName("mlflow")
    .config("spark.sql.execution.arrow.pyspark.enabled", True)
    .config("spark.driver.memory", "16G")
    .config("spark.ui.showConsoleProgress", True)
    .config("spark.sql.repl.eagerEval.enabled", True)
    .getOrCreate()
)
# avoid str truncation warning
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

filePath = str(sys.argv[1])
params = {
    "maxBins": float(sys.argv[2]),
    "maxDepth": float(sys.argv[3]),
    "numTrees": float(sys.argv[4]),
}


def read_and_train_test_split(spark, filePath):
    airbnbDF = spark.read.parquet(filePath)
    return airbnbDF.randomSplit([0.8, 0.2], seed=42)


def processing_cols(trainDF):
    categoricalCols = [
        field for (field, dataType) in trainDF.dtypes if dataType == "string"
    ]
    indexOutputCols = [x + "Index" for x in categoricalCols]
    numericCols = [
        field
        for (field, dataType) in trainDF.dtypes
        if ((dataType == "double") & (field != "price"))
    ]
    assemblerInputs = indexOutputCols + numericCols
    vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    stringIndexer = StringIndexer(
        inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip"
    )
    return vecAssembler, stringIndexer


def define_rf_model(**params):
    rf = RandomForestRegressor(labelCol="price", seed=42, **params)
    mlflow.log_param("num_trees", rf.getNumTrees())
    mlflow.log_param("max_depth", rf.getMaxDepth())
    return rf


def train_pipeline(trainDf, **params):
    vecAssembler, stringIndexer = processing_cols(trainDf)
    rf = define_rf_model(**params)
    pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])
    pipelineModel = pipeline.fit(trainDF)
    mlflow.spark.log_model(pipelineModel, "model")
    return pipelineModel, vecAssembler


def feature_importance(pipelineModel, vecAssembler):
    rfModel = pipelineModel.stages[-1]
    pandasDF = pd.DataFrame(
        list(zip(vecAssembler.getInputCols(), rfModel.featureImportances)),
        columns=["feature", "importance"],
    ).sort_values(by="importance", ascending=False)
    pandasDF.to_csv("results/feature-importance.csv", index=False)
    # Log artifact: feature importance scores.
    # First write to local filesystem, then tell MLflow where to find that file
    mlflow.log_artifact("results/feature-importance.csv")
    return pandasDF


def serve_evaluate(pipelineModel):
    predDF = pipelineModel.transform(testDF)
    regressionEvaluator = RegressionEvaluator(
        predictionCol="prediction", labelCol="price"
    )
    rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
    r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)

    mlflow.log_metrics({"rmse": rmse, "r2": r2})
    return predDF


with mlflow.start_run(run_name="random-forest") as run:
    trainDF, testDF = read_and_train_test_split(spark, filePath)
    pipelineModel, vecAssembler = train_pipeline(trainDF, **params)
    pandasDF = feature_importance(pipelineModel, vecAssembler)
    predDF = serve_evaluate(pipelineModel)
