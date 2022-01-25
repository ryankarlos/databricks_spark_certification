from mlflow.tracking import MlflowClient
from mlflow import run


client = MlflowClient()
runs = client.search_runs(
    run.info.experiment_id, order_by=["attributes.start_time desc"], max_results=1
)
run_id = runs[0].info.run_id
runs[0].data.metrics
