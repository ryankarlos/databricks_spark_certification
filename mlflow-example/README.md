# mlflow-project-example

From the command line, cd to the mlflow-project-example folder and run:
* mlflow run .

You can check the tracking ui by running the following command and navigating to tracking url:
* mlflow serve

or to set DB (e.g. sqlite) as backend-store:
* mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./artifacts
