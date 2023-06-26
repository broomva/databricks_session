from pydantic import BaseSettings
from typing import Optional
import os
from pydantic import validator

import re
import mlflow


class MLFlowSession(BaseSettings):
    deployment_client: Optional[object] = None

    class Config:
        env_file = ".env"

    def get_deployment_client(self, client_name: str):
        from mlflow.deployments import get_deploy_client

        self.deployment_client = get_deploy_client(client_name)
        return self.deployment_client


class SparkSession(BaseSettings):
    ...

    class Config:
        env_file = ".env"


class DatabricksSparkSession(SparkSession):
    databricks_token: str
    databricks_host: str
    databricks_cluster_id: str
    spark: Optional[object] = None

    def get_session(self):
        from databricks.connect import DatabricksSession

        connection_string = f"sc://{self.databricks_host}:443/;token={self.databricks_token};x-databricks-cluster-id={self.databricks_cluster_id}"
        self.spark = DatabricksSession.builder.remote(
            conn_string=connection_string
        ).getOrCreate()
        return self.spark


class DatabricksMLFlowSession(MLFlowSession):
    databricks_experiment_name: str = "mlflow_experiments"
    databricks_experiment_id: Optional[str] = None
    databricks_username: Optional[str] = None
    databricks_token: Optional[str] = None
    databricks_host: Optional[str] = None
    databricks_password: Optional[str] = databricks_token
    _mlflow_tracking_uri: Optional[str] = "databricks"

    @validator("databricks_host", pre=True, always=True)
    def check_https_pattern(cls, path):
        if not re.match(r"^https://", path):
            path = "https://" + path
        return path

    def get_session(self):
        os.environ["experiment_id"] = self.databricks_experiment_id
        # Set the Databricks credentials
        os.environ["DATABRICKS_HOST"] = self.databricks_host
        os.environ["DATABRICKS_TOKEN"] = self.databricks_token
        os.environ["MLFLOW_TRACKING_URI"] = self._mlflow_tracking_uri
        os.environ["DATABRICKS_USERNAME"] = self.databricks_username
        os.environ["DATABRICKS_PASSWORD"] = self.databricks_password

        # Set the tracking uri
        mlflow.set_tracking_uri(self._mlflow_tracking_uri)
        mlflow.set_experiment(self.databricks_experiment_name)

        return mlflow
