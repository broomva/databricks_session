# import dependencies and define hard coded variables
import argparse
import glob
import json
import os
import time
from pathlib import Path

import requests
from pydantic import BaseModel

# COMMAND ----------

FILE_EXTENSION = ".json"
API_VERSION = "2.1"
NOTEBOOK_BASE_PATH = "/Shared"


class DatabricksJobsSettings(BaseModel):
    """
    This class defines the settings for a Databricks job.
    """
    name: str
    email_notifications: dict
    timeout_seconds: int
    max_concurrent_runs: int
    tasks: list
    run_as: dict
    job_clusters: list
    task_config_template = {
            "task_key": 'default_task',
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "/Shared",
                "source": "WORKSPACE",
            },
            "job_cluster_key": "default_job_cluster",
            "timeout_seconds": 0,
            "max_retries": 2,
            "min_retry_interval_millis": 60000,
            "retry_on_timeout": True,
            "email_notifications": {
                    # "on_failure": [""]
                },
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False,
            },
            "webhook_notifications": {},
        }
    job_config = {
            "name": 'default_job_name',
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [task_config_template],
            # "run_as": {"user_name": "default_user_name"},
            "job_clusters": [
                {
                    "job_cluster_key": "default_job_cluster",
                    "new_cluster": {
                        "cluster_name": "",
                        "spark_version": "13.3.x-scala2.12",
                        "azure_attributes": {
                            "first_on_demand": 1,
                            "availability": "SPOT_WITH_FALLBACK_AZURE",
                            "spot_bid_max_price": -1,
                        },
                        "node_type_id": "Standard_DS3_v2",
                        "spark_env_vars": {
                            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                        },
                        "enable_elastic_disk": True,
                        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                        "runtime_engine": "PHOTON",
                        "autoscale": {"min_workers": 1, "max_workers": 4},
                    },
                }
            ],
        }


class DatabricksJobsCreator():
    """
    This class allows creating .json files from dependency files for Databricks jobs.
    """
    def __init__(self):
        """
        Initializes the DatabricksJobsCreator object.
        """
        self.settings = DatabricksJobsSettings()

    def create_task_config(self, table_name, dependencies, job_name):
        """
        Creates a task configuration for a given table.

        :param table_name: Name of the table (will be used as task_key).
        :param dependencies: Dictionary of table dependencies.
        :return: A dictionary representing the task configuration.
        """
        task_config = self.settings.task_config_template.copy()
        task_config["task_key"] = table_name
        task_config["notebook_task"]["notebook_path"] = f"{NOTEBOOK_BASE_PATH}/{table_name}"
        task_config["job_cluster_key"] = f"{job_name}_job_cluster"

        # Add dependencies to the task
        task_config["depends_on"] = [
            {"task_key": dep} for dep in dependencies.get(table_name, [])
        ]
        return task_config


    def create_job_config(self, dependencies, job_name):
        """
        Creates a job configuration based on table dependencies.

        :param dependencies: Dictionary of table dependencies.
        :return: A dictionary representing the job configuration.
        """
        tasks = [
            self.create_task_config(table, dependencies, job_name) for table in dependencies
        ]
        job_config = self.settings.job_config.copy()
        job_config["name"] = job_name
        job_config["tasks"] = tasks
        job_config["job_clusters"][0]["job_cluster_key"] = f"{job_name}_job_cluster"
        return job_config


    def create_jobs(self, input_folder, output_folder):
        """
        Processes each JSON file in the input folder, generates job configurations,
        and writes them to the output folder.

        :param input_folder: Path to the folder containing dependency JSON files.
        :param output_folder: Path to the folder where output files will be written.
        """
        # Ensure the output folder exists
        os.makedirs(output_folder, exist_ok=True)

        # Process each .json file individually
        for file in glob.glob(f"{input_folder}/*.json"):
            with open(file, "rb") as f:
                dependencies = json.load(f)
            job_name = os.path.splitext(os.path.basename(file))[0]

            # Define the output path in the output folder
            output_file_name = job_name + ".json"
            output_path = os.path.join(output_folder, output_file_name)

            # Create the job configuration
            job_config = self.create_job_config(dependencies, job_name)

            # Convert the job configuration to JSON
            job_config_json = json.dumps(job_config, indent=4)

            # Write the job configuration to the file
            with open(output_path, "w") as output_file:
                output_file.write(job_config_json)

            print(f"Config for {output_file_name} written to {output_path} \n")


# COMMAND ----------

class DatabricksAPI:
    """
    This in an interface to the Databricks API.
    """
    def __init__(self, environment: str = "dev", domain: str = "", token: str = ""):
        """
        Initializes the ADB object with the provided parameters.

        :param environment: The environment to work on. Defaults to 'dev'.
        :param domain: The Databricks domain to connect to. Defaults to an empty string.
        :param token: The Databricks token for API requests. Defaults to an empty string.
        """
        self.domain = domain
        self.environment = environment
        self.headers = {"Authorization": f"Bearer {token}"}

    def send_request(
        self, method: str, url: str, params: dict = None, data: dict = None
    ) -> dict:
        """
        Sends a request to the Databricks API and returns the response as a JSON object.

        :param method: The HTTP method ('GET', 'POST', etc.).
        :param url: The URL to send the request to.
        :param params: Additional URL parameters. Defaults to None.
        :param data: The body of the request (for 'POST', 'PUT', etc.). Defaults to None.
        :return: The response from the Databricks API as a JSON object.
        :raises Exception: If the request fails for any reason.
        """
        try:
            response = requests.request(
                method,
                url,
                headers=self.headers,
                params=params,
                data=json.dumps(data, indent=4) if data is not None else data,
                timeout=10,
            )  # timeout at 10 seconds
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise requests.exceptions.RequestException(
                f"An error occurred: {err} \n {response.text}"
            ) from err
        return response.json()

# COMMAND ----------

class DatabricksWorkflows(DatabricksAPI):
    """
    This class allows creating and managing workflows in the Databricks workspace.

    It extends the ADB class to provide specific methods for managing Databricks workflows.
    """

    def __init__(
        self,
        environment: str = "dev",
        domain: str = "",
        token: str = "",
        out_path: str = "Pipelines",
        in_path: str = "Dependencies",
    ):
        """
        Initializes the ADBWorkflows object with the provided parameters.

        :param environment: The environment to work on. Defaults to 'dev'.
        :param domain: The Databricks domain to connect to. Defaults to an empty string.
        :param token: The Databricks token for API requests. Defaults to an empty string.
        :param path: The local path to the workflow files. Defaults to an empty string.
        """
        super(DatabricksWorkflows, self).__init__(environment, domain, token)
        self.__out_path = Path(out_path)
        self.__in_path = Path(in_path)

    def __get_all_jobs(self, offset: int = 0, limit: int = 20) -> dict:
        """
        Fetches a list of jobs from the Databricks API with the provided offset and limit.

        :param offset: The offset to start listing jobs from. Defaults to 0.
        :param limit: The maximum number of jobs to return. Defaults to 20.
        :return: A dictionary with details of the jobs or None if an error occurred.
        """
        url = f"{self.domain}/api/{API_VERSION}/jobs/list"

        params = {"offset": offset, "limit": limit}

        try:
            response = self.send_request("GET", url, params=params)
        except Exception as e:
            print(f"Error fetching jobs: {e}")
            return None

        if "jobs" not in response:
            print("Unexpected response format. 'jobs' key not found.")
            return None

        return response

    def list_all_jobs(self) -> list:
        """
        Lists all jobs in the Databricks workspace.

        This method retrieves all jobs using the `__get_all_jobs` method with pagination.
        It uses a CustomSpinner to show the progress of the job retrieval.

        :return: A list of all jobs in the Databricks workspace.
        """
        offset = 0
        limit = 20
        jobs_list = []

        total_jobs = 0
        while True:
            jobs = self.__get_all_jobs(offset, limit)

            if jobs is None:
                break

            jobs_list.extend(jobs["jobs"])
            total_jobs += len(jobs["jobs"])

            if not jobs["has_more"]:
                break

            offset += limit
            time.sleep(1)

        return jobs_list

    def __find_existing_job(self, existing_jobs: list, job) -> int:
        """
        Finds a job in the list of existing jobs.

        :param existing_jobs: The list of existing jobs.
        :param job: The job to find.
        :return: The index of the found job in the list, or None if it was not found.
        :raises ValueError: If `existing_jobs` is None or not a list.
        """
        return next(
            (
                i
                for i, j in enumerate(existing_jobs)
                if j["settings"]["name"] == job['name']
            ),
            None,
        )

    def __update_existing_job(self, job_id: int, job) -> None:
        """
        Updates an existing job in the Databricks workspace.

        :param job_id: The ID of the job to update.
        :param job: The new job data.
        """
        url = f"{self.domain}/api/{API_VERSION}/jobs/reset"
        data = {"job_id": job_id, "new_settings": dict(job)}
        print(f"Updating job {job_id} \n")
        res = self.send_request("POST", url, data=data)
        if res is None:
            print(f"Job {job_id} returned: {res} with settings: {data}")
        return res

    def __create_new_job(self, job) -> None:
        """
        Creates a new job in the Databricks workspace.

        :param job: The job data to create.
        """
        url = f"{self.domain}/api/{API_VERSION}/jobs/create"
        data = dict(job)
        print(f"Creating new job for {data['name']}")
        res = self.send_request("POST", url, data=data)
        print(f"Created with job_id: {res['job_id']} \n")
        if res is None:
            print(f"Job returned: {res} with settings: {data}")
        return res

    def write_jobs(self) -> None:
        """
        Writes jobs to the Databricks workspace.

        This method reads job definitions from Python files in the specified path,
        then creates new jobs or updates existing ones in the Databricks workspace.
        """
        existing_jobs = self.list_all_jobs()
        # Create Jobs from Dependencies files
        print("Creating Jobs from Dependencies ------------------------- \n")
        DatabricksJobsCreator().create_jobs(input_folder=self.__in_path, output_folder=self.__out_path)

        files = list(self.__out_path.glob(f"*{FILE_EXTENSION}"))
        if files == []:
            print(f"Files not found in {self.__out_path}")
        else:
            #print(files)
            print("Deploying to Databricks API ------------------------- \n")
            for file in files:
                with open(file, "rb") as f:
                    job = json.load(f)
                existing_job_index = self.__find_existing_job(existing_jobs, job)
                if existing_job_index is not None:
                    print(f"Updating workflow: {job['name']}")
                    res = self.__update_existing_job(
                        existing_jobs[existing_job_index]["job_id"], job
                    )
                else:
                    print(f"Creating workflow: {job['name']}")
                    res = self.__create_new_job(job)


# COMMAND ----------

def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Pass in domain and token')

    # Add the arguments
    parser.add_argument('--domain', type=str, required=True, help='The domain to use')
    parser.add_argument('--token', type=str, required=True, help='The token to use')
    parser.add_argument('--out_path', type=str, required=False, help='The path to write the jobs', default='Pipelines')
    parser.add_argument('--in_path', type=str, required=False, help='The path to read the dependencies', default='Dependencies')
    # Parse the arguments
    args = parser.parse_args()

    # Use the arguments
    adb = DatabricksWorkflows(
        domain=args.domain,
        token=args.token,
        out_path=args.out_path,
        in_path=args.in_path,
    )
    adb.write_jobs()

if __name__ == "__main__":
    main()
# python databricks_workflows.py --domain $(adb_domain) --token $(adb_token)