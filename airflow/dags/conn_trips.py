import os
import pathlib
from airflow import DAG
from datetime import datetime
from google.cloud import bigquery
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f"{pathlib.Path(__file__).parent.resolve()}/credential.json"


#ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = "marcelo-jobsity-challenge" #os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

DAG_ID = "trips_pipeline"

REPOSITORY_ID = "data-engineering-challenge"
REGION = "us-east1"
WORKSPACE_ID = "trips"


def update_fail_status(context):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of table to append to.
    table_id = "marcelo-jobsity-challenge.pipeline_status.status"

    rows_to_insert = [
        {"status": "Failed", "updated_at": f"{datetime.now()}"}
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def update_success_status():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of table to append to.
    table_id = "marcelo-jobsity-challenge.pipeline_status.status"

    rows_to_insert = [
        {"status": "Succeeded", "updated_at": f"{datetime.now()}"}
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


targets = [
    {
        "database": PROJECT_ID,
        "schema": "raw",
        "name": "raw_trips"
    }, 
    {
        "database": PROJECT_ID,
        "schema": "curated",
        "name": "cur_trips"
    }
]


dag = DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    on_failure_callback=update_fail_status,
    tags=[ "dataform"],
)

create_compilation_result = DataformCreateCompilationResultOperator(
    task_id="create-compilation-result",
    project_id=PROJECT_ID,
    region=REGION,
    repository_id=REPOSITORY_ID,
    compilation_result={
        "git_commitish": "trips",
        "workspace": (
            f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
            f"workspaces/{WORKSPACE_ID}"
        ),
    },
    dag=dag
)

get_compilation_result = DataformGetCompilationResultOperator(
    task_id="get-compilation-result",
    project_id=PROJECT_ID,
    region=REGION,
    repository_id=REPOSITORY_ID,
    compilation_result_id=(
        "{{ task_instance.xcom_pull('create-compilation-result')['name'].split('/')[-1] }}"
    ),
    dag=dag
)

create_workflow_invocation_raw = DataformCreateWorkflowInvocationOperator(
    task_id=f"create-workflow-invocation-{targets[0]['name']}",
    project_id=PROJECT_ID,
    region=REGION,
    repository_id=REPOSITORY_ID,
    workflow_invocation={
        "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}",
        "invocation_config": {
            "included_targets": [
                targets[0]
            ],
        }
    },
    dag=dag
)

create_workflow_invocation_cur = DataformCreateWorkflowInvocationOperator(
    task_id=f"create-workflow-invocation-{targets[1]['name']}",
    project_id=PROJECT_ID,
    region=REGION,
    repository_id=REPOSITORY_ID,
    workflow_invocation={
        "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}",
        "invocation_config": {
            "included_targets": [
                targets[1]
            ],
        }
    },
    dag=dag
)

success_update = PythonOperator(
        task_id=f"success_bigquery_update",
        python_callable=update_success_status,
        provide_context=True,
        dag=dag
    )

create_compilation_result >> get_compilation_result >> create_workflow_invocation_raw >> create_workflow_invocation_cur >> success_update