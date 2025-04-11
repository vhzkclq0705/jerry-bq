from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonVirtualenvOperator

def check_bq_table_exists(**kwargs):
    ds = kwargs['ds']
    ds_nodash = ds.replace('-', '')

    table_name = f"t{ds_nodash}"
    full_table_id = f"wiki-455500.temp.{table_name}"

    from google.cloud import bigquery
    client = bigquery.Client()

    try:
        table = client.get_table(full_table_id)
        if table.num_rows > 0:
            return 'skip.task'
        else:
            return 'load.wiki'
    except Exception as e:
        return 'load.wiki'

DAG_ID = "wiki_bq_load"

with DAG(
    DAG_ID,
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 4, 8),
    catchup=True,
    tags=['bigquery', 'wiki', 'load'],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    check_table = BranchPythonVirtualenvOperator(
        task_id="check.table",
        python_callable=check_bq_table_exists,
        requirements=['google-cloud-bigquery'],
        system_site_packages=False
    )

    skip_task = EmptyOperator(task_id="skip.task")

    load_wiki = BashOperator(
        task_id="load.wiki",
        bash_command="""
        bq load \
        --source_format=PARQUET \
        temp.t{{ ds_nodash }} \
        'gs://jerry-wiki-bucket/wiki/parquet/ko/date={{ ds }}/*.parquet'
        """
    )

    start >> check_table >> [load_wiki, skip_task] >> end