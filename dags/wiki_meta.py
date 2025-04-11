from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.timezone import datetime

DAG_ID = "wiki_meta"

def check_date(**kwargs):
    import pendulum
    ds_nodash = kwargs["data_interval_start"] \
        .in_timezone("Asia/Seoul") \
        .strftime("%Y%m%d")
    
    if ds_nodash in ["20240110", "20240111"]:
        return "end"
    else:
        return "merge_data"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="wiki data processing",
    schedule="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 4, 1),
    catchup=True,
    tags=["spark", "submit", "wiki"],
) as dag:
    SPARK_HOME = "/Users/joon/swcamp4/app/spark-3.5.1-bin-hadoop3"
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    validate_data = BranchPythonOperator(
        task_id="validate_data",
        python_callable=check_date
    )
    
    merge_data = BashOperator(
        task_id="merge_data",
        bash_command="""
            echo {{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m%dT%H%M%S') }}
            ssh -i ~/.ssh/gcp-joon-key joon@34.64.166.10 \
            "/home/joon/code/wiki/meta/run.sh {{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m%dT%H%M%S') }} /home/joon/code/wiki/meta/meta.py"
        """
    )
    
    start >> validate_data >> [merge_data, end]
    merge_data >> end