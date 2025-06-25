from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import boto3
import pytz
import time
import logging


# DAG defaults
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1), 
}

REGION = "eu-west-1"
SNS_TOPIC_ARN = "arn:aws:sns:eu-west-1:371439860588:music-pipeline-alerts"
S3_BUCKET = "lab3-dynamo-glue-etl"

logger = logging.getLogger("music_pipeline")
logger.setLevel(logging.INFO)

def notify_sns(**kwargs):
    status = kwargs.get("status", "UNKNOWN")
    message = kwargs.get("message", "")
    subject = f"Music Pipeline ETL - {status}"
    boto3.client("sns", region_name=REGION).publish(
        TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message
    )

def generate_runtime_context(**kwargs):
    utc_now = datetime.now(pytz.utc)
    execution_date = utc_now.date().isoformat()
    execution_timestamp = utc_now.isoformat()
    kwargs['ti'].xcom_push(key='execution_date', value=execution_date)
    kwargs['ti'].xcom_push(key='execution_timestamp', value=execution_timestamp)

def run_glue_job_with_wait(job_name, script_args=None, **kwargs):
    glue = boto3.client("glue", region_name=REGION)
    logger.info(f"Starting Glue job: {job_name}")
    response = glue.start_job_run(JobName=job_name, Arguments=script_args or {})
    job_run_id = response['JobRunId']

    while True:
        job_status = glue.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
        logger.info(f"Status for {job_name}: {job_status}")
        if job_status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(30)

    if job_status != 'SUCCEEDED':
        raise Exception(f"Glue job {job_name} failed. Check CloudWatch.")

def archive_validated_files(**kwargs):
    execution_date = kwargs['ti'].xcom_pull(task_ids='generate_execution_context', key='execution_date')
    s3 = boto3.resource("s3", region_name=REGION)
    prefix = f"validated/processed_date={execution_date}/"
    archive_prefix = f"archive/validated/processed_date={execution_date}/"
    bucket = s3.Bucket(S3_BUCKET)

    for obj in bucket.objects.filter(Prefix=prefix):
        dest_key = obj.key.replace("validated/", "archive/validated/", 1)
        s3.Object(S3_BUCKET, dest_key).copy_from(CopySource={"Bucket": S3_BUCKET, "Key": obj.key})
        s3.Object(S3_BUCKET, obj.key).delete()
    logger.info("Archival completed.")

with DAG(
    dag_id="music_pipeline_etl",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["music", "glue", "etl"],
) as dag:

    generate_context = PythonOperator(
        task_id="generate_execution_context",
        python_callable=generate_runtime_context
    )

    wait_for_stream_file = S3KeySensor(
        task_id="wait_for_stream_file",
        bucket_key="raw/streams/{{ ds }}/streams1.csv",
        bucket_name=S3_BUCKET,
        aws_conn_id="aws_default",
        timeout=60 * 30,
        poke_interval=60,
        mode="poke"
    )

    extract_validate_join = PythonOperator(
        task_id="extract_validate_join_streams",
        python_callable=run_glue_job_with_wait,
        op_kwargs={
            "job_name": "music-pipeline-extract-validate-join-streams",
            "script_args": {
                "--execution_timestamp": "{{ ti.xcom_pull(task_ids='generate_execution_context', key='execution_timestamp') }}"
            }
        }
    )

    compute_metrics = PythonOperator(
        task_id="compute_metrics",
        python_callable=run_glue_job_with_wait,
        op_kwargs={
            "job_name": "music-pipeline-compute-metrics",
            "script_args": {
                "--input_validated_streams_path": f"s3://{S3_BUCKET}/validated",
                "--output_metrics_base_path": f"s3://{S3_BUCKET}/metrics",
                "--job_status_output_path": f"s3://{S3_BUCKET}/job-status",
                "--execution_timestamp": "{{ ti.xcom_pull(task_ids='generate_execution_context', key='execution_timestamp') }}"
            }
        }
    )

    archive_validated = PythonOperator(
        task_id="archive_validated_files",
        python_callable=archive_validated_files,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    load_to_dynamodb = PythonOperator(
        task_id="load_to_dynamodb",
        python_callable=run_glue_job_with_wait,
        op_kwargs={
            "job_name": "music-pipeline-load-to-dynamodb",
            "script_args": {
                "--input_base_path": f"s3://{S3_BUCKET}/metrics/daily_genre_metrics",
                "--dynamodb_table_name": "music_genre_metrics",
                "--job_status_output_path": f"s3://{S3_BUCKET}/job-status",
                "--execution_date": "{{ ti.xcom_pull(task_ids='generate_execution_context', key='execution_date') }}"
            }
        }
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_sns,
        op_kwargs={
            "status": "SUCCESS",
            "message": "Music pipeline completed successfully."
        },
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_sns,
        op_kwargs={
            "status": "FAILURE",
            "message": "Music pipeline failed. Check MWAA logs."
        },
        trigger_rule=TriggerRule.ONE_FAILED
    )

    generate_context >> wait_for_stream_file >> extract_validate_join >> compute_metrics >> archive_validated >> load_to_dynamodb
    load_to_dynamodb >> notify_success
    [extract_validate_join, compute_metrics, archive_validated, load_to_dynamodb] >> notify_failure
