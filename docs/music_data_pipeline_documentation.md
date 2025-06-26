## Music Data Pipeline Project Documentation
1. Project Overview
This project implements an automated, serverless data pipeline designed to ingest, validate, process, and analyze daily music stream data. The primary objective is to transform raw streaming logs into actionable Key Performance Indicators (KPIs) and load them into a low-latency database for downstream consumption, such as analytical dashboards or applications.

Business Value:

Automated Insights: Provides daily, up-to-date music streaming metrics without manual intervention.

Data Quality: Incorporates validation steps to ensure the reliability of processed data.

Scalability: Leverages serverless AWS services (Glue, S3, DynamoDB) to handle varying data volumes.

Operational Efficiency: Automates ETL processes and provides clear orchestration via Airflow.

2. System Architecture
The pipeline leverages a combination of AWS services orchestrated by Apache Airflow, following a modern data lake architecture pattern.


3. Setup and Deployment
This section outlines the steps to set up the necessary AWS resources and deploy the Airflow DAG and Glue job scripts.

Prerequisites:

AWS Account with administrative access.

AWS CLI installed and configured.

Python 3.8+ installed locally (for boto3, pyarrow, etc.).

An Apache Airflow environment (e.g., Amazon MWAA, self-managed EC2 instance, Docker Compose).

Local project directory structured (with airflow/dags, glue_jobs, etc.).

3.1. AWS S3 Setup

Create a Primary S3 Bucket:

Create an S3 bucket (e.g., lab3-music-pipeline-with-airflow-and-glue-buck) in eu-west-1 region. This bucket will hold all your raw data, processed data, KPI outputs, archives, and Glue job scripts.

Crucial: Ensure the bucket policy allows the IAM roles associated with your Glue jobs and Airflow sufficient read/write access.

Define S3 Folder Structure:

Raw Stream Data:

Raw CSV files must be organized in a partitioned structure that Glue can infer.

Correct format: s3://<your-bucket>/data/raw/streams/year=YYYY/month=MM/day=DD/

Example: s3://lab3-music-pipeline-with-airflow-and-glue-buck/data/raw/streams/year=2025/month=06/day=23/stream_data_20250622_batch1_b7d8e6f1.csv

How to create this structure: When uploading files via the S3 console, specify year=YYYY/month=MM/day=DD/ in the "Destination folder" field. S3 will automatically create the apparent folder hierarchy. If you have existing data in YYYY/MM/DD/ format, use the restructure_s3.py script (provided in previous steps) to migrate it.

Processed/Validated Data:

s3://<your-bucket>/data/processed/validated_joined_streams_data/

KPI Output:

s3://<your-bucket>/data/processed/kpi_metrics/

Static Data (Users/Songs):

s3://<your-bucket>/data/processed/users_parquet/

s3://<your-bucket>/data/processed/songs_parquet/ (Ensure these are pre-processed and available in Parquet format.)

Glue Scripts:

s3://<your-bucket>/dags/glue_scripts/ (This is where the Glue Python scripts will be uploaded).

Job Status Logs:

s3://<your-bucket>/glue_job_logs/

Archive/Quarantine:

s3://<your-bucket>/quarantine/streams/

3.2. AWS Glue Setup

Create IAM Role for Glue Jobs (glue-etl-role-music-pipeline):

Navigate to IAM Console -> Roles.

Create a new role for Glue service (AWS Glue).

Attach the following policies:

AWSGlueServiceRole (Managed policy, provides broad Glue permissions)

AmazonS3FullAccess (For S3 read/write)

AmazonDynamoDBFullAccess (For DynamoDB write access from Glue)

CloudWatchLogsFullAccess (For Glue job logs)

Note down its ARN. This is GLUE_JOB_ROLE_ARN in your DAG.

Create IAM Role for Glue Crawler (AWSGlueServiceRole_Default or similar):

If you don't have a default Glue service role, create one. This role typically needs:

AWSGlueServiceRole

AmazonS3ReadOnlyAccess (or specific bucket access for data/raw/streams/)

CloudWatchLogsFullAccess

Verify this role is attached to your music_pipeline_streams_data_crawler.

Create Glue Database:

Go to Glue Console -> Data Catalog -> Databases.

Create a database (e.g., music_pipeline_db). This is where your crawler will register tables.

Create Glue Crawler (music_pipeline_streams_data_crawler):

Go to Glue Console -> Crawlers.

Create a new crawler:

Name: music_pipeline_streams_data_crawler

Data sources: Add S3 path s3://<your-bucket>/data/raw/streams/.

IAM role: Select the Glue Crawler IAM role you created/verified.

Output database: music_pipeline_db

Important: After setting up S3 partitioning and before running the DAG, manually run this crawler once to ensure it creates the streams table with year, month, day as partition keys.

Create AWS Glue Jobs:

For each of your Python scripts, create a corresponding Glue ETL job in the Glue Console.

music-pipeline-extract-validate-join-streams

Name: music-pipeline-extract-validate-join-streams

IAM role: glue-etl-role-music-pipeline

Type: Spark

Glue version: (Choose a recent Python 3 compatible version, e.g., Glue 3.0, 4.0, or 5.0).

Python library path (for pyarrow in the DynamoDB job): You might need to add s3://<your-bucket>/glue_libs/pyarrow.zip to the Python library path if pyarrow isn't included by default in your Glue version and you're not using a Python Shell job for the DynamoDB load.

Script path: s3://<your-bucket>/dags/glue_scripts/music-pipeline-extract-validate-join-streams.py

Worker type & DPU: As per your DAG, DPUs=5.

music-pipeline-compute-metrics

Follow similar steps for this job.

Name: music-pipeline-compute-metrics

Script path: s3://<your-bucket>/dags/glue_scripts/music-pipeline-compute-metrics.py

DPUs=5.

music-pipeline-load-to-dynamodb

Follow similar steps for this job.

Name: music-pipeline-load-to-dynamodb

Script path: s3://<your-bucket>/dags/glue_scripts/music-pipeline-load-to-dynamodb.py

DPUs=1.

3.3. AWS DynamoDB Setup

Create DynamoDB Table:

Go to DynamoDB Console -> Tables.

Create a table (e.g., MusicKpiMetrics).

Primary Key:

Partition key: metric_date (String, will store YYYY-MM-DD)

Sort key: track_genre (String)

Ensure its IAM role has dynamodb:PutItem, dynamodb:BatchWriteItem permissions.

3.4. AWS SNS Setup (for alerts)

Create SNS Topic:

Go to SNS Console -> Topics.

Create a Standard topic (e.g., music-pipeline-alerts).

Create a subscription (e.g., Email) to receive notifications.

3.5. Airflow Environment & DAG Deployment

Airflow Environment:

Set up your Airflow environment (e.g., Amazon MWAA).

Ensure your Airflow execution role has permissions to:

Trigger Glue jobs (glue:StartJobRun, glue:GetJobRun)

Interact with S3 (s3:GetObject, s3:ListBucket, s3:PutObject, s3:DeleteObject for various pipeline stages)

Publish to SNS (sns:Publish)

Access CloudWatch logs.

Upload Glue Job Scripts:

Upload your Python scripts (music-pipeline-extract-validate-join-streams.py, music-pipeline-compute-metrics.py, music-pipeline-load-to-dynamodb.py) to s3://<your-bucket>/dags/glue_scripts/.

Upload Airflow DAG:

Upload your music_kpi_pipeline.py file to your Airflow environment's DAGs folder (e.g., s3://<your-mwaa-bucket>/dags/).

3.6. Update DAG with Actual S3 Paths and Configuration

Before uploading your music_kpi_pipeline.py DAG, make sure you replace the placeholder values at the top of the file:

GLUE_SCRIPTS_S3_BUCKET = 's3://lab3-music-pipeline-with-airflow-and-glue-buck/dags/glue_scripts/' # Example
DATA_S3_BUCKET = 's3://lab3-music-pipeline-with-airflow-and-glue-buck/' # Example

4. Usage
Monitor Airflow UI: Access your Airflow UI. The music_kpi_pipeline DAG should appear.

Trigger the DAG:

You can wait for its scheduled run (schedule_interval='@daily or 15 minutes') or trigger it manually for testing.

When triggering manually, you can provide a logical date (e.g., 2025-06-23) if you want to backfill or process data for a specific day.

Observe Task Execution:

Monitor the DAG run in the Airflow Graph View.

Check task logs in Airflow for general status and progression.

For detailed Glue job execution logs (especially for failures), always refer to the specific Glue Job Run link provided in the Airflow task logs, which directs you to CloudWatch.

Verify Outputs:

Check your S3 bucket for processed data and KPI outputs.

Verify data in your DynamoDB MusicKpiMetrics table.

Check your SNS topic for failure notifications.

5. Troubleshooting
This section details common issues encountered during the development of this pipeline and their resolutions.

Broken DAG: [/path/to/dag.py] airflow.exceptions.DuplicateTaskIdFound: Task id '...' has already been added to the DAG

Cause: Two tasks in the same DAG have the exact same task_id.

Resolution: Meticulously review your DAG file. Ensure every task_id (e.g., for PythonOperator, GlueJobOperator, EmptyOperator) is unique within the DAG. This usually happens when refactoring or copying tasks without renaming IDs.

jinja2.exceptions.UndefinedError: 'ds_year' is undefined (in GlueJobOperator script_args)

Cause: Airflow's templating engine (Jinja) could not resolve ds_year, ds_month, or ds_day in the context where script_args were being rendered. This often occurs when Airflow's default date macros are used in complex nested structures or when certain operators don't expose them directly in the expected way.

Resolution: Replaced problematic macros with {{ data_interval_start.strftime('%Y') }} etc. This provides a more robust and consistently available context for date parsing, ensuring the year, month, and day are always passed correctly.

AttributeError: module 'pendulum' has no attribute 'sleep' (in _wait_for_and_get_s3_key)

Cause: The pendulum library, while excellent for date/time objects, does not have a sleep function. Python's built-in time module is used for pausing execution.

Resolution: Added import time at the top of the DAG file and changed pendulum.sleep(poke_interval) to time.sleep(poke_interval).

Error Category: RESOURCE_NOT_FOUND_ERROR; ... Entity Not Found (Service: Glue, Status Code: 400) (in extract_validate_join_streams_glue_job)

Cause: The AWS Glue Data Catalog table (e.g., streams) that the Glue job was trying to read from did not exist or could not be found. This happened because the Glue Crawler either didn't run, ran but couldn't infer a table, or the table was deleted.

Resolution:

Verify S3 Data Partitioning: The primary fix was to ensure raw data in S3 was partitioned correctly using the key=value/ convention (e.g., year=YYYY/month=MM/day=DD/). An auxiliary restructure_s3.py script was provided to migrate existing data.

Delete and Re-run Glue Crawler: If the streams table previously existed with incorrect partition names (partition_0, partition_1, etc.), it needed to be deleted from Glue Data Catalog. Then, the run_streams_data_crawler Airflow task (or manual crawler run) was executed to force Glue to re-discover the now-correctly-partitioned S3 data and create a new table with year, month, and day as partition keys.

Check Crawler Logs: Always verify the Glue Crawler's CloudWatch logs to ensure it successfully CREATED or UPDATED the expected table. If it runs successfully but doesn't mention table creation, it means it didn't find data to infer a schema from.

An error occurred (ConcurrentRunsExceededException) when calling the StartJobRun operation: Concurrent runs exceeded.

Cause: The specific AWS Glue job (music-pipeline-extract-validate-join-streams in this case) was already running or stuck in a "starting"/"stopping" state, preventing a new run from being initiated due to Glue's concurrency limits for a single job definition.

Resolution: Manually navigate to the AWS Glue Console -> Jobs -> [Your Job Name] -> History/Runs tab. Stop or terminate any active runs. Wait a few minutes for the status to clear before retrying the Airflow task.

Failed to connect to /IP_ADDRESS:PORT ... Connection refused (within Glue job's CloudWatch logs)

Cause: A Spark executor within the Glue job's cluster was unable to connect to the Spark driver on the specified IP and port. This is almost always a network configuration issue within your AWS environment, not an issue with the Glue script itself.

Resolution: This requires investigation of AWS network configurations:

Security Groups: Ensure the security group attached to your Glue job's network interfaces allows inbound/outbound traffic on the necessary ports (especially 43319 for the driver) between the driver and executor instances.

Network ACLs: Check if any Network ACLs are blocking traffic.

VPC Endpoints: If your Glue job runs in a private VPC, ensure VPC endpoints for S3, CloudWatch, and Glue are correctly configured and their security groups allow traffic from Glue.

Glue Job succeeds but no output/partial output (in S3 or DynamoDB)

Cause: The Glue job ran without crashing but either found no input data, or logic within the Python script filtered out all records, or the output path/permissions were incorrect.

Resolution:

Check Input Data: Verify the S3 input path for the Glue job contains actual data files (not just empty folders or _SUCCESS markers).

Review Glue Job Logs (CloudWatch): Look for INFO or WARNING messages within the Glue job's specific CloudWatch logs (not just Airflow logs) that indicate zero records processed, warnings about empty DataFrames, or issues writing to the output path. The write_job_status and write_validation_status functions in your scripts are designed to capture these scenarios.

Check Output Permissions: Ensure the IAM role for the Glue job has s3:PutObject and s3:ListBucket (and dynamodb:PutItem/BatchWriteItem for DynamoDB) permissions for the target output paths/tables.

By following this documentation, users should be able to understand the pipeline, set it up, run it, and effectively troubleshoot common issues.