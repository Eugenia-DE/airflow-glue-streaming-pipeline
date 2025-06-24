## Project Summary: Automated Music Data Pipeline
This project aimed to build a robust and automated data pipeline for processing daily music stream data, extracting key performance indicators (KPIs), and making these insights available for business consumption. The core business need was to transform raw, high-volume stream logs into actionable metrics in a timely and reliable manner, ensuring data quality throughout the process.

Business Understanding & Problem Statement
The business required a system that could:

Automatically ingest daily music stream data as it arrived in an S3 landing zone.

Validate and cleanse this data to ensure quality and consistency.

Compute various KPIs (e.g., listen counts, unique listeners, top songs/genres) from the validated data.

Load these KPIs into a low-latency database (DynamoDB) for dashboards and applications.

Automate the entire process end-to-end, with clear visibility into success/failure and proper error handling.

Solution & System Architecture
To meet these business needs, I designed and implemented a serverless data pipeline leveraging key AWS services orchestrated by Apache Airflow:

AWS S3 (Data Lake): Used as the central repository for:

Raw incoming stream data, organized with a specific partitioning structure (year=YYYY/month=MM/day=DD/).

Processed, validated, and joined stream data.

Computed KPIs.

Archived raw data.

Glue job scripts and status logs.

AWS Glue (Serverless ETL): Employed for the heavy lifting of data processing:

Glue Crawler: Automatically discovers schema and partitions of raw data in S3 and registers them in the AWS Glue Data Catalog.

Glue ETL Jobs: Three distinct Spark-based Python jobs perform:

music-pipeline-extract-validate-join-streams: Validates raw stream data, performs basic transformations, and joins with static user/song data. It also writes a validation status.

music-pipeline-compute-metrics: Computes daily genre-level KPIs, top songs per genre, and top genres from the validated stream data.

music-pipeline-load-to-dynamodb: Loads the computed KPIs into a DynamoDB table for rapid access.

Apache Airflow (Orchestration): The control for the entire pipeline:

Schedules and manages the execution of tasks.

Handles dependencies between tasks.

Provides robust error handling, retries, and branching logic based on task outcomes.

The final output, stored in DynamoDB, directly addresses the business need for accessible, daily music KPIs, enabling data-driven decision-making.

Steps Taken to Build and Align with Business Needs
My iterative development process was driven by resolving specific technical blockers to align with the overall business requirements:

Initial DAG Structure & Basic Orchestration:

I started by defining the main Airflow DAG (music_kpi_pipeline) with the core tasks (start, wait_for_stream_event, data_validation, compute_kpis, load_to_dynamo, validation_failed, end) and their dependencies, mirroring the desired visual flow.

EmptyOperator tasks were used for the start, wait_for_stream_event, validation_failed, and end tasks to define the flow structure.

GlueJobOperator tasks were used to integrate with the actual AWS Glue ETL jobs.

Resolving Airflow DAG Syntax and Logic Errors:

DuplicateTaskIdFound: This was addressed by meticulously reviewing and removing redundant task definitions in the DAG, ensuring each task_id was unique.

AttributeError: module 'pendulum' has no attribute 'sleep': Corrected the Python time.sleep() within the _wait_for_and_get_s3_key function, ensuring the task could correctly pause and retry.

Implementing Robust Data Ingestion (wait_for_streams_data):

The wait_for_streams_data task (implemented as a PythonOperator with a custom _wait_for_and_get_s3_key function) was crucial for automating data ingestion. It was configured to dynamically search for stream_data_*.csv files, ensuring flexibility for varying daily filenames.

A key aspect was teaching this sensor to locate files within the correct partitioned S3 path, using data_interval_start.strftime for dynamic date segments.

Integrating and Debugging Glue Job Execution:

Once S3 and the Glue Data Catalog were correctly aligned, I integrated the specific Glue job scripts: music-pipeline-extract-validate-join-streams.py, music-pipeline-compute-metrics.py, and music-pipeline-load-to-dynamodb.py.

Airflow DAG arguments for these Glue jobs were meticulously configured to pass the correct S3 paths and execution dates using data_interval_start.strftime.

ConcurrentRunsExceededException: I troubleshooted this by advising to check and manually terminate any lingering Glue job runs in the AWS Glue console, ensuring the pipeline could launch new instances.

Implementing Robust Branching and Error Handling (Airflow trigger_rule):

To meet the business need for clear success/failure paths, trigger_rule parameters were implemented:

validation_failed uses trigger_rule='one_failed' to activate only if the data_validation Glue job fails.

The final end task uses trigger_rule='none_failed_min_one_success' to ensure it runs regardless of whether the main processing path or the failure handling path completes. This provides a clear completion signal for the DAG.

By systematically addressing each error, refining the S3 data structure, correctly configuring Glue resources, and precisely orchestrating with Airflow, we built a resilient and automated music data pipeline capable of meeting the defined business requirements for data quality, KPI generation, and operational visibility.