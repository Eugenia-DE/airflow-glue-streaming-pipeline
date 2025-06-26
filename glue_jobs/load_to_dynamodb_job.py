import sys
import json
from datetime import datetime
import boto3
import logging
from decimal import Decimal

try:
    import pyarrow.parquet as pq
except ImportError:
    print("[ERROR] pyarrow is required. Install it in Glue Python shell job.")
    sys.exit(1)


JOB_NAME = "music-pipeline-load-to-dynamodb"
EXEC_DATE = "2025-06-25"  
INPUT_PATH = f"s3://lab3-dynamo-glue-etl/metrics/daily_genre_metrics/metric_date={EXEC_DATE}/"
DYNAMODB_TABLE_NAME = "music_genre_metrics"
STATUS_PATH = "s3://lab3-dynamo-glue-etl/job-status/"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)


# Write job status to S3
def write_status(state, msg):
    try:
        bucket, key = STATUS_PATH.replace("s3://", "").split("/", 1)
        file_key = f"{key}/{JOB_NAME}_status_{EXEC_DATE}.json"
        status_data = {
            "status": state,
            "message": msg,
            "timestamp": datetime.now().isoformat(),
            "execution_date": EXEC_DATE
        }
        s3.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=json.dumps(status_data),
            ContentType='application/json'
        )
    except Exception as e:
        logger.error(f"[ERROR] Failed to write status file: {e}")


# Load and Insert Records

try:
    bucket_name, prefix = INPUT_PATH.replace("s3://", "").split("/", 1)
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    parquet_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj['Key'].endswith(".parquet") and obj['Size'] > 0:
                parquet_files.append(f"s3://{bucket_name}/{obj['Key']}")

    if not parquet_files:
        write_status("SUCCESS_NO_DATA", f"No Parquet files found in {INPUT_PATH}")
        sys.exit(0)

    all_records = []
    for file_path in parquet_files:
        try:
            table_data = pq.read_table(file_path)
            all_records.extend(table_data.to_pylist())
        except Exception as e:
            logger.error(f"[ERROR] Could not read Parquet file {file_path}: {e}")

    if not all_records:
        write_status("SUCCESS_NO_DATA", f"All Parquet files empty for {EXEC_DATE}")
        sys.exit(0)

    load_count, error_count = 0, 0
    with table.batch_writer() as batch:
        for idx, record in enumerate(all_records):
            try:
                item = {
                    'metric_date': str(record.get('metric_date') or EXEC_DATE),
                    'track_genre': str(record.get('track_genre')),
                    'listen_count': int(record.get('listen_count') or 0),
                    'unique_listeners': int(record.get('unique_listeners') or 0),
                    'total_listening_time_ms': int(record.get('total_listening_time_ms') or 0),
                    'avg_listening_time_per_user_ms': Decimal(str(record.get('avg_listening_time_per_user_ms') or "0"))
                }
                cleaned_item = {k: v for k, v in item.items() if v not in [None, ""]}
                batch.put_item(Item=cleaned_item)
                load_count += 1
            except Exception as e:
                logger.error(f"[ERROR] Record {idx+1} failed: {e}")
                error_count += 1

    if error_count > 0:
        write_status("PARTIAL_SUCCESS", f"{load_count} succeeded, {error_count} failed")
        if load_count == 0:
            raise Exception("All records failed.")
    else:
        write_status("SUCCESS", f"{load_count} records loaded into {DYNAMODB_TABLE_NAME}")

except Exception as e:
    logger.error(f"[FATAL] Job failed: {e}", exc_info=True)
    write_status("FAILED", str(e))
    raise
