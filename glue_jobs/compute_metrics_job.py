import sys
import json
from datetime import datetime
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, count, countDistinct, sum, avg, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import DateType

INPUT_VALIDATED_STREAMS_PATH = "s3://lab3-dynamo-glue-etl/validated"
OUTPUT_METRICS_BASE_PATH = "s3://lab3-dynamo-glue-etl/metrics"
JOB_STATUS_OUTPUT_PATH = "s3://lab3-dynamo-glue-etl/job-status"


# AWS Glue Setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = {
    'JOB_NAME': 'music-pipeline-compute-metrics'
}
job.init(args['JOB_NAME'], args)

# Auto-detect latest processed_date folder
s3_client = boto3.client("s3")
bucket_name, prefix = INPUT_VALIDATED_STREAMS_PATH.replace("s3://", "").split("/", 1)

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}/processed_date=")
folders = set()

for obj in response.get("Contents", []):
    key = obj["Key"]
    parts = key.split("/")
    for part in parts:
        if part.startswith("processed_date="):
            folders.add(part)

if not folders:
    raise Exception(f"No processed_date= folders found under {INPUT_VALIDATED_STREAMS_PATH}")

latest_folder = sorted(folders)[-1]
INPUT_PATH = f"{INPUT_VALIDATED_STREAMS_PATH}/{latest_folder}"
EXEC_TIMESTAMP = latest_folder.replace("processed_date=", "")

print(f"[INFO] Using input path: {INPUT_PATH}")
print(f"[INFO] Derived EXEC_TIMESTAMP: {EXEC_TIMESTAMP}")

# Status Logger
def write_status(state, msg):
    try:
        bucket, key = JOB_STATUS_OUTPUT_PATH.replace("s3://", "").split("/", 1)
        file_key = f"{key}/{args['JOB_NAME']}_status_{EXEC_TIMESTAMP}.json"
        data = {
            "status": state,
            "message": msg,
            "timestamp": datetime.now().isoformat(),
            "execution_timestamp": EXEC_TIMESTAMP
        }
        s3_client.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
    except Exception as e:
        raise Exception(f"Status write failed: {e}")

# Compute and Save Metrics
try:
    bucket, prefix = INPUT_PATH.replace("s3://", "").split("/", 1)
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in resp or not any(obj['Size'] > 0 for obj in resp.get('Contents', [])):
        write_status("SUCCESS_NO_DATA", f"No input files found in {INPUT_PATH}")
        sys.exit(0)

    df = spark.read.parquet(INPUT_PATH)
    if df.count() == 0:
        write_status("SUCCESS_NO_DATA", f"No rows found in {INPUT_PATH}")
        sys.exit(0)

    expected_cols = {'track_genre', 'track_id', 'track_name', 'user_id', 'duration_ms'}
    actual_cols = set(df.columns)
    missing = expected_cols - actual_cols
    if missing:
        raise Exception(f"Missing required columns: {missing}")

    # Genre metrics
    genre_df = df.groupBy("track_genre").agg(
        count("track_id").alias("listen_count"),
        countDistinct("user_id").alias("unique_listeners"),
        sum("duration_ms").alias("total_listening_time_ms")
    )
    user_avg_df = df.groupBy("track_genre", "user_id").agg(
        sum("duration_ms").alias("user_total")
    ).groupBy("track_genre").agg(
        avg("user_total").alias("avg_listening_time_per_user_ms")
    )
    genre_metrics = genre_df.join(user_avg_df, on="track_genre", how="inner") \
        .withColumn("metric_date", lit(EXEC_TIMESTAMP[:10]).cast(DateType()))
    genre_metrics.write.mode("overwrite").partitionBy("metric_date").parquet(f"{OUTPUT_METRICS_BASE_PATH}/daily_genre_metrics")

    # Top 3 songs per genre
    top_songs_df = df.groupBy("track_genre", "track_id", "track_name").agg(
        count("track_id").alias("song_plays")
    )
    w = Window.partitionBy("track_genre").orderBy(desc("song_plays"))
    top_3 = top_songs_df.withColumn("rank", row_number().over(w)) \
        .filter("rank <= 3") \
        .withColumn("metric_date", lit(EXEC_TIMESTAMP[:10]).cast(DateType())) \
        .select("metric_date", "track_genre", "track_name", "song_plays", "rank")
    top_3.write.mode("overwrite").partitionBy("metric_date").parquet(f"{OUTPUT_METRICS_BASE_PATH}/top_songs_per_genre")

    # Top 5 genres overall
    genre_rank_df = df.groupBy("track_genre").agg(count("track_id").alias("total_genre_listens"))
    w2 = Window.orderBy(desc("total_genre_listens"))
    top_5 = genre_rank_df.withColumn("rank", row_number().over(w2)) \
        .filter("rank <= 5") \
        .withColumn("metric_date", lit(EXEC_TIMESTAMP[:10]).cast(DateType())) \
        .select("metric_date", "track_genre", "total_genre_listens", "rank")
    top_5.write.mode("overwrite").partitionBy("metric_date").parquet(f"{OUTPUT_METRICS_BASE_PATH}/top_genres")

    write_status("SUCCESS", f"Metrics computed successfully from {INPUT_PATH} with {df.count()} rows.")

except Exception as e:
    write_status("FAILED", str(e))
    raise

job.commit()
