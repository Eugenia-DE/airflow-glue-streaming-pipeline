import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'execution_timestamp'])

# Initialize Glue context and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


s3_prefix = "s3://lab3-dynamo-glue-etl"
catalog_db = "lab3"
processed_ts = args['execution_timestamp']

# Output paths
validated_path = f"{s3_prefix}/validated/processed_date={processed_ts}/"
bad_path = f"{s3_prefix}/bad-records"

# Validation helper
def check_required_columns(df, required_columns, name):
    actual = set(df.columns)
    missing = required_columns - actual
    if missing:
        raise Exception(f"{name} missing columns: {missing}")

try:
    # Read from Glue Data Catalog
    users_dyf = glueContext.create_dynamic_frame.from_catalog(database=catalog_db, table_name="users")
    songs_dyf = glueContext.create_dynamic_frame.from_catalog(database=catalog_db, table_name="songs")
    streams_dyf = glueContext.create_dynamic_frame.from_catalog(database=catalog_db, table_name="streams")

    # Convert to Spark DataFrames
    users_df = users_dyf.toDF()
    songs_df = songs_dyf.toDF()
    streams_df = streams_dyf.toDF()

    # Validate required columns
    check_required_columns(users_df, {'user_id', 'user_name', 'user_age', 'user_country', 'created_at'}, "users")
    check_required_columns(songs_df, {'track_id', 'track_name', 'track_genre', 'duration_ms'}, "songs")
    check_required_columns(streams_df, {'user_id', 'track_id', 'listen_time'}, "streams")

    # Split bad and good records
    bad_users = users_df.filter("user_id IS NULL OR user_name IS NULL OR user_age IS NULL OR user_country IS NULL OR created_at IS NULL")
    good_users = users_df.dropna(subset=["user_id", "user_name", "user_age", "user_country", "created_at"])

    bad_songs = songs_df.filter("track_id IS NULL OR track_genre IS NULL OR duration_ms IS NULL")
    good_songs = songs_df.dropna(subset=["track_id", "track_genre", "duration_ms"])

    bad_streams = streams_df.filter("user_id IS NULL OR track_id IS NULL OR listen_time IS NULL")
    good_streams = streams_df.dropna(subset=["user_id", "track_id", "listen_time"])

    # Join and further validation
    joined_df = good_streams.join(good_songs, on="track_id", how="inner").join(good_users, on="user_id", how="inner")
    bad_joined = joined_df.filter((joined_df["duration_ms"] <= 0) | (joined_df["user_age"] <= 0))
    clean_df = joined_df.filter((joined_df["duration_ms"] > 0) & (joined_df["user_age"] > 0))

    # Write clean data to validated path
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(clean_df, glueContext, "clean_df"),
        connection_type="s3",
        connection_options={"path": validated_path},
        format="parquet"
    )

    # Write bad records to corresponding folders
    for name, df in [("users", bad_users), ("songs", bad_songs), ("streams", bad_streams), ("joined", bad_joined)]:
        if df.count() > 0:
            glueContext.write_dynamic_frame.from_options(
                frame=DynamicFrame.fromDF(df, glueContext, f"bad_{name}"),
                connection_type="s3",
                connection_options={"path": f"{bad_path}/{name}/"},
                format="parquet"
            )

except Exception as e:
    print(f"[ERROR] Job failed: {str(e)}")
    raise

job.commit()