# GCP service account ID: cricket-project-service-accoun@melodic-rig-496917-c5.iam.gserviceaccount.com
# GCP service account name: cricket_project_service_account

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import numpy as np
import os

from google.cloud import storage

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# =========================================================
# SPARK SESSION
# =========================================================

spark = SparkSession.builder \
    .appName("bronze_to_silver") \
    .getOrCreate()




# =========================================================
# DUMMY TASK
# =========================================================

# def dummy_task():
#     print("Printing DAG details...")
#     print("DAG ID: Bronze_TO_SILVER")
#     print("Task ID: dummy_task")
#     print(spark)


# # =========================================================
# # TRANSFORM FUNCTION
# # =========================================================

# def tranform_json_data(df,blob_name):

#     # =====================================================
#     # INNINGS DF
#     # =====================================================

#     teams = df.select(explode(col("info.teams")))
#     numpy_array = teams.toPandas().to_numpy()
#     team1 = numpy_array[0][0]
#     team2 = numpy_array[1][0]

#     print(team1,team2)

#     innings_df = df.select(

#         coalesce(col("match_id"), lit(None)).alias("match_id"),
#         coalesce(col("match"), lit(None)).alias("match"),
#         coalesce(col("info.dates")[0], lit(None)).alias("match_date"),

#         coalesce(
#             get_json_object(to_json(col("info.event")), "$.match_number"),
#             lit(None)
#         ).alias("match_number"),

#         coalesce(col("info.event.name"), lit(None)).alias("event_name"),
#         coalesce(col("info.season"), lit(None)).alias("season"),

#         # ❌ FIX: force NULL (avoid Spark crash)
#         col("info.teams")[0].alias("team1"),
#         col("info.teams")[1].alias("team2"),

#         col(f"info.players.{team1}").alias("team1_players"),
#         col(f"info.players.{team2}").alias("team2_players"),

#         coalesce(col("info.venue"), lit(None)).alias("venue"),
#         coalesce(col("info.toss.winner"), lit(None)).alias("toss_winner"),
#         coalesce(col("info.toss.decision"), lit(None)).alias("toss_decision"),

#         coalesce(
#             get_json_object(to_json(col("info.outcome")), "$.by.runs"),
#             lit(None)
#         ).alias("won_by_runs"),

#         coalesce(
#             get_json_object(to_json(col("info.outcome")), "$.by.wickets"),
#             lit(None)
#         ).alias("won_by_wickets"),

#         coalesce(
#             get_json_object(to_json(col("info.outcome")), "$.winner"),
#             lit(None)
#         ).alias("winner_team"),

#         explode_outer(col("innings")).alias("match_innings")
#     )

#     #print("===== INNINGS DF =====")
#     #innings_df.show(2, truncate=False)

#     # =====================================================
#     # OVERS DF
#     # =====================================================

#     overs_df = innings_df.select(
#         "match_id", "match", "match_date", "match_number",
#         "event_name", "season",
#         "team1", "team2",
#         "team1_players", "team2_players",
#         "venue", "toss_winner", "toss_decision",
#         "won_by_runs", "won_by_wickets", "winner_team",

#         coalesce(col("match_innings.team"), lit(None)).alias("batting_team"),

#         explode_outer(col("match_innings.overs")).alias("over_data")
#     )

#     # print("===== OVERS DF =====")
#     # overs_df.show(2, truncate=False)

#     # =====================================================
#     # DELIVERIES DF
#     # =====================================================

#     deliveries_df = overs_df.select(
#         "match_id", "match", "match_date", "match_number",
#         "event_name", "season",
#         "team1", "team2",
#         "team1_players", "team2_players",
#         "venue", "toss_winner", "toss_decision",
#         "won_by_runs", "won_by_wickets", "winner_team",
#         "batting_team",

#         coalesce(col("over_data.over"), lit(None)).alias("over"),

#         explode_outer(col("over_data.deliveries")).alias("deliveries")
#     )

#     # print("===== DELIVERIES DF =====")
#     # deliveries_df.show(2, truncate=False)

#     # =====================================================
#     # FINAL DF
#     # =====================================================

#     final_df = deliveries_df.select(

#         "match_id", "match", "match_date", "match_number",
#         "event_name", "season",
#         "team1", "team2",
#         "team1_players", "team2_players",
#         "batting_team",

#         "venue", "toss_winner", "toss_decision",
#         "won_by_runs", "won_by_wickets", "winner_team",

#         "over",

#         coalesce(col("deliveries.batter"), lit(None)).alias("striker"),
#         coalesce(col("deliveries.non_striker"), lit(None)).alias("non_striker"),
#         coalesce(col("deliveries.bowler"), lit(None)).alias("bowler"),

#         coalesce(col("deliveries.runs.batter"), lit(None)).alias("batter_runs"),
#         coalesce(col("deliveries.runs.extras"), lit(None)).alias("extra_runs"),
#         coalesce(col("deliveries.runs.total"), lit(None)).alias("total_runs"),

#         col("deliveries.wickets")[0]["kind"].alias("wicket_kind"),
#         col("deliveries.wickets")[0]["player_out"].alias("player_out"),
#         col("deliveries.wickets")[0]["fielders"][0]["name"].alias("fielder_name")
#     )

#     print("===== FINAL DF =====")
#     final_df.show(10, truncate=False)

#     output_path = f"/tmp/cricket_silver_output/{blob_name}/"

#     final_df.write \
#     .mode("overwrite") \
#     .parquet(output_path)


# # =========================================================
# # READ FROM GCS
# # =========================================================

# def get_google_info():

#     client = storage.Client()

#     bucket = client.bucket("project-cricket-bronze")

#     blobs = bucket.list_blobs()

#     for blob in blobs:

#         print(f"Processing File : {blob.name}")

#         json_data = blob.download_as_text()

#         rdd = spark.sparkContext.parallelize([json_data])

#         df = spark.read \
#             .option("multiline", "true") \
#             .json(rdd)

#         print("===== RAW DF =====")


#         df.show(1, truncate=False)

#         blob_name = blob.name.split("/")[-1].replace(".json", "")
#         tranform_json_data(df,blob_name=blob_name)


def upload_parquet_files_to_GCS():
    client = storage.Client()
    bucket = client.bucket("project-cricket-silver")

    local_base_path = "/tmp/cricket_silver_output/"

    for root, dirs, files in os.walk(local_base_path):
        for file in files:

            local_file_path = os.path.join(root, file)

            # skip hidden/system files if needed
            if file.startswith("."):
                continue

            relative_path = os.path.relpath(local_file_path, local_base_path)

            # this keeps folder structure in GCS
            gcs_blob_path = f"{relative_path}"

            blob = bucket.blob(gcs_blob_path)

            blob.upload_from_filename(local_file_path)

            print(f"Uploaded: {local_file_path} → gs://{bucket}/{gcs_blob_path}")


def validate_count():

    client = storage.Client()

    bucket = client.bucket("project-cricket-silver")

    local_base_path = "/tmp/cricket_silver_output/"

    # ==========================================
    # COUNTERS
    # ==========================================

    total_local_files = 0
    uploaded_files = 0

    for root,dirs,files in os.walk(local_base_path):
        for file in files:
            if file.startswith("."):
                continue
            total_local_files += 1

    blobs = bucket.list_blobs()
    for blob in blobs:
        if blob.name.startswith("."):
            continue

        uploaded_files += 1

    print(uploaded_files, total_local_files)

    if uploaded_files == total_local_files:
        print("=========================================================")
        print("Validation ✅ ")
        print("=========================================================")
    
    else:
        print("=========================================================")
        print("❌ FILE COUNT MISMATCH")
        print("=========================================================")
    

        

# =========================================================
# AIRFLOW DAG
# =========================================================

with DAG(

    dag_id="Bronze_TO_SILVER",

    start_date=datetime(2025, 1, 1),

    schedule="@daily",

    catchup=False,

    tags=["spark", "bronze", "silver"]

) as dag:

    # task1 = PythonOperator(
    #     task_id="dummy_task",
    #     python_callable=dummy_task
    # )

    # task2 = PythonOperator(
    #     task_id="get_google_connection_info",
    #     python_callable=get_google_info
    # )

    task3 = PythonOperator(
        task_id = "upload_parquet_files_to_GCS",
        python_callable = upload_parquet_files_to_GCS
    )

    task1 = PythonOperator(
        task_id = "upload_status",
        python_callable = validate_count
    )

    # DEPENDENCY
    task3 >> task1