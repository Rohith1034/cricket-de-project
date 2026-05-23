from google.cloud import storage
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("airflow-jobs").getOrCreate()

def google_cloud_info():
    client = storage.Client(project="melodic-rig-496917-c5")
    bucket_name = client.get_bucket("project-cricket-bronze")
    print(f"{bucket_name}")
    blobs = bucket_name.list_blobs()
    for blob in blobs:
        json_data = blob.download_as_text()
        parsed_json = json.loads(json_data)
        df = spark.read.option("multiline", "true").json(parsed_json)
        df.show()

google_cloud_info()
