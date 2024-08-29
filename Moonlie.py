# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib.parse

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0afff86fcd1b-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/mount_AWS_S3"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

#test to see if it works

display(dbutils.fs.ls("/mnt/mount_AWS_S3/"))



# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC In order to clean and query your batch data, you will need to read this data from your S3 bucket into Databricks. To do this, you will need to mount the desired S3 bucket to the Databricks account. The Databricks account you have access to has already been granted full access to S3, so you will not need to create a new Access Key and Secret Access Key for Databricks. The credentials have already been uploaded to Databricks for you. You will only need to read in the data from the Delta table, located at dbfs:/user/hive/warehouse/authentication_credentials.
# MAGIC
# MAGIC When reading in the JSONs from S3, make sure to include the complete path to the JSON objects, as seen in your S3 bucket (e.g topics/<your_UserId>.pin/partition=0/).
# MAGIC
# MAGIC
# MAGIC You should create three different DataFrames:
# MAGIC
# MAGIC df_pin for the Pinterest post data
# MAGIC df_geo for the geolocation data
# MAGIC df_user for the user data.

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_pin = "/mnt/mount_AWS_S3/topics/0afff86fcd1b.pin/partition=0//*.json"
file_location_geo = "/mnt/mount_AWS_S3/topics/0afff86fcd1b.geo/partition=0//*.json"
file_location_user = "/mnt/mount_AWS_S3/topics/0afff86fcd1b.user/partition=0//*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_pin)

df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_geo)


df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_user)
# Display Spark dataframe to check its content


# COMMAND ----------

display(df_pin)


# COMMAND ----------

display(df_user)


# COMMAND ----------

display(df_geo)

# COMMAND ----------


