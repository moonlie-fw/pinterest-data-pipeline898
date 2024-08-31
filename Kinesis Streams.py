# Databricks notebook source
#Import modules
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json


# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structured Streaming Schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pin Schema

# COMMAND ----------



# Define a streaming schema using StructType
pin_struct = StructType([
    StructField("category", StringType(), True),
    StructField("description", StringType(), True),
    StructField("downloaded", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("index", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("title", StringType(), True),
    StructField("unique_id", StringType(), True)
])

pin_df = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName','streaming-0afff86fcd1b-pin') \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

pin_df = pin_df.selectExpr("CAST(data as STRING)")
pin_df = pin_df.select(from_json("data", pin_struct).alias("data")).select("data.*")

display(pin_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geo Schema

# COMMAND ----------


geo_struct = StructType([
    StructField("country", StringType(), True),
    StructField("ind", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("timestamp", StringType(), True)
])

geo_df = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName','streaming-0afff86fcd1b-geo') \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

geo_df = geo_df.selectExpr("CAST(data as STRING)")
geo_df = geo_df.select(from_json("data", geo_struct).alias("data")).select("data.*")

display(geo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - ['category', 'description', 'downloaded', 'follower_count', 'image_src', 'index', 'is_image_or_video', 'poster_name', 'save_location', 'tag_list', 'title', 'unique_id']
# MAGIC - ['country', 'ind', 'latitude', 'longitude', 'timestamp']
# MAGIC - ['age', 'date_joined', 'first_name', 'ind', 'last_name']

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Schema

# COMMAND ----------


user_struct = StructType([
    StructField("age", StringType(), True),
    StructField("date_joined", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("ind", StringType(), True),
    StructField("last_name", StringType(), True)
])

user_df = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName','streaming-0afff86fcd1b-user') \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

user_df = user_df.selectExpr("CAST(data as STRING)")
user_df = user_df.select(from_json("data", user_struct).alias("data")).select("data.*")

display(user_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning the data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cleaning pin
# MAGIC

# COMMAND ----------

#To clean the df_pin DataFrame you should perform the following transformations:


replacements = {
    'No description available': None,
    'No description available Story format': None,
    'No Title Data Available': None,
    'User Info Error': None,
    'Image src error.': None,
    'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None
}

cleaned_df_pin = pin_df.replace(replacements)

#Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.


#cleaned_df_pin = cleaned_df_pin.replace({'k': '000'}, subset=['follower_count'])


cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", cleaned_df_pin["follower_count"].cast("int"))

# Exclude the text 'local save in' from the save_location column
cleaned_df_pin = cleaned_df_pin.withColumn(
    'save_location', 
    regexp_replace('save_location', 'Local save in ', '')
)

# Rename the index column to ind
cleaned_df_pin = cleaned_df_pin.withColumnRenamed('index', 'ind')

# Reorder the DataFrame columns
desired_order = [
    'ind', 'unique_id', 'title', 'description', 'follower_count', 
    'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 
    'save_location', 'category'
]

cleaned_df_pin = cleaned_df_pin.select(desired_order)

display(cleaned_df_pin)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### **Cleaning Geo Data**

# COMMAND ----------

# Create a new column 'coordinates' that contains an array of 'latitude' and 'longitude'
cleaned_df_geo = geo_df.withColumn('coordinates', array(col('latitude'), col('longitude')))

# Drop the latitude and longitude columns
cleaned_df_geo = cleaned_df_geo.drop('latitude', 'longitude')

# Convert the timestamp column from a string to a timestamp data type
cleaned_df_geo = cleaned_df_geo.withColumn('timestamp', to_timestamp(col('timestamp')))

# Reorder the DataFrame columns
desired_order = ['ind', 'country', 'coordinates', 'timestamp']
cleaned_df_geo = cleaned_df_geo.select(desired_order)

display(cleaned_df_geo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Cleaning User Data**

# COMMAND ----------

# Create a new column 'user_name' that concatenates 'first_name' and 'last_name'
cleaned_df_user = user_df.withColumn('user_name', concat_ws(' ', 'first_name', 'last_name'))

# Drop the 'first_name' and 'last_name' columns
cleaned_df_user = cleaned_df_user.drop('first_name', 'last_name')

# Convert the 'date_joined' column from a string to a timestamp data type
cleaned_df_user = cleaned_df_user.withColumn('date_joined', to_timestamp('date_joined'))

# Reorder the DataFrame columns
desired_order_user = ['ind', 'user_name', 'age', 'date_joined']
cleaned_df_user = cleaned_df_user.select(desired_order_user)

display(cleaned_df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ## loading to delta table

# COMMAND ----------

# Save cleaned_df_pin to Delta Table
cleaned_df_pin.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/user/hive/warehouse/0afff86fcd1b_pin_table/_checkpoints/") \
    .table("0afff86fcd1b_pin_table")

# Save cleaned_df_geo to Delta Table
cleaned_df_geo.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/user/hive/warehouse/0afff86fcd1b_geo_table/_checkpoints/") \
    .table("0afff86fcd1b_geo_table")

# Save cleaned_df_user to Delta Table
cleaned_df_user.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/user/hive/warehouse/0afff86fcd1b_user_table/_checkpoints/") \
    .table("0afff86fcd1b_user_table")
