# Databricks notebook source

# Import necessary modules
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, concat_ws, array, regexp_replace, to_timestamp
import urllib

# Function to load AWS credentials
def load_aws_credentials():
    delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
    aws_keys_df = spark.read.format("delta").load(delta_table_path)
    ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
    SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
    ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, safe="")
    return ACCESS_KEY, ENCODED_SECRET_KEY

ACCESS_KEY, SECRET_KEY = load_aws_credentials()

# Disable Delta format check for Kinesis
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# Kinesis stream setup function
def setup_kinesis_stream(stream_name, schema):
    return (spark.readStream
            .format('kinesis')
            .option('streamName', stream_name)
            .option('initialPosition', 'earliest')
            .option('region', 'us-east-1')
            .option('awsAccessKey', ACCESS_KEY)
            .option('awsSecretKey', SECRET_KEY)
            .load()
            .selectExpr("CAST(data as STRING)")
            .select(from_json("data", schema).alias("data"))
            .select("data.*"))

# Define schemas for Pin, Geo, and User streams
pin_schema = StructType([
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

geo_schema = StructType([
    StructField("country", StringType(), True),
    StructField("ind", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("timestamp", StringType(), True)
])

user_schema = StructType([
    StructField("age", StringType(), True),
    StructField("date_joined", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("ind", StringType(), True),
    StructField("last_name", StringType(), True)
])

# Setup streaming DataFrames
pin_df = setup_kinesis_stream('streaming-0afff86fcd1b-pin', pin_schema)
geo_df = setup_kinesis_stream('streaming-0afff86fcd1b-geo', geo_schema)
user_df = setup_kinesis_stream('streaming-0afff86fcd1b-user', user_schema)

# Data cleaning functions
def clean_pin_data(pin_df):
    replacements = {
        'No description available': None,
        'No description available Story format': None,
        'No Title Data Available': None,
        'User Info Error': None,
        'Image src error.': None,
        'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None
    }
    df = pin_df.replace(replacements)
    df = df.withColumn("follower_count", df["follower_count"].cast("int"))
    df = df.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))
    df = df.withColumnRenamed('index', 'ind')
    return df.select(['ind', 'unique_id', 'title', 'description', 'follower_count', 
                      'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 
                      'save_location', 'category'])

def clean_geo_data(geo_df):
    df = geo_df.withColumn('coordinates', array(col('latitude'), col('longitude')))
    df = df.drop('latitude', 'longitude')
    return df.withColumn('timestamp', to_timestamp(col('timestamp'))).select(['ind', 'country', 'coordinates', 'timestamp'])

def clean_user_data(user_df):
    df = user_df.withColumn('user_name', concat_ws(' ', 'first_name', 'last_name'))
    df = df.drop('first_name', 'last_name')
    return df.withColumn('date_joined', to_timestamp('date_joined')).select(['ind', 'user_name', 'age', 'date_joined'])

# Clean dataframes
cleaned_df_pin = clean_pin_data(pin_df)
cleaned_df_geo = clean_geo_data(geo_df)
cleaned_df_user = clean_user_data(user_df)

# Write to Delta tables with a function
def write_to_delta_table(df, table_name):
    df.writeStream \
      .format("delta") \
      .outputMode("append") \
      .option("checkpointLocation", f"dbfs:/user/hive/warehouse/{table_name}/_checkpoints/") \
      .table(table_name)

write_to_delta_table(cleaned_df_pin, "0afff86fcd1b_pin_table")
write_to_delta_table(cleaned_df_geo, "0afff86fcd1b_geo_table")
write_to_delta_table(cleaned_df_user, "0afff86fcd1b_user_table")
