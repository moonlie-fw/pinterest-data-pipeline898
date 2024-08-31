# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## importing modules

# COMMAND ----------

#Import modules


from pyspark.sql.functions import *
from pyspark.sql.functions import array
from pyspark.sql.functions import col
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import count
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import row_number
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import when
from pyspark.sql.functions import year

from pyspark.sql.window import Window

import urllib.parse




# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting the Bucket

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
## dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing

# COMMAND ----------


display(dbutils.fs.ls("/mnt/mount_AWS_S3/"))



# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating DataFrames:
# MAGIC
# MAGIC - df_pin for the Pinterest post data
# MAGIC - df_geo for the geolocation data
# MAGIC - df_user for the user data.

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
display(df_pin)
display(df_geo)
display(df_user)



# COMMAND ----------

pin_columns = df_pin.columns
print(pin_columns)

geo_columns = df_geo.columns
print(geo_columns)

user_columns = df_user.columns
print(user_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cleaning df_pin
# MAGIC ### 
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

cleaned_df_pin = df_pin.replace(replacements)

#Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.


cleaned_df_pin = cleaned_df_pin.replace({'k': '000'}, subset=['follower_count'])


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
# MAGIC ### **Cleaning Geo Data**

# COMMAND ----------

# Create a new column 'coordinates' that contains an array of 'latitude' and 'longitude'
cleaned_df_geo = df_geo.withColumn('coordinates', array(col('latitude'), col('longitude')))

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
cleaned_df_user = df_user.withColumn('user_name', concat_ws(' ', 'first_name', 'last_name'))

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
# MAGIC ## Data **analysis**

# COMMAND ----------

# MAGIC %md
# MAGIC ### joining the dataframes

# COMMAND ----------

#joining pin and geo DF

pingeo_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_pin["ind"] == cleaned_df_geo["ind"], how="inner").drop(cleaned_df_geo["ind"])

#joining user and pingeo DF
usergeopin_df = cleaned_df_user.join(
    pingeo_df,
    cleaned_df_user["ind"] == pingeo_df["ind"],
    how="inner"
).drop(pingeo_df["ind"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Find the most popular Pinterest category people post to based on their country.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - country
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------


# Group by country and category, and count the occurrences
grouped_df = pingeo_df.groupBy("country","category").agg(count("category").alias("category_count"))

# Define a window specification to rank categories within each country
window_spec = Window.partitionBy("country").orderBy(col("category_count").desc())

# Add a row number to each row within the window
ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))

# Filter to get only the most popular category per country
most_pop_category_country_df = ranked_df.filter(col("rank") == 1).select("country", "category", "category_count")

display(most_pop_category_country_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Find how many posts each category had between 2018 and 2022.
# MAGIC ### 
# MAGIC
# MAGIC Find how many posts each category had between 2018 and 2022.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------


# Extract the year from the timestamp column and create a new column 'post_year'
pingeo_df = pingeo_df.withColumn('post_year', year(col('timestamp')))

# Filter the DataFrame to include only posts between 2018 and 2022
filtered_df = pingeo_df.filter((col('post_year') >= 2018) & (col('post_year') <= 2022))

# Group by 'post_year' and 'category', and count the occurrences
grouped_df = filtered_df.groupBy('post_year', 'category').agg(count('category').alias('category_count'))

# Select the desired columns
post_per_cat_results= grouped_df.select('post_year', 'category', 'category_count')

display(post_per_cat_results)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Task 6: Find the user with most followers in each country
# MAGIC
# MAGIC Step 1: For each country find the user with the most followers.
# MAGIC
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - country
# MAGIC - poster_name
# MAGIC - follower_count
# MAGIC
# MAGIC
# MAGIC Step 2: Based on the above query, find the country with the user with most followers.
# MAGIC
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - country
# MAGIC - follower_count
# MAGIC - This DataFrame should have only one entry.

# COMMAND ----------

# Define a window specification to rank users within each country by follower count
window_spec = Window.partitionBy("country").orderBy(col("follower_count").desc())

# Add a row number to each row within the window
ranked_df = usergeopin_df.withColumn("rank", row_number().over(window_spec))

# Filter to get only the user with the most followers per country
user_most_follower_per_country_result = ranked_df.filter(col("rank") == 1).select("country", "poster_name", "follower_count")

display(user_most_follower_per_country_result)

# COMMAND ----------


# Use the result from the previous query
# Find the country with the user with the most followers
max_followers_df = user_most_follower_per_country_result.agg(max("follower_count").alias("max_follower_count"))

# Join to get the country with the max follower count
country_with_max_followers_df = user_most_follower_per_country_result.join(
    max_followers_df,
    user_most_follower_per_country_result["follower_count"] == max_followers_df["max_follower_count"]
).select("country", "follower_count")

display(country_with_max_followers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Task 7: Find the most popular category for different age groups
# MAGIC ### 
# MAGIC
# MAGIC What is the most popular category people post to based on the following age groups:
# MAGIC
# MAGIC - 18-24
# MAGIC - 25-35
# MAGIC - 36-50
# MAGIC - +50
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - age_group, a new column based on the original age column
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output
# MAGIC

# COMMAND ----------

# Define age groups
age_groups_df = usergeopin_df.withColumn(
    'age_group',
    when((col('age') >= 18) & (col('age') <= 24), '18-24')
    .when((col('age') >= 25) & (col('age') <= 35), '25-35')
    .when((col('age') >= 36) & (col('age') <= 50), '36-50')
    .when(col('age') > 50, '+50')
)

# Group by age_group and category, and count the occurrences
age_groups_cat_df = age_groups_df.groupBy('age_group', 'category').agg(count('category').alias('category_count'))

# Define a window specification to rank categories within each age group by category count
window_spec = Window.partitionBy("age_group").orderBy(col("category_count").desc())

# Add a row number to each row within the window
ranked_df = age_groups_cat_df.withColumn("rank", row_number().over(window_spec))

# Filter to get only the most popular category per age group
result_df = ranked_df.filter(col("rank") == 1).select("age_group", "category", "category_count")

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8: Find the median follower count for different age groups
# MAGIC ### 
# MAGIC What is the median follower count for users in the following age groups:
# MAGIC
# MAGIC 18-24
# MAGIC 25-35
# MAGIC 36-50
# MAGIC +50
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - age_group, a new column based on the original age column
# MAGIC - median_follower_count, a new column containing the desired query output

# COMMAND ----------




# Calculate the median follower count for each age group
age_groups_follower_df = age_groups_df.groupBy("age_group",'follower_count').agg(
    expr('percentile_approx(follower_count, 0.5)').alias('median_follower_count')
)

display(median_follower_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9: How many users joined each year?
# MAGIC Find how many users have joined between 2015 and 2020.
# MAGIC sw
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - number_users_joined, a new column containing the desired query output

# COMMAND ----------


# Extract the year from the date_joined column and create a new column 'post_year'
cleaned_df_user = cleaned_df_user.withColumn('post_year', year(col('date_joined')))

# Filter the DataFrame to include only users who joined between 2015 and 2020
filtered_df = cleaned_df_user.filter((col('post_year') >= 2015) & (col('post_year') <= 2020))

# Group by post_year and count the number of users who joined each year
grouped_df = filtered_df.groupBy('post_year').agg(count('ind').alias('number_users_joined'))

# Select the desired columns
result_df = grouped_df.select('post_year', 'number_users_joined')

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 10: Find the median follower count of users have joined between 2015 and 2020
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC post_year, a new column that contains only the year from the timestamp column
# MAGIC median_follower_count, a new column containing the desired query output

# COMMAND ----------


# Extract the year from the date_joined column and create a new column 'post_year'
usergeopin_df = usergeopin_df.withColumn('post_year', year(col('date_joined')))

# Filter the DataFrame to include only users who joined between 2015 and 2020
filtered_df = usergeopin_df.filter((col('post_year') >= 2015) & (col('post_year') <= 2020)& (col('follower_count').isNotNull()))

# Calculate the median follower count for each post_year
median_follower_counts = filtered_df.groupBy("post_year").agg(
    expr('percentile_approx(follower_count, 0.5)').alias('median_follower_count')
)

display(median_follower_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 11: Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of
# MAGIC
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC age_group, a new column based on the original age column
# MAGIC post_year, a new column that contains only the year from the timestamp column
# MAGIC median_follower_count, a new column containing the desired query output

# COMMAND ----------

# Extract the year from the date_joined column and create a new column 'post_year'
usergeopin_df = usergeopin_df.withColumn('post_year', year(col('date_joined')))

# Define age groups
usergeopin_df = usergeopin_df.withColumn(
    'age_group',
    when((col('age') >= 18) & (col('age') <= 24), '18-24')
    .when((col('age') >= 25) & (col('age') <= 35), '25-35')
    .when((col('age') >= 36) & (col('age') <= 50), '36-50')
    .when(col('age') > 50, '+50')
)

# Filter the DataFrame to include only users who joined between 2015 and 2020
filtered_df = usergeopin_df.filter((col('post_year') >= 2015) & (col('post_year') <= 2020))

# Check if the 'follower_count' column exists in the 'cleaned_df_user' DataFrame
if 'follower_count' not in usergeopin_df.columns:
    # Add necessary transformation to create the 'follower_count' column
    usergeopin_df = usergeopin_df.withColumn('follower_count', expr('<transformation_expression>'))

# Include the 'follower_count' column in the 'filtered_df' DataFrame
filtered_df = filtered_df.select('*', 'follower_count')

# Calculate the median follower count for each age_group and post_year
median_follower_counts = filtered_df.groupBy("age_group", "post_year").agg(
    expr('percentile_approx(follower_count, 0.5)').alias('median_follower_count')
)

display(median_follower_counts)
