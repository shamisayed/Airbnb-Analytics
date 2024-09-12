from pyspark.sql import SparkSession

import geopandas as gpd
import pandas as pd
from pyspark.sql.functions import to_date, col, trim, sum, when, round, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType, FloatType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("GeoPandas with PySpark") \
    .getOrCreate()

df = spark.read.csv("s3://airbnbproject-group4vita/raw/geojson/airbnb-listings.csv", sep=";", header=True, inferSchema=True)

columns_to_drop = [
    "Listing Url", "Scrape ID", "Host URL", "Thumbnail Url", "Medium Url",
    "Picture Url", "XL Picture Url", "Host Thumbnail Url", "Host Picture Url",
    "Summary", "Space", "Description", "Experiences Offered", "Neighborhood Overview",
    "Notes", "Transit", "Access", "Interaction", "House Rules", "Host About",
    "Smart Location", "Jurisdiction Names", "License", "Neighbourhood Group Cleansed",
    "Geolocation", "Latitude", "Longitude"
]

df = df.drop(*columns_to_drop)
original_columns = df.columns
projectdf = df.drop(*columns_to_drop)
projectdf = projectdf.withColumn("Calendar last Scraped", to_date("Calendar last Scraped", "yyyy-MM-dd"))
projectdf  = projectdf .withColumn("ID", trim(col("ID").cast("string")))
projectdf = projectdf.dropna(subset=['ID'])
projectdf = projectdf.select(original_columns)

gdf = gpd.read_file("s3://airbnbproject-group4vita/raw/geojson/airbnb-listings.geojson")
gdf.rename(columns={'id': 'ID'}, inplace=True)
columns_to_keep = ['latitude', 'longitude', 'ID', 'geometry']
gdf = gdf[columns_to_keep]
gdf['ID'] = gdf['ID'].astype(str).str.strip()
gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt if x is not None else None)

spark_gdf = spark.createDataFrame(gdf)
spark_gdf = spark_gdf.withColumn("latitude", col("latitude").cast("double"))
spark_gdf = spark_gdf.withColumn("longitude", col("longitude").cast("double"))
spark_gdf  = spark_gdf.dropna(subset=['ID'])
spark_gdf  = spark_gdf .withColumn("ID", trim(col("ID").cast("string")))
spark_gdf  = spark_gdf.dropDuplicates(['ID'])

merged_df = projectdf.join(spark_gdf, on='ID', how='right')
merged_df = merged_df.dropDuplicates(['ID'])

# Use a loop to cast columns to IntegerType or Float
integer_columns = [
    'Number of Reviews',
    'Review Scores Rating',
    'Review Scores Accuracy',
    'Review Scores Cleanliness',
    'Review Scores Checkin',
    'Review Scores Communication',
    'Review Scores Location',
    'Review Scores Value',
    'Availability 30',
    'Availability 60',
    'Availability 90',
    'Availability 365',
    'Minimum Nights',
    'Maximum Nights',
    'Host Listings Count',
    'Host Total Listings Count',
    'Accommodates',
    'Bathrooms',
    'Bedrooms',
    'Calculated host listings count'
]
for column in integer_columns:
    merged_df = merged_df.withColumn(column, col(column).cast(IntegerType()))

numeric_columns = [
    "Minimum Nights", "Maximum Nights", "Calculated host listings count", "Reviews per Month", "Price", "Weekly Price", "Monthly Price"
]
for column in numeric_columns:
    merged_df = merged_df.withColumn(column, F.col(column).cast("float"))

#Remove '%' and convert to integer
merged_df = merged_df.withColumn(
    'Host Response Rate',
    regexp_replace(col('Host Response Rate'), '%', '')  
).withColumn(
    'Host Response Rate',
    col('Host Response Rate').cast('int')
)
merged_df = merged_df.withColumn(
    'Host Acceptance Rate',
    regexp_replace(col('Host Acceptance Rate'), '%', '')
).withColumn(
    'Host Acceptance Rate',
    col('Host Acceptance Rate').cast('int') 
)

#Convert columns to date type
merged_df = merged_df.withColumn("Last Scraped", to_date(col("Last Scraped"), "yyyy-MM-dd"))
merged_df = merged_df.withColumn("Host Since", to_date(col("Host Since"), "yyyy-MM-dd"))
merged_df = merged_df.withColumn("Calendar last Scraped", to_date(col("Calendar last Scraped"), "yyyy-MM-dd"))

# Fill column using a proxy column
def fill_with_proxy(df, primary_col, proxy_col):
    return df.withColumn(primary_col, when(col(primary_col).isNull(), col(proxy_col)).otherwise(col(primary_col)))

merged_df = fill_with_proxy(merged_df, 'Market', 'City')
merged_df = fill_with_proxy(merged_df, 'Country Code', 'Country')
merged_df = fill_with_proxy(merged_df, 'Neighbourhood', 'Neighbourhood Group Cleansed')
merged_df = fill_with_proxy(merged_df, 'Host Neighbourhood', 'Host Location')
merged_df = fill_with_proxy(merged_df, 'Host Location', 'Market')

# Repartition the DataFrame to a single partition (for saving to a single file)
merged_df = merged_df.coalesce(1)
merged_df.write.parquet('s3://airbnbproject-group4vita/raw/geojson/output/', mode='overwrite', compression='snappy')
