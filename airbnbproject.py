from pyspark.sql import SparkSession

import geopandas as gpd
import pandas as pd
from pyspark.sql.functions import to_date, col, trim, sum, when, round, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType, FloatType
import pyspark.sql.functions as F
import logging

logging.basicConfig(filename='logs.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logging.info('Starting PySpark job')

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

numeric_columns = [
    "Minimum Nights", "Maximum Nights", "Calculated host listings count", "Reviews per Month", "Price", "Weekly Price", "Monthly Price"
]

for column in numeric_columns:
    merged_df = merged_df.withColumn(column, F.col(column).cast("float"))

# Cast columns to appropriate data types
merged_df = merged_df.withColumn('Number of Reviews', col('Number of Reviews').cast(IntegerType()))
merged_df = merged_df.withColumn('Review Scores Rating', col('Review Scores Rating').cast(IntegerType()))
merged_df = merged_df.withColumn('Review Scores Accuracy', col('Review Scores Accuracy').cast(IntegerType()))
merged_df = merged_df.withColumn('Review Scores Cleanliness', col('Review Scores Cleanliness').cast(IntegerType()))
merged_df = merged_df.withColumn('Review Scores Checkin', col('Review Scores Checkin').cast(IntegerType()))
merged_df = merged_df.withColumn('Review Scores Communication', col('Review Scores Communication').cast(IntegerType()))
merged_df = merged_df.withColumn('Review Scores Location', col('Review Scores Location').cast(IntegerType()))
merged_df = merged_df.withColumn('Review Scores Value', col('Review Scores Value').cast(IntegerType()))
merged_df = merged_df.withColumn('Availability 30', col('Availability 30').cast(IntegerType()))
merged_df = merged_df.withColumn('Availability 60', col('Availability 60').cast(IntegerType()))
merged_df = merged_df.withColumn('Availability 90', col('Availability 90').cast(IntegerType()))
merged_df = merged_df.withColumn('Availability 365', col('Availability 365').cast(IntegerType()))
merged_df = merged_df.withColumn('Minimum Nights', col('Minimum Nights').cast(IntegerType()))
merged_df = merged_df.withColumn('Maximum Nights', col('Maximum Nights').cast(IntegerType()))
merged_df = merged_df.withColumn('Host Listings Count', col('Host Listings Count').cast(IntegerType()))
merged_df = merged_df.withColumn('Host Total Listings Count', col('Host Total Listings Count').cast(IntegerType()))
merged_df = merged_df.withColumn('Accommodates', col('Accommodates').cast(IntegerType()))
merged_df= merged_df.withColumn('Bathrooms', col('Bathrooms').cast(IntegerType()))
merged_df = merged_df.withColumn('Bedrooms', col('Bedrooms').cast(IntegerType()))
merged_df = merged_df.withColumn('Calculated host listings count', col('Calculated host listings count').cast(IntegerType()))

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

# Applying the proxy method
merged_df = fill_with_proxy(merged_df, 'City', 'Smart Location')
merged_df = fill_with_proxy(merged_df, 'Market', 'City')
merged_df = fill_with_proxy(merged_df, 'Country Code', 'Country')
merged_df = fill_with_proxy(merged_df, 'Neighbourhood', 'Neighbourhood Group Cleansed')
merged_df = fill_with_proxy(merged_df, 'Host Neighbourhood', 'Host Location')
merged_df = fill_with_proxy(merged_df, 'Host Location', 'Market')  # or 'State' as another option

# Check null counts to confirm changes
null_counts = merged_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in merged_df.columns])
null_counts.show()

# Repartition the DataFrame to a single partition (for saving to a single file)
merged_df = merged_df.coalesce(1)
merged_df.write.parquet('s3://airbnbproject-group4vita/raw/geojson/output/', mode='overwrite', compression='snappy')
