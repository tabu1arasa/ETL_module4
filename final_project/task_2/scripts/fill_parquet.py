import random
from pyspark.sql import SparkSession

SOURCE_BUCKET = "s3a://etl-part2-bucket-read-only"
TARGET_BUCKET = "s3a://etl-part2-bucket"

def recieve_data(spark):
     df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{SOURCE_BUCKET}/vehicles.csv")
     # data = [(i, random.random()) for i in range(1000)]
     return df

def write_data(data):
    # data = [(i, random.random()) for i in range(1000)]
    # df = spark.createDataFrame(
    #     data,
    #     schema=[
    #         "id", "url", "region",
    #         "region_url", "price", "year",
    #         "manufacturer", "model", "condition",
    #         "cylinders", "fuel", "odometer",
    #         "title_status", "transmission", "VIN",
    #         "drive", "size", "type",
    #         "paint_color", "image_url", "description",
    #         "county", "state", "lat",
    #         "long", "posting_date"
    #     ]
    # )
    data.write.mode("overwrite").parquet(f"{TARGET_BUCKET}/etl-part2-bucket/vehicles.parquet")

def main():
    spark = (
        SparkSession
        .builder
        .appName('job_with_table')
        .enableHiveSupport()
        .getOrCreate()
    )
    data = recieve_data(spark)
    # prepare_table(spark, database, table)
    write_data(data)

if __name__ == '__main__':
    main()
