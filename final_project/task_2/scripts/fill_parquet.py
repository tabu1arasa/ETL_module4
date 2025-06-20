from pyspark.sql import SparkSession

SOURCE_BUCKET = "s3a://etl-part2-bucket-read-only"
TARGET_BUCKET = "s3a://etl-part2-bucket"

def recieve_data(spark):
     df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{SOURCE_BUCKET}/vehicles.csv")
     return df

def write_data(data):
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
    write_data(data)

if __name__ == '__main__':
    main()
