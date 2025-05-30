sql = SQLContext(sc)
import pyspark.sql.functions as F

df = spark.read.option("multiline","true").json('s3a://etl-part2-bucket-read-only/cars.json', multiLine=True)

names = df.select("Name")
names.show()

names.write.parquet("s3a://etl-part2-bucket/car_names.parquet")
get_names = spark.read.parquet("s3a://etl-part2-bucket/car_names.parquet")
get_names.show()

origins = df.select("Origin")
origins.show()

origins_distinct = origins.distinct()
origins_distinct.show()
origins_distinct.write.parquet("s3a://etl-part2-bucket/cars_distinct_origins.parquet")
get_origins_distinct = spark.read.parquet("s3a://etl-part2-bucket/cars_distinct_origins.parquet")
get_origins_distinct.show()
