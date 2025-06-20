#!/usr/bin/env python3

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct

BUCKET = "etl-part2-bucket"
FQDN = "rc1a-r7emso0mugu0fogh.mdb.yandexcloud.net"
TOPIC = "kafkatopic"

def main():
   spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

   df = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(f"s3a://{BUCKET}/etl-part2-bucket/vehicles.parquet")
   df = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))
   df.write.format("kafka") \
      .option("kafka.bootstrap.servers", f"{FQDN}:9091") \
      .option("topic", TOPIC) \
      .option("kafka.security.protocol", "SASL_SSL") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
      .option("kafka.sasl.jaas.config",
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
              "username=user1 "
              "password=password1 "
              ";") \
      .save()

if __name__ == "__main__":
   main()
