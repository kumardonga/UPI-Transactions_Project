from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

username = dbutils.secrets.get(scope="credentials", key="RED_PANDA_USERNAME")
password = dbutils.secrets.get(scope="credentials", key="RED_PANDA_PASSWORD")

@dp.table(
    name = "upi_transactions_bronze",
    comment = "Loads upi kafka transactions"
)

def upi_transactions_bronze():
    transactions_raw_binary = (spark.readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", "dajuabq7294n2a8d0k50.any.ap-south-1.mpx.prd.cloud.redpanda.com:9092")
                   .option("subscribe", "UPI_Transactions")
                   .option("startingOffsets", "earliest")
                   .option("kafka.security.protocol", "SASL_SSL")
                   .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{username}' password='{password}';")
                   .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
                   .load()
                  )
    upi_transactions_bronze = (transactions_raw_binary
                               .withColumn("upi_bronze_ingestion_time",current_timestamp())
                                .select(col("key").cast("string"),
                                        col("value").cast("string"),
                                        col("topic"),
                                        col("partition"),
                                        col("offset"),
                                        col("timestamp"),
                                        col("timestampType"),
                                        col("upi_bronze_ingestion_time")
                                        )
                                
                                )
                                
    return upi_transactions_bronze
    

settlement_schema = StructType([
StructField("txn_id", StringType()),
StructField("rrn", StringType()),
StructField("settlement_amount", DecimalType(10,2)),
StructField("settlement_status", StringType()),
StructField("settlement_date", DateType()),
StructField("settled_timestamp", TimestampType())
])



@dp.table(
    name = "bronze_settlement_data",
    comment = "Loads batch files from S3"
)

def bronze_settlement_data():
    settled_read = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format","csv")
                .option("header","true")
                .schema(settlement_schema)
                .option("cloudFiles.schemaEvolutionMode","rescue")\
                .load("s3://settlement-file-databricks/*.csv")
                )
    return settled_read.withColumn("ingested_time", current_timestamp())















