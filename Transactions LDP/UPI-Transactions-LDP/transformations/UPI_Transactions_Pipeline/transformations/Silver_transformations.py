from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("rrn", StringType(), True),
    StructField("payer_vpa", StringType(), True),
    StructField("payee_vpa", StringType(), True),
    StructField("payer_bank", StringType(), True),
    StructField("payee_bank", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("txn_type", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("npci_response_code", StringType(), True),
    StructField("initiated_timestamp", StringType(), True)
    ])

@dp.table(
    name = "upi_transactions_bronze_inflated",
    comment = "Inflates the UPI transactions table"
)

def upi_transactions_bronze_inflated():
    silver_inflated =(
        spark.readStream.table("workspace.default.upi_transactions_bronze")
        .select(
            col("key"),from_json(col("value"),schema).alias("fields"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
            col("timestampType"),
            col("upi_bronze_ingestion_time")
        )
        .select
        (
            col("key").cast("string").alias("key"),
            col("fields.txn_id").cast("string").alias("txn_id"),
            col("fields.rrn").cast("string").alias("rrn"),
            col("fields.payer_vpa").cast("string").alias("payer_vpa"),
            col("fields.payee_vpa").cast("string").alias("payee_vpa"),
            col("fields.payer_bank").cast("string").alias("payer_bank"),
            col("fields.payee_bank").cast("string").alias("payee_bank"),
            col("fields.amount").cast("decimal(10,2)").alias("amount"),
            col("fields.txn_type").cast("string").alias("txn_type"),
            col("fields.channel").cast("string").alias("channel"),
            col("fields.status").cast("string").alias("status"),
            col("fields.npci_response_code").cast("string").alias("npci_response_code"),
            col("fields.initiated_timestamp").cast("timestamp").alias("initiated_timestamp"),
            col("topic").cast("string").alias("topic"),
            col("partition").cast("int").alias("partition"),
            col("offset").cast("long").alias("offset"),
            col("timestamp").cast("timestamp").alias("timestamp"),
            col("timestampType").cast("int").alias("timestampType"),
            col("upi_bronze_ingestion_time")
        )
    )
    silver_inflated = (silver_inflated.withColumn("late_flag",when(col("initiated_timestamp") < current_timestamp() - expr("interval 2 minute"),lit(1)).otherwise(lit(0)))
        .withColumn("txn_date",date_format(col("initiated_timestamp"),"yyyy-MM-dd"))
       .withColumn("txn_hour",date_format(col("initiated_timestamp"),"HH")))

    return silver_inflated



column_validations = (
                        col("txn_id").isNotNull() 
                        & col("rrn").isNotNull() 
                        & col("amount").isNotNull() 
                        & col("initiated_timestamp").isNotNull()
                        & ((col("status") == "PENDING")
                            | col("npci_response_code").isNotNull()
                          )
                        & ((col("amount") > 0) & (col("amount")<100000))
                        & upper(col("status")).isin(["SUCCESS", "FAILED", "PENDING"])
                        & col("channel").isin(["APP", "QR", "COLLECT"])
                        & col("txn_type").isin(["P2P", "P2M"])
                    )





@dp.table(
    name = "upi_transactions_quarantine",
    comment = "Rejected UPI transactions"
)

def upi_transactions_quarantine():
    quarantine_df = spark.readStream.table("upi_transactions_bronze_inflated").filter(~column_validations)
    return quarantine_df





@dp.table(
    name = "UPI_transactions_silver",
    comment = "Cleaned UPI transactions"
)

def UPI_transactions_silver():
        column_validated_df = spark.readStream.table("upi_transactions_bronze_inflated").filter(column_validations)

        column_standardizations_df = (column_validated_df
                                .withColumn("status",upper(col("status")))
                                .withColumn("txn_type",upper(col("txn_type")))
                                .withColumn("channel",upper(col("channel")))
                                .withColumn("payer_vpa",lower(col("payer_vpa")))
                                .withColumn("payee_vpa",lower(col("payee_vpa")))
                                )
        
        silver_dedup = (column_standardizations_df
                            .withWatermark("initiated_timestamp", "20 minutes")
                            .dropDuplicatesWithinWatermark(["txn_id"])
                            .withColumn("silver_processed_timestamp",current_timestamp())
                        )

        return silver_dedup
    



column_validations_settlements = (
                        col("txn_id").isNotNull() 
                        & col("rrn").isNotNull() 
                        & col("settlement_amount").isNotNull() 
                        & col("settled_timestamp").isNotNull()
                        & ((col("settlement_amount") > 0) & (col("settlement_amount")<100000))
                        & upper(col("settlement_status")).isin(["SETTLED", "PENDING"])
                    )


@dp.table(
    name = "settlement_silver",
    comment = "Cleaned settlement data"
)

def settlement_silver():
        settlement_bronze_df = spark.readStream.table("workspace.default.bronze_settlement_data").drop("_rescued_data")

        settlement_bronze_df_dedup = (settlement_bronze_df.filter(column_validations_settlements)
                        .withColumn("settlement_status",upper(col("settlement_status")))
                        .withWatermark("settled_timestamp", "1 day")
                        .dropDuplicatesWithinWatermark(["txn_id"])
                        )
        return settlement_bronze_df_dedup


@dp.table(
    name = "settlement_quarantine",
    comment = "Rejected settlement data"
)

def settlement_quarantine():
    settlement_quarantine = spark.readStream.table("workspace.default.bronze_settlement_data").drop("_rescued_data").filter(~column_validations_settlements)

    return settlement_quarantine












































