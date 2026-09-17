from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name = "reconciled",
    comment = "Gold Ready Table"
)

def reconciled():
    settlement_silver_df = spark.read.table("settlement_silver")
    upi_stream_silver_view_df = spark.read.table("upi_stream_silver_view")

    settlement_silver_df = settlement_silver_df.alias("sm")
    upi_stream_silver_view_df = upi_stream_silver_view_df.alias("st")
    joined_df = upi_stream_silver_view_df.join(settlement_silver_df,col("st.txn_id") == col("sm.txn_id"),how = "fullouter").select(
        col("st.txn_id").alias("upi_txn_id"),
        col("st.amount").alias("upi_trans_amount")
        ,col("st.rrn").alias("upi_trans_rrn")
        ,col("sm.txn_id").alias("settlement_txn_id")
        ,coalesce(col("st.upi_bronze_ingestion_time"),col("sm.ingested_time")).alias("ingested_time")
        ,col("sm.rrn").alias("settlement_rrn")
        ,col("key")
        ,col("payer_vpa")
        ,col("payee_vpa")
        ,col("payer_bank")
        ,col("payee_bank")
        ,col("txn_type")
        ,col("channel")
        ,col("status")
        ,col("npci_response_code")
        ,col("initiated_timestamp")
        ,col("topic")
        ,col("partition")
        ,col("offset")
        ,col("timestamp")
        ,col("timestampType")
        ,col("sm.settlement_status")
        ,col("sm.settlement_amount")
        ,col("upi_bronze_ingestion_time")
        ,col("sm.settlement_date")
        ,col("late_flag")
        ,col("txn_date")
        ,col("txn_hour")
        ,col("silver_processed_timestamp")
        ,col("sm.settled_timestamp")
        )
        

    reconciled_df = (
    joined_df
    .withColumn("reconciliation"
    ,when((col("upi_txn_id").isNotNull()) & (col("settlement_txn_id").isNotNull()) & (col("status")=="SUCCESS") & (col("settlement_status")=="SETTLED"), "RECONCILED")

    .when((col("upi_txn_id").isNotNull()) & (col("settlement_txn_id").isNotNull()) & (col("settlement_status")=="SETTLED") & (col("status")=="PENDING"),"SETTLED RECONCILED")
    
    .when((col("status")=="PENDING") & (col("settlement_status")=="PENDING") & (col("initiated_timestamp").cast("timestamp") < current_timestamp() - expr("interval 1 day")), "STALE PENDING")
    .when((col("status")=="PENDING") & (col("settlement_status")=="PENDING") & (date_add(col("initiated_timestamp"),1) >= (col("settlement_date"))), "PENDING")
    .when((col("status") == "SUCCESS") & (col("settlement_status") == "PENDING"), "DISCREPANCY STATUS MISMATCH")
    .when((col("status")== "SUCCESS") & (col("settlement_txn_id").isNull()), "DISCREPANCY UNSETTLED SUCCESS")

    .when((col("status")=="PENDING") & (col("settlement_txn_id").isNull()) & (date_add(col("initiated_timestamp"),1) > current_date()),"PENDING WITHIN SLA")
    .when((col("status")=="PENDING") & (col("settlement_txn_id").isNull()) & (col("initiated_timestamp").cast("timestamp") < current_timestamp() - expr("interval 1 day")),"DISCREPANCY STALE PENDING")

    .when((col("upi_txn_id").isNull()) & (col("settlement_txn_id").isNotNull()), "ORPHAN RECORD")
    .otherwise("UNKNOWN")
    )
    .withColumn("flag",when(col("reconciliation").contains("RECONCILED"),0).otherwise(1))
    )

    return reconciled_df





















    