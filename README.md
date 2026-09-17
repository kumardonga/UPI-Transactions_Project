# UPI Transactions Data Engineering Project

An end-to-end UPI transaction data platform built using Databricks, PySpark, Kafka, AWS S3, Delta Lake, and GenAI.

The project simulates a real-world payment processing system where UPI transactions arrive continuously through Kafka, while settlement data is received as daily CSV files in S3. The pipeline processes both sources, reconciles transactions with settlement records, monitors pipeline health, detects unusual behavior, and generates simple AI-based summaries of detected anomalies.

## Architecture

```text
                    Python Producer
                    /             \
                   /               \
                Kafka              S3
                  |          Settlement CSV
                  |               |
                  v               v
               Bronze           Bronze
                  |               |
                  v               v
               Silver           Silver
                  \               /
                   \             /
                    v           v
                  Gold Reconciliation
                           |
                           v
                  Pipeline Metrics
                           |
                           v
                   Anomaly Detection
                           |
                           v
                      GenAI / LLM
                           |
                           v
					Anomaly Summary
```

## What the Pipeline Does

1. A Python producer generates simulated UPI transaction events and publishes them to **Kafka topic**.
2. Daily settlement CSV files are generated and stored in **AWS S3 bucket**.
3. Both sources are ingested into Databricks and processed through the **Bronze → Silver → Gold** architecture.
4. Kafka transactions are processed using **Structured Streaming**, while S3 settlement files are ingested using **Auto Loader**.
5. The Silver layer handles data cleansing, validation and deduplication.
6. The Gold layer reconciles UPI transactions against settlement records.
7. Reconciliation identifies outcomes such as:

   * RECONCILED
   * SETTLED RECONCILED
   * PENDING
   * STALE PENDING
   * DISCREPANCY STATUS MISMATCH
   * DISCREPANCY UNSETTLED SUCCESS
   * PENDING WITHIN SLA
   * ORPHAN RECORD
   * UNKNOWN
8. Metrics from each pipeline run are consolidated and used for pipeline monitoring.
9. Statistical analysis identifies unusual changes in the metrics.
10. Detected anomalies are passed to an LLM, which generates a simple human-readable summary.

Pipeline Monitoring & Anomaly Detection:

Every pipeline run produces metrics such as transaction counts, processing durations, error rates and reconciliation statistics.

These metrics are consolidated into a single **upi_pipeline_metrics** Delta table, providing one place to monitor the health of the pipeline.

A frozen **`upi_baseline_stats`** table contains a snapshot of 50 previous runs and acts as the reference for anomaly detection.

Two detection strategies are used based on the behavior of the metric:

* **Dense metrics:** 8 frequently occurring count/duration metrics use mean, standard deviation and z-score.
* **Rare metrics:** 7 rare-event metrics, including rejection/discrepancy-related metrics, use maximum-based thresholds because their high variation makes z-score less reliable.

The per-run table stores raw metrics. A view joins the metrics with the frozen baseline and calculates the statistical values and anomaly flags when queried.

For anomalous metrics, values crossing the configured threshold **`|z| > 2 × 0.9`** which is 90% near to threshold  are selected and passed to an LLM.

The roles are intentionally separated:

**Statistics detect the anomaly → GenAI explains the anomaly.**

For example, instead of requiring someone to inspect several metric columns, the system can produce a summary such as:

> "Discrepancy rate spiked in this run."

Technology Stack:

*Databricks
*Apache Spark / PySpark
*Delta Lake
*Kafka
*AWS S3
*Auto Loader
*Structured Streaming
*Databricks Jobs & Scheduling
*Databricks CLI
*Databricks Secrets
*Databricks Widgets
*Databricks Asset Bundles (DAB)
*GenAI / LLM
*GitHub

## Infrastructure & Deployment

The project uses **Databricks Asset Bundles (DAB)** to define and deploy the Databricks infrastructure.

The application code and DAB configuration are maintained in GitHub, allowing the Databricks workloads to be version controlled and deployed consistently.

## Repository Structure


UPI-Transactions_Project/
│
├── Transactions LDP/
│   └── Batch processing, reconciliation and metrics
│
├── Transactions_stream/
│   └── Streaming transaction processing
│
├── resources/
│   └── Databricks deployment resources
│
├── databricks.yml
│
└── README.md


## Future Enhancement

The next planned enhancement is a **conversational chatbot** that can interact with the processed transaction and pipeline data.

The goal is to allow users to ask questions in natural language about transactions, reconciliation results, pipeline metrics and detected anomalies.

## Project Goal

This project brings together the main components of a modern data engineering workflow:

**Streaming ingestion → Data processing → Reconciliation → Monitoring → Statistical anomaly detection → GenAI analysis → Infrastructure as Code**

It is designed to demonstrate how these components can work together in a practical UPI transaction processing scenario.
