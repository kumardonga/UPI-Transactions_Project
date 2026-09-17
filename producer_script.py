!pip install confluent-kafka faker -q
!pip install boto3

import random
import time
import string
import json
import csv
from faker import Faker
from zoneinfo import ZoneInfo
from confluent_kafka import Producer
from datetime import datetime, timedelta
from google.colab import userdata
import boto3

fake = Faker()

TS_FORMAT = "%Y-%m-%dT%H:%M:%S+05:30"
BANKS = ["HDFC", "ICICI", "SBI", "AXIS", "KOTAK", "PNB", "BOB", "CANARA", "INDUSIND", "YES"]
TOPIC = "UPI_Transactions"


def now_ist_str(offset_days: int = 0) -> str:
    return (datetime.now(ZoneInfo("Asia/Kolkata")) - timedelta(days=offset_days)).strftime(TS_FORMAT)


def generate_txn_id(prefix: str) -> str:
    return prefix + "".join(random.choices(string.ascii_uppercase + string.digits, k=9))


bootstrap_server = userdata.get("REDPANDA_BOOTSTRAP_SERVER")
username = userdata.get("REDPANDA_USERNAME")
password = userdata.get("REDPANDA_PASSWORD")

producer_config = {
    "bootstrap.servers": bootstrap_server,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": username,
    "sasl.password": password
}
producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


access_key = userdata.get("ACCESS_KEY")
secret_key = userdata.get("SECRET_ACCESS_KEY")
aws_region = userdata.get("AWS_REGION")
bucket_name = userdata.get("S3_BUCKET")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=aws_region
)

SETTLEMENT_COLUMNS = [
    "txn_id",
    "rrn",
    "settlement_amount",
    "settlement_status",
    "settlement_date",
    "settled_timestamp"
]


def write_and_upload_settlements(settlement_rows, file_prefix: str):
    file_name = f"{file_prefix}_{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    with open(file_name, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SETTLEMENT_COLUMNS)
        writer.writeheader()
        writer.writerows(settlement_rows)
    s3_client.upload_file(file_name, bucket_name, file_name)


def produce_batch(transactions):
    for txn in transactions:
        producer.produce(
            TOPIC,
            key=txn["txn_id"],
            value=json.dumps(txn),
            callback=delivery_report
        )
    producer.flush()


# ---------------------------------------------------------------------------
# Good scenarios
# ---------------------------------------------------------------------------

def good_scenarios(count):
    transactions = []
    for _ in range(count):
        txn_id = generate_txn_id("G")
        transaction = {
            "txn_id": txn_id,
            "rrn": f"R{random.randint(10000000, 99999999)}",
            "payer_vpa": f"{fake.first_name().lower()}@upi",
            "payee_vpa": f"{fake.first_name().lower()}@upi",
            "payer_bank": random.choice(BANKS),
            "payee_bank": random.choice(BANKS),
            "amount": round(random.uniform(100, 100000), 2),
            "txn_type": random.choice(["P2P", "P2M"]),
            "channel": random.choice(["APP", "QR", "COLLECT"]),
            "status": "SUCCESS",
            "npci_response_code": "00",
            "initiated_timestamp": now_ist_str()
        }
        transactions.append(transaction)
    return transactions


# ---------------------------------------------------------------------------
# DQ scenarios
# ---------------------------------------------------------------------------

def dq_scenarios():
    dq_types = [
        "NULL_AMOUNT",
        "NEGATIVE_AMOUNT",
        "INVALID_STATUS",
        "INVALID_CHANNEL",
        "INVALID_TXN_TYPE",
    ]
    transactions = []
    for dq_type in dq_types:
        txn_id = generate_txn_id("D")
        transaction = {
            "txn_id": txn_id,
            "rrn": f"R{random.randint(10000000, 99999999)}",
            "payer_vpa": fake.user_name().lower() + "@upi",
            "payee_vpa": fake.user_name().lower() + "@upi",
            "payer_bank": random.choice(BANKS),
            "payee_bank": random.choice(BANKS),
            "amount": round(random.uniform(100, 50000), 2),
            "txn_type": random.choice(["P2P", "P2M"]),
            "channel": random.choice(["APP", "QR", "COLLECT"]),
            "status": "SUCCESS",
            "npci_response_code": "00",
            "initiated_timestamp": now_ist_str()
        }

        if dq_type == "NULL_AMOUNT":
            transaction["amount"] = None
        elif dq_type == "NEGATIVE_AMOUNT":
            transaction["amount"] = -2990.70
        elif dq_type == "INVALID_STATUS":
            transaction["status"] = "REVERSED"
        elif dq_type == "INVALID_CHANNEL":
            transaction["channel"] = "WEB"
        elif dq_type == "INVALID_TXN_TYPE":
            transaction["txn_type"] = "ATM"

        transactions.append(transaction)
    return transactions


# ---------------------------------------------------------------------------
# Business scenarios
# ---------------------------------------------------------------------------

def business_scenarios():
    business_types = [
        "SUCCESS_SETTLED",
        "PENDING_SETTLED",
        "PENDING_STALE",
        "PENDING_WITHIN_SLA",
        "SUCCESS_PENDING",
        "SUCCESS_UNSETTLED",
        "PENDING_UNSETTLED_WITHIN_SLA",
        "PENDING_UNSETTLED_STALE",
        "ORPHAN"
    ]
    transactions = []
    settlements = []

    for business_type in business_types:
        settlement_status = None
        txn_id = generate_txn_id("B")

        transaction = {
            "txn_id": txn_id,
            "rrn": f"R{random.randint(10000000, 99999999)}",
            "payer_vpa": fake.user_name().lower() + "@upi",
            "payee_vpa": fake.user_name().lower() + "@upi",
            "payer_bank": random.choice(BANKS),
            "payee_bank": random.choice(BANKS),
            "amount": round(random.uniform(100, 50000), 2),
            "txn_type": random.choice(["P2P", "P2M"]),
            "channel": random.choice(["APP", "QR", "COLLECT"]),
            "status": "SUCCESS",
            "npci_response_code": "00",
            "initiated_timestamp": now_ist_str()
        }

        # --- transaction status ---
        if business_type == "SUCCESS_SETTLED":
            transaction["status"] = "SUCCESS"
        elif business_type == "PENDING_SETTLED":
            transaction["status"] = "PENDING"
        elif business_type == "PENDING_STALE":
            transaction["status"] = "PENDING"
            transaction["initiated_timestamp"] = now_ist_str(offset_days=2)
        elif business_type == "PENDING_WITHIN_SLA":
            transaction["status"] = "PENDING"
        elif business_type == "SUCCESS_PENDING":
            transaction["status"] = "SUCCESS"
        elif business_type == "PENDING_UNSETTLED_WITHIN_SLA":
            transaction["status"] = "PENDING"
        elif business_type == "PENDING_UNSETTLED_STALE":
            transaction["status"] = "PENDING"
            transaction["initiated_timestamp"] = now_ist_str(offset_days=2)
        # SUCCESS_UNSETTLED and ORPHAN keep the default status="SUCCESS"

        # --- settlement status ---
        if business_type == "SUCCESS_SETTLED":
            settlement_status = "SETTLED"
        elif business_type == "PENDING_SETTLED":
            settlement_status = "SETTLED"
        elif business_type == "PENDING_STALE":
            settlement_status = "PENDING"
        elif business_type == "PENDING_WITHIN_SLA":
            settlement_status = "PENDING"
        elif business_type == "SUCCESS_PENDING":
            settlement_status = "PENDING"
        elif business_type in ("SUCCESS_UNSETTLED", "PENDING_UNSETTLED_WITHIN_SLA", "PENDING_UNSETTLED_STALE"):
            settlement_status = None

        if settlement_status is not None:
            settlements.append({
                "txn_id": transaction["txn_id"],
                "rrn": transaction["rrn"],
                "settlement_amount": transaction["amount"],
                "settlement_status": settlement_status,
                "settlement_date": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d"),
                "settled_timestamp": now_ist_str()
            })

        if business_type == "ORPHAN":
            # Settlement-only record -- never sent to Kafka as a transaction.
            orphan_txn_id = generate_txn_id("B")
            settlements.append({
                "txn_id": orphan_txn_id,
                "rrn": f"R{random.randint(10000000, 99999999)}",
                "settlement_amount": round(random.uniform(100, 50000), 2),
                "settlement_status": "SETTLED",
                "settlement_date": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d"),
                "settled_timestamp": now_ist_str()
            })
        else:
            transactions.append(transaction)

    return transactions, settlements




# Good
good_data = good_scenarios(65)
produce_batch(good_data)
good_settlements = [
    {
        "txn_id": t["txn_id"],
        "rrn": t["rrn"],
        "settlement_amount": t["amount"],
        "settlement_status": "SETTLED",
        "settlement_date": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d"),
        "settled_timestamp": now_ist_str()
    }
    for t in good_data
]
write_and_upload_settlements(good_settlements, "good_batch_settlement")



dq_data = dq_scenarios()
produce_batch(dq_data)


business_data, business_settlements = business_scenarios()
produce_batch(business_data)



rescue_records = random.sample(business_settlements, 5)   #here give active business scenarios count other than unsettled scenarios. because there will be
                                                          #records for unsettled scenarios.

rescue_records[0]["settlement_amount"] = "INVALID_AMOUNT"
rescue_records[1]["settlement_date"] = "INVALID_DATE"
rescue_records[2]["settled_timestamp"] = "INVALID_TIMESTAMP"
rescue_records[3]["settlement_amount"] = "INVALID_AMOUNT"
rescue_records[4]["settlement_date"] = "INVALID_DATE"



write_and_upload_settlements(business_settlements, "business_batch_settlement")
