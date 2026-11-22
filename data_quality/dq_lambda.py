
"""
Lambda: Data Quality Checks
YouTube Data Pipeline — Silver Layer Validation

Called by Step Functions after Bronze-to-Silver processing.
Validates Silver tables before Gold aggregation.
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS Configuration ──────────────────────────────────────────────

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")

ATHENA_OUTPUT = os.environ.get(
    "ATHENA_OUTPUT",
    "s3://yt-data-pipeline-glue-athena-query-result-ishi/athena-results/"
)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

session = boto3.Session(region_name=AWS_REGION)

sns_client = session.client("sns")
s3_client = session.client("s3")

# ── Thresholds ─────────────────────────────────────────────────────

MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = 50_000_000_000
FRESHNESS_HOURS = 48

CRITICAL_COLUMNS = {
    "clean_statistics": [
        "video_id", "title", "channel_title", "views", "region"
    ],
    "clean_reference_data": [
        "id", "region"
    ],
}


# ── Athena Configuration Validation ───────────────────────────────

def validate_athena_output():

    if not ATHENA_OUTPUT.startswith("s3://"):
        raise ValueError(
            f"Invalid ATHENA_OUTPUT location: {ATHENA_OUTPUT}"
        )

    bucket = ATHENA_OUTPUT.replace("s3://", "").split("/")[0]

    logger.info(f"Validating Athena output bucket: {bucket}")

    s3_client.head_bucket(Bucket=bucket)

    logger.info("Athena output bucket exists.")


# ── Data Quality Checks ────────────────────────────────────────────

def check_row_count(df, table_name):

    count = len(df)
    passed = count >= MIN_ROW_COUNT

    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Rows: {count}, Minimum: {MIN_ROW_COUNT}"
    }


def check_null_percentage(df, table_name):

    results = []

    for col in CRITICAL_COLUMNS.get(table_name, []):

        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Missing column: {col}"
            })
            continue

        null_pct = (
            df[col].isna().sum() / len(df) * 100
            if len(df) > 0 else 0
        )

        passed = null_pct <= MAX_NULL_PCT

        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(float(null_pct), 2),
            "threshold": MAX_NULL_PCT,
            "passed": passed,
            "message": f"{col} null percentage: {null_pct:.2f}%"
        })

    return results


def check_schema(df, table_name):

    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(df.columns)

    missing = expected - actual

    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": sorted(list(missing)),
        "passed": len(missing) == 0,
        "message": (
            f"Missing columns: {missing}"
            if missing else "All expected columns present"
        )
    }


def check_value_ranges(df, table_name):

    results = []

    if table_name != "clean_statistics":
        return results

    if "views" not in df.columns:
        return [{
            "check": "value_range",
            "table": table_name,
            "passed": False,
            "message": "Views column is missing"
        }]

    views = pd.to_numeric(df["views"], errors="coerce")

    negative = int((views < 0).sum())
    extreme = int((views > MAX_VIEWS).sum())

    passed = negative == 0 and extreme == 0

    results.append({
        "check": "value_range",
        "table": table_name,
        "column": "views",
        "negative_count": negative,
        "extreme_count": extreme,
        "passed": passed,
        "message": (
            f"Negative: {negative}, "
            f"Extreme: {extreme}"
        )
    })

    return results


def check_freshness(df, table_name):

    timestamp_columns = [
        "_processed_at",
        "_ingestion_timestamp"
    ]

    ts_col = next(
        (col for col in timestamp_columns if col in df.columns),
        None
    )

    if ts_col is None:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found; freshness skipped"
        }

    timestamps = pd.to_datetime(
        df[ts_col],
        errors="coerce",
        utc=True
    ).dropna()

    if timestamps.empty:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": False,
            "message": "Timestamp column contains no valid timestamps"
        }

    latest = timestamps.max()
    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(hours=FRESHNESS_HOURS)
    )

    passed = latest.to_pydatetime() >= cutoff

    return {
        "check": "freshness",
        "table": table_name,
        "latest_record": str(latest),
        "cutoff": str(cutoff),
        "passed": passed,
        "message": f"Latest: {latest}, Cutoff: {cutoff}"
    }


# ── Main Lambda Handler ────────────────────────────────────────────

def lambda_handler(event, context):

    logger.info("Starting Silver layer Data Quality Checks")

    database = event.get(
        "database",
        "yt_pipeline_silver_dev"
    )

    tables = event.get(
        "tables",
        ["clean_statistics", "clean_reference_data"]
    )

    all_results = []
    overall_passed = True

    try:
        validate_athena_output()

    except Exception as e:

        logger.exception("Athena output validation failed")

        return {
            "quality_passed": False,
            "checks_passed": 0,
            "checks_total": 1,
            "details": [{
                "check": "athena_output",
                "passed": False,
                "message": str(e)
            }]
        }

    for table_name in tables:

        logger.info(
            f"Checking table: {database}.{table_name}"
        )

        try:

            query = f'SELECT * FROM "{table_name}" LIMIT 10000'

            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                s3_output=ATHENA_OUTPUT,
                workgroup=ATHENA_WORKGROUP,
                boto3_session=session,
                ctas_approach=False
            )

            logger.info(
                f"Successfully read {len(df)} rows from {table_name}"
            )

        except Exception as e:

            logger.exception(
                f"Failed to read table: {table_name}"
            )

            all_results.append({
                "check": "read_table",
                "table": table_name,
                "passed": False,
                "message": str(e)
            })

            overall_passed = False
            continue

        checks = []

        checks.append(check_row_count(df, table_name))
        checks.extend(check_null_percentage(df, table_name))
        checks.append(check_schema(df, table_name))
        checks.extend(check_value_ranges(df, table_name))
        checks.append(check_freshness(df, table_name))

        for check in checks:

            status = "PASS" if check["passed"] else "FAIL"

            logger.info(
                f"{status}: {check['check']} - {check['message']}"
            )

            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # ── Final Summary ──────────────────────────────────────────────

    passed_count = sum(
        1 for result in all_results if result["passed"]
    )

    total_count = len(all_results)

    logger.info(
        f"DQ SUMMARY: {passed_count}/{total_count} checks passed"
    )

    logger.info(
        f"Overall status: {'PASS' if overall_passed else 'FAIL'}"
    )

    # ── SNS Alert ──────────────────────────────────────────────────

    if not overall_passed and SNS_TOPIC:

        failed_checks = [
            result for result in all_results
            if not result["passed"]
        ]

        try:
            sns_client.publish(
                TopicArn=SNS_TOPIC,
                Subject="[YT Pipeline] Data Quality Checks FAILED",
                Message=json.dumps(
                    failed_checks,
                    indent=2,
                    default=str
                )
            )

            logger.info("SNS failure notification sent.")

        except Exception:
            logger.exception("Failed to send SNS notification")

    # ── Step Functions Response ────────────────────────────────────

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "database": database,
        "details": all_results
    }
