YouTube Trending Data Engineering Pipeline | AWS

An end-to-end AWS Data Engineering project that integrates historical YouTube trending datasets from Kaggle and live data from YouTube Data API v3. The pipeline processes data through a Bronze → Silver → Gold architecture, performs data quality validation, and generates analytics-ready datasets using AWS Glue, Lambda, S3, and Step Functions.
<img width="1117" height="632" alt="image" src="https://github.com/user-attachments/assets/3781e75f-86dd-47b7-adbe-65e94fbda4db" />

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Data Flow](#data-flow)
  - [Bronze Layer](#1-bronze-layer-raw-data)
  - [Silver Layer](#2-silver-layer-transformed-data)
  - [Data Quality Validation](#3-data-quality-validation)
  - [Gold Layer](#4-gold-layer-analytics)
- [Gold Layer Output Tables](#gold-layer-output-tables)
- [Step Functions Orchestration](#step-functions-orchestration)
- [Configuration](#configuration)
- [Deployment Overview](#deployment-overview)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring and Notifications](#monitoring-and-notifications)
- [Key Features](#key-features)
- [Future Enhancements](#future-enhancements)

---

## Overview

This project implements an automated YouTube Data Engineering Pipeline using AWS cloud services.

It integrates two different data sources:

**1. Kaggle – Historical Data**

- Historical YouTube trending video datasets.
- CSV files covering 10 countries.
- Category mapping JSON files.
- Used for historical data processing and analysis.

**2. YouTube Data API v3 – Live Data**

- Fetches current trending video information.
- Retrieves video statistics, metadata, and category information.
- Uses Python and AWS Lambda for automated ingestion.
- Amazon EventBridge can trigger ingestion on a scheduled basis.

Both data formats are handled by the Bronze-to-Silver Glue ETL job, which standardizes the records into a common schema.

The processed data is transformed into analytics-ready Gold tables for querying and reporting.

---

## Architecture

The project follows the Medallion Architecture pattern:

**Bronze → Silver → Gold**

### High-Level Architecture

```text
             DATA SOURCES
                   |
        +----------+----------+
        |                     |
    KAGGLE DATASET       YOUTUBE DATA API
    Historical CSV       Live API Data
    Category JSON        Video Statistics
        |                     |
        |                EventBridge
        |                     |
        |                 AWS Lambda
        |                     |
        +----------+----------+
                   |
                   v
              AMAZON S3
              BRONZE LAYER
                   |
                   v
              AWS GLUE
          Bronze to Silver
                   |
                   v
              AMAZON S3
              SILVER LAYER
                   |
          +--------+--------+
          |                 |
     Reference Data    Statistics Data
     Transformation    Transformation
          |                 |
          +--------+--------+
                   |
                   v
           DATA QUALITY LAMBDA
                   |
             Quality Check
                   |
             +-----+-----+
             |           |
           PASS         FAIL
             |           |
             v           v
         AWS GLUE       SNS
       Silver to Gold   Alert
             |
             v
          AMAZON S3
          GOLD LAYER
             |
      +------+------+------+
      |             |      |
   Trending      Channel  Category
   Analytics    Analytics Analytics
      |             |      |
      +------+------+------+
             |
             v
         AMAZON ATHENA

      AWS STEP FUNCTIONS
       Orchestrates Workflow
```

AWS Step Functions coordinates the ingestion, transformation, data quality validation, Gold processing, and notification stages.

---

## Tech Stack

| Component | Technology |
|---|---|
| Programming Language | Python |
| Data Processing | PySpark |
| Data Ingestion | YouTube Data API v3 |
| Historical Data | Kaggle |
| Cloud Platform | Amazon Web Services |
| Compute | AWS Lambda |
| ETL Processing | AWS Glue |
| Data Lake Storage | Amazon S3 |
| Orchestration | AWS Step Functions |
| Scheduling | Amazon EventBridge |
| Data Catalog | AWS Glue Data Catalog |
| Data Quality | AWS Lambda |
| Query Engine | Amazon Athena |
| Notifications | Amazon SNS |
| Monitoring | Amazon CloudWatch |
| Data Format | CSV, JSON, Parquet |
| Version Control | Git, GitHub |

---

## Project Structure

```text
YouTube-Trending-Data-Pipeline/
│
├── data/
│   ├── CAvideos.csv
│   ├── DEvideos.csv
│   ├── FRvideos.csv
│   ├── GBvideos.csv
│   ├── INvideos.csv
│   ├── JPvideos.csv
│   ├── KRvideos.csv
│   ├── MXvideos.csv
│   ├── RUvideos.csv
│   ├── USvideos.csv
│   │
│   └── *_category_id.json
│
├── data_quality/
│   └── dq_lambda.py
│
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py
│   └── silver_to_gold_analytics.py
│
├── lambas/
│   ├── YouTube_api_ingestion/
│   │   └── lambda_function.py
│   │
│   └── jason_to_parquet/
│       └── lambda_function.py
│
├── scripts/
│   ├── aws_copy.sh
│   └── information.md
│
├── step_functions/
│   └── pipeline_orchestation.json
│
└── README.md
```

---

## Data Sources

### 1. Kaggle – Historical YouTube Dataset

The project uses historical YouTube trending video datasets containing information such as:

- Video ID
- Video title
- Channel title
- Category ID
- Trending date
- Views
- Likes
- Dislikes
- Comment count
- Publish time
- Video description

The repository contains CSV datasets for 10 countries:

`CA, DE, FR, GB, IN, JP, KR, MX, RU, US`

Category mapping JSON files are also included.

The historical datasets can be uploaded to the Bronze S3 bucket using the `aws_copy.sh` script.

### 2. YouTube Data API v3 – Live Data

The live ingestion process uses the YouTube Data API v3.

The ingestion Lambda retrieves:

**Trending Videos**
- Video ID
- Title
- Channel information
- Category ID
- Publish time
- Views
- Likes
- Comments
- Thumbnail information
- Video description

**Video Categories**
- Category ID
- Category name
- Region-specific category reference data

The Lambda processes configured regions and stores raw API responses in Amazon S3 as JSON.

---

## Data Flow

### 1. Bronze Layer (Raw Data)

The Bronze layer stores raw data from both sources.

**Kaggle Ingestion**

Historical CSV and category JSON files are uploaded to Amazon S3 using the AWS CLI script.

**YouTube API Ingestion**

The ingestion Lambda:

1. Calls the YouTube Data API v3.
2. Retrieves trending videos for configured regions.
3. Retrieves video category mappings.
4. Adds ingestion metadata.
5. Stores raw JSON responses in the Bronze S3 bucket.

Example S3 structure:

```text
s3://bronze-bucket/
│
└── youtube/
    ├── raw_statistics/
    │   └── region=us/
    │       └── date=YYYY-MM-DD/
    │           └── hour=HH/
    │               └── ingestion.json
    │
    └── raw_statistics_reference_data/
        └── region=us/
            └── category_id.json
```

The raw data is catalogued using AWS Glue Data Catalog.

### 2. Silver Layer (Transformed Data)

The AWS Glue job:

`bronze_to_silver_statistics.py`

Reads data from the Bronze Glue Catalog and processes both Kaggle CSV and YouTube API JSON formats.

**Transformation activities:**

- Schema enforcement.
- Handling different source formats.
- Flattening nested YouTube API JSON fields.
- Data type casting.
- Null handling.
- Region standardization.
- Date parsing and standardization.
- Removing records with null video IDs.
- Deduplication using video ID, region, and trending date.
- Generating derived metrics.

**Derived Metrics**

`like_ratio`

Percentage of likes relative to video views.

`engagement_rate`

Engagement calculated using likes, dislikes, and comments relative to views.

**Output:**

- Cleaned statistics data.
- Parquet format.
- Snappy compression.
- Region-based partitioning.
- AWS Glue Data Catalog registration.

The processed statistics are written to the Silver layer.

### Reference Data Transformation

The Lambda function:

`json_to_parquet/lambda_function.py`

Processes category reference data and converts JSON category mappings into Parquet format for downstream processing.

### 3. Data Quality Validation

The data quality Lambda:

`dq_lambda.py`

Validates the Silver layer before Gold processing.

The Step Functions workflow invokes this Lambda after the Bronze-to-Silver and reference data transformations.

The quality result determines whether the Gold ETL job should execute.

- **PASS:** Continue to Gold processing.
- **FAIL:** Trigger the data quality failure notification path.

### 4. Gold Layer (Analytics)

The AWS Glue job:

`silver_to_gold_analytics.py`

Reads the cleaned Silver statistics and optional reference data.

It joins category reference information with statistics and generates three analytical datasets.

The output is stored in Amazon S3 in Parquet format with Snappy compression and registered in the Gold Glue database.

---

## Gold Layer Output Tables

The Gold layer contains three analytical tables.

### 1. `trending_analytics`

Provides daily trending video summaries by region.

| Column | Description |
|---|---|
| `region` | Country or region code |
| `trending_date_parsed` | Parsed trending date |
| `total_videos` | Total video records |
| `total_views` | Sum of video views |
| `total_likes` | Sum of likes |
| `total_dislikes` | Sum of dislikes |
| `total_comments` | Sum of comments |
| `avg_views_per_video` | Average views per video |
| `avg_like_ratio` | Average like ratio |
| `avg_engagement_rate` | Average engagement rate |
| `max_views` | Maximum views |
| `unique_channels` | Number of unique channels |
| `unique_categories` | Number of unique categories |
| `_aggregated_at` | Aggregation timestamp |

### 2. `channel_analytics`

Provides channel-level performance metrics and rankings.

| Column | Description |
|---|---|
| `channel_title` | YouTube channel name |
| `region` | Country or region code |
| `total_videos` | Distinct videos for the channel |
| `total_views` | Total views |
| `total_likes` | Total likes |
| `total_comments` | Total comments |
| `avg_views_per_video` | Average views per video |
| `avg_engagement_rate` | Average engagement rate |
| `peak_views` | Highest video views |
| `times_trending` | Number of trending records |
| `first_trending` | First trending date |
| `last_trending` | Most recent trending date |
| `categories` | Categories associated with the channel |
| `rank_in_region` | Channel ranking by total views within region |
| `_aggregated_at` | Aggregation timestamp |

### 3. `category_analytics`

Provides category-level performance and trend analysis.

| Column | Description |
|---|---|
| `category_name` | Video category name |
| `category_id` | YouTube category ID |
| `region` | Country or region code |
| `trending_date_parsed` | Parsed trending date |
| `video_count` | Number of videos in category |
| `total_views` | Total views |
| `total_likes` | Total likes |
| `total_comments` | Total comments |
| `avg_engagement_rate` | Average engagement rate |
| `unique_channels` | Number of unique channels |
| `view_share_pct` | Percentage share of total regional views |
| `_aggregated_at` | Aggregation timestamp |

All three Gold datasets are written as Parquet files with Snappy compression and partitioned by region.

They are registered in the AWS Glue Data Catalog for querying through Amazon Athena.

---

## Step Functions Orchestration

AWS Step Functions coordinates the complete data pipeline.

The state machine definition is available at:

`step_functions/pipeline_orchestation.json`

### Workflow Execution

1. **IngestFromYouTubeAPI** – Invokes the YouTube ingestion Lambda.
2. **ProcessInParallel** – Runs reference data transformation and Bronze-to-Silver Glue processing in parallel.
3. **RunDataQualityChecks** – Invokes the data quality Lambda.
4. **EvaluateDataQuality** – Evaluates the quality result.
5. **RunSilverToGoldGlueJob** – Executes the Gold transformation if validation passes.
6. **NotifySuccess** – Sends the successful pipeline notification.

The state machine also contains failure-handling paths for ingestion, transformation, data quality, and Gold processing.

Retry and catch configurations are used for supported execution failures.

---

## Configuration

### YouTube Ingestion Lambda Environment Variables

| Variable | Description |
|---|---|
| `YOUTUBE_API_KEY` | YouTube Data API v3 API key |
| `S3_BUCKET_BRONZE` | Target Bronze S3 bucket |
| `YOUTUBE_REGIONS` | Comma-separated region codes |
| `SNS_ALERT_TOPIC_ARN` | SNS topic ARN for alerts |

Example region configuration:

```text
US,GB,CA,DE,FR,IN,JP,KR,MX,RU
```

### AWS Glue Job Parameters

**Bronze to Silver**

| Parameter | Description |
|---|---|
| `--bronze_database` | Bronze Glue database |
| `--bronze_table` | Bronze statistics table |
| `--silver_bucket` | Silver S3 bucket |
| `--silver_database` | Silver Glue database |
| `--silver_table` | Silver statistics table |

**Silver to Gold**

| Parameter | Description |
|---|---|
| `--silver_database` | Silver Glue database |
| `--gold_bucket` | Gold S3 bucket |
| `--gold_database` | Gold Glue database |

### Security

- Store API keys in Lambda environment variables.
- Use IAM roles for AWS service permissions.
- Follow least-privilege access principles.
- Never commit AWS credentials or API keys to GitHub.

---

## Deployment Overview

The following AWS resources are required for the pipeline:

- Amazon S3 buckets for Bronze, Silver, and Gold layers.
- AWS Lambda functions.
- AWS Glue jobs.
- AWS Glue Data Catalog databases and tables.
- AWS Step Functions state machine.
- Amazon EventBridge schedule.
- Amazon SNS topic.
- IAM roles and policies.
- Amazon Athena for querying Gold datasets.

### Deployment Steps

1. Configure AWS IAM roles and policies.
2. Create the required S3 buckets.
3. Create Glue Data Catalog databases.
4. Upload historical Kaggle datasets to Bronze S3.
5. Deploy the YouTube API ingestion Lambda.
6. Deploy the JSON-to-Parquet Lambda.
7. Deploy the data quality Lambda.
8. Upload and configure AWS Glue scripts.
9. Configure the Step Functions state machine.
10. Configure EventBridge scheduling and SNS notifications.
11. Execute and validate the pipeline.

---

## Running the Pipeline

### Historical Data Ingestion

Use the AWS CLI script:

```bash
bash scripts/aws_copy.sh
```

This script is used to upload historical datasets to the configured S3 location.

### Live Data Ingestion

Configure the YouTube API key and required environment variables in the ingestion Lambda.

The Lambda can be triggered through its configured AWS trigger or through the Step Functions workflow.

### Pipeline Execution

Start the Step Functions state machine from the AWS Console or AWS CLI.

Monitor each execution stage and verify the output in the respective S3 layers.

---

## Monitoring and Notifications

The pipeline uses AWS services for execution monitoring and failure notifications.

**Amazon CloudWatch**
- Lambda execution logs.
- AWS Glue job logs.
- Step Functions execution history.
- Error investigation.

**Amazon SNS**
- Pipeline success notifications.
- Ingestion failure notifications.
- Transformation failure notifications.
- Data quality failure notifications.
- Gold processing failure notifications.

---

## Key Features

- Two-source data ingestion: Kaggle and YouTube Data API v3.
- Historical and live YouTube trending data processing.
- Automated API-based ingestion using AWS Lambda.
- Scheduled execution using Amazon EventBridge.
- S3-based data lake architecture.
- Bronze, Silver, and Gold data layers.
- Unified schema for historical and API-based data.
- JSON-to-Parquet transformation.
- PySpark-based AWS Glue ETL processing.
- Data cleansing and deduplication.
- Data quality validation.
- AWS Step Functions orchestration.
- Retry and failure-handling mechanisms.
- SNS-based notifications.
- Three analytics-ready Gold datasets.
- Amazon Athena query support.

---

## Future Enhancements

- Add Amazon QuickSight dashboards.
- Implement incremental processing improvements.
- Expand data quality rules.
- Add additional YouTube API metrics.
- Improve pipeline observability and operational monitoring.
- Introduce automated deployment using CI/CD.

---

## Author

**Istiyaq**

GitHub: [@Istiyaq1](https://github.com/Istiyaq1)

Project Repository: [YouTube Trending Data Engineering Pipeline](https://github.com/Istiyaq1/YouTube-Trending-Data-Pipeline)

---

*This project was developed as a hands-on Data Engineering implementation using Python, PySpark, and AWS cloud services.*
