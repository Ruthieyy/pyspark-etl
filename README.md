# PySpark ETL Pipeline

End-to-end ETL pipeline using PySpark with AWS S3 integration.

## Pipeline
Extract (raw data) → Transform (clean & normalize) → Load (AWS S3)

## Transformations
- Filter null values
- Normalize city names to uppercase
- Add processing date column

## Tech Stack
- PySpark
- AWS S3, boto3
- Python 3.14
