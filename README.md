# Customer events ETL

A small PySpark job that reads a CSV of customer events, cleans it, and writes Parquet partitioned by `event_date`.

Paths are arguments. A local path and an `s3a://` URI go through the same Spark read and write calls. This repository runs and tests the local path. It does not ship AWS credentials or Hadoop's S3A jars.

## What it does

1. Read CSV with an explicit schema (`name`, `age`, `city`, `event_date`). Column order in the file must match that schema. An empty field is null. A value that does not match the type fails the job.
2. Drop rows with a null `age`. Zero is kept.
3. Drop rows with a null `event_date`, so every partition value is a real date.
4. Trim and uppercase `city`.
5. Add `processed_date` from `--processed-on` (default: today's date in UTC).
6. Write Parquet with Spark's DataFrame writer, `mode("overwrite")`, Snappy compression, partitioned by `event_date`.

`processed_date` is a column, not the partition key. It is the run date, so partitioning on it would put the whole output in one directory. `event_date` comes from the source and varies by row.

The sample file has the four original people, plus a city with surrounding spaces and an `event_date` so the partition write has something to partition on:

| name  | age  | city         | event_date |
|-------|------|--------------|------------|
| Alice | 25   | beijing      | 2024-03-01 |
| Bob   | 30   | shanghai     | 2024-03-01 |
| Carol | 22   | ` guangzhou ` | 2024-03-02 |
| David | null | chengdu      | 2024-03-02 |

David is dropped. Carol's city becomes `GUANGZHOU`. The output has `event_date=2024-03-01/` and `event_date=2024-03-02/`.

## Requirements

- Python 3.11 or 3.12. Tested on CPython 3.12.3.
- Java 17. Spark 3.5 supports Java 8, 11, and 17.
- Packages in `requirements.txt` (`pyspark==3.5.9`, `pytest==8.4.2`).

Python 3.14 is not a target. The previous README listed it. PySpark 3.5 does not support 3.14 (that support starts in Spark 4.1). AWS Glue 5 runs Spark 3.5.4 on Python 3.11, and Amazon EMR 7.x runs Spark 3.5.x on Python 3.9 or 3.11. This repo pins the Spark 3.5 line and is tested on the Python 3.12 interpreter in this environment, which PySpark 3.5.9 accepts (`Requires-Python >=3.8`).

If `JAVA_HOME` is unset and Java 17 is installed at `/usr/lib/jvm/java-17-openjdk-amd64` (or `java-17-openjdk`), the job selects that JDK. An explicit `JAVA_HOME` is left as you set it.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

From the repository root:

```bash
python -m etl \
  --input data/sample/events.csv \
  --output /tmp/events_out \
  --processed-on 2024-06-01
```

Omit `--processed-on` to stamp `processed_date` with today's UTC date.

The same flags take S3A URIs. This only works where Spark already has the `hadoop-aws` jars and credentials. See Limitations.

```bash
python -m etl \
  --input s3a://your-bucket/raw/events.csv \
  --output s3a://your-bucket/curated/events \
  --processed-on 2024-06-01
```

`--output` is a directory. Spark writes `event_date=YYYY-MM-DD/part-*.snappy.parquet` under it, and replaces that directory on the next run (`overwrite`).

## Tests

```bash
pytest
```

The tests build one local `SparkSession` (`local[1]`) and use it for the transformation checks and for a read/write against a temp directory. They do not call AWS.

## Layout

```
etl/schema.py       explicit input schema and partition column
etl/transform.py    one function per cleaning rule
etl/pipeline.py     Spark session, CSV read, Parquet write, CLI
data/sample/events.csv
tests/              pytest, local SparkSession
requirements.txt
```

## Limitations / Next steps

- S3 is a path scheme, not a tested integration. `s3a://` is handed to Spark's Hadoop filesystem. The pip PySpark package does not include `hadoop-aws` or `aws-java-sdk-bundle`, and this repo does not configure credentials, a bucket, or an EMR/Glue job. A local path is what the tests run.
- There is no boto3 upload. The previous script wrote one Parquet file with `toPandas()` and uploaded it to a hardcoded bucket. That path is gone.
- Writes use static overwrite. The next run replaces the whole output directory. There is no dynamic partition overwrite, no merge, and no upsert.
- One bad value fails the file (`mode=FAILFAST`). Rows dropped for a null age or a null event date are not written to a quarantine path, and the job does not record how many rows it dropped.
- CSV only, with columns in schema order. No JSON, no header-based reordering, no schema evolution.
- The sample is four made-up rows. It is not a production dataset.
- The session is `local[1]`. There is no `spark-submit` script, cluster sizing, or retry policy.
- `processed_date` is whatever you pass in. Nothing checks that the source file matches that date.

Next steps if this grew into a job you would actually schedule: add the S3A jars and a credentials story (or run it on EMR/Glue, where those exist), quarantine rejected rows, switch to dynamic partition overwrite, and publish a row-count check next to the dataset.
