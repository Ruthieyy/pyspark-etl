"""Read events CSV, clean it, and write partitioned Parquet.

Input and output are paths Spark can open. A local directory and an
``s3a://`` URI use the same code path. Nothing in this module imports boto3
or converts the DataFrame to pandas.
"""

from __future__ import annotations

import argparse
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from etl.schema import EVENT_SCHEMA, PARTITION_COLUMN
from etl.transform import transform_events

_JAVA_17_CANDIDATES = (
    Path("/usr/lib/jvm/java-17-openjdk-amd64"),
    Path("/usr/lib/jvm/java-17-openjdk"),
)


def _prefer_java_17_when_home_unset() -> None:
    """Spark 3.5 supports Java 8, 11, and 17.

    Leave an explicit JAVA_HOME alone. If it is unset and a Java 17 install
    is on the usual Debian path, point JAVA_HOME at it so a machine whose
    default ``java`` is 21 still starts Spark 3.5.
    """
    if os.environ.get("JAVA_HOME"):
        return
    for candidate in _JAVA_17_CANDIDATES:
        if (candidate / "bin" / "java").is_file():
            os.environ["JAVA_HOME"] = str(candidate)
            return


def build_spark_session(app_name: str = "events-etl") -> SparkSession:
    """Local Spark session for this small job and for pytest."""
    _prefer_java_17_when_home_unset()
    warehouse = tempfile.mkdtemp(prefix="events-etl-spark-")
    session = (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def read_events(spark: SparkSession, path: str) -> DataFrame:
    """Read CSV from ``path`` with ``EVENT_SCHEMA``.

    ``path`` is passed through unchanged (local path or ``s3a://`` URI).
    A value that does not match the schema fails the job (FAILFAST) instead
    of becoming a silent null. An empty field is null.
    """
    return (
        spark.read.format("csv")
        .schema(EVENT_SCHEMA)
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("nullValue", "")
        .option("mode", "FAILFAST")
        .load(path)
    )


def write_parquet(df: DataFrame, path: str) -> None:
    """Write Parquet with Spark's data source writer, partitioned by event_date.

    ``path`` is a directory. Spark creates ``event_date=YYYY-MM-DD/`` under it.
    The same call accepts a local directory or an ``s3a://`` URI.
    """
    (
        df.write.mode("overwrite")
        .partitionBy(PARTITION_COLUMN)
        .option("compression", "snappy")
        .parquet(path)
    )


def run(spark: SparkSession, input_path: str, output_path: str, processed_on: date) -> None:
    """Read ``input_path``, transform, and write Parquet to ``output_path``."""
    source = read_events(spark, input_path)
    cleaned = transform_events(source, processed_on)
    write_parquet(cleaned, output_path)


def resolve_processed_on(value: date | None, today: date) -> date:
    """Use the caller-supplied date, or ``today`` when the CLI omits one."""
    return today if value is None else value


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--processed-on must be YYYY-MM-DD, got {value!r}"
        ) from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean a customer-events CSV and write Parquet partitioned by event_date."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="CSV file to read. Local path or s3a:// URI.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Parquet directory to write. Local path or s3a:// URI.",
    )
    parser.add_argument(
        "--processed-on",
        type=_parse_iso_date,
        default=None,
        help="Run date written to processed_date (YYYY-MM-DD). Defaults to today, UTC.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    processed_on = resolve_processed_on(
        args.processed_on,
        datetime.now(timezone.utc).date(),
    )
    spark = build_spark_session()
    try:
        run(spark, args.input, args.output, processed_on)
    finally:
        spark.stop()
    print(
        f"Wrote Parquet partitioned by {PARTITION_COLUMN} to {args.output} "
        f"(processed_date={processed_on.isoformat()})"
    )
