"""Row-level cleaning rules.

Each function takes a DataFrame and returns a new one so tests can exercise
one rule at a time. ``processed_on`` is an argument: the job does not call
``current_date()``, which would make the result depend on the wall clock.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, trim, upper


def drop_null_ages(df: DataFrame) -> DataFrame:
    """Drop rows with a null age. Zero is a real age and is kept."""
    return df.filter(col("age").isNotNull())


def drop_null_event_dates(df: DataFrame) -> DataFrame:
    """Drop rows with a null event_date so the partition value is a real date."""
    return df.filter(col("event_date").isNotNull())


def normalize_city(df: DataFrame) -> DataFrame:
    """Trim surrounding whitespace and uppercase the city name."""
    return df.withColumn("city", upper(trim(col("city"))))


def add_processed_date(df: DataFrame, processed_on: date) -> DataFrame:
    """Add the run date supplied by the caller."""
    return df.withColumn("processed_date", lit(processed_on).cast("date"))


def transform_events(df: DataFrame, processed_on: date) -> DataFrame:
    """Apply the cleaning rules and attach ``processed_date``."""
    cleaned = drop_null_ages(df)
    cleaned = drop_null_event_dates(cleaned)
    cleaned = normalize_city(cleaned)
    return add_processed_date(cleaned, processed_on)
