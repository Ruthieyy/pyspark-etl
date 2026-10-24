"""Column types for the events CSV.

Field order is the CSV column order. ``read_events`` applies this schema
positionally; it does not reorder columns by header name.
"""

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

EVENT_SCHEMA = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("event_date", DateType(), nullable=True),
    ]
)

PARTITION_COLUMN = "event_date"
