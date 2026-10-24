from datetime import date

from etl.schema import EVENT_SCHEMA
from etl.transform import (
    add_processed_date,
    drop_null_ages,
    drop_null_event_dates,
    normalize_city,
    transform_events,
)


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=EVENT_SCHEMA)


def test_drop_null_ages_keeps_zero(spark):
    df = _frame(
        spark,
        [
            ("Ann", 0, "paris", date(2024, 3, 1)),
            ("Bo", None, "lyon", date(2024, 3, 1)),
        ],
    )

    result = drop_null_ages(df).collect()

    assert [row.name for row in result] == ["Ann"]
    assert result[0].age == 0


def test_drop_null_event_dates(spark):
    df = _frame(
        spark,
        [
            ("Ann", 20, "paris", date(2024, 3, 1)),
            ("Bo", 21, "lyon", None),
        ],
    )

    result = drop_null_event_dates(df).collect()

    assert [row.name for row in result] == ["Ann"]


def test_normalize_city_trims_and_uppercases(spark):
    df = _frame(spark, [("Ann", 20, "  paris ", date(2024, 3, 1))])

    result = normalize_city(df).collect()

    assert result[0].city == "PARIS"


def test_add_processed_date_uses_the_given_date(spark):
    df = _frame(spark, [("Ann", 20, "paris", date(2024, 3, 1))])

    result = add_processed_date(df, date(2024, 6, 1)).collect()

    assert result[0].processed_date == date(2024, 6, 1)


def test_transform_events_applies_every_rule(spark):
    df = _frame(
        spark,
        [
            ("Alice", 25, "beijing", date(2024, 3, 1)),
            ("Carol", 22, " guangzhou ", date(2024, 3, 2)),
            ("David", None, "chengdu", date(2024, 3, 2)),
            ("Erin", 31, "wuhan", None),
        ],
    )

    rows = {
        (row.name, row.age, row.city, row.event_date, row.processed_date)
        for row in transform_events(df, date(2024, 6, 1)).collect()
    }

    assert rows == {
        ("Alice", 25, "BEIJING", date(2024, 3, 1), date(2024, 6, 1)),
        ("Carol", 22, "GUANGZHOU", date(2024, 3, 2), date(2024, 6, 1)),
    }
