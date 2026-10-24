from datetime import date
from pathlib import Path

import pytest

from etl.pipeline import parse_args, read_events, resolve_processed_on, run
from etl.schema import EVENT_SCHEMA

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "data" / "sample" / "events.csv"


def test_read_sample_uses_explicit_types(spark):
    df = read_events(spark, str(SAMPLE))

    actual = [(field.name, type(field.dataType)) for field in df.schema.fields]
    expected = [(field.name, type(field.dataType)) for field in EVENT_SCHEMA.fields]
    assert actual == expected

    ages = {row.name: row.age for row in df.collect()}
    assert ages["Alice"] == 25
    assert ages["David"] is None
    assert ages["Carol"] == 22


def test_run_writes_partitioned_parquet(spark, tmp_path):
    output = tmp_path / "out"

    run(spark, str(SAMPLE), str(output), date(2024, 6, 1))

    assert (output / "_SUCCESS").is_file()
    march_1 = output / "event_date=2024-03-01"
    march_2 = output / "event_date=2024-03-02"
    assert march_1.is_dir()
    assert march_2.is_dir()
    assert list(march_1.glob("part-*.parquet"))
    assert list(march_2.glob("part-*.parquet"))

    rows = {
        (row.name, row.age, row.city, row.event_date, row.processed_date)
        for row in spark.read.parquet(str(output)).collect()
    }
    assert rows == {
        ("Alice", 25, "BEIJING", date(2024, 3, 1), date(2024, 6, 1)),
        ("Bob", 30, "SHANGHAI", date(2024, 3, 1), date(2024, 6, 1)),
        ("Carol", 22, "GUANGZHOU", date(2024, 3, 2), date(2024, 6, 1)),
    }


def test_overwrite_replaces_previous_partitions(spark, tmp_path):
    output = tmp_path / "out"
    run(spark, str(SAMPLE), str(output), date(2024, 6, 1))

    replacement = tmp_path / "one.csv"
    replacement.write_text(
        "name,age,city,event_date\nEve,40,seattle,2024-04-01\n",
        encoding="utf-8",
    )
    run(spark, str(replacement), str(output), date(2024, 7, 1))

    assert not (output / "event_date=2024-03-01").exists()
    written = spark.read.parquet(str(output)).collect()
    assert len(written) == 1
    assert written[0].name == "Eve"
    assert written[0].city == "SEATTLE"
    assert written[0].event_date == date(2024, 4, 1)
    assert written[0].processed_date == date(2024, 7, 1)


def test_filtered_input_still_commits(spark, tmp_path):
    source = tmp_path / "only-null-age.csv"
    source.write_text(
        "name,age,city,event_date\nDavid,,chengdu,2024-03-02\n",
        encoding="utf-8",
    )
    output = tmp_path / "out"

    run(spark, str(source), str(output), date(2024, 6, 1))

    assert (output / "_SUCCESS").is_file()
    assert list(output.glob("event_date=*")) == []


def test_malformed_age_fails_the_read(spark, tmp_path):
    source = tmp_path / "bad.csv"
    source.write_text(
        "name,age,city,event_date\nZoe,not-a-number,beijing,2024-03-01\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="(?i)malformed|parse|fail"):
        read_events(spark, str(source)).collect()


def test_parse_args_keeps_s3a_uris_and_iso_date():
    args = parse_args(
        [
            "--input",
            "s3a://lake/raw/events.csv",
            "--output",
            "s3a://lake/curated/events",
            "--processed-on",
            "2024-06-01",
        ]
    )

    assert args.input == "s3a://lake/raw/events.csv"
    assert args.output == "s3a://lake/curated/events"
    assert args.processed_on == date(2024, 6, 1)
    assert resolve_processed_on(None, date(2024, 1, 2)) == date(2024, 1, 2)
    assert resolve_processed_on(date(2024, 6, 1), date(2024, 1, 2)) == date(2024, 6, 1)


def test_parse_args_rejects_non_iso_date():
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--input",
                "events.csv",
                "--output",
                "out",
                "--processed-on",
                "06-01-2024",
            ]
        )
