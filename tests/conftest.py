import pytest

from etl.pipeline import build_spark_session


@pytest.fixture(scope="session")
def spark():
    session = build_spark_session("events-etl-tests")
    yield session
    session.stop()
