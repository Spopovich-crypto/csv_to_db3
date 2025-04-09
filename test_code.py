import pytest
import duckdb
import pandas as pd
from pathlib import Path
from main import (
    collect_sensor_files,
    group_sensor_files,
    filter_unprocessed_file_sets,
    convert_group_to_long_df,
    register_to_duckdb,
    mark_file_event_as_processed,
    FileMetadata
)

TEST_FOLDER = Path("./test_data")
TEST_DB = "test_sensor_data.duckdb"
ENCODING = "utf-8"
NAME_PATTERN = [".csv"]

@pytest.fixture(scope="module")
def setup_db():
    con = duckdb.connect(TEST_DB)
    yield con
    con.close()
    Path(TEST_DB).unlink(missing_ok=True)

# --- ファイル探索テスト ---
def test_collect_sensor_files():
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    results = collect_sensor_files(DummyInput())
    assert isinstance(results, list)
    assert all(isinstance(f, FileMetadata) for f in results)
    assert all(hasattr(f, "source_file") for f in results)

# --- グルーピング処理テスト ---
def test_group_sensor_files():
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    files = collect_sensor_files(DummyInput())
    groups = group_sensor_files(files)
    assert isinstance(groups, list)
    assert len(groups) > 0

# --- イベント・処理済み判定テスト ---
def test_filter_unprocessed_file_sets(setup_db):
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    groups = group_sensor_files(collect_sensor_files(DummyInput()))
    dummy_events = [{"event": "TEST", "start_time": "2020-01-01T00:00:00", "end_time": "2030-01-01T00:00:00"}]
    filtered = filter_unprocessed_file_sets(groups, dummy_events, TEST_DB)
    assert isinstance(filtered, list)

# --- データ整形処理テスト ---
def test_convert_group_to_long_df():
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    groups = group_sensor_files(collect_sensor_files(DummyInput()))
    df = convert_group_to_long_df(groups[0], ENCODING)
    assert isinstance(df, pd.DataFrame)
    assert "timestamp" in df.columns
    assert "parameter_id" in df.columns

# --- DuckDB登録テスト ---
def test_register_to_duckdb(setup_db):
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    groups = group_sensor_files(collect_sensor_files(DummyInput()))
    df = convert_group_to_long_df(groups[0], ENCODING)
    register_to_duckdb(TEST_DB, df)
    con = duckdb.connect(TEST_DB)
    result = con.sql("SELECT COUNT(*) FROM sensor_data").fetchone()
    assert result[0] > 0

# --- 処理済みマークテスト ---
def test_mark_file_event_as_processed(setup_db):
    mark_file_event_as_processed(TEST_DB, "dummy.csv", {"event": "TEST", "start_time": "2020-01-01T00:00:00", "end_time": "2030-01-01T00:00:00"})
    con = duckdb.connect(TEST_DB)
    result = con.sql("SELECT COUNT(*) FROM processed_file_periods WHERE source_file='dummy.csv'").fetchone()
    assert result[0] > 0
