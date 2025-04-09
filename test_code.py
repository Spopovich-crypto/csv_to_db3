import pytest
from datetime import datetime
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
    FileMetadata,
    EventInfo
)

TEST_FOLDER = Path("./test_data")
TEST_DB = "test_sensor_data.duckdb"
ENCODING = "utf-8"
NAME_PATTERN = [".csv"]

@pytest.fixture(scope="module")
def setup_db():
    con = duckdb.connect(TEST_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_file_periods (
            source_file TEXT,
            source_zip TEXT,
            event TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            PRIMARY KEY (source_file, event)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            timestamp TIMESTAMP,
            parameter_id TEXT,
            parameter_name TEXT,
            unit TEXT,
            value TEXT,
            source_file TEXT,
            sensor_type TEXT
        )
    """)
    yield con
    con.close()
    Path(TEST_DB).unlink(missing_ok=True)

# --- ファイル探索テスト ---
def test_collect_sensor_files():
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    results = collect_sensor_files(DummyInput.target_folder, DummyInput.name_patterns)
    assert isinstance(results, list)
    assert all(isinstance(f, FileMetadata) for f in results)
    assert all(hasattr(f, "source_file") for f in results)

# --- グルーピング処理テスト ---
def test_group_sensor_files():
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    files = collect_sensor_files(DummyInput.target_folder, DummyInput.name_patterns)
    groups = group_sensor_files(files)
    assert isinstance(groups, list)
    assert len(groups) > 0

# --- イベント・処理済み判定テスト ---
def test_filter_unprocessed_file_sets(setup_db):
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    groups = group_sensor_files(collect_sensor_files(DummyInput.target_folder, DummyInput.name_patterns))
    dummy_events = [EventInfo(event= "TEST", start_time= datetime(2020,1,1, 0,0,0), end_time=datetime(2030,1,1, 0,0,0))]
    filtered = filter_unprocessed_file_sets(groups, dummy_events, TEST_DB)
    assert isinstance(filtered, list)

# --- データ整形処理テスト ---
def test_convert_group_to_long_df():
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    groups = group_sensor_files(collect_sensor_files(DummyInput.target_folder, DummyInput.name_patterns))
    df = convert_group_to_long_df(groups[0], ENCODING)
    assert isinstance(df, pd.DataFrame)
    assert "timestamp" in df.columns
    assert "parameter_id" in df.columns

# --- DuckDB登録テスト ---
def test_register_to_duckdb(setup_db):
    class DummyInput:
        target_folder = TEST_FOLDER
        name_patterns = NAME_PATTERN

    groups = group_sensor_files(collect_sensor_files(DummyInput.target_folder, DummyInput.name_patterns))
    df = convert_group_to_long_df(groups[0], ENCODING)
    register_to_duckdb(TEST_DB, df)
    con = duckdb.connect(TEST_DB)
    result = con.sql("SELECT COUNT(*) FROM sensor_data").fetchone()
    assert result[0] > 0

# --- 処理済みマークテスト ---
def test_mark_file_event_as_processed(setup_db):
    mark_file_event_as_processed(TEST_DB, "dummy.csv", EventInfo(event= "TEST", start_time= datetime(2020,1,1, 0,0,0), end_time=datetime(2030,1,1, 0,0,0)),)
    con = duckdb.connect(TEST_DB)
    result = con.sql("SELECT COUNT(*) FROM processed_file_periods WHERE source_file='dummy.csv'").fetchone()
    assert result[0] > 0
