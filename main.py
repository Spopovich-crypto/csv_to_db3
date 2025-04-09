import io
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
from pydantic import BaseModel, ValidationError
from tqdm import tqdm


def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    raise TypeError(f"Type {type(obj)} not serializable")


# --- 入力スキーマ定義 ---
class EventInfo(BaseModel):
    event: str
    description: str
    start_time: datetime
    end_time: datetime


class UserInput(BaseModel):
    target_folder: str
    name_patterns: List[str]
    encoding: str
    db_path: str
    plant_name: str
    machine_no: str
    label: str
    label_description: str
    events: List[EventInfo]


class FileMetadata(BaseModel):
    plant_name_from_file: str
    machine_no_from_file: str
    sensor_type: str
    start_time: datetime
    end_time: datetime
    source_file: str
    source_zip: Optional[str] = None
    internal_path: Optional[str] = None


class GroupedSensorFileSet(BaseModel):
    prefix: str
    plant_name_from_file: str
    machine_no_from_file: str
    start: datetime
    end: datetime
    files: List[FileMetadata]


def init_processed_file_periods_table(db_path: str):
    con = duckdb.connect(db_path)
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
    con.close()


def is_file_event_already_processed(
    db_path: str,
    source_file: str,
    event: str,
    event_start: datetime,
    event_end: datetime,
) -> bool:
    con = duckdb.connect(db_path)
    result = con.execute(
        """
        SELECT 1 FROM processed_file_periods
        WHERE source_file = ? AND event = ?
        AND start_time <= ? AND end_time >= ?
    """,
        (source_file, event, event_start, event_end),
    ).fetchone()
    con.close()
    return result is not None


def mark_file_event_as_processed(
    db_path: str,
    source_file: str,
    source_zip: Optional[str],
    event: str,
    start_time: datetime,
    end_time: datetime,
):
    con = duckdb.connect(db_path)
    con.execute(
        """
        INSERT INTO processed_file_periods
        (source_file, source_zip, event, start_time, end_time)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (source_file, event) DO UPDATE
        SET start_time = excluded.start_time,
            end_time = excluded.end_time
        WHERE excluded.end_time > processed_file_periods.end_time
    """,
        (source_file, source_zip, event, start_time, end_time),
    )
    con.close()


# --- ファイル名からメタ情報を抽出 ---
def extract_metadata_from_filename(file: Path) -> Optional[Dict[str, Any]]:
    pattern = r"(?P<plant_code>[A-Z]+)#(?P<machine_code>\d+)(?P<datestr>\d{6})(?P<timestr>\d{6})_(?P<sensor_type>[^.]+)"
    match = re.match(pattern, file.name)
    if not match:
        return None

    date_str = match.group("datestr")
    time_str = match.group("timestr")
    dt = datetime.strptime(date_str + time_str, "%d%m%y%H%M%S")
    max_end_time = dt + timedelta(hours=2)

    file_mtime = None
    if file.exists():
        file_mtime = datetime.fromtimestamp(file.stat().st_mtime)

    end_time = min(max_end_time, file_mtime) if file_mtime else max_end_time

    return {
        "plant_name_from_file": match.group("plant_code"),
        "machine_no_from_file": match.group("machine_code"),
        "sensor_type": match.group("sensor_type"),
        "start_time": dt,
        "end_time": end_time,
        "source_file": str(file),
    }


# --- 指定フォルダからファイルを収集 ---
def collect_sensor_files(user_input: UserInput) -> List[Dict[str, Any]]:
    all_files = list(Path(user_input.target_folder).rglob("*"))
    collected = []

    tqdm.write(f"🔍 検索対象ファイル数: {len(all_files)}")
    tqdm.write(f"📂 name_patterns: {user_input.name_patterns}")

    for file in all_files:
        if file.suffix.lower() == ".csv":
            if any(pat in file.name for pat in user_input.name_patterns):
                tqdm.write(f"✅ マッチ: {file.name}")
                metadata = extract_metadata_from_filename(file)
                if metadata:
                    collected.append(metadata)

        elif file.suffix.lower() == ".zip":
            try:
                with zipfile.ZipFile(file, "r") as zipf:
                    for zip_info in zipf.infolist():
                        if any(
                            pat in zip_info.filename for pat in user_input.name_patterns
                        ):
                            tqdm.write(f"📦 ZIP内マッチ: {zip_info.filename}")
                            metadata = extract_metadata_from_filename(
                                Path(zip_info.filename)
                            )
                            if metadata:
                                metadata["source_zip"] = str(file)
                                metadata["internal_path"] = zip_info.filename
                                collected.append(metadata)
            except zipfile.BadZipFile:
                tqdm.write(f"⚠️ ZIPファイルが壊れています: {file}")
                continue

    return collected


# --- グルーピング処理 ---


def group_sensor_files(files: List[FileMetadata]) -> List[GroupedSensorFileSet]:
    grouped = defaultdict(list)
    for f in files:
        prefix = Path(f.source_file).stem.split("_")[0]
        grouped[prefix].append(f)

    result = []
    for prefix, file_list in grouped.items():
        start_times = [f.start_time for f in file_list]
        end_times = [f.end_time for f in file_list]

        result.append(
            GroupedSensorFileSet(
                prefix=prefix,
                plant_name_from_file=file_list[0].plant_name_from_file,
                machine_no_from_file=file_list[0].machine_no_from_file,
                start=min(start_times),
                end=max(end_times),
                files=file_list,
            )
        )

    return result


# --- イベント時間と処理済みかどうかでフィルタリング ---
def filter_unprocessed_file_sets(
    grouped_sets: List[GroupedSensorFileSet],
    events: List[EventInfo],
    db_path: str,
) -> List[GroupedSensorFileSet]:
    filtered_sets: List[GroupedSensorFileSet] = []
    for group in grouped_sets:
        matched_files = []

        for file in group.files:
            for ev in events:
                if ev.start_time <= file.end_time and file.start_time < ev.end_time:
                    if not is_file_event_already_processed(
                        db_path=db_path,
                        source_file=file.source_file,
                        event=ev.event,
                        event_start=ev.start_time,
                        event_end=ev.end_time,
                    ):
                        matched_files.append(file)
                        break
        if matched_files:
            filtered_sets.append(
                GroupedSensorFileSet(
                    prefix=group.prefix,
                    plant_name_from_file=group.plant_name_from_file,
                    machine_no_from_file=group.machine_no_from_file,
                    start=group.start,
                    end=group.end,
                    files=matched_files,
                )
            )
    return filtered_sets


def read_csv_cleaned(file: FileMetadata, encoding: str) -> pd.DataFrame:
    if file.source_zip:
        with zipfile.ZipFile(file.source_zip, "r") as zipf:
            raw_bytes = zipf.read(file.internal_path)
            raw_str = raw_bytes.decode(encoding)
    else:
        with open(file.source_file, "r", encoding=encoding) as f:
            raw_str = f.read()

    cleaned_lines = [line.rstrip(",") for line in raw_str.splitlines()]
    cleaned_str = "\n".join(cleaned_lines)

    return pd.read_csv(io.StringIO(cleaned_str), header=[0, 1, 2], dtype=str)


def convert_csv_to_long_format(file: "FileMetadata", encoding: str) -> pd.DataFrame:
    tqdm.write(f"   ├─ 処理中: {Path(file.source_file).name} [{file.sensor_type}]")

    df = read_csv_cleaned(file, encoding=encoding)
    df = df.loc[:, ~df.columns.duplicated()]

    valid_cols = []
    time_col = df.columns[0]
    for col in df.columns:
        if col == time_col:
            valid_cols.append(col)
            continue
        _, param_name, unit = map(str.strip, col)
        if param_name != "-" or unit != "-":
            valid_cols.append(col)

    df = df.loc[:, valid_cols]
    df.columns = ["|".join(filter(None, map(str, col))).strip() for col in df.columns]

    time_col = df.columns[0]
    df_long = df.melt(id_vars=[time_col], var_name="parameter_full", value_name="value")

    df_long.rename(columns={time_col: "timestamp"}, inplace=True)
    df_long["timestamp"] = pd.to_datetime(df_long["timestamp"], errors="coerce")

    df_long[["parameter_id", "parameter_name", "unit"]] = df_long[
        "parameter_full"
    ].str.split("|", expand=True)

    df_long["parameter_id"] = df_long["parameter_id"].str.strip()
    df_long["parameter_name"] = df_long["parameter_name"].str.strip()
    df_long["unit"] = df_long["unit"].str.strip()

    df_long["source_file"] = file.source_file
    df_long["sensor_type"] = file.sensor_type

    return df_long[
        [
            "timestamp",
            "parameter_id",
            "parameter_name",
            "unit",
            "value",
            "source_file",
            "sensor_type",
        ]
    ]


def convert_group_to_long_df(
    group: "GroupedSensorFileSet", encoding: str
) -> pd.DataFrame:
    tqdm.write(f"\n📦 セット処理開始: {group.prefix}")
    tqdm.write(f"├─ ファイル数: {len(group.files)}")

    dfs = []
    seen_params = set()

    for f in group.files:
        try:
            tqdm.write(
                f"   └─ ファイル: {f.source_file} / ZIP: {f.source_zip} / internal: {f.internal_path}"
            )
            df = convert_csv_to_long_format(f, encoding)
            df = df[~df["parameter_id"].isin(seen_params)]
            seen_params.update(df["parameter_id"].unique())
            dfs.append(df)
        except Exception as e:
            tqdm.write(f"⚠️ {f.source_file} の読み込みに失敗: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def register_to_duckdb(db_path: str, df: pd.DataFrame):
    con = duckdb.connect(db_path)
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

    con.register("temp_df", df)
    con.execute("""
        INSERT INTO sensor_data
        SELECT * FROM temp_df
        EXCEPT
        SELECT * FROM sensor_data
    """)
    con.unregister("temp_df")
    con.close()
    tqdm.write(f"✅ DuckDB登録: {len(df)} 行追加しました")


def main(user_input):
    # 処理済みファイル記録テーブルの初期化
    init_processed_file_periods_table(user_input.db_path)

    # 対象ファイル収集
    all_files = collect_sensor_files(user_input)

    # セット化（センサータイプごと、ファイル名prefixごと）
    grouped_sets = group_sensor_files(all_files)

    # 未処理かつイベントと重なるセットのみ抽出
    filtered_sets = filter_unprocessed_file_sets(
        grouped_sets, user_input.events, user_input.db_path
    )

    # セット単位で処理
    for group in tqdm(filtered_sets, desc="📦 セット処理中"):
        tqdm.write(f"📦 {group.prefix} を処理中")

        df = convert_group_to_long_df(group, user_input.encoding)

        if df.empty:
            tqdm.write(f"⚠️ 空データスキップ: {group.prefix}")
            continue

        register_to_duckdb(user_input.db_path, df)

        # 対象イベントに対して処理済み記録
        for ev in user_input.events:
            if ev.start_time <= group.end and group.start < ev.end_time:
                for f in group.files:
                    mark_file_event_as_processed(
                        db_path=user_input.db_path,
                        source_file=f.source_file,
                        source_zip=f.source_zip,
                        event=ev.event,
                        start_time=ev.start_time,
                        end_time=ev.end_time,
                    )


if __name__ == "__main__":
    sample_input = {
        "target_folder": "./data",
        "name_patterns": ["Cond", "Vib", "Tmp"],
        "encoding": "shift_jis",
        "db_path": "./sensor_data.duckdb",
        "plant_name": "東京工場",
        "machine_no": "No.101",
        "label": "2024年定期点検",
        "label_description": "負荷試験含む",
        "events": [
            {
                "event": "起動試験",
                "description": "冷間始動",
                "start_time": "2024-11-21T00:00:00",
                "end_time": "2024-11-21T00:30:00",
            }
        ],
    }
    try:
        user_input = UserInput(**sample_input)
    except ValidationError as e:
        tqdm.write("⚠️ 入力データにエラーがあります:")
        tqdm.write(e.json(indent=2, ensure_ascii=False))
        exit(1)

    main(user_input)
