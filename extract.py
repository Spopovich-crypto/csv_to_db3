import duckdb
import pandas as pd
from typing import Optional, List

def extract_sensor_data(
    db_path: str,
    plant_code: str,
    machine_code: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    event: Optional[str] = None,
    sensor_names: Optional[List[str]] = None,
    limit: int = 1000
) -> pd.DataFrame:
    con = duckdb.connect(db_path)

    # 基本クエリ（センサーデータ + イベントJOIN）
    query = """
        SELECT
            s.timestamp,
            s.parameter_id,
            s.parameter_name,
            s.unit,
            TRY_CAST(s.value AS DOUBLE) AS value_numeric
        FROM sensor_data s
        LEFT JOIN processed_file_periods p
        ON s.source_file = p.source_file
        WHERE s.plant_code = ? AND s.machine_no_from_file = ?
    """

    params = [plant_code, machine_code]

    if event:
        query += " AND p.event = ?"
        params.append(event)

    if start_time:
        query += " AND s.timestamp >= ?"
        params.append(start_time)

    if end_time:
        query += " AND s.timestamp <= ?"
        params.append(end_time)

    if sensor_names:
        placeholders = ",".join(["?"] * len(sensor_names))
        query += f" AND s.parameter_name IN ({placeholders})"
        params.extend(sensor_names)

    query += " ORDER BY s.timestamp LIMIT ?"
    params.append(limit)

    df = con.execute(query, params).df()
    con.close()

    # 横持ち形式に変換（timestampごとにpivot）
    if not df.empty:
        pivot_df = df.pivot(index="timestamp", columns="parameter_name", values="value_numeric")
        pivot_df.reset_index(inplace=True)

        # IDと単位の一覧（センサーマスタ情報として別途抽出）
        meta_df = df[["parameter_name", "parameter_id", "unit"]].drop_duplicates().sort_values("parameter_name")
        pivot_df.attrs["sensor_metadata"] = meta_df

        return pivot_df

    return df
