
import pandas as pd 
import requests
from typing import Any
from pathlib import Path
from datetime import datetime
from sqlalchemy import MetaData, Table, update
from sqlalchemy.dialects.mysql import insert as mysql_insert

from ..DataBase.database import engine
from .create_ssl import build_ssl_context, SSLContextAdapter
from .clean_helper import clean_region_name, df_to_records

#============
# Config
#============
BASE_DIR = Path(__file__).resolve().parents[0]
DATA_DIR = BASE_DIR / "raw_data"

OPEN_DATA_CSV_URL = "https://opdadm.moi.gov.tw/api/v1/no-auth/resource/api/dataset/EA5E6FCD-B82D-43B7-A5CF-E9893253187E/resource/8E9B68E1-185D-4376-BE88-214ADDD910FA/download"

EXPECTED = [
    'city_name', 'region_name', 'address', 
    'dept_name', 'branch_name', 'longgitude', 
    'latitude', 'direct', 'speed_limit'
    ]

COLS_MAP = {
    "CityName"  : "city_name",
    "RegionName": "region_name",
    "Address"   : "address",
    "DeptNm"    : "dept_name",
    "BranchNm"  : "branch_name",
    "Logitude"  : "longitude",
    "Latitude"  : "latitude",
    "direct"    : "direct",
    "limit"     : "speed_limit"
}

#============
# Extract
#============
def download_data(csv_url: str, save_dir: Path, cafile : str | None = None) -> Path :
    save_dir.mkdir(exist_ok = True, parents = True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = save_dir / f"raw_open_data_{ts}.csv"

    ctx = build_ssl_context(cafile, relax_strict = True)
    session = requests.Session()
    session.mount("https://", SSLContextAdapter(ctx))

    try:
        with session.get(csv_url, timeout = (10, 60), stream = True) as resp:
            resp.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size = 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                           
    except requests.exceptions.RequestException as e:
        if file_path.exists():
            file_path.unlink(missing_ok = True)
        raise RuntimeError(f"下載失敗：{e}") from e 
    
    print(f"[INFO] CSV 已下載至：{file_path}")

    return file_path

#============
# load_raw_data
#============
def load_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        csv_path,
        encoding = "utf-8-sig",
        dtype = "string",
        na_values = ['', ' ', 'NaN', 'Na']
    )

    return df 

#============
# Transform
#============
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns = COLS_MAP).copy()
    
    missing = [c for c in EXPECTED if c not in df.columns]
    if missing:
        raise ValueError(f"[WARN] 缺失欄位：{missing}，目前欄位：{df.columns.tolist()}")
    
    str_cols = [
        "city_name", "region_name", "address",
        "dept_name", "branch_name", "direct"
    ]
    df[str_cols] = df[str_cols].fillna("").astype(str)

    df['longitude'] = pd.to_numeric(df['longitude'].str.strip(), errors = "coerce")
    df['latitude'] = pd.to_numeric(df['latitude'].str.strip(), errors = "coerce")
    df["speed_limit"] = pd.to_numeric(df["speed_limit"].str.strip(), errors = "coerce").astype("Int64")

    df = clean_region_name(df)
    df = df[EXPECTED]
    
    return df 


#============
# Load DB
#============
def insert_etl_run_with_own_transaction(rows_fetched: int) -> int:
    meta = MetaData()
    etl_runs = Table("etl_runs", meta, autoload_with = engine)
    
    payload: dict[str, Any] = {
        "started_at"   : datetime.now(),
        "finished_at"  : None,
        "status"       : "running",
        "rows_fetched" : rows_fetched,
        "rows_inserted": 0,
        "rows_updated" : 0,
        "error_message": None
    }

    with engine.begin() as conn:
        result = conn.execute(etl_runs.insert().values(**payload))
        etl_run_id = result.inserted_primary_key[0]
    return int(etl_run_id)


def update_etl_run_with_own_transaction(
        etl_run_id: int, status: str, 
        rows_inserted: int, rows_updated: int, 
        error_message: str | None = None
) -> None:
    meta = MetaData()
    etl_runs = Table("etl_runs", meta, autoload_with = engine)

    stmt = (
        update(etl_runs)
        .where(etl_runs.c.id == etl_run_id)
        .values(
            finished_at   = datetime.now(),
            status        = status,
            rows_inserted = rows_inserted,
            rows_updated  = rows_updated,
            error_message = error_message
        )
    )
    with engine.begin() as conn:    
        conn.execute(stmt)


def insert_raw_data(conn, df: pd.DataFrame, chunk_size: int = 500) -> int:
    if df is None or df.empty:
        return 0

    meta = MetaData()
    table = Table('raw_data', meta, autoload_with = conn)
    
    records = df_to_records(df)
    total = len(records)

    stmt = mysql_insert(table)

    for i in range(0, total, chunk_size):
        batch = records[i: i + chunk_size]
        conn.execute(stmt, batch)

    return int(total)

def upsert_camera_points(conn, df: pd.DataFrame, chunk_size: int = 500) -> int:
    if df is None or df.empty:
        return 0
    
    camera_cols = [
        "city_name",
        "region_name",
        "address", 
        "longitude",
        "latitude",
        "direct",
        "speed_limit"
    ]

    df_camera = df[camera_cols].copy()

    meta = MetaData()
    table = Table('camera_points', meta, autoload_with = conn)
    
    records = df_to_records(df_camera)
    total = len(records)

    ins = mysql_insert(table)
    stmt = ins.on_duplicate_key_update(
        longitude   = ins.inserted.longitude,
        latitude    = ins.inserted.latitude,
        speed_limit = ins.inserted.speed_limit,
        direct      = ins.inserted.direct
    )
    for i in range(0, total, chunk_size):
        batch = records[i: i + chunk_size]
        conn.execute(stmt, batch)
    
    return int(total)

def load_all(df_raw, df_camera, chunk_size = 500):
    if (df_raw is None or df_raw.empty) and (df_camera is None or df_camera.empty):
        print("[INFO] df_raw 與 df_camera 都為空，略過匯入")
        return {
            "etl_run_id"     : None,
            "raw_inserted"   : 0,
            "camera_processed": 0
        }
    etl_run_id = insert_etl_run_with_own_transaction(
        rows_fetched = 0 if df_raw is None else len(df_raw)
    )
    raw_inserted = 0
    camera_processed = 0

    try:
        with engine.begin() as conn:
            if df_raw is not None and not df_raw.empty:
                df_raw_to_insert = df_raw.copy()
                df_raw_to_insert['etl_run_id'] = etl_run_id 
                raw_inserted = insert_raw_data(conn, df_raw_to_insert, chunk_size = chunk_size)
            
            if df_camera is not None and not df_camera.empty:
                df_camera_to_insert = df_camera.copy()            
                camera_processed = upsert_camera_points(conn, df_camera_to_insert, chunk_size = chunk_size)

        # 更新 etl_runs
        update_etl_run_with_own_transaction(
            etl_run_id    = etl_run_id,
            status       = "success",
            rows_inserted = camera_processed,
            rows_updated  = 0,
            error_message = None
        )
        return {
            "etl_run_id": etl_run_id,
            "raw_inserted": raw_inserted,
            "camera_processed": camera_processed
        }
    except Exception as e:
        update_etl_run_with_own_transaction(
            etl_run_id = etl_run_id,
            status = 'failed',
            rows_inserted = 0,
            rows_updated = 0,
            error_message = str(e)[:255]
        )
        raise ValueError("ETL匯入失敗") from e 