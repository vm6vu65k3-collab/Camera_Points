import enum
from sqlalchemy import (Column, String, Integer, Boolean, 
                        ForeignKey, Enum, text, UniqueConstraint, CheckConstraint)
from sqlalchemy.dialects.mysql import DECIMAL, TIMESTAMP
from camera_points.DataBase.database import Base

class EtlStatus(enum.Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"


class BaseModel(Base):
    __abstract__ = True
    
    id = Column(Integer, primary_key = True, autoincrement = True, comment = "ID主鍵")


class RawCameraPoints(BaseModel):
    __tablename__ = "raw_data"

    etl_run_id  = Column(Integer, ForeignKey("etl_runs.id"), nullable = False, comment = "ETL執行批次ID")
    city_name   = Column(String(10), nullable = False, comment = "設置縣市")
    region_name = Column(String(20), nullable = False, comment = "設置市區鄉鎮")
    address     = Column(String(100), nullable = False, comment = "設置地點")
    dept_name   = Column(String(50), nullable = False, comment = "管轄警局")
    branch_name = Column(String(50), nullable = False, comment = "管轄分局")
    longitude   = Column(String(20), nullable = False, comment = "經度")
    latitude    = Column(String(20), nullable = False, comment = "緯度")
    direct      = Column(String(20), nullable = False, comment = "拍攝方向")
    speed_limit = Column(String(10), nullable = False, comment = "速限")
    created_at  = Column(TIMESTAMP, nullable = False, server_default = text("CURRENT_TIMESTAMP"), comment = "匯入時間")
    

    def __repr__(self):
        return (
            f"RawCameraPoints<(id = {self.id},"
            f"etl_run_id = {self.etl_run_id},"
            f"city_name = {self.city_name!r},"
            f"region_name = {self.region_name!r},"
            f"address = {self.address!r},"
            f"dept_name = {self.dept_name!r},"
            f"branch_name = {self.branch_name!r},"
            f"longitude = {self.longitude!r},"
            f"latitude = {self.latitude!r},"
            f"direct = {self.direct!r},"
            f"limit = {self.speed_limit!r})>"
        )
    

class CameraPoints(BaseModel):
    __tablename__ = "camera_points"
    
    city_name   = Column(String(10),   nullable = False, comment = "設置縣市")
    region_name = Column(String(20),  nullable = False, comment = "設置市區鄉鎮")
    address     = Column(String(100),  nullable = False, comment = "設置地點")
    longitude   = Column(DECIMAL(10, 7),  nullable = False, comment = "經度")
    latitude    = Column(DECIMAL(10, 7),  nullable = False, comment = "緯度")
    direct      = Column(String(20),   nullable = False, comment = "拍攝方向")
    speed_limit = Column(Integer,     nullable = False, comment = "速限")
    is_active   = Column(Boolean,     nullable = False, server_default = text("1"), comment = "是否啟用")
    created_at  = Column(TIMESTAMP,   nullable = False, server_default = text("CURRENT_TIMESTAMP"), comment = "建立時間")
    updated_at  = Column(TIMESTAMP,   nullable = False, server_default = text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment = "更新時間")

    __table_args__ = (
        UniqueConstraint("city_name", "region_name", "address", "longitude", "latitude", "direct",
                         name = "uk_camera_points_location"),
        CheckConstraint("speed_limit >= 0", name = "ck_camera_points_limit_nonneg"),
    )

    def __repr__(self):
        return (
            f"CameraPoints<(id = {self.id},"
            f"city_name = {self.city_name!r},"
            f"region_name = {self.region_name!r},"
            f"address = {self.address!r},"
            f"longitude = {self.longitude!r},"
            f"latitude = {self.latitude!r},"
            f"direct = {self.direct!r},"
            f"limit = {self.speed_limit!r},"
            f"is_active = {self.is_active})>"
        )
    
class ETLRuns(BaseModel):
    __tablename__ = "etl_runs"

    started_at  = Column(TIMESTAMP(fsp = 6), nullable = False, comment = "開始時間")
    finished_at = Column(TIMESTAMP(fsp = 6), nullable = True, comment = "結束時間")
    status      = Column(
        Enum(
            EtlStatus,
            values_callable = lambda x: [e.value for e in x],
            name            = "etl_status_enum"
        ),
        nullable = False,
        comment  = "執行狀態"
    )
    rows_fetched  = Column(Integer, nullable = False, server_default = text("0"), comment = "取得資料數")
    rows_inserted = Column(Integer, nullable = False, server_default = text("0"), comment = "插入資料數")
    rows_updated  = Column(Integer, nullable = False, server_default = text("0"), comment = "更新資料數")
    error_message = Column(String(255), nullable = True, comment = "錯誤訊息")

    __table_args__ = (
        CheckConstraint("rows_fetched >= 0", name = "ck_etl_rows_fetched_nonneg"),
        CheckConstraint("rows_inserted >= 0", name = "ck_etl_rows_inserted_nonneg"),
        CheckConstraint("rows_updated >= 0", name = "ck_etl_rows_updated_nonneg"),
        CheckConstraint("finished_at >= started_at", name = "ck_etl_speed_time_order")
    )

    def __repr__(self):
        return (
            f"EtlRuns<(id = {self.id},"
            f"started_at = {self.started_at!r},"
            f"finished_at = {self.finished_at!r},"
            f"status = {self.status.value if self.status else None},"
            f"rows_fetched = {self.rows_fetched},"
            f"rows_inserted = {self.rows_inserted},"
            f"rows_updated = {self.rows_updated},"
            f"error_message = {self.error_message!r})>"
        )