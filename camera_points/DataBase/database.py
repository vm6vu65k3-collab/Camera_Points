import os 
import pymysql
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

user     = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
host     = os.getenv("MYSQL_HOST")
port     = int(os.getenv("MYSQL_PORT"))
database = os.getenv("MYSQL_NAME")


ssl_ca = os.getenv("MYSQL_SSL_CA")

ssl_dict = None
if ssl_ca:
    ssl_dict = {}
    ssl_dict["ssl_ca"] = ssl_ca
    ssl_dict["check_hostname"] = False


def create_database_if_not_exists(database):
    with pymysql.connect(
        user = user,
        password = password,
        host = host,
        port = port,
        autocommit = True,
        charset = "utf8mb4",
        ssl = ssl_dict
    ) as con:
        with con.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")


db_url = URL.create(
    "mysql+pymysql",
    username = user,
    password = password,
    host     = host,
    port     = port,
    database = database,
    query    = {"charset": "utf8mb4"}
)


connect_args = {}
if ssl_dict:
    connect_args['ssl'] = ssl_dict

engine = create_engine(
    db_url,
    pool_size     = 5,
    max_overflow  = 10,
    pool_timeout  = 30,
    pool_recycle  = 3600,
    pool_pre_ping = True,
    future        = True,
    echo          = False,
    connect_args  = connect_args
)

SessionLocal = sessionmaker(bind = engine, autoflush = False, autocommit = False, expire_on_commit = False)

Base = declarative_base()