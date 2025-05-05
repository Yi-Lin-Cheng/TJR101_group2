import os
import pymysql
from dotenv import load_dotenv


load_dotenv()  # 載入 .env 檔


def get_connection():
    host = os.getenv("MySQL_host")
    port = 3306
    user = os.getenv("MySQL_user")
    passwd = os.getenv("MySQL_passwd")
    db = os.getenv("MySQL_db")
    charset = "utf8mb4"

    conn = pymysql.connect(
        host=host, port=port, user=user, passwd=passwd, db=db, charset=charset
    )
    print("Successfully connected!")
    cursor = conn.cursor()
    return conn, cursor


def close_connection(conn, cursor=None):
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Connection closed.")
