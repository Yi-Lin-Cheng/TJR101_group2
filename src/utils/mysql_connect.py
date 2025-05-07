import os

import pymysql
from dotenv import load_dotenv

load_dotenv()  # 載入 .env 檔


def get_connection():
    """
    Establish a connection to the MySQL database and return the connection and cursor objects.
    
    Returns:
        tuple: A tuple containing:
            - conn: The MySQL connection object.
            - cursor: The MySQL cursor object.
    """
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
    """
    Close the database cursor and connection.
    Args:
        conn: A database connection object.
        cursor (optional): A database cursor object. If provided, it will be closed.
    Returns:
        None
    """
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Connection closed.")
