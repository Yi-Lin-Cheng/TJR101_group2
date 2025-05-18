import uuid
from pathlib import Path

import pandas as pd
import pymysql

from utils import close_connection, get_connection

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/accupass")
else:
    data_dir = Path("data/accupass")

data_name = {"four_step_t": data_dir / "t_04_accupass_clean.csv"}


def save_to_csv(data, key):
    """共用：存檔"""
    filename = data_name.get(key)
    data.to_csv(filename, index=False, encoding="utf-8")


def read_from_csv(key):
    """共用：讀檔"""
    filename = data_name.get(key)
    df = pd.read_csv(filename, encoding="utf-8")
    return df


def l_accupass_mysql_con():
    df = read_from_csv("four_step_t")
    df = df.where(pd.notnull(df), None)

    try:
        # 連線SQL
        conn, cursor = get_connection()

        # 設定uuid
        df["events_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df["geo_loc"] = "Point(" + df["geo_loc"].str.replace(",", " ") + ")"
        sql_search = "SELECT ev_name FROM EVENTS"  # 取出ev_name的資料
        db_df = pd.read_sql(sql_search, conn)  # 存進df
        print(f"目前資料庫中的資料筆數：{len(db_df)}")

        db_ev_name = db_df["ev_name"].tolist()  # 轉換成list
        df = df[~df["ev_name"].isin(db_ev_name)]  # 去除重複資料，留新資料

        sql_insert = """
        INSERT INTO EVENTS (events_id, ev_name, county, address, geo_loc, pic_url, accu_url, s_time, e_time)
        VALUES (%s,%s,%s,%s,ST_GeomFromText(%s, 4326),%s,%s,%s,%s);
        """

        df = df[
            [
                "events_id",
                "ev_name",
                "county",
                "address",
                "geo_loc",
                "pic_url",
                "accu_url",
                "s_time",
                "e_time",
            ]
        ].values.tolist()

        print(f"要插入的資料筆數：{len(df)}")

        if len(df) > 0:
            cursor.executemany(sql_insert, df)
            conn.commit()
            print("資料已成功寫入資料庫。")
        else:
            print("無新增資料，已全部存在於資料庫中。")

        cursor.execute("SELECT COUNT(*) FROM EVENTS;")
        result = cursor.fetchone()
        if result:
            print("目前資料筆數：", result[0])
        else:
            print("查無結果，請確認 EVENTS 表格是否存在或語法錯誤")

    except Exception as e:
        print(f"錯誤資訊: {e}")
        conn.rollback()
        raise

    finally:
        close_connection(conn, cursor)


if __name__ == "__main__":
    l_accupass_mysql_con()
