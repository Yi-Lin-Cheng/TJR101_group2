from pathlib import Path

import pandas as pd
from pandas import DataFrame
from utils import close_connection, get_connection
from pymysql.cursors import Cursor

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/klook")
else:
    data_dir = Path("data/klook")

source_data_path = data_dir / "final_data.csv"
target_table = "EXHIBIT_TEST"

def update_address_with_location(row):
    if pd.isna(row["address"]) and pd.notna(row["location"]):
        row["address"] = row["location"]
    
    return row

def db_get_update_event_ids(cursor: Cursor, event_ids: list[str]):
    # 取得要更新的資料
    to_update_db_records_sql = f"""
        SELECT exhibit_id 
        FROM {target_table}
        WHERE exhibit_id IN ({", ".join(event_ids)})
    """
    cursor.execute(to_update_db_records_sql)
    data = cursor.fetchall()

    return [event_id[0] for event_id  in data]

def db_update_records(cursor: Cursor, df: DataFrame):
    for hash, item in df.iterrows():
        
        sql = f"""
        UPDATE {target_table}
        SET ex_name = %s,
            county = %s,
            address = %s,
            pic_url = %s,
            klook_url = %s
        WHERE exhibit_id = {item["event_id"]}
        """
        params = (
            item["ex_name"],
            item["county"],
            item["address"],
            item["pic_url"],
            item["klook_url"]
        )        
        
        
        cursor.execute(sql, params)
    pass


def db_add_new_records(cursor: Cursor, df: DataFrame):
    

    
    # 把所有 NaN 替換成 None 以利 MySQL 使用
    for col in df.columns:
        if df[col].dtype == "float64":
            df[col] = df[col].astype(object)
    df = df.where(pd.notna(df), None)    
    
    sql_insert = f"""
    INSERT INTO {target_table} (exhibit_id, ex_name, county, address, geo_loc, pic_url, klook_url, s_time, e_time)
    VALUES (%s,%s,%s,%s,ST_SRID(POINT(%s, %s), 4326),%s,%s,%s,%s);
    """    
    
    cursor.executemany(sql_insert, df[
        [
            "event_id",
            "ex_name",
            "county",
            "address",
            "lng",
            "lat",
            "pic_url",
            "klook_url",
            "s_time",
            "e_time",
        ]
    ].values.tolist())


def main():

    df = pd.read_csv(f"{source_data_path}", encoding="utf-8-sig") 
    event_ids = df["event_id"].astype(str).to_list()
    df = df[
            (df['event_id'].notna() | df['event_id'].notnull())  
            & (df['ex_name'].notna() | df['ex_name'].notnull())  
            & (df['pic_url'].notna() | df['pic_url'].notnull())  
            & (df['klook_url'].notna() | df['klook_url'].notnull())  
            & ((df['address'].notna() & df['address'].notnull()) | (df['location'].notna() & df['location'].notnull()))  
            & (df['county'].notna() | df['county'].notnull())  
            & (df['s_time'].notna() | df['s_time'].notnull())  
            & (df['e_time'].notna() | df['e_time'].notnull())  
            & (df['lng'].notna() | df['lng'].notnull())  
            & (df['lat'].notna() | df['lat'].notna())
        ]
    
    # 若location 不為空且address為空
    df = df.apply(update_address_with_location, axis=1)

    try:
        # 連線SQL
        conn, cursor = get_connection()
        
        to_update_event_ids = db_get_update_event_ids(cursor, event_ids)
        to_update_df = df[df["event_id"].astype(str).isin(to_update_event_ids)]
        to_insert_df = df[~df["event_id"].astype(str).isin(to_update_event_ids)]

        # print(to_insert_df.shape)
        db_update_records(cursor, to_update_df)
        db_add_new_records(cursor, to_insert_df)
        conn.commit()
        print("資料已成功寫入資料庫。")
        
    except Exception as e:
        print(str(e))
    finally:
        close_connection(conn, cursor)
        pass

if __name__ == "__main__":
    main()
