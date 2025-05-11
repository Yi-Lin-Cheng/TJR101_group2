from pathlib import Path

import pandas as pd

from utils import close_connection, get_connection

data_dir = Path("data", "spot")


def main():
    file = data_dir / "spot06_cleaned_final.csv"
    data = pd.read_csv(file, encoding="utf-8", engine="python")
    # 處理 geo_loc 欄位為 SQL 語法格式
    data["geo_loc"] = data["geo_loc"].apply(lambda x: f"ST_GeomFromText('{x}')" if pd.notna(x) else None)
    # 把所有 NaN 替換成 None 以利 MySQL 使用
    data = data.where(pd.notna(data), None)
    # 欄位處理
    update_columns = ["b_hours", "rate", "pic_url", "comm", "update_time"]
    all_columns = list(data.columns)
    columns_str = ", ".join([f"`{col}`" for col in all_columns])
    placeholders = ", ".join(["%s"] * len(all_columns))
    update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])
    # SQL語法
    sql = f"""
    INSERT INTO SPOT ({columns_str})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_clause}
    """
    # 準備資料
    values_list = [row for row in data.values.tolist()]
    # 執行插入或更新
    conn, cursor = get_connection()
    cursor.executemany(sql, values_list)
    conn.commit()
    close_connection(conn, cursor)


if __name__ == "__main__":
    main()
