from pathlib import Path

import pandas as pd

from utils import close_connection, get_connection

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/food")
else:
    data_dir = Path("data/food")


def main():
    file = data_dir / "food06_cleaned_final.csv"
    data = pd.read_csv(file, encoding="utf-8", engine="python")
    # 把所有 NaN 替換成 None 以利 MySQL 使用
    for col in data.columns:
        if data[col].dtype == "float64":
            data[col] = data[col].astype(object)
    data = data.where(pd.notna(data), None)
    # 欄位處理
    update_columns = ["b_hours", "rate", "pic_url", "comm", "update_time"]
    # 移除 location 這欄的字串形式（它不是 DataFrame 欄位）
    base_columns = [col for col in data.columns if col not in ["geo_loc"]]
    all_columns = base_columns + ["geo_loc"]

    # 動態生成 SQL：location 要改成 ST_GeomFromText
    columns_str = ", ".join([f"`{col}`" for col in all_columns])
    placeholders = ", ".join(["%s"] * len(base_columns) + ["ST_GeomFromText(%s, 4326)"])
    update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])
    # SQL語法
    sql = f"""
    INSERT INTO FOOD ({columns_str})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_clause}
    """
    # 準備資料
    values_list = data[all_columns].values.tolist()
    # 執行插入或更新
    conn, cursor = get_connection()
    cursor.executemany(sql, values_list)
    conn.commit()
    close_connection(conn, cursor)


if __name__ == "__main__":
    main()
