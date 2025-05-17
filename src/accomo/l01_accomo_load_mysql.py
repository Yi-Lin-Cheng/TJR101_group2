from pathlib import Path

import pandas as pd

from utils import close_connection, get_connection  # DB 連線模組

# -------- 路徑設定 --------
if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/hotel")
else:
    data_dir = Path("data/hotel")
file_path = data_dir / "accomo06_for_db.csv"


def main():
    # -------- 載入資料 --------
    df = pd.read_csv(file_path, encoding="utf-8")
    for col in df.columns:
        if df[col].dtype == "float64":
            df[col] = df[col].astype(object)
    df = df.where(pd.notna(df), None)

    # -------- 欄位順序定義 --------
    all_columns = [
        "accomo_id",
        "a_name",
        "county",
        "address",
        "rate",
        "pic_url",
        "b_url",
        "ac_type",
        "comm",
        "area",
        "fac",
        "geo_loc",
    ]

    # -------- SQL 組合 --------
    update_columns = [
        "a_name",
        "county",
        "address",
        "rate",
        "pic_url",
        "b_url",
        "ac_type",
        "comm",
        "area",
        "fac",
        "geo_loc",
    ]

    columns_str = ", ".join(f"`{col}`" for col in all_columns)
    placeholders = ", ".join(
        ["%s"] * (len(all_columns) - 1) + ["ST_GeomFromText(%s, 4326)"]
    )
    update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])

    sql = f"""
        INSERT INTO ACCOMO ({columns_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    values_list = df[all_columns].values.tolist()

    # -------- 寫入資料庫 --------
    conn, cursor = get_connection()
    cursor.executemany(sql, values_list)
    conn.commit()
    close_connection(conn, cursor)

    print(f"成功更新 ACCOMO 資料表，共處理 {len(values_list)} 筆資料")


if __name__ == "__main__":
    main()
