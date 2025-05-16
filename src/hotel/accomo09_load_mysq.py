import pandas as pd
from pathlib import Path
from utils import get_connection, close_connection  # 原生連線函式

# -------- 根據是否在 Airflow container 中，動態設定資料夾路徑 --------
if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/hotel")
else:
    data_dir = Path("data/hotel")

# -------- 讀取 CSV 檔案 --------
file_path = data_dir / "accomo06_for_db.csv"
df = pd.read_csv(file_path, encoding="utf-8")

# -------- 將 float64 欄位轉為 object，準備 NaN → None --------
for col in df.columns:
    if df[col].dtype == "float64":
        df[col] = df[col].astype(object)
df = df.where(pd.notna(df), None)  # 將 NaN 替換為 None，符合 MySQL 認識的 NULL

# -------- 欄位處理 --------
update_columns = [
    "a_name", "county", "address", "rate",
    "geo_loc", "pic_url", "b_url", "ac_type",
    "comm", "area", "fac"
]

# 注意 geo_loc 是 ST_GeomFromText 處理，其餘直接插入
base_columns = [col for col in df.columns if col != "geo_loc"]
all_columns = base_columns + ["geo_loc"]

# -------- 動態產出 SQL 語句 --------
columns_str = ", ".join([f"`{col}`" for col in all_columns])
placeholders = ", ".join(["%s"] * len(base_columns) + ["ST_GeomFromText(%s, 4326)"])
update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])

sql = f"""
    INSERT INTO ACCOMO ({columns_str})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_clause}
"""

# -------- 資料轉換為 list 傳入 executemany --------
values_list = df[all_columns].values.tolist()

# -------- 資料寫入資料庫 --------
conn, cursor = get_connection()
cursor.executemany(sql, values_list)
conn.commit()
close_connection(conn, cursor)

print(f"成功更新 ACCOMO  資料表，共處理 {len(values_list)} 筆資料")