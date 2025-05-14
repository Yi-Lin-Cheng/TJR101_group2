import pandas as pd
from pathlib import Path
from utils import get_connection, close_connection  # 原生連線函式

# -------- 根據是否在 Airflow container 中，動態設定資料夾路徑 --------
if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/hotel")  # Airflow container 路徑
else:
    data_dir = Path("data/hotel")  # 本機開發環境路徑

# -------- 讀取 CSV 檔案 --------
file_path = data_dir / "accomo07_for_db.csv"
df = pd.read_csv(file_path, encoding="utf-8")

# -------- 整理成批次寫入格式（轉成 tuple）--------
records = []
for _, row in df.iterrows():
    records.append((
        row["accomo_id"],
        row["a_name"],
        row["county"],
        row["address"],
        float(row["rate"]) if not pd.isna(row["rate"]) else None,
        row["geo_loc"],  # WKT 格式，如 "POINT(121.5 25.0)"
        row["pic_url"],
        row["b_url"],
        row["ac_type"],
        int(row["comm"]) if not pd.isna(row["comm"]) else None,
        row["area"],
        row["fac"] if not pd.isna(row["fac"]) else None
    ))

# -------- 原生 PyMySQL 支援的 SQL 語法（使用 %s）--------
sql = """
    INSERT INTO ACCOMO (
        accomo_id, a_name, county, address, rate,
        geo_loc, pic_url, b_url, ac_type, comm, area, fac
    ) VALUES (
        %s, %s, %s, %s, %s,
        ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s
    )
"""

# -------- 執行資料表清空 + 匯入 --------
conn, cursor = get_connection()

# 清空原有資料（注意：不可在生產環境亂清除）
cursor.execute("DELETE FROM ACCOMO")

# 批次匯入資料
cursor.executemany(sql, records)

# 提交並關閉連線
conn.commit()
close_connection(conn, cursor)

print(f"資料表 ACCOMO 清空並匯入 {len(records)} 筆資料")