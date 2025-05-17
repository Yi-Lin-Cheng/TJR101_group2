import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import date

# -------- 計算今日日期、自動檔名 --------
today_str = date.today().strftime("%Y%m%d")
file_name = f"booking_update_{today_str}.csv"
file_path = Path(__file__).resolve().parents[2] / "data" / "hotel" / file_name

# -------- 讀取資料 --------
df = pd.read_csv(file_path, encoding="utf-8")
print(f"讀入檔案：{file_name}，筆數：{len(df)}")

# -------- 資料庫連線設定 --------
db_config = {
    "host": "35.234.56.196",
    "port": 3306,
    "user": "TJR101_2",
    "password": "TJR101_2pass",
    "database": "mydb",
    "charset": "utf8mb4"
}
conn_str = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
engine = create_engine(conn_str)

# -------- 整理要寫入的資料 --------
records = []
for _, row in df.iterrows():
    records.append({
        "accomo_id": row["accomo_id"],
        "rate": float(row["rate"]) if not pd.isna(row["rate"]) else None,
        "comm": int(row["comm"]) if not pd.isna(row["comm"]) else None
    })

# -------- 批次更新資料庫 --------
sql = text("""
    INSERT INTO ACCOMO (accomo_id, rate, comm)
    VALUES (:accomo_id, :rate, :comm)
    ON DUPLICATE KEY UPDATE
        rate = VALUES(rate),
        comm = VALUES(comm)
""")

with engine.begin() as conn:
    conn.execute(sql, records)

print(f"成功更新 {len(records)} 筆資料（來源檔案：{file_name}）")