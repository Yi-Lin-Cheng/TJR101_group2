import pandas as pd
from pathlib import Path
from datetime import date
from sqlalchemy import text

from utils import get_connection, close_connection  # ← 改為統一連線管理

# -------- 計算今日日期、自動檔名 --------
today_str = date.today().strftime("%Y%m%d")
file_name = f"booking_update_{today_str}.csv"
file_path = Path(__file__).resolve().parents[2] / "data" / "hotel" / file_name

# -------- 讀取資料 --------
df = pd.read_csv(file_path, encoding="utf-8")
print(f"讀入檔案：{file_name}，筆數：{len(df)}")

# -------- 整理要寫入的資料 --------
records = []
for _, row in df.iterrows():
    records.append({
        "accomo_id": row["accomo_id"],
        "rate": float(row["rate"]) if not pd.isna(row["rate"]) else None,
        "comm": int(row["comm"]) if not pd.isna(row["comm"]) else None
    })

# -------- 批次更新資料庫（使用模組化連線） --------
sql = text("""
    INSERT INTO ACCOMO (accomo_id, rate, comm)
    VALUES (:accomo_id, :rate, :comm)
    ON DUPLICATE KEY UPDATE
        rate = VALUES(rate),
        comm = VALUES(comm)
""")

engine, _ = get_connection(use_sqlalchemy=True)
with engine.begin() as conn:
    conn.execute(sql, records)
close_connection(engine, None)

print(f"成功更新 {len(records)} 筆資料（來源檔案：{file_name}）")