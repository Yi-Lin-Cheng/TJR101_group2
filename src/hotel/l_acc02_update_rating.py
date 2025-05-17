import pandas as pd
from pathlib import Path
from datetime import date
from utils import get_connection, close_connection  # 原生連線函式

# -------- 計算今日日期、自動檔名 --------
today_str = date.today().strftime("%Y%m%d")
file_name = f"booking_update_{today_str}.csv"
file_path = Path(__file__).resolve().parents[2] / "data" / "hotel" / file_name

# -------- 讀取資料 --------
df = pd.read_csv(file_path, encoding="utf-8")
print(f"讀入檔案：{file_name}，筆數：{len(df)}")

# -------- 整理要寫入的資料（轉型） --------
values_list = []
for _, row in df.iterrows():
    values_list.append((
        float(row["rate"]) if not pd.isna(row["rate"]) else None,
        int(row["comm"]) if not pd.isna(row["comm"]) else None,
        row["accomo_id"]
    ))

# -------- SQL 語法：改為 UPDATE --------
sql = """
    UPDATE ACCOMO
    SET rate = %s,
        comm = %s
    WHERE accomo_id = %s
"""

# -------- 資料寫入資料庫 --------
conn, cursor = get_connection()
cursor.executemany(sql, values_list)
conn.commit()
close_connection(conn, cursor)

print(f"成功更新 {len(values_list)} 筆資料（來源檔案：{file_name}）")