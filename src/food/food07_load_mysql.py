from pathlib import Path
import pandas as pd
from utils import close_connection, get_connection  # 自訂連線模組

# 判斷目前是否執行在 Airflow container 中（適用部署情境）
if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/food")
else:
    data_dir = Path("data/food")  # 本機開發情境

def main():
    # ---------- 讀取清理後的資料 ----------
    file = data_dir / "food06_cleaned_final.csv"
    data = pd.read_csv(file, encoding="utf-8", engine="python")

    # ---------- 處理 NaN，避免寫入資料庫錯誤 ----------
    # 將 float 欄位轉為 object 型別（否則 NaN 無法被 None 替代）
    for col in data.columns:
        if data[col].dtype == "float64":
            data[col] = data[col].astype(object)
    # 將整個 DataFrame 中的 NaN 轉成 None（MySQL 支援 NULL）
    data = data.where(pd.notna(data), None)

    # ---------- 定義資料欄位 ----------
    update_columns = ["b_hours", "rate", "pic_url", "comm", "update_time"]  # 若主鍵已存在則會更新這些欄位

    # 移除 geo_loc 欄位，因為它是地理資料，要用 ST_GeomFromText() 處理
    base_columns = [col for col in data.columns if col != "geo_loc"]
    all_columns = base_columns + ["geo_loc"]  # 最終插入的欄位順序

    # ---------- 動態生成 SQL ----------
    # 1. 欄位名稱部分（加上反引號處理特殊欄位名）
    columns_str = ", ".join([f"`{col}`" for col in all_columns])

    # 2. VALUES 部分：普通欄位用 %s，geo_loc 欄位用 ST_GeomFromText 處理地理空間資料
    placeholders = ", ".join(["%s"] * len(base_columns) + ["ST_GeomFromText(%s, 4326)"])

    # 3. 更新語句：ON DUPLICATE KEY UPDATE 部分
    update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])

    # 4. 最終組合 SQL 語法
    sql = f"""
    INSERT INTO FOOD ({columns_str})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_clause}
    """

    # ---------- 將資料轉為 list 傳入 executemany ----------
    values_list = data[all_columns].values.tolist()

    # ---------- 執行資料寫入 ----------
    conn, cursor = get_connection()             # 建立 MySQL 原生連線
    cursor.executemany(sql, values_list)        # 一次批次寫入所有資料
    conn.commit()                               # 提交變更
    close_connection(conn, cursor)              # 關閉連線資源

# ---------- 程式主入口 ----------
if __name__ == "__main__":
    main()