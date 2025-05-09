from pathlib import Path

import pandas as pd

from utils import close_connection, get_connection

data_dir = Path("data", "spot")


def main():
    file = data_dir / "spot06_cleaned_final.csv"
    data = pd.read_csv(file, encoding="utf-8", engine="python")
    # 開啟連線
    conn, cursor = get_connection()
    update_columns = ["b_hours", "rate", "pic_url", "comm", "update_time"]
    # 移除 location 這欄的字串形式（它不是 DataFrame 欄位）
    base_columns = [col for col in data.columns if col not in ["geo_loc"]]
    all_columns = base_columns + ["geo_loc"]
    # 動態生成 SQL：location 要改成 ST_GeomFromText
    columns_str = ", ".join([f"`{col}`" for col in all_columns])
    placeholders = ", ".join(["%s"] * len(base_columns) + ["ST_GeomFromText(%s)"])
    update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])
    sql = f"""
    INSERT INTO SPOT ({columns_str})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_clause}
    """
    # 執行插入或更新
    for i, row in data.iterrows():
        values = [None if pd.isna(v) else v for v in row[base_columns]]
        # values = []
        # for v in row[base_columns]:
        #     if pd.isna(v):
        #         values.append(None)
        #     else:
        #         values.append(v)
        values.append(row["geo_loc"])
        cursor.execute(sql, values)
        print(f"第{i+1}筆完成")
    conn.commit()
    # 關閉連線
    close_connection(conn, cursor)


if __name__ == "__main__":
    main()
