import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

# -------- 資料庫連線設定 --------
db_config = {
    "host": "35.234.56.196",
    "port": 3306,
    "user": "TJR101_2",
    "password": "TJR101_2pass",
    "database": "mydb",
    "charset": "utf8mb4"
}

# -------- 讀取 CSV 檔案 --------
file_path = Path(__file__).resolve().parents[2] / "data" / "hotel" / "accomo07_for_db.csv"
df = pd.read_csv(file_path, encoding="utf-8")

# -------- 建立 SQLAlchemy 引擎 --------
conn_str = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}"
engine = create_engine(conn_str)

# -------- 手動 INSERT --------
with engine.begin() as conn:
    for _, row in df.iterrows():
        sql = text("""
            INSERT INTO ACCOMO (
                accomo_id, a_name, county, address, rate,
                geo_loc, pic_url, b_url, ac_type, comm, area, fac
            ) VALUES (
                :accomo_id, :a_name, :county, :address, :rate,
                ST_GeomFromText(:geo_loc), :pic_url, :b_url, :ac_type, :comm, :area, :fac
            )
        """)
        conn.execute(sql, {
            "accomo_id": row["accomo_id"],
            "a_name": row["a_name"],
            "county": row["county"],
            "address": row["address"],
            "rate": float(row["rate"]) if not pd.isna(row["rate"]) else None,
            "geo_loc": row["geo_loc"],
            "pic_url": row["pic_url"],
            "b_url": row["b_url"],
            "ac_type": row["ac_type"],
            "comm": int(row["comm"]) if not pd.isna(row["comm"]) else None,
            "area": row["area"],
            "fac": row["fac"] if not pd.isna(row["fac"]) else None
        })

print("資料已匯入 ACCOMO 資料表")