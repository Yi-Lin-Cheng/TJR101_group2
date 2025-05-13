import pymysql

# 建立連線
conn = pymysql.connect(
    host="35.234.56.196",
    user="TJR101_2",
    password="TJR101_2pass",
    database="mydb",
    charset="utf8mb4"
)

cursor = conn.cursor()

# 列出所有資料表
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

# 顯示結果
print("目前資料庫中有以下資料表：")
for table in tables:
    print("資料表名稱:", table[0])

# 結束連線
cursor.close()
conn.close()

import pymysql

# 建立連線
conn = pymysql.connect(
    host="35.234.56.196",
    user="TJR101_2",
    password="TJR101_2pass",
    database="mydb",
    charset="utf8mb4"
)

cursor = conn.cursor()

# 查 ACCOMO 資料表欄位結構
table_name = "ACCOMO"
cursor.execute(f"DESCRIBE {table_name}")
columns = cursor.fetchall()

# 顯示欄位名稱、資料型別、是否可為 NULL、是否有預設值等
print(f"{table_name} 資料表欄位結構如下：\n")
print(f"{'欄位名稱':<20} {'型態':<20} {'可為NULL':<10} {'主鍵':<10} {'預設值':<20}")
print("-" * 80)
for col in columns:
    field, dtype, null, key, default, extra = col
    print(f"{field:<20} {dtype:<20} {null:<10} {key:<10} {str(default):<20}")

cursor.close()
conn.close()