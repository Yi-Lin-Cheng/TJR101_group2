import pandas as pd
import mysql.connector
import os

# 需改成我們原放csv檔的folder_path
folder_path = 'your_folder_path'


# Connection Settings
conn = mysql.connector.connect(
    host='localhost',
    user='your_username',
    password='your_password',
    database='travel_bot'
)
cursor = conn.cursor()

# 檔名（不含.csv）對應到資料表名稱
file_to_table = {
    'attractions': 'attractions',
    'restaurants': 'restaurants',
    'hotels': 'hotels'
}

# 依序讀取資料夾內的csv檔
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        table_name = file_to_table.get(filename.replace('.csv', ''))

        if table_name:
            print(f"正在匯入 {filename} 到 {table_name} 資料表...")

            df = pd.read_csv(file_path)

            # 產生欄位名稱的字串
            columns = ', '.join(df.columns)

            # 產生 %s, %s, %s 這種 placeholders
            placeholders = ', '.join(['%s'] * len(df.columns))

            # 產生插入語法
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # 批次插入
            for index, row in df.iterrows():
                cursor.execute(sql, tuple(row))

            conn.commit()

cursor.close()
conn.close()

print("All files are imported.")