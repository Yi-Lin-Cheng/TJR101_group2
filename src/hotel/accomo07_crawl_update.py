import pandas as pd
import json
import time
from pathlib import Path
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from utils import get_connection, close_connection  # 使用原生連線方式

# -------- 從資料庫取得 accomo_id 與 b_url --------
conn, cursor = get_connection()
cursor.execute("SELECT accomo_id, b_url FROM ACCOMO")
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=["accomo_id", "b_url"])
close_connection(conn, cursor)

print(f"共取得 {len(df)} 筆資料準備爬蟲")

# -------- 建立 Selenium --------
service = Service(ChromeDriverManager().install())
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # 不開視窗模式
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 10)

# -------- 擷取評分與評論數 --------
ratings = []
comments = []

for i, row in df.iterrows():
    accomo_id = row["accomo_id"]
    url = row["b_url"]
    try:
        driver.get(url)
        time.sleep(1.5)

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "script[type='application/ld+json']")))
        ld_json = driver.find_element(By.CSS_SELECTOR, "script[type='application/ld+json']").get_attribute("innerText")
        data = json.loads(ld_json)

        rating = data.get("aggregateRating", {}).get("ratingValue", None)
        review_count = data.get("aggregateRating", {}).get("reviewCount", None)

        ratings.append(rating)
        comments.append(review_count)
    except Exception as e:
        print(f"[錯誤] {accomo_id} 無法爬取：{e}")
        ratings.append(None)
        comments.append(None)

driver.quit()

# -------- 組合並儲存 --------
df["rate"] = ratings
df["comm"] = comments

today_str = date.today().strftime("%Y%m%d")

# 判斷本機 VS Airflow container 的儲存資料夾路徑
if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/hotel")
else:
    data_dir = Path("data/hotel")

output_path = data_dir / f"booking_update_{today_str}.csv"
df[["accomo_id", "rate", "comm"]].to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"今日更新完成，共 {len(df)} 筆資料，已儲存至：{output_path}")