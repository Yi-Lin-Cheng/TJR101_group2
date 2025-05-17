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
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

from utils import get_connection, close_connection


def get_safe_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-data-dir=/tmp/chrome_{int(time.time())}")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# -------- 連線抓取資料 --------
conn, cursor = get_connection()
cursor.execute("SELECT accomo_id, b_url FROM ACCOMO")
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=["accomo_id", "b_url"])
close_connection(conn, cursor)

# -------- 過濾無效網址 --------
df = df[df["b_url"].notna() & df["b_url"].str.startswith("http")].reset_index(drop=True)

# -------- 開始爬蟲 --------
driver = get_safe_driver()
wait = WebDriverWait(driver, 10)

result_rows = []

for i, row in df.iterrows():
    accomo_id = row["accomo_id"]
    url = row["b_url"]

    if i % 100 == 0 and i != 0:
        print(f"處理中：第 {i}/{len(df)} 筆...")

    try:
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "script[type='application/ld+json']")))
        ld_json = driver.find_element(By.CSS_SELECTOR, "script[type='application/ld+json']").get_attribute("innerText")
        data = json.loads(ld_json)

        rating = data.get("aggregateRating", {}).get("ratingValue")
        review_count = data.get("aggregateRating", {}).get("reviewCount")

        if rating is not None and review_count is not None:
            rating = round(float(rating) / 2, 1)  # 轉成 5 分制
            result_rows.append((accomo_id, rating, int(review_count)))

    except TimeoutException:
        continue
    except Exception:
        continue

driver.quit()

# -------- 輸出 CSV --------
result_df = pd.DataFrame(result_rows, columns=["accomo_id", "rate", "comm"])
today_str = date.today().strftime("%Y%m%d")

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/hotel")
else:
    data_dir = Path("data/hotel")

output_path = data_dir / f"booking_update_{today_str}.csv"
result_df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"今日成功更新 {len(result_df)} 筆資料，已儲存至：{output_path}")