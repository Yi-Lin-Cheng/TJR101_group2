import pandas as pd
import random
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options


def spot05_extract_goolge_info():
    file_path = Path("data", "spot")
    if (file_path / "spot05_extract_googlemap_progress.csv").exists():
        print("發現進度檔案，從中斷處繼續")
        data = pd.read_csv(
            file_path / "spot05_extract_googlemap_progress.csv",
            encoding="utf-8",
            engine="python",
        )
    else:
        data = pd.read_csv(
            file_path / "spot04_compare_name_and_add_new.csv",
            encoding="utf-8",
            engine="python",
        )
        data["b_hours"] = ""
        data["rate"] = None
        data["pic_url"] = ""
        data["comm"] = None
        data["create_time"] = pd.NaT
        data["update_time"] = pd.NaT
        # 按欄位順序排列
        data = data[
            [
                "s_name",
                "b_hours",
                "county",
                "address",
                "rate",
                "geo_loc",
                "pic_url",
                "gmaps_url",
                "type",
                "comm",
                "area",
                "create_time",
                "update_time",
            ]
        ]

        if (file_path / "spot05_extract_googlemap.csv").exists():
            data1 = pd.read_csv(
                file_path / "spot05_extract_googlemap.csv",
                encoding="utf-8",
                engine="python",
            )
            data = pd.concat([data1, data], ignore_index=True)

    if data["update_time"].notna().any():
        condition = data["update_time"] <= datetime.today() - timedelta(hours=20)
        start_idx = data[condition].index.min()
        if pd.isna(start_idx):
            print("此階段已完成")
            return
    else:
        start_idx = 0
    data.loc[data["create_time"].isna(), "create_time"] = datetime.now()
    gmaps_url_list = data["gmaps_url"].tolist()
    err_log = ""

    for i in range(start_idx, len(gmaps_url_list)):
        if i % 200 == 0:
            b_hours_list = []
            rate_list = []
            pic_url_list = []
            comm_list = []
            update_time_list = []
            # service = Service(ChromeDriverManager().install())
            # options = webdriver.ChromeOptions()
            # driver = webdriver.Chrome(service=service,options=options)
            try:
                options = Options()
                options.add_argument("--headless")  # 無頭模式
                options.add_argument("--window-size=1920,1080")
                options.add_argument("--no-sandbox")  # 取消沙箱模式（容器內需加）
                options.add_argument("--disable-dev-shm-usage")  # 共享記憶體空間問題
                options.add_argument("--disable-gpu")  # 關閉 GPU（無 GUI 時可略過）
                options.add_argument("--lang=zh-TW")  # 設定語言為繁體中文
                # options.add_argument("--start-maximized") # 非 headless 模式才有效

                driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()), options=options
                )
                print("瀏覽器啟動!")
            except Exception as e:
                print(f"瀏覽器啟動失敗：{e}")
            wait = WebDriverWait(driver, 15)
        try:
            url = gmaps_url_list[i]
            driver.get(url)
            time.sleep(random.uniform(1, 2))
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "img[decoding='async']")
                )
            )
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            rate_comm_tag = soup.select_one(".F7nice")
            b_hours_tag = soup.select_one(".eK4R0e.fontBodyMedium")
            b_hours = b_hours_tag.text if b_hours_tag else ""
            rate = rate_comm_tag.select_one("span[aria-hidden]").text
            pic_url = soup.select_one("img[decoding='async']")["src"]
            comm = rate_comm_tag.select("span[aria-label]")[1].text
            update_time = datetime.now()
            b_hours_list.append(b_hours)
            rate_list.append(rate)
            pic_url_list.append(pic_url)
            comm_list.append(comm)
            update_time_list.append(update_time)
        except Exception as err:
            err_msg = f"第{i+1}筆資料{url}出現錯誤"
            err_log += err_msg + "\n"
            b_hours_list.append("")
            rate_list.append(None)
            pic_url_list.append("")
            comm_list.append(None)
            update_time_list.append(pd.NaT)
            time.sleep(random.uniform(5, 10))

        if ((i + 1) % 200 == 0) or i + 1 == len(gmaps_url_list):
            end_idx = i + 1
            start_idx = end_idx - len(b_hours_list)
            data.loc[start_idx : end_idx - 1, "b_hours"] = b_hours_list
            data.loc[start_idx : end_idx - 1, "rate"] = rate_list
            data.loc[start_idx : end_idx - 1, "pic_url"] = pic_url_list
            data.loc[start_idx : end_idx - 1, "comm"] = comm_list
            data.loc[start_idx : end_idx - 1, "update_time"] = update_time_list
            data.to_csv(
                file_path / "spot05_extract_googlemap.csv",
                encoding="utf-8",
                header=True,
                index=False,
            )
            data.to_csv(
                file_path / "spot05__extract_googlemap_progress.csv",
                encoding="utf-8",
                header=True,
                index=False,
            )
            with open(
                file_path / "spot05__extract_googlemap_err_log.txt",
                "a",
                encoding="utf-8",
            ) as f:
                f.write(err_log)
            print(f"第{i+1}筆儲存完成")
            time.sleep(random.uniform(2, 5))
            driver.quit()

    if (file_path / "spot05_extract_googlemap_progress.csv").exists():
        (file_path / "spot05_extract_googlemap_progress.csv").unlink()
    print("已完成全部資料，進度檔案已刪除")


spot05_extract_goolge_info()
