import random
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils import web_open


def spot05_extract_google_info(url, driver, wait):
    try:
        driver.get(url)
        time.sleep(random.uniform(2, 4))
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "img[decoding='async']"))
        )
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        b_hours_tag = soup.select_one(".eK4R0e.fontBodyMedium")
        b_hours = b_hours_tag.text if b_hours_tag else ""
        pic_url = soup.select_one("img[decoding='async']")["src"]
        rate_comm_tag = soup.select_one(".F7nice")
        if rate_comm_tag and rate_comm_tag.text != "":
            rate = rate_comm_tag.select_one("span[aria-hidden]").text
            comm = re.sub(
                r"\(|,|\)", "", rate_comm_tag.select("span[aria-label]")[1].text
            )
        return {
            "b_hours": b_hours,
            "rate": rate,
            "pic_url": pic_url,
            "comm": comm,
            "error": False,
        }
    except Exception as err:
        time.sleep(random.uniform(5, 10))
        return {"b_hours": "", "rate": None, "pic_url": "", "comm": None, "error": err}


def load_data(file_path):
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
        return data


def save_data(file_path, data, err_log):
    data.to_csv(
        file_path / "spot05_extract_googlemap.csv",
        encoding="utf-8",
        header=True,
        index=False,
    )
    data.to_csv(
        file_path / "spot05_extract_googlemap_progress.csv",
        encoding="utf-8",
        header=True,
        index=False,
    )
    with open(
        file_path / "spot05_extract_googlemap_err_log.txt",
        "a",
        encoding="utf-8",
    ) as f:
        f.write(err_log)


def main():
    file_path = Path("data", "spot")
    data = load_data(file_path)
    if data["update_time"].notna().any():
        condition = data["update_time"] <= datetime.today() - timedelta(hours=20)
        start_idx = data[condition].index.min()
        if pd.isna(start_idx):
            print("此階段已完成")
            return
    else:
        start_idx = 0
    now = datetime.now().replace(microsecond=0)
    data.loc[data["create_time"].isna(), "create_time"] = now
    gmaps_url_list = data["gmaps_url"].tolist()

    for i in range(start_idx, len(gmaps_url_list)):
        if i % 50 == 0:
            driver, wait, profile = web_open()
            if not driver:
                break
            b_hours_list = []
            rate_list = []
            pic_url_list = []
            comm_list = []
            update_time_list = []
            err_log = ""
        url = gmaps_url_list[i]
        result = spot05_extract_google_info(url, driver, wait)
        if not result["error"]:
            print(f"第{i+1}筆完成")
            update_time = now
        else:
            err_msg = f"{datetime.now()}第{i+1}筆 {url} 出現錯誤"
            print(err_msg)
            err_log += err_msg + "\n"
            update_time = (
                data.at[i, "update_time"] if data.at[i, "update_time"] else pd.NaT
            )
        b_hours_list.append(result["b_hours"])
        rate_list.append(result["rate"])
        pic_url_list.append(result["pic_url"])
        comm_list.append(result["comm"])
        update_time_list.append(update_time)
        if ((i + 1) % 50 == 0) or i + 1 == len(gmaps_url_list):
            end_idx = i + 1
            start_idx = end_idx - len(b_hours_list)
            data.loc[start_idx : end_idx - 1, "b_hours"] = b_hours_list
            data.loc[start_idx : end_idx - 1, "rate"] = rate_list
            data.loc[start_idx : end_idx - 1, "pic_url"] = pic_url_list
            data.loc[start_idx : end_idx - 1, "comm"] = comm_list
            data.loc[start_idx : end_idx - 1, "update_time"] = update_time_list
            save_data(file_path, data, err_log)
            print(f"第{i+1}筆儲存完成")
            time.sleep(random.uniform(4, 6))
            driver.quit()
            shutil.rmtree(profile)

    if (file_path / "spot05_extract_googlemap_progress.csv").exists():
        (file_path / "spot05_extract_googlemap_progress.csv").unlink()
    print("已完成全部資料，進度檔案已刪除")

if __name__ == "__main__":
    main()
