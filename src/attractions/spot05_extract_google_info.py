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

file_path = Path("data", "spot")


def get_google_info(url: str, driver, wait):
    """
    Crawl business info from a Google Maps place page.

    Args:
        url (str): The URL of the Google Maps place.
        driver (webdriver.Chrome): Selenium WebDriver instance.
        wait (WebDriverWait): WebDriverWait instance for explicit wait.

    Returns:
        dict: A dictionary containing:
            - b_hours (str): Business hours text, if available.
            - rate (str or None): Star rating, if available.
            - pic_url (str): The URL of the main photo.
            - comm (str or None): Number of comments/reviews, cleaned.
            - error (bool or Exception): False if success, or the exception object if failed.
    """
    try:
        driver.get(url)
        time.sleep(random.uniform(2, 4))
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "img[decoding='async']"))
        )
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        b_hours_tags = soup.select(".y0skZc")
        b_hours = ""
        if b_hours_tags:
            for tag in b_hours_tags:
                weekday = tag.select_one(".ylH6lf").text
                b_hours += f"\n{weekday}"
                times = tag.select(".G8aQO")
                for time_ in times:
                    b_hours += f" {time_.text}"
        elif "永久歇業" in soup.text:
            b_hours = "永久歇業"
        pic_url = soup.select_one("img[decoding='async']")["src"]
        rate_comm_tag = soup.select_one(".F7nice")
        rate = None
        comm = None
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


def read_data():
    """
    Load and prepare data for processing.

    If a progress file exists, resume from it (note: it will be deleted after a complete run).
    If not, load the new data, add required columns, then load the old data,
    merge them into a single DataFrame, and return it.

    Returns:
        pd.DataFrame: A combined DataFrame ready for processing.
    """
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
                "s_type",
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


def save_data(data, err_log):
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
    data = read_data()
    data["update_time"] = pd.to_datetime(data["update_time"], errors="coerce")
    if data["update_time"].notna().any():
        condition1 = data["update_time"] <= datetime.now() - timedelta(hours=20)
        condition2 = data["update_time"].isna()
        start_idx = data[condition1 | condition2].index.min()
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
        result = get_google_info(url, driver, wait)
        if not result["error"]:
            print(f"第{i+1}筆完成")
            update_time = now
        else:
            err_msg = f"{datetime.now()}第{i+1}筆 {url} 出現錯誤"
            print(err_msg)
            err_log += err_msg + "\n"
            update_time = (
                data.at[i, "update_time"] if data.at[i, "update_time"] else now
            )
        b_hours_list.append(result["b_hours"])
        rate_list.append(result["rate"])
        pic_url_list.append(result["pic_url"])
        comm_list.append(result["comm"])
        update_time_list.append(update_time)
        rate_list = [float(r) if r not in [None, ""] else None for r in rate_list]
        comm_list = [int(c) if str(c).isdigit() else None for c in comm_list]
        if ((i + 1) % 50 == 0) or i + 1 == len(gmaps_url_list):
            end_idx = i + 1
            start_idx = end_idx - len(b_hours_list)
            data.loc[start_idx : end_idx - 1, "b_hours"] = b_hours_list
            data.loc[start_idx : end_idx - 1, "rate"] = rate_list
            data.loc[start_idx : end_idx - 1, "pic_url"] = pic_url_list
            data.loc[start_idx : end_idx - 1, "comm"] = comm_list
            data.loc[start_idx : end_idx - 1, "update_time"] = update_time_list
            save_data(data, err_log)
            print(f"第{i+1}筆儲存完成")
            time.sleep(random.uniform(4, 6))
            driver.quit()
            shutil.rmtree(profile)

    if (file_path / "spot05_extract_googlemap_progress.csv").exists():
        (file_path / "spot05_extract_googlemap_progress.csv").unlink()
    print("已完成全部資料，進度檔案已刪除")


if __name__ == "__main__":
    main()
