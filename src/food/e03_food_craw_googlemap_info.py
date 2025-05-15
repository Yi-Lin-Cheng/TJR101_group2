import random
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytz
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils import web_open

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/food")
else:
    data_dir = Path("data/food")
new_file = data_dir / "restaurant04_compare_name_and_add_new.csv"
update_target_file = data_dir / "food05_extract_googlemap.csv"
progress_file = data_dir / "food05_extract_googlemap_progress.csv"
err_log_file = data_dir / "food05_extract_googlemap_err_log.txt"
tz = pytz.timezone("Asia/Taipei")
now = datetime.now(tz).replace(microsecond=0)


def read_data():
    """
    Load and prepare data for processing.

    If a progress file exists, resume from it (note: it will be deleted after a complete run).
    If not, load the new data, add required columns, then load the old data,
    merge them into a single DataFrame, and return it.

    Returns:
        pd.DataFrame: A combined DataFrame ready for processing.
    """
    if progress_file.exists():
        print("發現進度檔案，從中斷處繼續")
        data = pd.read_csv(
            progress_file,
            encoding="utf-8",
            engine="python",
        )
    else:
        data = pd.read_csv(
            new_file,
            encoding="utf-8",
            engine="python",
        )
        data["b_hours"] = ""
        data["pic_url"] = ""
        data["create_time"] = now
        data["update_time"] = pd.NaT
        # 按欄位順序排列
        data = data[
            [
                "food_id",
                "f_name",
                "b_hours",
                "county",
                "address",
                "rate",
                "geo_loc",
                "pic_url",
                "gmaps_url",
                "f_type",
                "comm",
                "area",
                "create_time",
                "update_time",
            ]
        ]

        if update_target_file.exists():
            data1 = pd.read_csv(
                update_target_file,
                encoding="utf-8",
                engine="python",
            )
            data = pd.concat([data1, data], ignore_index=True)
            data = data.drop_duplicates(subset=["gmaps_url"], keep="first")
    data["rate"] = data["rate"].astype("float64")
    data["comm"] = data["comm"].astype("Int64")
    data["update_time"] = pd.to_datetime(data["update_time"], errors="coerce")
    if data["update_time"].dt.tz is None:
        data["update_time"] = data["update_time"].dt.tz_localize("Asia/Taipei")
    return data


def get_start_idx(data, threshold_hours: int = 20) -> int:
    """
    Return the index of the first row that requires an update based on 'update_time'.

    Args:
        data (pd.DataFrame): A DataFrame with an 'update_time' column.
        threshold_hours (int): Number of hours after which a record is considered outdated.

    Returns:
        int: The index of the first outdated or missing record, or -1 if all are up to date.
    """
    data["update_time"] = pd.to_datetime(data["update_time"], errors="coerce")
    if data["update_time"].notna().any():
        condition1 = data["update_time"] <= datetime.now(tz) - timedelta(
            hours=threshold_hours
        )
        condition2 = data["update_time"].isna()
        start_idx = data[condition1 | condition2].index.min()
        if pd.isna(start_idx):
            print("此階段已完成")
            return -1
        return start_idx
    else:
        return 0


def process_batch(data, start_idx, batch_size):
    driver, wait, profile = web_open()
    b_hours_list = []
    rate_list = []
    pic_url_list = []
    comm_list = []
    update_time_list = []
    err_log = ""
    for i in range(start_idx, min(start_idx + batch_size, len(data))):
        url = data.at[i, "gmaps_url"]
        result = get_google_info(url, driver, wait)
        if not result["error"]:
            print(f"第{i+1}筆完成")
            update_time = now
        else:
            err_msg = f"{datetime.now(tz)}第{i+1}筆 {url} 出現錯誤{result["error"]}"
            print(err_msg)
            err_log += err_msg + "\n"
            update_time = data.at[i, "update_time"] or now
        b_hours_list.append(result["b_hours"])
        rate_list.append(result["rate"])
        pic_url_list.append(result["pic_url"])
        comm_list.append(result["comm"])
        update_time_list.append(update_time)

    rate_list = [float(r) if r not in [None, ""] else None for r in rate_list]
    comm_list = [int(c) if str(c).isdigit() else None for c in comm_list]
    end_idx = start_idx + len(b_hours_list)
    data.loc[start_idx : end_idx - 1, "b_hours"] = b_hours_list
    data.loc[start_idx : end_idx - 1, "rate"] = rate_list
    data.loc[start_idx : end_idx - 1, "pic_url"] = pic_url_list
    data.loc[start_idx : end_idx - 1, "comm"] = comm_list
    data.loc[start_idx : end_idx - 1, "update_time"] = update_time_list
    save_data(data, err_log)
    print(f"第{i+1}筆儲存完成")
    driver.quit()
    shutil.rmtree(profile)
    time.sleep(random.uniform(3, 5))
    return data


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
        time.sleep(random.uniform(1, 2))
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
        time.sleep(random.uniform(3, 5))
        return {"b_hours": "", "rate": None, "pic_url": "", "comm": None, "error": err}


def save_data(data, err_log):
    data.to_csv(
        update_target_file,
        encoding="utf-8",
        header=True,
        index=False,
    )
    data.to_csv(
        progress_file,
        encoding="utf-8",
        header=True,
        index=False,
    )
    with open(
        err_log_file,
        "a",
        encoding="utf-8",
    ) as f:
        f.write(err_log)


def main():
    data = read_data()
    start_idx = get_start_idx(data)
    if start_idx == -1:
        return
    # 批次寫入dataframe並存檔的大小
    batch_size = 200
    for i in range(start_idx, len(data), batch_size):
        data = process_batch(data, i, batch_size)

    if progress_file.exists():
        progress_file.unlink()
    print("已完成全部資料，進度檔案已刪除")


if __name__ == "__main__":
    main()
