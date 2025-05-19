import json
import random
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils import web_open

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/accomo")
else:
    data_dir = Path("data/accomo")
tz = pytz.timezone("Asia/Taipei")


# 多數註解都是第一輪抓取，同時執行5隻selenium用的
# def accomo03_extract_booking(start, end,step):
#     if (file_path/f"accomo03_extract_booking_progress_part{step}.csv").exists():
#         print("發現進度檔案，從中斷處繼續")
#         data = pd.read_csv(file_path/f"accomo03_extract_booking_progress_part{step}.csv",
#                encoding="utf-8", engine="python")
def read_data():
    if (data_dir / "accomo03_extract_booking_progress.csv").exists():
        print("發現進度檔案，從中斷處繼續")
        data = pd.read_csv(
            data_dir / "accomo03_extract_booking_progress.csv",
            encoding="utf-8",
            engine="python",
        )
    else:
        print("沒有進度檔案，從頭開始")
        data_open = pd.read_csv(
            data_dir / "accomo02_added_rows_from_newdata_filtered.csv",
            encoding="utf-8",
            engine="python",
        )
        data = data_open[
            ["Id", "Name", "Region", "Town", "Add", "Px", "Py", "Class"]
        ].copy()
        data.columns = [
            "id_open",
            "name_open",
            "region_open",
            "town_open",
            "add_open",
            "lng_open",
            "lat_open",
            "class_open",
        ]
        new_cols = [
            "url",
            "name",
            "add",
            "region",
            "town",
            "lng",
            "lat",
            "rating",
            "user_rating_total",
            "type",
            "facilities",
            "img_url",
        ]

        for col in new_cols:
            data[col] = None
        # data = data.loc[start:end,:].reset_index(drop=True)
    data["rating"] = data["rating"].astype("float64")
    data["user_rating_total"] = data["user_rating_total"].astype("Int64")
    return data


def get_start_idx(data):
    start_idx = data[data["url"].isna()].index.min()
    if pd.isna(start_idx):
        print("此階段已完成")
        return -1
    return start_idx


def process_batch(data, start_idx, batch_size):
    driver, wait, profile = web_open()
    url_list = []
    name_list = []
    add_list = []
    region_list = []
    town_list = []
    lng_list = []
    lat_list = []
    rating_list = []
    user_rating_total_list = []
    type_list = []
    facilities_list = []
    img_url_list = []
    err_log = ""

    for i in range(start_idx, min(start_idx + batch_size, len(data))):
        row = data.loc[i]
        result = get_single_booking_info(row, driver, wait)
        if not result["error"]:
            print(f"第{i+1}筆完成")
        else:
            err_msg = f"{datetime.now(tz)}第{i+1}筆 {data.loc[i,"name_open"]} 出現錯誤"
            print(err_msg)
            err_log += err_msg + "\n"

        name_list.append(result["name"])
        url_list.append(result["url"])
        add_list.append(result["add"])
        region_list.append(result["region"])
        town_list.append(result["town"])
        lng_list.append(result["lng"])
        lat_list.append(result["lat"])
        rating_list.append(result["rating"])
        user_rating_total_list.append(result["user_ratings_total"])
        type_list.append(result["type_"])
        facilities_list.append(result["facilities"])
        img_url_list.append(result["img_url"])

    rating_list = [float(r) if r not in [None, ""] else None for r in rating_list]
    lng_list = [float(g) if g not in [None, ""] else None for g in lng_list]
    lat_list = [float(t) if t not in [None, ""] else None for t in lat_list]
    user_rating_total_list = [
        int(u) if str(u).isdigit() else None for u in user_rating_total_list
    ]

    end_idx = start_idx + len(name_list)
    data.loc[start_idx : end_idx - 1, "name"] = name_list
    data.loc[start_idx : end_idx - 1, "url"] = url_list
    data.loc[start_idx : end_idx - 1, "add"] = add_list
    data.loc[start_idx : end_idx - 1, "region"] = region_list
    data.loc[start_idx : end_idx - 1, "town"] = town_list
    data.loc[start_idx : end_idx - 1, "lng"] = lng_list
    data.loc[start_idx : end_idx - 1, "lat"] = lat_list
    data.loc[start_idx : end_idx - 1, "rating"] = rating_list
    data.loc[start_idx : end_idx - 1, "user_rating_total"] = user_rating_total_list
    data.loc[start_idx : end_idx - 1, "type"] = type_list
    data.loc[start_idx : end_idx - 1, "facilities"] = facilities_list
    data.loc[start_idx : end_idx - 1, "img_url"] = img_url_list
    save_data(data, err_log)
    print(f"第{i+1}筆儲存完成")
    driver.quit()
    shutil.rmtree(profile)
    time.sleep(random.uniform(4, 6))
    return data


def get_single_booking_info(row, driver, wait):
    try:
        name_open = row["name_open"]
        region_open = row["region_open"]
        town_open = row["town_open"]
        driver.get(
            f"https://www.booking.com/searchresults.zh-tw.html?ss={region_open}+{town_open}+{name_open}"
        )
        time.sleep(random.uniform(0.5, 1.5))
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[data-testid='property-card-container']")
            )
        )
        div = soup.select_one("div[data-testid='property-card-container']")
        a = div.select("a")[1]
        url = a["href"]
        name = (
            a.select_one("div[data-testid='title']").text.strip()
            if a.select_one("div[data-testid='title']")
            else ""
        )
        driver.get(url)
        time.sleep(random.uniform(0.5, 1.5))
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".page-section.js-k2-hp--block.k2-hp--rt")
            )
        )
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        ld_json = soup.find("script", type="application/ld+json")
        if ld_json:
            ld_json = json.loads(ld_json.string)
        add = ld_json.get("address", {}).get("streetAddress").strip()
        region_town = soup.select(".bui-link.bui-link--primary.bui_breadcrumb__link")
        region = region_town[-2].text.strip()
        town = region_town[-1].text.strip()
        lng_lat = soup.select_one("a[data-atlas-latlng]")["data-atlas-latlng"].split(
            ","
        )
        lng = f"{round(float(lng_lat[1]), 5):.5f}"
        lat = f"{round(float(lng_lat[0]), 5):.5f}"
        rating = ld_json.get("aggregateRating", {}).get("ratingValue")
        user_ratings_total = ld_json.get("aggregateRating", {}).get("reviewCount")
        room_type_tag = soup.select_one(".page-section.js-k2-hp--block.k2-hp--rt")
        room_type = room_type_tag.text if room_type_tag else None
        if row["class_open"] == 4:
            type_ = "民宿"
        elif "宿舍" in room_type:
            type_ = "青旅"
        else:
            type_ = "飯店"
        facilities_tag = soup.select_one(".hp--popular_facilities.js-k2-hp--block")
        facilities = facilities_tag.text.strip() if facilities_tag else None
        img_url = ld_json.get("image")
        return {
            "name": name,
            "url": url,
            "add": add,
            "region": region,
            "town": town,
            "lng": lng,
            "lat": lat,
            "rating": rating,
            "user_ratings_total": user_ratings_total,
            "type_": type_,
            "facilities": facilities,
            "img_url": img_url,
            "error": False,
        }
    except Exception as err:
        time.sleep(random.uniform(3, 5))
        return {
            "name": None,
            "url": None,
            "add": None,
            "region": None,
            "town": None,
            "lng": None,
            "lat": None,
            "rating": None,
            "user_ratings_total": None,
            "type_": None,
            "facilities": None,
            "img_url": None,
            "error": err,
        }


def save_data(data, err_log):
    data.to_csv(
        data_dir / "accomo03_extract_booking.csv",
        encoding="utf-8",
        header=True,
        index=False,
    )
    data.to_csv(
        data_dir / "accomo03_extract_booking_progress.csv",
        encoding="utf-8",
        header=True,
        index=False,
    )
    with open(data_dir / "accomo03_extract_booking.txt", "a", encoding="utf-8") as f:
        f.write(err_log)


def main():
    data = read_data()
    if data.empty:
        return
    start_idx = get_start_idx(data)
    if start_idx == -1:
        return
    batch_size = 200
    for i in range(start_idx, len(data), batch_size):
        data = process_batch(data, i, batch_size)

    if (data_dir / "accomo03_extract_booking_progress.csv").exists():
        (data_dir / "accomo03_extract_booking_progress.csv").unlink()
        print("已完成全部資料，進度檔案已刪除")


if __name__ == "__main__":
    main()

    # 同時執行5隻selenium
    # from multiprocessing import Process

    # ranges = [(0, 2500), (2501, 5000), (5001, 7500), (7501, 10000), (10001, 12521)]
    # processes = []
    # for i, (start, end) in enumerate(ranges):
    #     p = Process(target=accomo03_extract_booking, args=(start, end, i))
    #     p.start()
    #     processes.append(p)

    # for p in processes:
    #     p.join()

    # data1 = pd.read_csv(file_path/"accomo03_extract_booking_part0.csv",encoding="utf-8",engine="python")
    # for i in range(1,5):
    #     data2 = pd.read_csv(file_path/"accomo03_extract_booking_part{i}.csv",encoding="utf-8",engine="python")
    #     data1 = pd.concat([data1,data2],ignore_index=True)
    # data1.to_csv(file_path/"accomo03_extract_booking.csv",encoding="utf-8",header=True,index=False)
