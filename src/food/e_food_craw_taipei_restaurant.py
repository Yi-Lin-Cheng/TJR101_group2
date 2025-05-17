import random
import re
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path

import googlemaps
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils import web_open

now = datetime.now().replace(microsecond=0)
# Google Maps API 金鑰
API_KEY = ""
gmaps = googlemaps.Client(key=API_KEY)
data_dir = Path("data", "restaurant")
file_name = data_dir / "taipei_restaurant.csv"


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
            err_msg = f"{datetime.now()}第{i+1}筆 {url} 出現錯誤{result["error"]}"
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


def get_place_info(district, type_):
    query = f"{district} {type_}"

    # 1. 先搜尋地點
    search_result = gmaps.places(query=query, language="zh-TW")
    if not search_result["results"]:
        err_msg = f"{district} couldn't find the result.\n"
        return [], err_msg

    info_sublist = []
    for place in search_result["results"]:
        location = place.get("geometry", {}).get("location", {})
        info = {
            "county": "台北市",
            "area": district,
            "type": type_,
            "place_id": place.get("place_id", ""),
            "f_name": place.get("name", ""),
            "rate": place.get("rating", None),
            "comm": place.get("user_ratings_total", None),
            "address": place.get("formatted_address", ""),
            "lng": location.get("lng", ""),
            "lat": location.get("lat", ""),
        }
        info_sublist.append(info)

    return info_sublist, ""


def get_all_place():
    info_list = []
    err_log = ""
    district_set = [
        "中正區",
        "大同區",
        "中山區",
        "松山區",
        "大安區",
        "萬華區",
        "信義區",
        "士林區",
        "北投區",
        "內湖區",
        "南港區",
        "文山區",
    ]
    type_set = ["餐廳", "咖啡廳"]

    for district in district_set:
        for type_ in type_set:
            info, err_msg = get_place_info(district, type_)
            info_list += info
            err_log += err_msg
    data = pd.DataFrame(info_list)

    data.to_csv(
        f"Taipei_restaurant_cafe.csv", encoding="utf-8", header=True, index=False
    )

    return data


def add_column(data):
    data["b_hours"] = ""
    data["pic_url"] = ""
    data["create_time"] = now
    data["update_time"] = now
    data["food_id"] = [str(uuid.uuid4()) for _ in range(len(data))]
    data["gmaps_url"] = (
        "https://www.google.com/maps/place/?q=place_id:" + data["place_id"]
    )
    data["geo_loc"] = data.apply(
        lambda row: f"POINT({row['lat']:.5f} {row['lng']:.5f})", axis=1
    )
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
    return data


def main():
    data = get_all_place()
    data = add_column(data)
    data = process_batch(data, 0, len(data))

    data.to_csv(f"{file_name}", encoding="utf-8", header=True, index=False)


if __name__ == "__main__":
    main()
