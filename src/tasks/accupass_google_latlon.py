import random
import time

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

data_name = {
    "address": "./data/accupass/02_accupass_crawler_address.csv",
    "latlon_url": "./data/accupass/03_accupass_latlon_url.csv",
    "latlon": "./data/accupass/04_accupass_latlon.csv"
}

def save_to_csv(data, key):
    filename = data_name.get(key)
    data.to_csv(filename, index=False, encoding="utf-8")


def read_from_csv(key):
    filename = data_name.get(key)
    df = pd.read_csv(filename, encoding="utf-8")
    return df


def google_latlon():
    df = read_from_csv("address")

    temporary_maps_url_list = []
    driver = web_open(headless=False)

    for num in range(len(df)):
        try:
            url = "https://www.google.com.tw/maps/"
            driver.get(url)
            time.sleep(random.uniform(3, 5))

            address = df["Address"].iloc[num]
            print(f"查詢第 {num + 1} 筆地址：{address}")

            # 等待搜尋框出現
            temporary_map_search = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input#searchboxinput"))
            )
            temporary_map_search.clear()
            temporary_map_search.send_keys(address)

            # 點擊搜尋按鈕
            temporary_search_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button#searchbox-searchbutton"))
            )
            temporary_search_button.click()

            # 等待地圖載入完成
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#QA0Szd"))
            )
            time.sleep(random.uniform(5, 8))  # 給地圖載入一點時間

            # 抓網址
            maps_url = driver.current_url
            temporary_maps_url_list.append(maps_url)

        except Exception as e:
            print(f"第 {num + 1} 筆查詢失敗：{e}")
            temporary_maps_url_list.append(None)

    df["Temporary_map_url"] = temporary_maps_url_list
    save_to_csv(df, "latlon_url")
    driver.quit()

    #解析經緯度
    latlon_list = []
    # lat_list = []
    # lon_list = []

    for i in range(len(df)):
        try:
            maps_url = df["Temporary_map_url"].iloc[i]
            if maps_url and "@" in maps_url:
                latlon = maps_url.split("@")[1].split(",")[:2]
                latlon = ",".join(latlon)
                # lat = latlon.split(",")[0].lstrip("@")
                # lon = latlon.split(",")[1]
            else:
                latlon = None
                print(f"第 {i + 1} 筆資料無法獲得經緯度，URL: {maps_url}")
        except Exception as e:
            latlon = None
            # lat = None
            # lon = None
            print(f"無法解析第 {i + 1} 筆資料的經緯度：{e}")

        latlon_list.append(latlon)
        # lat_list.append(lat)
        # lon_list.append(lon)

    # df["latitude"] = lat_list
    # df["longitude"] = lon_list

    address_index = df.columns.get_loc("Address")
    df.insert(loc=address_index + 1, column="Latlon",value=latlon_list)

    df = df.drop(columns=["Temporary_map_url"])
    save_to_csv(df, "latlon")
    print("經緯度執行完畢")
