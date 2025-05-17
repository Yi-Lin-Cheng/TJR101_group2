import random
import re
import time
from pathlib import Path

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.web_open import web_open

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/accupass")
else:
    data_dir = Path("data/accupass")

data_name = {
    "first_step_e": data_dir / "e_01_accupass_crawler_list.csv",
    "second_step_e": data_dir / "e_02_accupass_crawler_address.csv",
    "third_step_e": data_dir / "e_03_accupass_latlon.csv",
}


def save_to_csv(data, key):
    filename = data_name.get(key)
    data.to_csv(filename, index=False, encoding="utf-8")


def read_from_csv(key):
    filename = data_name.get(key)
    df = pd.read_csv(filename, encoding="utf-8")
    return df


def scroll_to_bottom(driver, pause_time=5, max_wait_time=300):
    """爬活動：滾動頁面，搜尋全部活動"""

    start_time = time.time()
    last_count = 0
    retries = 0

    while True:
        # 滾動到底部
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause_time)

        # 取得活動數量
        cards = driver.find_elements(By.CSS_SELECTOR, ".Events-c1fc08e1-event-card")
        current_count = len(cards)
        print(f"目前已載入活動數量：{current_count}")

        # 重試確認有沒有新活動
        if current_count == last_count:
            retries += 1
            print(f"無新增卡片，第 {retries} 次")
        else:
            retries = 0  # 有新增就重置
            last_count = current_count

        # 超過兩次都沒有新增，就停止
        if retries >= 2:
            print("卡片數未增加，停止滾動")
            break

        # 超過最大等待時間
        if time.time() - start_time > max_wait_time:
            print("超過最大等待時間，停止滾動")
            break


def accupass_crawler_list(driver):
    """爬取accupass第1層資料，活動列表"""
    url = "https://www.accupass.com/search?c=arts,handmade,food,sports,pet,entertainment,film,technology,photography,game,music&pt=offline&s=relevance"
    driver.get(url)

    # 等待活動載入
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (
                By.CSS_SELECTOR,
                "#content > div > div.SearchPage-d3ff7972-container > main > section > div.Grid-e7cd5bad-container > div:nth-child(2) > div",
            )
        )
    )

    # 滾到底部直到不再載入新資料
    scroll_to_bottom(driver)

    accupass_cards = driver.find_elements(
        By.XPATH, '//div[contains(@class, "EventCard-e27ded2d-home-event-card")]'
    )

    accupass_data = []

    for card in accupass_cards:
        try:
            # 名稱
            name_elm = card.find_element(
                By.XPATH,
                './/div[contains(@class, "EventCard-f0d917f9-event-content")]//p[contains(@class, "EventCard-de38a23c-event-name")]',
            )
            e_name = name_elm.text
            if not e_name:
                continue

            # 時間（日期）
            time_elm = card.find_element(
                By.XPATH,
                './/div[contains(@class, "EventCard-f0d917f9-event-content")]//p[contains(@class, "EventCard-c051398a-event-time")]',
            )
            e_time = time_elm.text

            # 縣市
            county_elm = card.find_element(
                By.XPATH,
                './/div[contains(@class, "EventCard-a800ada2-sub-info-container")]//span',
            )
            e_county = county_elm.text

            # 標籤
            tag_elm = card.find_element(
                By.XPATH,
                './/div[contains(@class, "TagStatsBottom-c31d7527-tags-container")]//a',
            )
            e_tag = tag_elm.text

            # 圖片連結
            pic_elm = card.find_element(
                By.XPATH,
                './/div[contains(@class, "EventCard-c48c2d9c-event-photo")]//img',
            )
            e_pic_url = pic_elm.get_attribute("src")

            # 連結
            url_elm = card.find_element(By.XPATH, './/a[starts-with(@href, "/event/")]')
            accupass_url = url_elm.get_attribute("href")

            accupass_data.append(
                {
                    "e_name": e_name,
                    "e_time": e_time,
                    "county": e_county,
                    "tag": e_tag,
                    "pic_url": e_pic_url,
                    "accupass_url": accupass_url,
                }
            )

        except Exception as e:
            print(f"沒有活動：{e}")
            accupass_data.append(
                {
                    "e_name": None,
                    "e_time": None,
                    "county": None,
                    "tag": None,
                    "pic_url": None,
                    "accupass_url": None,
                }
            )

    df = pd.DataFrame(accupass_data)
    df = df[df["e_name"].notna() & (df["e_name"].str.strip() != "")]
    df = df.reset_index(drop=True)

    save_to_csv(df, "first_step_e")
    print("Accupass第1輪列表爬蟲完成!")


def accupass_crawler_address(driver):
    """爬取accupass第2層的資料，每個活動網頁點擊進去"""

    df = read_from_csv("first_step_e")
    print(df["accupass_url"])

    address_list = []

    for num in range(len(df)):
        url = df["accupass_url"].iloc[num]
        driver.get(url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[contains(@class, 'EventDetail-module-f47e87db-event-subtitle-content')]",
                )
            )
        )

        try:
            # 活動通地址
            e_address_elm = driver.find_element(
                By.XPATH,
                "//div[contains(@class, 'EventDetail-module-d6b190fd-event-external-link')]",
            )
            e_address = e_address_elm.text

        except Exception as e:
            e_address = None

        address_list.append(e_address)
        print(f"第2輪進行中：{num + 1}/{len(df)}")

    loc = df.columns.get_loc("county")
    df.insert(loc=loc + 1, column="address", value=address_list)

    save_to_csv(df, "second_step_e")
    print("Accupass第2輪地址爬蟲完成!")


def clean_for_search(df):
    """爬經緯度：暫時分離樓層"""
    df["floor"] = df["address"].str.extract(r"(\d+樓(?:之\d+)?)")
    df["clean_address"] = df["address"].str.replace(
        r"(\d+樓(?:之\d+)?)", "", regex=True
    )
    return df


def change_for_search(df):
    """修改地址"""
    df["clean_address"] = df["clean_address"].str.replace("台灣", "", regex=False)
    df["clean_address"] = df["clean_address"].str.replace(
        "Sherlock Board game store", "台北市大安區仁愛路四段345巷4弄24號", regex=False
    )
    df["clean_address"] = df["clean_address"].str.replace(
        "台北市DeRoot休閒空間", "台北市中正區新生南路一段60號B1", regex=False
    )
    return df


def google_latlon(driver):
    """爬取accupass經緯度"""
    df = read_from_csv("second_step_e")

    df = clean_for_search(df)
    df = change_for_search(df)
    address_to_latlon = df["clean_address"].dropna().unique()

    address_to_geo = {}

    url = "https://www.google.com.tw/maps/"

    for num, address in enumerate(address_to_latlon):
        print(f"第 {num + 1} 筆地址：{address}")

        try:
            driver.get(url)
            time.sleep(random.uniform(1, 3))

            # 搜尋框
            search_box = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input#searchboxinput")
                )
            )
            search_box.clear()
            search_box.send_keys(address)

            # 點擊搜尋按鈕
            search_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button#searchbox-searchbutton")
                )
            )
            search_button.click()

            # 等經緯度出現
            WebDriverWait(driver, 5).until(EC.url_contains("!3d"))
            WebDriverWait(driver, 5).until(EC.url_matches(r"!3d([-.\d]+)!4d([-.\d])+"))

            # 抓經緯度
            maps_url = driver.current_url
            if "!3d" and "!4d" in maps_url:
                match = re.search(r"!3d([-.\d]+)!4d([-.\d]+)", maps_url)
                geo_loc = f"{match.group(1)},{match.group(2)}"
                print(f"第 {num + 1} 筆經緯度：{geo_loc}")
            else:
                geo_loc = None

        except Exception as e:
            print(f"錯誤：{e}")
            geo_loc = None

        address_to_geo[address] = geo_loc

    driver.quit()

    df["geo_loc"] = df["clean_address"].map(address_to_geo)

    # 合併地址樓層
    df["address"] = df.apply(
        lambda add: (
            add["clean_address"] + add["floor"]
            if pd.notna(add["floor"])
            else add["clean_address"]
        ),
        axis=1,
    )

    save_to_csv(df, "third_step_e")
    print("Accupass第3輪經緯度爬蟲完成!")


def e_accupass_crawler():
    try:
        driver, wait, user_data_dir = web_open()
        accupass_crawler_list(driver)
        driver.quit()

        driver, wait, user_data_dir = web_open()
        accupass_crawler_address(driver)
        driver.quit()

        driver, wait, user_data_dir = web_open(headless=True)
        google_latlon(driver)
        driver.quit()

    except Exception as e:
        print("執行主流程時發生錯誤：", e)


if __name__ == "__main__":
    e_accupass_crawler()
