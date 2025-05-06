from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import pandas as pd
import time
import re

data_name = {
    "list": "./data/accupass/e_01_accupass_crawler_list.csv",
    "address": "./data/accupass/e_02_accupass_crawler_address.csv",
}

def save_to_csv(data, key):
    filename = data_name.get(key)
    data.to_csv(filename, index=False, encoding="utf-8")


def read_from_csv(key):
    filename = data_name.get(key)
    df = pd.read_csv(filename, encoding="utf-8")
    return df

def scroll_to_bottom(driver, pause_time=5, max_wait_time=300):
    """持續滾動頁面直到活動卡片數量不再增加，或超過最大等待時間。"""

    start_time = time.time()
    last_count = 0
    retries = 0

    while True:
        # 滾動到底部
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause_time)

        # 取得目前活動卡片數量
        cards = driver.find_elements(By.CSS_SELECTOR, ".Events-c1fc08e1-event-card")
        current_count = len(cards)
        print(f"目前已載入活動數量：{current_count}")

        # 如果沒有新增卡片數，就加一次重試次數
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

    # 等待活動卡片載入
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "#content > div > div.SearchPage-d3ff7972-container > main > section > div.Grid-e7cd5bad-container > div:nth-child(2) > div"
             )))

    # 滾到底部直到不再載入新資料
    scroll_to_bottom(driver)

    # 抓取所有活動卡片
    accupass_cards = driver.find_elements(
        By.XPATH, '//div[contains(@class, "EventCard-e27ded2d-home-event-card")]'
    )

    accupass_data = []

    for card in accupass_cards:
        try:

            # 名稱
            name_elem = card.find_element(
                By.XPATH, './/div[contains(@class, "EventCard-f0d917f9-event-content")]//p[contains(@class, "EventCard-de38a23c-event-name")]',
            )
            event_name = name_elem.text

            if not event_name:
                continue

            # 時間（日期）
            time_elem = card.find_element(
                By.XPATH, './/div[contains(@class, "EventCard-f0d917f9-event-content")]//p[contains(@class, "EventCard-c051398a-event-time")]',
            )
            event_time = time_elem.text

            # 縣市
            region_elem = card.find_element(
                By.XPATH, './/div[contains(@class, "EventCard-a800ada2-sub-info-container")]//span',
            )
            event_region = region_elem.text

            # 標籤
            tag_elem = card.find_element(
                By.XPATH, './/div[contains(@class, "TagStatsBottom-c31d7527-tags-container")]//a',
            )
            event_tag = tag_elem.text

            # 圖片連結
            img_elem = card.find_element(
                By.XPATH, './/div[contains(@class, "EventCard-c48c2d9c-event-photo")]//img',
            )
            event_img = img_elem.get_attribute("src")

            # 連結
            link_elem = card.find_element(
                By.XPATH, './/a[starts-with(@href, "/event/")]'
            )
            event_href = link_elem.get_attribute("href")

            accupass_data.append(
                {
                    "Acivity_name": event_name,
                    "Acivity_time": event_time,
                    "Region": event_region,
                    "Tag": event_tag,
                    "Picture_url": event_img,
                    "Url": event_href,
                })

        except Exception as e:
            print(f"沒有活動：{e}")
            accupass_data.append(
                {
                    "Acivity_name": None,
                    "Acivity_time": None,
                    "Region": None,
                    "Tag": None,
                    "Picture_url": None,
                    "Url": None,
                })

    df = pd.DataFrame(accupass_data)
    df = df[df["Acivity_name"].notna() & (df["Acivity_name"].str.strip() != "")]
    df = df.reset_index(drop=True)
    save_to_csv(df, "list")
    print("Accupass第1輪列表爬蟲完成!")


def accupass_crawler_address(driver):
    """爬取accupass第2層的資料，每個活動網頁點擊進去"""

    df = read_from_csv("list")
    print(df["Url"])

    google_maps_url_list = []
    address_list = []

    for num in range(len(df)):
        url = df["Url"].iloc[num]
        driver.get(url)

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((
                    By.XPATH,"//div[contains(@class, 'EventDetail-module-f47e87db-event-subtitle-content')]")
                )
            )
            # Google_map連結
            event_google_maps_url_elm = driver.find_element(
                By.XPATH, "//div[contains(@class, 'EventDetail-module-f47e87db-event-subtitle-content')]"
            )
            event_google_maps_url = event_google_maps_url_elm.get_attribute("href")

            # 地址
            event_address_elm = driver.find_element(
                By.XPATH,"//div[contains(@class, 'EventDetail-module-d6b190fd-event-external-link')]"
                )
            event_address = event_address_elm.text

        except Exception as e:
            event_google_maps_url = None
            event_address = None

        google_maps_url_list.append(event_google_maps_url)
        address_list.append(event_address)
        print(f"第2輪進行中：{num + 1}/{len(df)}")

    region_index = df.columns.get_loc("Region")
    df.insert(loc=region_index + 1, column="Address", value=address_list)
    address_index = df.columns.get_loc("Address")
    df.insert(
        loc=address_index + 1, column="Google_Maps_url", value=google_maps_url_list
    )
    df["Address_fail"] = df["Address"].apply(
        lambda x: bool(re.search(r"(縣|市)", x) and re.search(r"區", x))
    )

    save_to_csv(df, "address")
    print("Accupass第2輪地址爬蟲完成!")