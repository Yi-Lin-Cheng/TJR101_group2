from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import random
import shutil
import time
import re
import os


def get_unfinished_keywords(temp_folder="temp"):
    """暫存每一個城市的暫存檔"""
    temp_city = set()
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    for file in os.listdir(temp_folder):
        if file.startswith("temp_") and file.endswith(".csv"):
            city_keyword = file.replace("temp_", "").replace(".csv", "")
            temp_city.add(city_keyword)

    return temp_city


def merge_all_temp_csv(temp_folder="temp"):
    """合併暫存檔，並刪除所有 temp 檔案"""
    dfs = []
    for file in os.listdir(temp_folder):
        if file.startswith("temp_") and file.endswith(".csv"):
            df = pd.read_csv(os.path.join(temp_folder, file))
            dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv("e_01_coffee_crawler.csv", encoding="utf-8", index=False)

    # 合併完刪除暫存資料
    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)


def web_open(headless=False):
    """第一步、開啟瀏覽器"""
    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def google_search(driver, keyword):
    """第二步、google_map搜尋"""
    # 等待搜尋列表出現
    coffee_search = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input#searchboxinput"))
    )
    coffee_search.clear()
    coffee_search.send_keys(keyword)

    # 點擊搜尋按鈕
    coffee_search_button = WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button#searchbox-searchbutton"))
    )
    coffee_search_button.click()


def scroll_to_bottom(driver, pause_time=3, max_wait_time=300):
    """第三步、滾動google_map搜尋列表"""
    start_time = time.time()
    last_count = 0
    retries = 0

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div[role='feed']"))
    )

    # google map左邊面板
    leftside_panel = driver.find_element(
        By.CSS_SELECTOR, "div[role='feed']"
    )

    while True:
        # 滾動到底部
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight", leftside_panel)
        time.sleep(pause_time)

        coffees = driver.find_elements(
            By.CSS_SELECTOR, "div.Nv2PK.THOPZb.CpccDe"
        )

        current_count = len(coffees)
        print(f"目前已載入的數量：{current_count}")

        # 沒有增加數量，就加一次重試次數
        if current_count == last_count:
            retries += 1
            print(f"沒有增加數量，第 {retries} 次")
        else:
            retries = 0  # 新增就重置
            last_count = current_count

        # 超過兩次都沒有新增就停止
        if retries >= 2:
            print("總數量未增加，停止滾動")
            break

        # 超過最大等待時間停止滾動
        if time.time() - start_time > max_wait_time:
            print("超過最大等待時間，停止滾動")
            break


def get_google_map_data(driver):
    """第四步、抓取google_map上需要的資料"""
    coffee_list = []

    coffees = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
    print(f"找到 {len(coffees)} 筆咖啡店資料")

    for coffee in coffees:
        try:
            # 名字&連結
            coffee_name_url_elm = coffee.find_element(By.TAG_NAME, "a")
            coffee_name = coffee_name_url_elm.get_attribute("aria-label")
            coffee_url = coffee_name_url_elm.get_attribute("href")
            if not coffee_name:
                continue

            # 評分
            coffee_rate_elm = coffee.find_element(
                By.CSS_SELECTOR,
                "div.UaQhfb.fontBodyMedium > div:nth-child(3) > div > span.e4rVHe.fontBodyMedium > span > span.MW4etd"
            )
            coffee_rate = coffee_rate_elm.text

            # 評論數
            coffee_comm_elm = coffee.find_element(
                By.CSS_SELECTOR, "span.e4rVHe.fontBodyMedium > span > span.UY7F9")
            coffee_comm = coffee_comm_elm.text.strip("()")

            coffee_list.append(
                {
                    "f_name": coffee_name,
                    # "Address": coffee_address,
                    "rate": coffee_rate,
                    "comm": coffee_comm,
                    "url": coffee_url,
                }
            )
        except Exception as e:
            print(f"Error with coffee: {e}")
            coffee_list.append(
                {
                    "f_name": None,
                    # "Address": None,
                    "rate": None,
                    "comm": None,
                    "url": None,
                }
            )
            continue

    return coffee_list


def get_latlon_from_url(url):
    """第五步、抓取經緯度"""
    if "!3d" in url and "!4d" in url:
        match = re.search(r"!3d([\d.]+)!4d([\d.]+)", url)
        if match:
            return f"{match.group(1)},{match.group(2)}"
        else:
            print(f"URL 包含經緯度關鍵字，但無法解析：{url}")
            return None
    print(f"Google Maps 回傳 URL 不包含經緯度：{url}")
    return None


def get_latlon_from_search(keyword):
    """第六步、重新查詢經緯度"""
    try:
        driver = web_open(headless=True)
        driver.get("https://www.google.com.tw/maps/")
        google_search(driver, keyword)
        time.sleep(2)
        url = driver.current_url
        driver.quit()
        return get_latlon_from_url(url)
    except Exception as e:
        print(f"重新查詢經緯度失敗：{e}")
        return None


def coffee_other_data(driver, coffee_list):
    """第七步、進入每個咖啡廳裡面抓取資料"""
    for coffee in coffee_list:
        try:
            url = coffee["url"]
            driver.get(url)
            time.sleep(random.uniform(3, 5))

            coffee_address_elm = driver.find_element(
                By.CSS_SELECTOR, "div.Io6YTe.fontBodyMedium.kR99db.fdkmkc")
            coffee["address"] = coffee_address_elm.text

            coffee_business_time_elm = driver.find_element(
                By.CSS_SELECTOR, "div.t39EBf.GUrTXd")
            coffee["b_time"] = coffee_business_time_elm.get_attribute(
                "aria-label")

        except:
            coffee["address"] = None
            coffee["b_time"] = None

    return coffee_list


def get_search_keywords(category):
    """列出台灣縣市與指定類別的搜尋關鍵字"""
    taiwan_cities = [
        "基隆市", "台北市", "新北市", "桃園市", "新竹市", "新竹縣", "宜蘭縣",
        "苗栗縣", "台中市", "彰化縣", "南投縣", "雲林縣",
        "嘉義市", "嘉義縣", "台南市", "高雄市", "屏東縣",
        "花蓮縣", "台東縣",
    ]
    finished_cities = get_unfinished_keywords(temp_folder="temp")
    unfinished_cities = [
        city for city in taiwan_cities if city not in finished_cities]

    return [f"{city} {category}" for city in unfinished_cities]


def main():
    search_keywords = get_search_keywords("咖啡廳")
    url = "https://www.google.com.tw/maps/"

    for keyword in search_keywords:
        driver = web_open(headless=False)
        driver.get(url)
        google_search(driver, keyword)
        scroll_to_bottom(driver)
        coffee_list = get_google_map_data(driver)
        coffee_list = coffee_other_data(driver, coffee_list)

        driver.quit()

    df = pd.DataFrame(coffee_list)

    for num in range(len(df)):
        s_latlon = df.loc[num, "url"]
        latlon = get_latlon_from_url(s_latlon)

        if not latlon:
            print(f"原始 URL 無法獲得經緯度，第 {num+1} 筆資料重新搜尋：{s_latlon}")
            latlon = get_latlon_from_search(s_latlon)
        if not latlon:
            print(f"第 {num+1} 筆資料仍然無法獲得經緯度。")

        df.loc[num, "geo_loc"] = latlon

    # 暫存每個城市的資料
    temp_keyword = keyword.split()[0]
    df.to_csv(f"temp/temp_{temp_keyword}.csv", encoding="utf-8", index=False)

    merge_all_temp_csv()


if __name__ == "__main__":
    main()
