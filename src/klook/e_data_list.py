import datetime
import json
import math
import random
import re
import time
import pandas as pd
import numpy as np
import requests
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/klook")
else:
    data_dir = Path("data/klook")
    
# region e_request_list
def e_request_list():
    page_num = 1
    return_data = []
    
    while True:
        print(f"========== 列表第{page_num}頁，開始爬蟲 ===========")
        # region Request 設定
        
        url = f"https://www.klook.com/v1/enteventapisrv/public/content/query_v3?k_lang=zh_TW&k_currency=TWD&area=coureg_1014&page_size=23&page_num={page_num}&filters=&sort=latest&date=date_all&start_date=&end_date="
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",  
            "cache-control": "max-age=0",          
            "cookie": "kepler_id=ffeaf708-edc4-4634-8b02-df4c581f0794; referring_domain_channel=seo; persisted_source=www.google.com; k_tff_ch=google_seo; _yjsu_yjad=1744425240.c3f25a7c-0516-4fab-bd8d-9e065157146f; _fwb=2053UFY75hvUDalBF4jt6Ky.1744425240618; dable_uid=89885905.1682426400506; __lt__cid=ba5c30c9-994c-4edc-94ef-72d088456631; __lt__cid.c83939be=ba5c30c9-994c-4edc-94ef-72d088456631; KOUNT_SESSION_ID=CFD8BEF74F1322C8D130EA3664BCAC53; _tt_enable_cookie=1; _ttp=01JRKXHJ9T4RFMZ0PC0FBSDXHS_.tt.1; _gcl_au=1.1.1723942387.1744425241; clientside-cookie=39b33b8cc8f8d49386ffa6497ee0ec77418c8635e47cbbb486d9cc8627c9619d5f04711893037c6401b2cc5c48abf258dbaa3dd2d1acb2bc3565e5ac96d08859a025a04183e463baa2d45274abcbcc00ea5eb6db5c3166d851b17d132afc3ec65d8d83e50fc25eb457729c06b866df1e2a7e6ae7773decaf2e74c43225ca1fd43c13042566c9483f7f99f42a54d0544912a31c3d5b5d0c8a4d8ee9; klk_ps=1; klk_currency=TWD; _gid=GA1.2.1444590508.1745827309; locale=en-us; klk_rdc=TW; _rt=eyJhbGciOiJIUzI1NiJ9.eyJkdmlkIjoiZmZlYWY3IiwicmlkIjoiZXZ0MTAyMDAwNDU4IiwiZWZ0IjoxLjc0NTkyMjg2NjgxOUU5LCJwb3MiOjI0MDIsImV4cCI6MS43NDU5MjM0NjY4MTlFOX0.UIP1CLBMJQy_BEaYGwsAb3_CA0DYj-8QGU2En0JRFo0; tr_update_tt=1745941029119; campaign_tag=klc_l1%3DSEO; _cfuvid=Cwoanc6yqsI14BVLgeIMgeXNwbakc6gCeJfcv8c6O64-1745942046660-0.0.1.1-604800000; traffic_retain=true; _uetsid=09075310240711f099710367c4ab5acc; _uetvid=caa671f0301611eead501958c5e163ed; wcs_bt=s_2cb388a4aa34:1745942060; ttcsid=1745940723936::6KOfWkKXdLBwKRcMdmFd.25.1745942061062; ttcsid_C1SIFQUHLSU5AAHCT7H0=1745940723936::6_OUY6rOG8Q1nGvN6arF.25.1745942061310; _ga=GA1.1.867332311.1690858494; _ga_FW3CMDM313=GS1.1.1745940723.34.1.1745942060.0.0.0; _ga_HSY7KJ18X2=GS1.1.1745940723.33.1.1745942060.0.0.0; forterToken=414392c4395d4851b638e8730dbf5996_1745942060975__UDF43-m4_21ck_; _ga_V8S4KC8ZXR=GS1.1.1745940723.26.1.1745942145.0.0.1183378818;datadome=Qr_C2v5d1M9jcbNlihi8uJ21dFqE4s7lU7OII5jAt7mhYTSlBurDHwWlyZtMjEnCbM7f4iMl6Jo9HT4SZIXaJYEFUTBUD8nuwc9dTYQeqX6vvINT_d9V~iGPj_5P6lEX",
            "priority": "u=0, i",
            "sec-ch-device-memory": "8",
            # "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            "sec-ch-ua-arch": "x86",
            # "sec-ch-ua-full-version-list": '"Google Chrome";v="135.0.7049.115", "Not-A.Brand";v="8.0.0.0", "Chromium";v="135.0.7049.115"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        }
        # endregion Request 設定
        
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return response.status_code, return_data   
        
        response_data = response.json() 
        
        # 合併每個Request回傳的json
        return_data = return_data + response_data["result"]["data_list"]
        
        # region 判斷是否結束
        page_num = response_data["result"]["page_num"]
        page_size = response_data["result"]["page_size"]
        total = response_data["result"]["total"]        
        

        
        if math.floor(total/page_size) == page_num:
            if total%page_num == 0:
                break
        elif math.floor(total/page_size) < page_num:
            break
        # endregion 判斷是否結束
        
        page_num += 1
        wait_secode = random.randint(1,3)
        print(f" ------------ 睡眠{wait_secode}秒 ------------ ")
        time.sleep(random.randint(1,3))

    return 200, return_data

# endregion e_request_list

# region e_reuqest_list_by_selenium
def e_reuqest_list_by_selenium():

    page_num = 1
    return_data = []    


    while True:
        service = Service(executable_path="/usr/local/bin/chromedriver")
        options = webdriver.ChromeOptions()
        # options.add_argument('--disable-blink-features=AutomationControlled')
        # options.add_argument('--start-maximized')  
        # options.add_argument("--headless=new")
        # options.add_argument("--no-sandbox")
        # options.add_argument('--disable-dev-shm-usage')        
        # # options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")                
        # options.add_argument(f"user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0")         
        
        # Headless 模式（無介面、伺服器常用）
        options.add_argument("--headless=new")  # "new" 是新版 headless 模式（更穩定）
        options.add_argument("--no-sandbox")  # 避免 sandbox 問題
        options.add_argument("--disable-dev-shm-usage")  # 避免 /dev/shm 空間不足

        # 偽裝參數（防止 Selenium 被網站偵測）
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # 指定使用者代理（User-Agent）可以提升成功率
        options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )

        # 可選：模擬最大化視窗（有些網站會根據螢幕尺寸載入不同結構）
        options.add_argument("start-maximized")        
        
               
        driver = webdriver.Chrome(service=service, options=options)   
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
            "headers": {
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            }
        })         
        driver.set_page_load_timeout(1)        
        
        response_data = None
        response = None        
        print(f"========== Selenium 列表第{page_num}頁，開始爬蟲 ===========")

        url = f"https://www.klook.com/v1/enteventapisrv/public/content/query_v3?k_lang=zh_TW&k_currency=TWD&area=coureg_1014&page_size=23&page_num={page_num}&filters=&sort=latest&date=date_all&start_date=&end_date="

        try:
            print("====================url ================")
            print(url)
            driver.get(url) # 更改網址以前往不同網頁
            page_source = driver.page_source
            print("page_source ================== ")
            print(page_source)

        except Exception as e:
            print("===============111111111111111111")
            
        
        try:
            
            response = driver.find_element(by=By.TAG_NAME, value="pre").text
            
            # print(response)
            response_data = json.loads(response)
        except Exception:
            pass
        finally:
            driver.quit()

         
        
        # 合併每個Request回傳的json
        try:
            return_data = return_data + response_data["result"]["data_list"]
            
            # region 判斷是否結束
            page_num = response_data["result"]["page_num"]
            page_size = response_data["result"]["page_size"]
            total = response_data["result"]["total"]        

            if math.floor(total/page_size) == page_num:
                if total%page_num == 0:
                    break
            elif math.floor(total/page_size) < page_num:
                break
            # endregion 判斷是否結束
            
            page_num += 1
            wait_secode = random.randint(1,3)
            print(f" ------------ 睡眠{wait_secode}秒 ------------ ")
            time.sleep(random.randint(1,3))  
        except Exception as e:
            print("=========== Blocked by Target Host =============")
            return return_data   
    
    return return_data

# endregion e_reuqest_list_by_selenium

# region e_parse_response_json
def e_parse_response_json(source_dataset: list[object]):
    return_data = []
    for source_data in source_dataset:
        
        return_data.append(
            {
                "title": source_data["title"],
                "free": source_data["free"],
                "from_price": source_data["from_price"],
                "date": source_data["date_list"][0]["date"] if len(source_data["date_list"]) > 0 else None ,
                "image_url": source_data["image_url"],
                "event_url": source_data["event_url"],
                "event_id": source_data["event_id"],
                "address": "",
                "location": "",
                "lng": "",
                "lat": "", 
            }
        )
    
    return return_data

# endregion e_parse_response_json


def main():
    if not Path.exists(data_dir):
        Path.mkdir(data_dir)
            
    save_data_path = data_dir / "e_data_list.csv"    
    
    # region 取得列表頁資料
    response_code, response_data = e_request_list()
    if response_code != 200:
        print(f"請求失敗，status code: {response_code}")
        response_data = e_reuqest_list_by_selenium()
        
    try:
        parsed_data = e_parse_response_json(response_data)
        activity_df = pd.DataFrame(parsed_data)
    finally:
        activity_df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)
    # endregion 取得列表頁資料

if __name__ == "__main__":

    main()

