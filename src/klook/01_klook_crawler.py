import math
import random
import time
import pandas as pd
import numpy as np
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def e_request_list():
    page_num = 1
    return_data = []
    
    while True:
        print(f"========== 列表第{page_num}頁，開始爬蟲 ===========")
        # region Request 設定
        url = f"https://www.klook.com/v1/enteventapisrv/public/content/query_v3?k_lang=zh_TW&k_currency=TWD&area=coureg_1014&page_size=23&page_num={page_num}&filters=&sort=latest&date=date_all&start_date=&end_date="
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "cookie": "kepler_id=ffeaf708-edc4-4634-8b02-df4c581f0794; referring_domain_channel=seo; persisted_source=www.google.com; k_tff_ch=google_seo; _yjsu_yjad=1744425240.c3f25a7c-0516-4fab-bd8d-9e065157146f; _fwb=2053UFY75hvUDalBF4jt6Ky.1744425240618; dable_uid=89885905.1682426400506; __lt__cid=ba5c30c9-994c-4edc-94ef-72d088456631; __lt__cid.c83939be=ba5c30c9-994c-4edc-94ef-72d088456631; KOUNT_SESSION_ID=CFD8BEF74F1322C8D130EA3664BCAC53; _tt_enable_cookie=1; _ttp=01JRKXHJ9T4RFMZ0PC0FBSDXHS_.tt.1; _gcl_au=1.1.1723942387.1744425241; clientside-cookie=39b33b8cc8f8d49386ffa6497ee0ec77418c8635e47cbbb486d9cc8627c9619d5f04711893037c6401b2cc5c48abf258dbaa3dd2d1acb2bc3565e5ac96d08859a025a04183e463baa2d45274abcbcc00ea5eb6db5c3166d851b17d132afc3ec65d8d83e50fc25eb457729c06b866df1e2a7e6ae7773decaf2e74c43225ca1fd43c13042566c9483f7f99f42a54d0544912a31c3d5b5d0c8a4d8ee9; klk_ps=1; klk_currency=TWD; klk_rdc=TW; _gid=GA1.2.1444590508.1745827309; tr_update_tt=1745897910815; campaign_tag=klc_l1%3DSEO; _cfuvid=W3NpTfMK_xW02rbuCSFwtJTO8FU_wbK6xaEbbsEb4O0-1745897973390-0.0.1.1-604800000; traffic_retain=true; locale=en-us; __lt__sid=2854999c-03637665; __lt__sid.c83939be=2854999c-03637665; wcs_bt=s_2cb388a4aa34:1745901369; _uetsid=09075310240711f099710367c4ab5acc; _uetvid=caa671f0301611eead501958c5e163ed; klk_ga_sn=2536696353..1745901369207; ttcsid=1745901369320::KJ6JEvAgmrcdg7H8C77c.20.1745901369320; ttcsid_C1SIFQUHLSU5AAHCT7H0=1745901369319::gDEnwB5f1ZIID5Xk6K9c.20.1745901369641; _ga=GA1.1.867332311.1690858494; _ga_FW3CMDM313=GS1.1.1745897794.27.1.1745901369.0.0.0; _ga_HSY7KJ18X2=GS1.1.1745897794.26.1.1745901369.0.0.0; _ga_V8S4KC8ZXR=GS1.1.1745897795.20.1.1745901370.0.0.311159962; forterToken=414392c4395d4851b638e8730dbf5996_1745901369305__UDF43-m4_21ck_; klk_i_sn=9719981315..1745902443233; datadome=pHBTdhgkgoHIcIxWo6Tyr34VUKFTujGO1VhTy9SYsrHcQbymroFXrTqJeNRTmKliJgcTnHhtoc50FMdf12jGhX6hgW4iBuWHvscJxQcOvVIKYm34vLJ7Xe6pqoFjQ5Xm",
            "currency": "TWD",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "priority": "u=0, i",
            "sec-ch-device-memory": "8",
            # "sec-ch-ua": "Google Chrome;v='135', Not-A.Brand;v="8", "Chromium";v="135",
            "sec-ch-ua-arch": "x86",
            # "sec-ch-ua-full-version-list": "Google Chrome";v="135.0.7049.115", "Not-A.Brand";v="8.0.0.0", "Chromium";v="135.0.7049.115",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0"
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
                "address": "",
                "location": "",
            }
        )
    
    return return_data

def e_request_detail(url: str, error_count:int = 0):
    service = Service(executable_path="./chromedriver.exe")
    options = webdriver.ChromeOptions()

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(1)
    try:
        try:
            driver.get(url) # 更改網址以前往不同網頁
        except TimeoutException as e:
            pass
        except Exception as e:
            pass

        try:
            page_source = driver.page_source
        except Exception as e:
            time.sleep(3)
            error_count += 1
            if error_count > 5:
                return np.NaN, np.NaN
            return e_request_detail(url, error_count)
            
        location_xpath_list = [
            {
                "location_xpath": "//*[@id='top']/div/div/div[2]/div[2]/div/div[1]/span",
                "address_xpath": "//*[@id='top']/div/div/div[2]/div[2]/div/div[2]",
            },
            {
                "location_xpath": "//*[@id='score_participants']/div[1]/div[2]/div/div/div/div",
            },
            {
                "mixed_xpath": "//*[@id='about-event-markdown']/ul[4]/li[3]",
            }
            
        ]
        
        for location_xpath_item in location_xpath_list:
            try:
                if "location_xpath" in location_xpath_item and "address_xpath" in location_xpath_item:
                    location_div = driver.find_element(by=By.XPATH, value=location_xpath_item["location_xpath"])
                    address_div = driver.find_element(by=By.XPATH, value=location_xpath_item["address_xpath"])
                    location = location_div.text.split(":")[1]
                    address = address_div.text
                elif "location_xpath" in location_xpath_item and "address_xpath" not in location_xpath_item:
                    location_div = driver.find_element(by=By.XPATH, value=location_xpath_item["location_xpath"]) 
                    location = location_div.text
                    address = ""
                elif "mixed_xpath" in location_xpath_item and "address_xpath" not in location_xpath_item:

                    more_button = driver.find_element(by=By.CLASS_NAME, value="fold-button")
                    more_button.click()
                    wait = WebDriverWait(driver, 3)  # 最多等待 n 秒
                    
                    # 等待某個元素出現
                    element = wait.until(EC.presence_of_element_located((By.XPATH, location_xpath_item["mixed_xpath"])))                    
                    
                    # 活動地點：圓山花博流行館（台北市中山區玉門街1號）
                    target_div = driver.find_element(by=By.XPATH, value=location_xpath_item["mixed_xpath"])
                    mixed_location_address = target_div.text.split("：")[1]
                    location = mixed_location_address.split("（")[0]
                    address = mixed_location_address.split("（")[1].strip("）")
                    
                    pass
                else:
                    location = ""
                    address = ""
                
                break
            except NoSuchElementException as e:
                continue
            
        return location, address
    except Exception as e:
        pass
    
    finally:
        driver.close() # 關閉瀏覽器視窗   

def e_update_address(df: pd.DataFrame):
    failed_count = 0
    while df[(df['address'].isnull()) & (df['location'].isnull())].shape[0] > 0:
        print(f"============== Failed Count: {failed_count} ==================")
        for hash, item in df.iterrows():

            url = item['event_url']
            
            if (item['address'] is np.nan or item["address"] == "") and (item["location"] is np.nan or item["location"] == ""):
                # print(url)
                try:
                    location, address = e_request_detail(url)
                    
                    df.at[hash, "address"] = address if address is not np.nan else ""
                    df.at[hash, "location"] = location if location is not np.nan else ""

                    # time.sleep(random.randint(3,10))
                    time.sleep(1)
                except Exception as e:
                    pass
        if failed_count > 3:
            break
        failed_count += 1
        
    return df  

def main():
    response_code, response_data = e_request_list()
    
    if response_code != 200:
        print(f"請求失敗，status code: {response_code}")
        return 
    
    parsed_data = e_parse_response_json(response_data)
    activity_df = e_update_address(pd.DataFrame(parsed_data))

    activity_df.to_csv("klook/01_klook_data.csv", encoding="utf-8-sig", index=False)
    # print(df)    
        


        
if __name__ == "__main__":
    print("=========== Program Start ===========")
    # main()
    
    # region 抓取detail address, location
    try:
        activity_df = e_update_address(pd.read_csv("klook/01_klook_data.csv", encoding="utf-8-sig"))
    finally:
        activity_df.to_csv("klook/01_klook_data.csv", encoding="utf-8-sig", index=False)
    # endregion 抓取detail address, location
    
    print("----------- Program End -----------")

