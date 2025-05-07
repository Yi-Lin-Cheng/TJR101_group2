import datetime
import json
import math
import random
import re
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
from dateutil import parser
import os

# region ============ utils ============

def parse_time(date_str: str) -> datetime.datetime:
    dt = None
    match = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        dt = datetime.datetime(year, month, day)
    else:
        match = re.match(r"(\d{1,2})月(\d{1,2})日", date_str)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            year = datetime.datetime.now().year  # 使用當前年份
            dt = datetime.datetime(year, month, day)

    return dt      
          


# endregion ============ utils ============

# region 清除 title 末端有包含 location 的資訊
def trim_location_from_title(row):
    title = str(row['title'])
    location = str(row['location'])
    if title.endswith(location):
        return title[: -len(location)].rstrip().rstrip('｜')
    
    return title    
# endregion 清除 title 末端有包含 location 的資訊

# region 清除 address 包含括號的資料
def trim_location_from_address(row):
    address = str(row['address'])
    if address and address != "nan":
        return address.split("（")[0]
    
    return row['address']   
# endregion 清除 address 包含括號的資料

# region 清除 address 內容為https url
def clear_url_as_arress(row):
    
    address = str(row['address'])
    if address.strip().startswith("http"):
        return ""  
    
    return row['address']
# endregion 清除 address 內容為https url

def parse_s_time(row):
    date = str(row['date'])
    s_time = date
    e_time = date
    
    if "-" in date:
        dates = date.split("-")
        s_time = dates[0].split("(")[0]
        e_time = dates[1].split("(")[0]
        
    else:
        s_time = date.split("(")[0]
        e_time = s_time
        
    return s_time

def parse_e_time(row):
    date = str(row['date'])
    s_time = date
    e_time = date
    
    if "-" in date:
        dates = date.split("-")
        s_time = dates[0].split("(")[0]
        e_time = dates[1].split("(")[0]
        
    else:
        s_time = date.split("(")[0]
        e_time = s_time
        
    return e_time     

def trim_s_time(row):
    date = str(row['s_time'])
    return date.strip()

def trim_e_time(row):
    date = str(row['e_time'])
    return date.strip()

def format_s_time(row):
    date = parse_time(row['s_time'])
    return date.strftime("%Y/%m/%d")

def format_e_time(row):
    date = parse_time(row['e_time'])
    return date.strftime("%Y/%m/%d")

# =============================================

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
        service = Service(executable_path="./chromedriver.exe")
        options = webdriver.ChromeOptions()

        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(1)        
        
        response_data = None
        response = None        
        print(f"========== Selenium 列表第{page_num}頁，開始爬蟲 ===========")

        url = f"https://www.klook.com/v1/enteventapisrv/public/content/query_v3?k_lang=zh_TW&k_currency=TWD&area=coureg_1014&page_size=23&page_num={page_num}&filters=&sort=latest&date=date_all&start_date=&end_date="

        try:
            driver.get(url) # 更改網址以前往不同網頁
            page_source = driver.page_source

        except Exception as e:
            pass
            
        
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
                "address": "",
                "location": "",
                "lng": "",
                "lat": "", 
            }
        )
    
    return return_data

# endregion e_parse_response_json

# region e_request_detail
def e_request_detail(url: str, error_count:int = 0):
    service = Service(executable_path="./chromedriver.exe")
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)    
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    # driver.set_page_load_timeout(3)
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
        
        # wait = WebDriverWait(driver, 3)
        # action = ActionChains(driver)     
        
        # location_div = wait.until(EC.visibility_of_any_elements_located([
        #     ((By.XPATH, "//*[@id='top']/div/div/div[2]/div[2]/div/div[1]/span")),
        #     ((By.XPATH, "//*[@id='top']/div/div/div[2]/div[2]/div/div[2]")),
        #     ((By.XPATH, "//*[@id='score_participants']/div[1]/div[2]/div/div/div/div")),
        #     ((By.XPATH, "//*[@id='activity_summary']/div/div/div/p[9]")),
        #     ((By.XPATH, '//*[@id="activity_summary"]/div[1]/div/div/ul/li[2]/text()')),
            
            
        #     ((By.XPATH, '//*[@id="policy"]/div/div/ul[2]/li[2]/text()')),
        #     ((By.XPATH, '//*[@id="policy"]/div/div/ul[2]/li[2]/text()')),
        #     ((By.XPATH, "//*[@id='top']/div/div/div[2]/div[2]/div/div[2]")),
        #     ((By.XPATH, "//*[@id='about-event-markdown']/ul[4]/li[3]")),
        # ]))
        # location = location_div.text
        address = ""
        location_xpath_list = [
            {
                "location_xpath": "//*[@id='top']/div/div/div[2]/div[2]/div/div[1]/span",
                "address_xpath": "//*[@id='top']/div/div/div[2]/div[2]/div/div[2]",
            },
            {
                "location_xpath": "//*[@id='score_participants']/div[1]/div[2]/div/div/div/div",
            },
            {
                "location_xpath": "//*[@id='activity_summary']/div/div/div/p[9]",
            },
            {
                "address_xpath": '//*[@id="activity_summary"]/div[1]/div/div/ul/li[2]/text()',
            },
            {
                "address_xpath": '//*[@id="policy"]/div/div/ul[2]/li[2]/text()',
            },            
            {
                "address_xpath": "//*[@id='top']/div/div/div[2]/div[2]/div/div[2]",
            },
            {
                "mixed_xpath": "//*[@id='about-event-markdown']/ul[4]/li[3]",
            },
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
            
            if location != "" or address != "":
                break
        return location, address
    except Exception as e:
        pass
    
    finally:
        driver.close() # 關閉瀏覽器視窗   
        # driver.quit()

# endregion 

# region t_title
def t_title(df: pd.DataFrame):
    df["county"] = None
    
    # # 去除並擷取最前面縣市資訊
    df['county'] = df['title'].astype(str).str.split('|').str[0]
    df["title"] = df['title'].astype(str).str.split('|').str[1:].str.join("|")

    # # 去除最後面地理位置資訊
    df['title'] = df.apply(trim_location_from_title, axis=1)
    
    return df
# endregion t_title

# region e_update_address
def e_update_address(df: pd.DataFrame):
    failed_count = 0
    
    while df[df['address'].isna() & df['location'].isna()].shape[0] > 0:
        print(f"============== Failed Count: {failed_count} ==================")
        for hash, item in df.iterrows():

            url = item['event_url']
            # print(item['address'])
            # if (item['address'] == np.nan or item["address"] == "" or df.at[hash, "address"].isna()) and (item["location"] == np.nan or item["location"] == ""):
            if pd.isna(item['address']) and  pd.isna(item['location']):
                print(f"Failed Count: {failed_count}")
                             
                try:
                    location, address = e_request_detail(url)
                    print(f"title: {df.at[hash, 'title']}, url: {df.at[hash, 'event_url']}")
                    print(f"address: {address}, location: {location}")
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

# endregion 

# region t_address_location
def t_address_location(df: pd.DataFrame):
    
    
    df["address"] = df["address"].astype(str).str.strip()
    # 清除括號內容
    # df[df["address"].notna()]["address"] = df[df["address"].notna()]['address'].astype(str).str.split('（').str[0]
    df["address"] = df.apply(trim_location_from_address, axis=1)
    df["address"] = df["address"].astype(str).str.strip()
    # 清除地址為https URL連結
    df["address"] = df.apply(clear_url_as_arress, axis=1)
    
    return df
# endregion t_address_location

# region e_request_coordinate
def e_request_coordinate(address: str):
    service = Service(executable_path="./chromedriver.exe")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--window-size=320,640")
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(3)
    url = "https://www.google.com.tw/maps/"
    try:
        try:
            driver.get(url) # 更改網址以前往不同網頁
            # search_input = driver.find_element(by=By.XPATH, value="//*[@id='searchboxinput']")
            # search_input.text
            

            wait = WebDriverWait(driver, 3)
            action = ActionChains(driver)

            # Perform Search
            searchTextbox = wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='searchboxinput']")))
            action.move_to_element(searchTextbox).click().send_keys(address).perform()
            search_result_first = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cell0x0"]/span[2]')))
            action.move_to_element(search_result_first).click().perform()
            # wait.until(EC.url_contains("!3d"))
            wait.until(EC.url_matches("!3d([-\d.]+)!4d([-\d.]+)!")) # url 經緯度格式: !3d22.9963798!4d120.1991459!
            
            match = re.search(r"!3d([-\d.]+)!4d([-\d.]+)!", driver.current_url)
            if match:
                latitude = match.group(2)
                longitude = match.group(1)
                print(f"緯度: {longitude}, 經度: {latitude}")
                print(f"google search validate string: {longitude}, {latitude}")
                
        except TimeoutException as e:
            print(str(e))
            print("TimeoutException: url not change")
            return None, None
        except Exception as e:
            print(str(e))
            return None, None
    finally:
        # time.sleep(100)
        driver.quit()
        pass

    return longitude, latitude    
    
# endregion e_request_coordinate

# region e_upadte_coordinate
def e_upadte_coordinate(df: pd.DataFrame):

    failed_count = 0
    
    while df[
                ((df['lng'].isnull() & df['lng'].isna()) | (df['lat'].isnull() & df['lat'].isna())) & 
                ((df['address'].notna() & df['address'].notnull())  | (df['location'].notna() & df['location'].notnull()))
            ].shape[0] > 0:
        if failed_count > 3:
            print("============== break")
            break        
        print(f"============== Failed Count: {failed_count} ==================")
        for hash, item in df.iterrows():

            location = item['location']
            address = item['address']
            lat = item['lat']
            lng = item['lng']
            
            search_content = address if pd.notna(address) else location 
            
            if pd.isna(lat) and  pd.isna(lng) and search_content:
                print(df.at[hash, "title"])
                try:
                    lat, lng = e_request_coordinate(search_content)
                    
                    df.at[hash, "lng"] = lng
                    df.at[hash, "lat"] = lat

                    # time.sleep(random.randint(3,10))
                    time.sleep(1)
                except Exception as e:
                    pass

        failed_count += 1
        
    return df  

# endregion 

# region t_date
def t_date(df: pd.DataFrame):
    
    df['s_time'] = None
    df['e_time'] = None
    
    # dates = item["date"].split("-")
    # if len(date)
    for hash, item in df.iterrows():
        df["s_time"] = df.apply(parse_s_time, axis=1)
        df["e_time"] = df.apply(parse_e_time, axis=1)
        df["s_time"] = df.apply(trim_s_time, axis=1)
        df["e_time"] = df.apply(trim_e_time, axis=1)
        df["s_time"] = df.apply(format_s_time, axis=1)
        df["e_time"] = df.apply(format_e_time, axis=1)
    return df
# endregion t_data

# region t_county
def t_county(df: pd.DataFrame):
    df["county"] = df["county"].astype(str).\
                                str.replace("臺", "台").\
                                str.replace(r'[縣市]', '', regex=True).\
                                str.strip()
    return df
# endregion t_county

def main():
    # region 取得列表頁資料
    # response_code, response_data = e_request_list()
    # if response_code != 200:
    #     print(f"請求失敗，status code: {response_code}")
    #     response_data = e_reuqest_list_by_selenium()
        
    # try:
    #     parsed_data = e_parse_response_json(response_data)
    #     activity_df = pd.DataFrame(parsed_data)
    # finally:
    #     activity_df.to_csv(f"{data_dir_path}/01_klook_data.csv", encoding="utf-8-sig", index=False)
    # endregion 取得列表頁資料
    
    # region 清理 title 資訊
    # try:
    #     df = pd.read_csv(f"{data_dir_path}/01_klook_data.csv", encoding="utf-8-sig")
    #     df = t_title(df)
    #     df.to_csv(f"{data_dir_path}/01_klook_transform_title_data.csv", encoding="utf-8-sig", index=False)
    #     pass
    # except Exception as e:
    #     print(str(e))
    # endregion 清理 title 資訊
    
    # region 取得地址、位置資訊
    # try:
    #     df = pd.read_csv(f"{data_dir_path}/01_klook_transform_title_data.csv", encoding="utf-8-sig") 
    #     df = e_update_address(df)
    #     df.to_csv(f"{data_dir_path}/01_klook_data_update_address.csv", encoding="utf-8-sig", index=False)
    # except Exception as e:
    #     print(str(e))
    # endregion 取得地址、位置資訊
    
    # region 清理地理位置資訊
    # try:
    #     df = pd.read_csv(f"{data_dir_path}/01_klook_data_update_address.csv", encoding="utf-8-sig") 
    #     df = t_address_location(df)
    #     df.to_csv(f"{data_dir_path}/01_klook_data_transform_address.csv", encoding="utf-8-sig", index=False)
    # except Exception as e:
    #     print(str(e))    
    # endregion 清理地理位置資訊
    
    # region 取得Google 座標
    # try:
    #     df = pd.read_csv(f"{data_dir_path}/01_klook_data_transform_address.csv", encoding="utf-8-sig") 
    #     df = e_upadte_coordinate(df)
    #     df.to_csv(f"{data_dir_path}/01_klook_data_update_coordinate.csv", encoding="utf-8-sig", index=False)
    # except Exception as e:
    #     print(str(e))    
    # endregion
        
    # region 清理日期格式
    # try:
        
    #     df = pd.read_csv(f"{data_dir_path}/01_klook_data_update_coordinate.csv", encoding="utf-8-sig") 
    #     df = t_date(df)
    #     df.to_csv(f"{data_dir_path}/01_klook_data_transform_date.csv", encoding="utf-8-sig", index=False)
    # except Exception as e:
    #     print(str(e))
    # endregion 清理日期格式
    
    # region 轉換字(臺->台)(county)
    try:
        df = pd.read_csv(f"{data_dir_path}/01_klook_data_transform_date.csv", encoding="utf-8-sig") 
        df = t_county(df)
        df.to_csv(f"{data_dir_path}/01_klook_data_transform_county.csv", encoding="utf-8-sig", index=False)        
        
    except Exception as e:
        print(str(e))
    
    # endregion

    # print(df)    
        


        
if __name__ == "__main__":
    data_dir_path = "klook/data_test"
    
    if not os.path.exists(data_dir_path) :
        os.mkdir(data_dir_path)    
    
    print("=========== Program Start ===========")

    main()
    # response_code, response_data = e_request_list()
    # region 抓取detail address, location
    # try:
    #     activity_df = e_update_address(pd.read_csv("klook/01_klook_data.csv", encoding="utf-8-sig"))
    # finally:
    #     activity_df.to_csv("klook/01_klook_data.csv", encoding="utf-8-sig", index=False)
    # endregion 抓取detail address, location
    
    print("----------- Program End -----------")

