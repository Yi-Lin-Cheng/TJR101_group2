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

data_dir = Path("data", "klook")

# region e_request_detail
def e_request_detail(url: str, error_count:int = 0):
    service = Service(executable_path="/usr/local/bin/chromedriver")
    options = webdriver.ChromeOptions()
    
    # options.add_argument('--disable-blink-features=AutomationControlled')
    # options.add_argument('--start-maximized')  
    # options.add_argument("--headless=new")
    # options.add_argument("--no-sandbox")
    # options.add_argument('--disable-dev-shm-usage')        
    # options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")                
    # driver = webdriver.Chrome(service=service, options=options)    
    
    # driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
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
    
    driver.set_page_load_timeout(3)
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
                "location_xpath": "#organiser > div > div > p:nth-child(2) > a > strong",
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

# region e_update_address
def e_update_address(df: pd.DataFrame):
    failed_count = 0
    
    while df[df['address'].isna() & df['location'].isna()].shape[0] > 0:
        for hash, item in df.iterrows():

            url = item['event_url']
            
            if pd.isna(item['address']) and  pd.isna(item['location']):

                try:
                    location, address = e_request_detail(url)
                    print(f"title: {df.at[hash, 'title']}, url: {df.at[hash, 'event_url']}")
                    print(f"address: {address}, location: {location}")
                    df.at[hash, "address"] = address if address is not np.nan else ""
                    df.at[hash, "location"] = location if location is not np.nan else ""

                    time.sleep(1)
                except Exception as e:
                    pass
        if failed_count > 3:
            break
        failed_count += 1
        
    return df  

# endregion 

# region main
def main():
    
    source_data_path = data_dir / "e_data_list.csv"
    save_data_path = data_dir / "e_data_detail.csv"
    
    if not Path.exists(source_data_path):
        print(f"Source File{source_data_path}:  Not Exists")
        return 
    
    # region 取得地址、位置資訊
    try:
        
        df = pd.read_csv(f"{source_data_path}", encoding="utf-8-sig") 
        df = e_update_address(df)
        df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)
        
        
        retry  = 1
        while df[df['address'].isna() & df['location'].isna()].shape[0] > 0:
            df = pd.read_csv(f"{save_data_path}", encoding="utf-8-sig")
            df = e_update_address(df)
            df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)
            retry += 1
            if retry >= 3:
                break      
        
    except Exception as e:
        print(str(e))
    # endregion 取得地址、位置資訊

# endregion main

if __name__ == "__main__":
    main()