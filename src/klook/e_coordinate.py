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

# data_dir = Path("data", "klook")
data_dir = Path("data", "klook")
# region e_request_coordinate
def e_request_coordinate(address: str):
    service = Service(executable_path="/usr/local/bin/chromedriver")
    options = webdriver.ChromeOptions()
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
    
    driver.set_page_load_timeout(3)
    url = "https://www.google.com.tw/maps/"
    try:
        try:
            driver.get(url) # 更改網址以前往不同網頁

            wait = WebDriverWait(driver, 3)
            action = ActionChains(driver)

            # Perform Search
            searchTextbox = wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='searchboxinput']")))
            action.move_to_element(searchTextbox).click().send_keys(address).perform()
            search_result_first = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cell0x0"]/span[2]')))
            action.move_to_element(search_result_first).click().perform()
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
                ((df['address'].notna() & df['address'].notnull() & df['address'].notnull() != "")  | (df['location'].notna() & df['location'].notnull() & df['location'] != ""))
            ].shape[0] > 0:
        if failed_count > 3:
            break        
        
        for hash, item in df.iterrows():

            location = item['location']
            address = item['address']
            lat = item['lat']
            lng = item['lng']
            if (location == "" and address == "") or (pd.isna(location) and pd.isna(address)):
                continue
            
            search_content = address if pd.notna(address) else location 
            
            if pd.isna(lat) and  pd.isna(lng) and search_content:
                print(df.at[hash, "title"])
                try:
                    lat, lng = e_request_coordinate(search_content)
                    
                    df.at[hash, "lng"] = lng
                    df.at[hash, "lat"] = lat

                    time.sleep(1)
                except Exception as e:
                    pass

        failed_count += 1
        
    return df  

# endregion 


def main():
    
    source_data_path = data_dir / "t_address.csv"
    save_data_path = data_dir / "e_coordinate.csv"
    
    if not Path.exists(source_data_path):
        print(f"Source File{source_data_path}:  Not Exists")
        return     
    
    # region 取得Google 座標
    try:
        df = pd.read_csv(f"{source_data_path}", encoding="utf-8-sig") 
        df = e_upadte_coordinate(df)
        df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)
    except Exception as e:
        print(str(e))    
    # endregion
            

if __name__ == "__main__":
    main()