import random
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
import json

def detail_page_crawler(url: str, error_count:int = 0):
    service = Service(executable_path="./chromedriver.exe")
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    # options.add_argument("--incognito")
    options.add_argument("--nogpu")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=320,640")
    # options.add_argument("--no-sandbox")
    # options.add_argument('--headless') 
    # options.add_argument('blink-settings=imagesEnabled=false') 
    # options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"')
    # options.add_argument("--disable-plugins")
    # options.add_argument("--disk-cache-size=0")
    # options.add_argument("--enable-javascript") 
    # options.add_argument("--disable-javascript")
    # options.add_argument("--enable-automation")
    # options.add_argument("--allow-running-insecure-content")
    # options.add_argument("--disable-web-security")
    # options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # options.add_experimental_option('useAutomationExtension', False)
    # options.add_argument('--disable-blink-features=AutomationControlled')
    # options.add_argument("--enable-javascript")
    # driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36'})
    # driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    # options.add_argument("--disable-blink-features=AutomationControlled")
    
    
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)    
    options.add_argument("--no-sandbox")
    
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_page_load_timeout(30)
    try:
        try:
            driver.get(url) # 更改網址以前往不同網頁
        except TimeoutException as e:
            # time.sleep(10)
            print("TimeoutException")
            pass
        except Exception as e:
            print("Exception")
            pass

        try:
            page_source = driver.page_source
        except Exception as e:
            print("page_source Exception")
            # time.sleep(60)
            # error_count += 1
            # if error_count > 5:
            #     return np.NaN, np.NaN
            # return detail_page_crawler(url, error_count)
            
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
        print(str(e))
        pass
    
    finally:
        time.sleep(3)
        driver.close() # 關閉瀏覽器視窗   


def main():
    activity_list = pd.read_csv("klook/data_test/get_detail2.csv", encoding="utf-8-sig") 

    try:
        for hash, item in activity_list.iterrows():
            print("============================================================")
            url = item['event_url']
            # print("address: ", item['address'], "location: ", item['location'])

            if pd.isna(item["address"]) and pd.isna(item["location"]):
                print(url)
                try:
                    location, address = detail_page_crawler(url)
                    
                    activity_list.at[hash, "address"] = address if address is not np.nan else ""
                    activity_list.at[hash, "location"] = location if location is not np.nan else ""
                    print(item["title"], "   ", item["event_url"])
                    print(address, location)
                    # time.sleep(random.randint(3,10))
                    # time.sleep(10)
                except Exception as e:
                    # time.sleep(10)
                    # return
                    pass
    finally:
        activity_list.to_csv("klook/data_test/get_detail2.csv", encoding="utf-8-sig", index=False)

if __name__ == "__main__":
    main()
    # activity_list = pd.read_csv("klook/data_test/01_klook_transform_title_data.csv", encoding="utf-8-sig") 
    # # print(activity_list["address"])
    # for hash, item in activity_list.iterrows():
    #     print(pd.isna(item["address"]))
    