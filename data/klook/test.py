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



def main():
    
    service = Service(executable_path="./chromedriver.exe")
    options = webdriver.ChromeOptions()

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

    
if __name__ == "__main__":
    main()