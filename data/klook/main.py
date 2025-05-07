# 載入需要的套件
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
import json
# 開啟瀏覽器視窗(Chrome)
# 方法一：執行前需開啟chromedriver.exe且與執行檔在同一個工作目錄
service = Service(executable_path="./chromedriver.exe")
options = webdriver.ChromeOptions()

# 方法二：或是直接指定exe檔案路徑
driver = webdriver.Chrome(service=service, options=options)
driver.set_page_load_timeout(3)
# driver.get("https://www.klook.com/v1/enteventapisrv/public/content/query_v3?k_lang=zh_TW&k_currency=TWD&area=coureg_1014&page_size=23&page_num=1&filters=festival_carnival&sort=latest&date=date_select&start_date=2025-04-20&end_date=2025-04-30&keywords=") # 更改網址以前往不同網頁
try:
    driver.get("https://www.klook.com/zh-TW/event-detail/101029199-2025-neon-oasis-fest/") # 更改網址以前往不同網頁
except Exception as e:
    pass
# driver.get("https://www.klook.com/v1/enteventapisrv/public/content/query_v3?k_lang=zh_TW&k_currency=TWD&area=coureg_1014&page_size=23&page_num=1&filters=&sort=latest&date=date_select&start_date=2025-04-16&end_date=2025-04-30")
# driver.get("https://www.klook.com/zh-TW/event-detail/101029243-2025-contested-waters/?spm=Event_Vertical.AllEvent_LIST&clickId=dbb55e1708")


page_source = driver.page_source
print(page_source)
with open("./2ec1c8ad8d.txt", "w", encoding='utf-8') as f:
    f.write(page_source)
time.sleep(3)
driver.close() # 關閉瀏覽器視窗