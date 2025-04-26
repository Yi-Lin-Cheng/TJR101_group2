from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def web_open(headless=False, incognito=False, proxy=None):
    """開啟瀏覽器"""

    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver
