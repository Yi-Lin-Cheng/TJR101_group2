import tempfile
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait


def web_open(headless=True, window_size=(1920, 1080), timeout=15):
    """
    Create and return a configured Selenium WebDriver instance.

    Args:
        headless (bool): Whether to run the browser in headless mode. Default is True.
        window_size (tuple): The size of the browser window as (width, height). Default is (1920, 1080).
        timeout (int): The maximum wait time (in seconds) for WebDriverWait. Default is 15.

    Returns:
        tuple:
            driver (webdriver.Chrome): The Selenium WebDriver instance.
            wait (WebDriverWait): The WebDriverWait instance for explicit waits.
            user_data_dir (str): The path to the Chrome user data directory used.
    """
    try:
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless")  # 無頭模式
            options.add_argument("--disable-gpu")  # 關閉 GPU（無 GUI 時可略過）
        options.add_argument(f"--window-size={window_size[0]},{window_size[1]}")
        options.add_argument("--no-sandbox")  # 取消沙箱模式（容器內需加）
        options.add_argument("--disable-dev-shm-usage")  # 共享記憶體空間問題
        options.add_argument("--lang=zh-TW")  # 設定語言為繁體中文
        # options.add_argument("--start-maximized") # 非 headless 模式才有效
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        options.add_argument(f"--user-agent={user_agent}")
        # 利用時間戳記產生暫存資料夾，避免chrome出錯
        user_data_dir = tempfile.mkdtemp(prefix=f"chrome-profile-{int(time.time())}-")
        options.add_argument(f"--user-data-dir={user_data_dir}")
        service = Service("/usr/local/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        })
                    """
            },
        )
        wait = WebDriverWait(driver, timeout)
        print("瀏覽器啟動!")
        return driver, wait, user_data_dir
    except Exception as e:
        print(f"瀏覽器啟動失敗：{e}")
        return None, None, None
