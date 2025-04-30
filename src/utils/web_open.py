from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def web_open():
    """開啟瀏覽器"""
    try:
        options = Options()
        options.add_argument("--headless")  # 無頭模式
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")  # 取消沙箱模式（容器內需加）
        options.add_argument("--disable-dev-shm-usage")  # 共享記憶體空間問題
        options.add_argument("--disable-gpu")  # 關閉 GPU（無 GUI 時可略過）
        options.add_argument("--lang=zh-TW") # 設定語言為繁體中文
        # options.add_argument("--start-maximized") # 非 headless 模式才有效

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        print("瀏覽器啟動!")
    except Exception as e:
        print(f"瀏覽器啟動失敗：{e}")

    return driver