import random
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils import web_open

data_dir = Path("data", "food")
file_path = data_dir / "cafe_craw_googlemap.csv"


def scroll_to_bottom(driver, max_results=10, pause_time=5, max_wait_time=300):

    start_time = time.time()
    last_count = 0
    retries = 0

    while True:
        # 滾動到底部
        scrollable_div = driver.find_element(
            By.XPATH,
            "//*[@id='QA0Szd']/div/div/div[1]/div[2]/div/div[1]/div/div/div[1]/div[1]",
        )
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_div
        )
        time.sleep(pause_time)

        # 取得目前咖啡廳數量
        cards = driver.find_elements(By.CSS_SELECTOR, ".Nv2PK.THOPZb.CpccDe")
        current_count = len(cards)
        print(f"目前已載入咖啡廳數量：{current_count}")
        if current_count >= max_results:
            break

        # 如果沒有新增咖啡廳數，就加一次重試次數
        if current_count == last_count:
            retries += 1
            print(f"無新增咖啡廳，第 {retries} 次")
        else:
            retries = 0  # 有新增就重置
            last_count = current_count

        # 超過兩次都沒有新增，就停止
        if retries >= 2:
            print("咖啡廳未增加，停止滾動")
            break

        # 超過最大等待時間
        if time.time() - start_time > max_wait_time:
            print("超過最大等待時間，停止滾動")
            break


def get_unfinished_towns(data):
    """
    這裡面有全台鄉鎮市區(不含外島)
    放入data，查看有哪些鄉鎮市區的未完成
    回傳未完成的 list
    """
    town_list = [
        "新北市板橋區",
        "新北市三重區",
        "新北市中和區",
        "新北市永和區",
        "新北市新莊區",
        "新北市新店區",
        "新北市樹林區",
        "新北市鶯歌區",
        "新北市三峽區",
        "新北市淡水區",
        "新北市汐止區",
        "新北市瑞芳區",
        "新北市土城區",
        "新北市蘆洲區",
        "新北市五股區",
        "新北市泰山區",
        "新北市林口區",
        "新北市深坑區",
        "新北市石碇區",
        "新北市坪林區",
        "新北市三芝區",
        "新北市石門區",
        "新北市八里區",
        "新北市平溪區",
        "新北市雙溪區",
        "新北市貢寮區",
        "新北市金山區",
        "新北市萬里區",
        "新北市烏來區",
        "臺北市松山區",
        "臺北市信義區",
        "臺北市大安區",
        "臺北市中山區",
        "臺北市中正區",
        "臺北市大同區",
        "臺北市萬華區",
        "臺北市文山區",
        "臺北市南港區",
        "臺北市內湖區",
        "臺北市士林區",
        "臺北市北投區",
        "桃園市桃園區",
        "桃園市中壢區",
        "桃園市大溪區",
        "桃園市楊梅區",
        "桃園市蘆竹區",
        "桃園市大園區",
        "桃園市龜山區",
        "桃園市八德區",
        "桃園市龍潭區",
        "桃園市平鎮區",
        "桃園市新屋區",
        "桃園市觀音區",
        "桃園市復興區",
        "臺中市中區",
        "臺中市東區",
        "臺中市南區",
        "臺中市西區",
        "臺中市北區",
        "臺中市西屯區",
        "臺中市南屯區",
        "臺中市北屯區",
        "臺中市豐原區",
        "臺中市東勢區",
        "臺中市大甲區",
        "臺中市清水區",
        "臺中市沙鹿區",
        "臺中市梧棲區",
        "臺中市后里區",
        "臺中市神岡區",
        "臺中市潭子區",
        "臺中市大雅區",
        "臺中市新社區",
        "臺中市石岡區",
        "臺中市外埔區",
        "臺中市大安區",
        "臺中市烏日區",
        "臺中市大肚區",
        "臺中市龍井區",
        "臺中市霧峰區",
        "臺中市太平區",
        "臺中市大里區",
        "臺中市和平區",
        "臺南市新營區",
        "臺南市鹽水區",
        "臺南市白河區",
        "臺南市柳營區",
        "臺南市後壁區",
        "臺南市東山區",
        "臺南市麻豆區",
        "臺南市下營區",
        "臺南市六甲區",
        "臺南市官田區",
        "臺南市大內區",
        "臺南市佳里區",
        "臺南市學甲區",
        "臺南市西港區",
        "臺南市七股區",
        "臺南市將軍區",
        "臺南市北門區",
        "臺南市新化區",
        "臺南市善化區",
        "臺南市新市區",
        "臺南市安定區",
        "臺南市山上區",
        "臺南市玉井區",
        "臺南市楠西區",
        "臺南市南化區",
        "臺南市左鎮區",
        "臺南市仁德區",
        "臺南市歸仁區",
        "臺南市關廟區",
        "臺南市龍崎區",
        "臺南市永康區",
        "臺南市東區",
        "臺南市南區",
        "臺南市北區",
        "臺南市安南區",
        "臺南市安平區",
        "臺南市中西區",
        "高雄市鹽埕區",
        "高雄市鼓山區",
        "高雄市左營區",
        "高雄市楠梓區",
        "高雄市三民區",
        "高雄市新興區",
        "高雄市前金區",
        "高雄市苓雅區",
        "高雄市前鎮區",
        "高雄市旗津區",
        "高雄市小港區",
        "高雄市鳳山區",
        "高雄市林園區",
        "高雄市大寮區",
        "高雄市大樹區",
        "高雄市大社區",
        "高雄市仁武區",
        "高雄市鳥松區",
        "高雄市岡山區",
        "高雄市橋頭區",
        "高雄市燕巢區",
        "高雄市田寮區",
        "高雄市阿蓮區",
        "高雄市路竹區",
        "高雄市湖內區",
        "高雄市茄萣區",
        "高雄市永安區",
        "高雄市彌陀區",
        "高雄市梓官區",
        "高雄市旗山區",
        "高雄市美濃區",
        "高雄市六龜區",
        "高雄市甲仙區",
        "高雄市杉林區",
        "高雄市內門區",
        "高雄市茂林區",
        "高雄市桃源區",
        "高雄市那瑪夏區",
        "宜蘭縣宜蘭市",
        "宜蘭縣羅東鎮",
        "宜蘭縣蘇澳鎮",
        "宜蘭縣頭城鎮",
        "宜蘭縣礁溪鄉",
        "宜蘭縣壯圍鄉",
        "宜蘭縣員山鄉",
        "宜蘭縣冬山鄉",
        "宜蘭縣五結鄉",
        "宜蘭縣三星鄉",
        "宜蘭縣大同鄉",
        "宜蘭縣南澳鄉",
        "新竹縣竹北市",
        "新竹縣關西鎮",
        "新竹縣新埔鎮",
        "新竹縣竹東鎮",
        "新竹縣湖口鄉",
        "新竹縣橫山鄉",
        "新竹縣新豐鄉",
        "新竹縣芎林鄉",
        "新竹縣寶山鄉",
        "新竹縣北埔鄉",
        "新竹縣峨眉鄉",
        "新竹縣尖石鄉",
        "新竹縣五峰鄉",
        "苗栗縣苗栗市",
        "苗栗縣頭份市",
        "苗栗縣苑裡鎮",
        "苗栗縣通霄鎮",
        "苗栗縣竹南鎮",
        "苗栗縣後龍鎮",
        "苗栗縣卓蘭鎮",
        "苗栗縣大湖鄉",
        "苗栗縣公館鄉",
        "苗栗縣銅鑼鄉",
        "苗栗縣南庄鄉",
        "苗栗縣頭屋鄉",
        "苗栗縣三義鄉",
        "苗栗縣西湖鄉",
        "苗栗縣造橋鄉",
        "苗栗縣三灣鄉",
        "苗栗縣獅潭鄉",
        "苗栗縣泰安鄉",
        "彰化縣彰化市",
        "彰化縣員林市",
        "彰化縣鹿港鎮",
        "彰化縣和美鎮",
        "彰化縣北斗鎮",
        "彰化縣溪湖鎮",
        "彰化縣田中鎮",
        "彰化縣二林鎮",
        "彰化縣線西鄉",
        "彰化縣伸港鄉",
        "彰化縣福興鄉",
        "彰化縣秀水鄉",
        "彰化縣花壇鄉",
        "彰化縣芬園鄉",
        "彰化縣大村鄉",
        "彰化縣埔鹽鄉",
        "彰化縣埔心鄉",
        "彰化縣永靖鄉",
        "彰化縣社頭鄉",
        "彰化縣二水鄉",
        "彰化縣田尾鄉",
        "彰化縣埤頭鄉",
        "彰化縣芳苑鄉",
        "彰化縣大城鄉",
        "彰化縣竹塘鄉",
        "彰化縣溪州鄉",
        "南投縣南投市",
        "南投縣埔里鎮",
        "南投縣草屯鎮",
        "南投縣竹山鎮",
        "南投縣集集鎮",
        "南投縣名間鄉",
        "南投縣鹿谷鄉",
        "南投縣中寮鄉",
        "南投縣魚池鄉",
        "南投縣國姓鄉",
        "南投縣水里鄉",
        "南投縣信義鄉",
        "南投縣仁愛鄉",
        "雲林縣斗六市",
        "雲林縣斗南鎮",
        "雲林縣虎尾鎮",
        "雲林縣西螺鎮",
        "雲林縣土庫鎮",
        "雲林縣北港鎮",
        "雲林縣古坑鄉",
        "雲林縣大埤鄉",
        "雲林縣莿桐鄉",
        "雲林縣林內鄉",
        "雲林縣二崙鄉",
        "雲林縣崙背鄉",
        "雲林縣麥寮鄉",
        "雲林縣東勢鄉",
        "雲林縣褒忠鄉",
        "雲林縣臺西鄉",
        "雲林縣元長鄉",
        "雲林縣四湖鄉",
        "雲林縣口湖鄉",
        "雲林縣水林鄉",
        "嘉義縣太保市",
        "嘉義縣朴子市",
        "嘉義縣布袋鎮",
        "嘉義縣大林鎮",
        "嘉義縣民雄鄉",
        "嘉義縣溪口鄉",
        "嘉義縣新港鄉",
        "嘉義縣六腳鄉",
        "嘉義縣東石鄉",
        "嘉義縣義竹鄉",
        "嘉義縣鹿草鄉",
        "嘉義縣水上鄉",
        "嘉義縣中埔鄉",
        "嘉義縣竹崎鄉",
        "嘉義縣梅山鄉",
        "嘉義縣番路鄉",
        "嘉義縣大埔鄉",
        "嘉義縣阿里山鄉",
        "屏東縣屏東市",
        "屏東縣潮州鎮",
        "屏東縣東港鎮",
        "屏東縣恆春鎮",
        "屏東縣萬丹鄉",
        "屏東縣長治鄉",
        "屏東縣麟洛鄉",
        "屏東縣九如鄉",
        "屏東縣里港鄉",
        "屏東縣鹽埔鄉",
        "屏東縣高樹鄉",
        "屏東縣萬巒鄉",
        "屏東縣內埔鄉",
        "屏東縣竹田鄉",
        "屏東縣新埤鄉",
        "屏東縣枋寮鄉",
        "屏東縣新園鄉",
        "屏東縣崁頂鄉",
        "屏東縣林邊鄉",
        "屏東縣南州鄉",
        "屏東縣佳冬鄉",
        "屏東縣車城鄉",
        "屏東縣滿州鄉",
        "屏東縣枋山鄉",
        "屏東縣三地門鄉",
        "屏東縣霧臺鄉",
        "屏東縣瑪家鄉",
        "屏東縣泰武鄉",
        "屏東縣來義鄉",
        "屏東縣春日鄉",
        "屏東縣獅子鄉",
        "屏東縣牡丹鄉",
        "臺東縣臺東市",
        "臺東縣成功鎮",
        "臺東縣關山鎮",
        "臺東縣卑南鄉",
        "臺東縣大武鄉",
        "臺東縣太麻里鄉",
        "臺東縣東河鄉",
        "臺東縣長濱鄉",
        "臺東縣鹿野鄉",
        "臺東縣池上鄉",
        "臺東縣延平鄉",
        "臺東縣海端鄉",
        "臺東縣達仁鄉",
        "臺東縣金峰鄉",
        "花蓮縣花蓮市",
        "花蓮縣鳳林鎮",
        "花蓮縣玉里鎮",
        "花蓮縣新城鄉",
        "花蓮縣吉安鄉",
        "花蓮縣壽豐鄉",
        "花蓮縣光復鄉",
        "花蓮縣豐濱鄉",
        "花蓮縣瑞穗鄉",
        "花蓮縣富里鄉",
        "花蓮縣秀林鄉",
        "花蓮縣萬榮鄉",
        "花蓮縣卓溪鄉",
        "基隆市中正區",
        "基隆市七堵區",
        "基隆市暖暖區",
        "基隆市仁愛區",
        "基隆市中山區",
        "基隆市安樂區",
        "基隆市信義區",
        "新竹市東區",
        "新竹市北區",
        "新竹市香山區",
        "嘉義市東區",
        "嘉義市西區",
    ]
    unfinished_towns = [
        town for town in town_list if town not in data["region_town"].values
    ]
    return unfinished_towns


def get_cafe_url(data, unfinished_towns, batch_size=50):
    for num, town in enumerate(unfinished_towns):
        if num % batch_size == 0:
            new_rows = []
            # headless=False 開啟瀏覽器畫面，預設True
            driver, wait, temp_dir = web_open(headless=False)
        search_url = f"https://www.google.com/maps/search/{town}+咖啡廳"
        driver.get(search_url)  # 開啟網址
        max_results = 10
        scroll_to_bottom(driver, max_results)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        elements = soup.select(".hfpxzc")
        if elements:
            for element in elements:
                name = element.get("aria-label")
                url = element.get("href")
                row = {"region_town": town, "f_name": name, "gmaps_url": url}
                new_rows.append(row)
        print(f"{town}完成，共{len(elements)}筆")
        if (num + 1) % batch_size == 0 or num + 1 == len(unfinished_towns):
            driver.quit()
            new_data = pd.DataFrame(new_rows)
            data = pd.concat([data, new_data], ignore_index=True)
            data.to_csv(file_path, encoding="utf-8", header=True, index=False)
    return data


def get_google_info(url: str, driver, wait):
    """
    Crawl business info from a Google Maps place page.

    Args:
        url (str): The URL of the Google Maps place.
        driver (webdriver.Chrome): Selenium WebDriver instance.
        wait (WebDriverWait): WebDriverWait instance for explicit wait.

    Returns:
        dict: A dictionary containing:
            - b_hours (str): Business hours text, if available.
            - rate (str or None): Star rating, if available.
            - pic_url (str): The URL of the main photo.
            - comm (str or None): Number of comments/reviews, cleaned.
            - error (bool or Exception): False if success, or the exception object if failed.
    """
    try:
        driver.get(url)
        time.sleep(random.uniform(2, 4))
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "img[decoding='async']"))
        )
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        b_hours_tags = soup.select(".y0skZc")
        b_hours = ""
        if b_hours_tags:
            for tag in b_hours_tags:
                weekday = tag.select_one(".ylH6lf").text
                b_hours += f"\n{weekday}"
                times = tag.select(".G8aQO")
                for time_ in times:
                    b_hours += f" {time_.text}"
        elif "永久歇業" in soup.text:
            b_hours = "永久歇業"
        address = ""
        address_tags = soup.select(".Io6YTe.fontBodyMedium.kR99db.fdkmkc")

        for tag in address_tags:
            if tag.string and re.search(r".+[縣市].+[鄉鎮市區].+號", tag.string):
                address = tag.string.strip()
                break

        match = re.search(r"!3d([0-9.\-]+)!4d([0-9.\-]+)", url)
        if match:
            lng = round(float(match.group(2)), 5)
            lat = round(float(match.group(1)), 5)
            geo_loc = f"POINT({lng} {lat})"
        else:
            geo_loc = ""
        pic_url = soup.select_one("img[decoding='async']")["src"]
        rate_comm_tag = soup.select_one(".F7nice")
        rate = None
        comm = None
        if rate_comm_tag and rate_comm_tag.text != "":
            rate = rate_comm_tag.select_one("span[aria-hidden]").text
            comm = re.sub(
                r"\(|,|\)", "", rate_comm_tag.select("span[aria-label]")[1].text
            )
        return {
            "b_hours": b_hours,
            "address": address,
            "rate": rate,
            "geo_loc": geo_loc,
            "pic_url": pic_url,
            "comm": comm,
            "error": False,
        }
    except Exception as err:
        time.sleep(random.uniform(5, 10))
        return {
            "b_hours": "",
            "address": "",
            "rate": None,
            "geo_loc": "",
            "pic_url": "",
            "comm": None,
            "error": err,
        }


def main():
    if file_path.exists():
        data = pd.read_csv(file_path, encoding="utf-8", engine="python")
    else:
        columns = [
            "region_town",
            "food_id",
            "f_name",
            "b_hours",
            "county",
            "address",
            "rate",
            "geo_loc",
            "pic_url",
            "gmaps_url",
            "f_type",
            "comm",
            "area",
            "create_time",
            "update_time",
        ]
        data = pd.DataFrame(columns=columns)
    unfinished_towns = get_unfinished_towns(data)
    data = get_cafe_url(data, unfinished_towns)
    now = datetime.now().replace(microsecond=0)
    data.loc[data["create_time"].isna(), "create_time"] = now
    gmaps_url_list = data["gmaps_url"].tolist()
    start_idx = data[data["update_time"].isna()].index.min()
    batch_size = 100
    for i in range(start_idx, len(gmaps_url_list)):
        if i % batch_size == 0:
            driver, wait, profile = web_open(headless=False)
            if not driver:
                break
            b_hours_list = []
            address_list = []
            rate_list = []
            geo_loc_list = []
            pic_url_list = []
            comm_list = []
            update_time_list = []
            err_log = ""
        url = gmaps_url_list[i]
        result = get_google_info(url, driver, wait)
        if not result["error"]:
            print(f"第{i+1}筆完成")
            update_time = now
        else:
            err_msg = f"{datetime.now()}第{i+1}筆 {url} 出現錯誤"
            print(err_msg)
            err_log += err_msg + "\n"
            update_time = (
                data.at[i, "update_time"] if data.at[i, "update_time"] else now
            )
        b_hours_list.append(result["b_hours"])
        address_list.append(result["address"])
        rate_list.append(result["rate"])
        geo_loc_list.append(result["geo_loc"])
        pic_url_list.append(result["pic_url"])
        comm_list.append(result["comm"])
        update_time_list.append(update_time)
        rate_list = [float(r) if r not in [None, ""] else None for r in rate_list]
        comm_list = [int(c) if str(c).isdigit() else None for c in comm_list]
        if ((i + 1) % batch_size == 0) or (i + 1) == len(gmaps_url_list):
            end_idx = i + 1
            start_idx = end_idx - len(b_hours_list)
            data.loc[start_idx : end_idx - 1, "b_hours"] = b_hours_list
            data.loc[start_idx : end_idx - 1, "address"] = address_list
            data.loc[start_idx : end_idx - 1, "rate"] = rate_list
            data.loc[start_idx : end_idx - 1, "geo_loc"] = geo_loc_list
            data.loc[start_idx : end_idx - 1, "pic_url"] = pic_url_list
            data.loc[start_idx : end_idx - 1, "comm"] = comm_list
            data.loc[start_idx : end_idx - 1, "update_time"] = update_time_list
            print(f"第{i+1}筆儲存完成")
            data.to_csv(file_path, encoding="utf-8", header=True, index=False)
            driver.quit()
            time.sleep(random.uniform(4, 6))


if __name__ == "__main__":
    main()
