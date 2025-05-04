import json
import pandas as pd
import random
import time
from bs4 import BeautifulSoup
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

file_path = Path("data", "hotel")


# 多數註解都是第一輪抓取，同時執行5隻selenium用的
# def accomo03_extract_booking(start, end,step):
#     if (file_path/f"accomo03_extract_booking_progress_part{step}.csv").exists():
#         print("發現進度檔案，從中斷處繼續")
#         data = pd.read_csv(file_path/f"accomo03_extract_booking_progress_part{step}.csv", encoding="utf-8", engine="python")
def accomo03_extract_booking():
    if (file_path / "accomo03_extract_booking_progress.csv").exists():
        print("發現進度檔案，從中斷處繼續")
        data = pd.read_csv(
            file_path / "accomo03_extract_booking_progress.csv",
            encoding="utf-8",
            engine="python",
        )
    else:
        print("沒有進度檔案，從頭開始")
        data_open = pd.read_csv(
            file_path / "accomo02_open_data_filtered_new.csv",
            encoding="utf-8",
            engine="python",
        )
        data = data_open[
            ["Id", "Name", "Region", "Town", "Add", "Px", "Py", "Class"]
        ].copy()
        data.columns = [
            "id_open",
            "name_open",
            "region_open",
            "town_open",
            "add_open",
            "lng_open",
            "lat_open",
            "class_open",
        ]
        new_cols = [
            "url",
            "name",
            "add",
            "region",
            "town",
            "lng",
            "lat",
            "rating",
            "user_rating_total",
            "type",
            "facilities",
            "img_url",
        ]

        for col in new_cols:
            data[col] = None
        # data = data.loc[start:end,:].reset_index(drop=True)

    start_idx = data[data["url"].isna()].index.min()
    if pd.isna(start_idx):
        print("此階段已完成")
        return

    name_open_list = data["name_open"].tolist()
    region_open_list = data["region_open"].tolist()
    town_open_list = data["town_open"].tolist()
    class_open_list = data["class_open"].tolist()

    for i in range(start_idx, len(name_open_list)):
        if i % 200 == 0:
            url_list = []
            name_list = []
            add_list = []
            region_list = []
            town_list = []
            lng_list = []
            lat_list = []
            rating_list = []
            user_rating_total_list = []
            type_list = []
            facilities_list = []
            img_url_list = []
            err_log = ""
            service = Service(ChromeDriverManager().install())
            options = webdriver.ChromeOptions()
            driver = webdriver.Chrome(service=service, options=options)
            wait = WebDriverWait(driver, 15)

            # 設定視窗位置（例如放在第2個螢幕，X=1920, Y=100）
            # driver.set_window_position(1920 + 100, end/10 - 250)
            # driver.set_window_size(900, 800)
        try:
            name_open = name_open_list[i]
            region_open = region_open_list[i]
            town_open = town_open_list[i]
            driver.get(
                f"https://www.booking.com/searchresults.zh-tw.html?ss={region_open}+{town_open}+{name_open}"
            )
            time.sleep(random.uniform(0.5, 1.5))
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div[data-testid='property-card-container']")
                )
            )
            div = soup.select_one("div[data-testid='property-card-container']")
            a = div.select("a")[1]
            url = a["href"]
            name = (
                a.select_one("div[data-testid='title']").text.strip()
                if a.select_one("div[data-testid='title']")
                else ""
            )
            driver.get(url)
            time.sleep(random.uniform(0.5, 1.5))
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".page-section.js-k2-hp--block.k2-hp--rt")
                )
            )
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            ld_json = soup.find("script", type="application/ld+json")
            if ld_json:
                ld_json = json.loads(ld_json.string)
            add = ld_json.get("address", {}).get("streetAddress").strip()
            region_town = soup.select(
                ".bui-link.bui-link--primary.bui_breadcrumb__link"
            )
            region = region_town[-2].text.strip()
            town = region_town[-1].text.strip()
            lng_lat = soup.select_one("a[data-atlas-latlng]")[
                "data-atlas-latlng"
            ].split(",")
            lng = f"{round(float(lng_lat[1]), 5):.5f}"
            lat = f"{round(float(lng_lat[0]), 5):.5f}"
            rating = ld_json.get("aggregateRating", {}).get("ratingValue")
            user_ratings_total = ld_json.get("aggregateRating", {}).get("reviewCount")
            room_type_tag = soup.select_one(".page-section.js-k2-hp--block.k2-hp--rt")
            room_type = room_type_tag.text if room_type_tag else None
            if class_open_list[i] == 4:
                type_ = "民宿"
            elif "宿舍" in room_type:
                type_ = "青旅"
            else:
                type_ = "飯店"
            facilities_tag = soup.select_one(".hp--popular_facilities.js-k2-hp--block")
            facilities = facilities_tag.text.strip() if facilities_tag else None
            img_url = ld_json.get("image")

            name_list.append(name)
            url_list.append(url)
            add_list.append(add)
            region_list.append(region)
            town_list.append(town)
            lng_list.append(lng)
            lat_list.append(lat)
            rating_list.append(rating)
            user_rating_total_list.append(user_ratings_total)
            type_list.append(type_)
            facilities_list.append(facilities)
            img_url_list.append(img_url)
        except Exception as err:
            err_msg = f"第{i+1}筆資料{name_open}出現錯誤"
            err_log += err_msg + "\n"
            name_list.append(None)
            url_list.append(None)
            add_list.append(None)
            region_list.append(None)
            town_list.append(None)
            lng_list.append(None)
            lat_list.append(None)
            rating_list.append(None)
            user_rating_total_list.append(None)
            type_list.append(None)
            facilities_list.append(None)
            img_url_list.append(None)
            time.sleep(random.uniform(2, 4))

        if ((i + 1) % 200 == 0) or i + 1 == len(name_open_list):
            end_idx = i + 1
            start_idx = end_idx - len(name_list)
            data.loc[start_idx : end_idx - 1, "name"] = name_list
            data.loc[start_idx : end_idx - 1, "url"] = url_list
            data.loc[start_idx : end_idx - 1, "add"] = add_list
            data.loc[start_idx : end_idx - 1, "region"] = region_list
            data.loc[start_idx : end_idx - 1, "town"] = town_list
            data.loc[start_idx : end_idx - 1, "lng"] = lng_list
            data.loc[start_idx : end_idx - 1, "lat"] = lat_list
            data.loc[start_idx : end_idx - 1, "rating"] = rating_list
            data.loc[start_idx : end_idx - 1, "user_rating_total"] = (
                user_rating_total_list
            )
            data.loc[start_idx : end_idx - 1, "type"] = type_list
            data.loc[start_idx : end_idx - 1, "facilities"] = facilities_list
            data.loc[start_idx : end_idx - 1, "img_url"] = img_url_list

            # data.to_csv(file_path/f"accomo03_extract_booking_part{step}.csv",encoding="utf-8",header=True,index=False)
            # data.to_csv(file_path/f"accomo03_extract_booking_progress_part{step}.csv",encoding="utf-8",header=True,index=False)
            # with open(file_path/f"accomo03_extract_booking_part{step}.txt","a",encoding="utf-8") as f:
            data.to_csv(
                file_path / "accomo03_extract_booking.csv",
                encoding="utf-8",
                header=True,
                index=False,
            )
            data.to_csv(
                file_path / "accomo03_extract_booking_progress.csv",
                encoding="utf-8",
                header=True,
                index=False,
            )
            with open(
                file_path / "accomo03_extract_booking.txt", "a", encoding="utf-8"
            ) as f:
                f.write(err_log)
            print(f"第{i+1}筆儲存完成")
            time.sleep(random.uniform(4, 6))
            driver.quit()

    # if (file_path/f"accomo03_extract_booking_progress_part{step}.csv").exists():
    #     (file_path/f"accomo03_extract_booking_progress_part{step}.csv").unlink()
    #     print("已完成全部資料，進度檔案已刪除")
    if (file_path / "accomo03_extract_booking_progress.csv").exists():
        (file_path / "accomo03_extract_booking_progress.csv").unlink()
        print("已完成全部資料，進度檔案已刪除")


def main():
    accomo03_extract_booking()


if __name__ == "__main__":
    main()

    # 同時執行5隻selenium
    # from multiprocessing import Process

    # ranges = [(0, 2500), (2501, 5000), (5001, 7500), (7501, 10000), (10001, 12521)]
    # processes = []
    # for i, (start, end) in enumerate(ranges):
    #     p = Process(target=accomo03_extract_booking, args=(start, end, i))
    #     p.start()
    #     processes.append(p)

    # for p in processes:
    #     p.join()

    # data1 = pd.read_csv(file_path/"accomo03_extract_booking_part0.csv",encoding="utf-8",engine="python")
    # for i in range(1,5):
    #     data2 = pd.read_csv(file_path/"accomo03_extract_booking_part{i}.csv",encoding="utf-8",engine="python")
    #     data1 = pd.concat([data1,data2],ignore_index=True)
    # data1.to_csv(file_path/"accomo03_extract_booking.csv",encoding="utf-8",header=True,index=False)
