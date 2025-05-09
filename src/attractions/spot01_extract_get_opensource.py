from pathlib import Path

import requests

from utils import encoding_transform

data_dir = Path("data", "spot")


def main():
    latest_file = data_dir / "spot01_open_data_raw_latest.csv"
    previous_file = data_dir / "spot01_open_data_raw_previous.csv"
    url = "https://media.taiwan.net.tw/XMLReleaseALL_public/Scenic_Spot_C_f.csv"
    response = requests.get(url)
    if response.status_code == 200:
        if latest_file.exists():
            latest_file.rename(previous_file)
        with open(latest_file, "wb") as f:
            f.write(response.content)

        encoding_transform(latest_file)
    else:
        print(f"無法下載資料，HTTP 狀態碼: {response.status_code}")


if __name__ == "__main__":
    main()

# # 餐廳
# url = "https://media.taiwan.net.tw/XMLReleaseALL_public/Restaurant_C_f.csv"
# response = requests.get(url)

# with open("Restaurant_C_f(NEW).csv", "wb") as f:
#     f.write(response.content)

# # 住宿
# url = "https://media.taiwan.net.tw/XMLReleaseALL_public/Hotel_C_f.csv"
# response = requests.get(url)

# with open("Hotel_C_f(NEW).csv", "wb") as f:
#     f.write(response.content)
