from pathlib import Path

import requests

from utils import encoding_transform

data_dir = Path("data", "hotel")


def main():
    latest_file = data_dir / "accomo01_open_data_raw_latest.csv"
    previous_file = data_dir / "accomo01_open_data_raw_previous.csv"
    url = "https://media.taiwan.net.tw/XMLReleaseALL_public/Hotel_C_f.csv"
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
