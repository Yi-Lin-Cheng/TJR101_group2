from pathlib import Path


def get_data_path(filename: str, folder: str = "./data") -> Path:
    """取得指定資料夾下的完整檔案路徑。"""

    data_folder = Path(folder)
    return data_folder / filename


def data_mkdir(folder: str):
    Path(folder).mkdir(parents=True, exist_ok=True)
