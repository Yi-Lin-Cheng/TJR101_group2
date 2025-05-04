import os
from pathlib import Path

import googlemaps
import pandas as pd


def get_place_info(row, err_log, gmaps_client):
    id_open = row.Id
    name_open = row.Name
    region_open = row.Region
    town_open = row.Town
    add_open = row.Add
    query = f"{town_open} {name_open}"
    search_result = gmaps_client.places(query=query, language="zh-TW")

    if not search_result["results"]:
        err_log += f"{name_open} couldn't find the result.\n"
        return [], err_log

    info_sublist = []
    for place in search_result["results"]:
        location = place.get("geometry", {}).get("location", {})
        info = {
            "id_open": id_open,
            "name_open": name_open,
            "region_open": region_open,
            "town_open": town_open,
            "add_open": add_open,
            "place_id": place.get("place_id", ""),
            "s_name": place.get("name", ""),
            "rate": place.get("rating", None),
            "comm": place.get("user_ratings_total", None),
            "types": place.get("types", []),
            "address": place.get("formatted_address", ""),
            "lng": location.get("lng", ""),
            "lat": location.get("lat", ""),
            "business_status": place.get("business_status", ""),
        }
        info_sublist.append(info)

    return info_sublist, err_log


def main():
    file_path = Path("data", "spot")
    data = pd.read_csv(
        file_path / "spot02_added_rows_from_newdata_filtered.csv",
        encoding="utf-8",
        engine="python",
    )

    API_KEY = os.getenv("MySQL_host")
    gmaps_client = googlemaps.Client(key=API_KEY)
    info_list = []
    err_log = ""
    for row in data.itertuples():
        info, err_log = get_place_info(row, err_log, gmaps_client)
        info_list += info
        print(info)

    info_df = pd.DataFrame(info_list)
    info_df.to_csv(
        file_path / "spot03_googleapi_newdata.csv",
        encoding="utf-8",
        header=True,
        index=False,
    )
    with open(
        file_path / "spot03_googleapi_newdata_err_log.txt", "a", encoding="utf-8"
    ) as f:
        f.write(err_log)
