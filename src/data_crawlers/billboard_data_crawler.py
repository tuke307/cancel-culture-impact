import requests
import csv
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH


def fetch_and_save_billboard_data():
    # Step 1: Read the valid dates from the provided JSON file
    valid_dates_url = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/valid_dates.json"
    response = requests.get(valid_dates_url)
    valid_dates = response.json()

    # Step 2: Filter the dates to include only those from 2015 until today
    start_date = datetime.strptime("2015-01-01", "%Y-%m-%d")
    end_date = datetime.today()
    filtered_dates = [
        date
        for date in valid_dates
        if start_date <= datetime.strptime(date, "%Y-%m-%d") <= end_date
    ]

    # Step 3: Fetch the chart data for each filtered date
    chart_data = []
    for date in filtered_dates:
        chart_url = f"https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/date/{date}.json"
        response = requests.get(chart_url)
        if response.status_code == 200:
            chart = response.json()
            for entry in chart["data"]:
                chart_data.append(
                    {
                        "date": chart["date"],
                        "song": entry["song"],
                        "artist": entry["artist"],
                        "this_week": entry["this_week"],
                        "last_week": entry.get("last_week", None),
                        "peak_position": entry["peak_position"],
                        "weeks_on_chart": entry["weeks_on_chart"],
                    }
                )

    # Step 4: Save the fetched data into a CSV file
    csv_file = os.path.join(RAW_DATA_PATH, "billboard_charts_2015_to_today.csv")
    csv_columns = [
        "date",
        "song",
        "artist",
        "this_week",
        "last_week",
        "peak_position",
        "weeks_on_chart",
    ]

    try:
        with open(csv_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in chart_data:
                writer.writerow(data)
        print(f"Data successfully saved to {csv_file}")
    except IOError:
        print("I/O error")


if __name__ == "__main__":
    fetch_and_save_billboard_data()
