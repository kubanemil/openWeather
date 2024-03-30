import asyncio
import logging
import time
from datetime import datetime

import aiohttp
import pymongo
import requests
from bs4 import BeautifulSoup
from coordinates import parse_coordinates

logging.basicConfig(level=logging.DEBUG)

CHECK_PERIOD = 5  # seconds
URL = "https://tgftp.nws.noaa.gov/data/observations/metar/decoded/"
DATE_SORTED_URL = URL + "?C=M;O=D"

db = pymongo.MongoClient("mongo")["db"]
last_report_collection = db["last_report"]
weather_data = db["weather_data"]
weather_data.create_index([("name", 1), ("last_modified_timestamp", -1)], unique=True)


def get_new_reports(latest_report, url: str) -> list:
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to retrieve the webpage.")

    reports = []
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table")

    if table is None:
        raise Exception("No table found on the page.")

    rows = table.find_all("tr")
    for row in rows:
        report = [col.text.strip() for col in row.find_all("td")[:2]]
        if len(report) > 0 and "txt" in report[0].lower():
            if report == latest_report:
                break  # parsed all the new reports at this point
            reports.append(report)
    return reports


def parse_report_content(report_content):
    lines = report_content.split("\n")

    location = lines[0].strip()
    datetime_str = lines[1].split("/")[-1].strip()
    timestamp = datetime.strptime(datetime_str, "%Y.%m.%d %H%M %Z").timestamp()

    weather_info = {"loc": location, "timestamp": timestamp}

    for line in lines[2:]:
        line = line.strip()
        if line:
            parts = line.split(":", 1)
            key = parts[0].strip().lower()
            value = parts[1].strip()
            weather_info[key] = value
    return weather_info


async def fetch_report_details(session: aiohttp.ClientSession, report: list[str, str]):
    report_url = URL + report[0]
    try:
        async with session.get(report_url) as response:
            content = await response.read()
        last_modified_timestamp = datetime.strptime(
            report[1], "%d-%b-%Y %H:%M"
        ).timestamp()
        weather_info = parse_report_content(content.decode("latin-1"))
        doc = {
            "name": report[0].split(".")[0],
            "last_modified_timestamp": last_modified_timestamp,
        }
        doc.update(weather_info)
        return doc
    except Exception as e:
        logging.exception(e)


async def retrieve_and_insert_reports(new_reports: list):
    async with aiohttp.ClientSession() as session:
        logging.info("Fetching new reports...")
        docs = await asyncio.gather(
            *[fetch_report_details(session, report) for report in new_reports]
        )

    bulk_ops = [
        pymongo.UpdateOne(
            {
                "name": doc["name"],
                "last_modified_timestamp": doc["last_modified_timestamp"],
            },
            {"$setOnInsert": doc},
            upsert=True,
        )
        for doc in docs
        if doc is not None
    ]
    logging.info("Bulking new reports...")
    weather_data.bulk_write(bulk_ops)

    last_report_collection.delete_many({})
    last_report_collection.insert_one(
        {"name": new_reports[0][0], "date": new_reports[0][1]}
    )


if __name__ == "__main__":
    parse_coordinates()
    logging.info("Coordinates parsed.")

    while True:
        start = time.time()

        last_report = last_report_collection.find_one() or {"name": "", "date": ""}
        new_reports = get_new_reports(
            [last_report["name"], last_report["date"]], DATE_SORTED_URL
        )
        logging.info(f"Number of new reports: {len(new_reports)}")

        if new_reports:
            asyncio.run(retrieve_and_insert_reports(new_reports))

        logging.info(f"Done in {round(time.time() - start, 3)} seconds.")
        time.sleep(CHECK_PERIOD)
