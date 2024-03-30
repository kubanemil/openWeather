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

logging.info("Trying to connect to the database...")

db = pymongo.MongoClient("mongo")["db"]
last_report_collection = db["last_report"]
weather_data = db["weather_data"]
weather_data.create_index([("name", 1), ("timestamp", -1)], unique=True)

logging.info("Connected to the database.")


def get_new_reports(latest_report, url: str) -> list:
    reports = []
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")

        if not table:
            raise Exception("No table found on the page.")

        rows = table.find_all("tr")
        for row in rows:
            report = [col.text.strip() for col in row.find_all("td")[:2]]
            if len(report) > 0 and "txt" in report[0].lower():
                reports.append(report)
                if report == latest_report:
                    break  # parsed all the new reports at this point
        logging.info(f"Number of new reports: {len(reports)}")
        return reports
    raise Exception("Failed to retrieve the webpage.")


async def fetch_url(session: aiohttp.ClientSession, report: list[str, str]):
    report_url = URL + report[0]
    async with session.get(report_url) as response:
        try:
            content = await response.read()
            return report[0], report[1], content.decode("latin-1")
        except Exception as e:
            print(e)


def result_to_docs(result: list[str]) -> dict:
    timestamp = datetime.strptime(result[1], "%d-%b-%Y %H:%M").timestamp()
    content = result[2].split("\n")
    return {
        "name": result[0].split(".")[0],
        "timestamp": timestamp,
        "date_str": content[1],
    }


async def retrieve_and_insert_reports(new_reports: list):
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[fetch_url(session, report) for report in new_reports]
        )

    docs = [result_to_docs(result) for result in results]
    bulk_ops = [
        pymongo.UpdateOne(
            {"name": doc["name"], "timestamp": doc["timestamp"]},
            {"$setOnInsert": doc},
            upsert=True,
        )
        for doc in docs
    ]
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
        asyncio.run(retrieve_and_insert_reports(new_reports))
        logging.info(f"Done in {round(time.time() - start, 3)} seconds.")
        time.sleep(CHECK_PERIOD)
