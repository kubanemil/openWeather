import logging
import os

import pymongo
import requests

logging.basicConfig(level=logging.INFO)
URL = "http://www.moratech.com/aviation/metar-stations.txt"

db = pymongo.MongoClient(os.environ["DB_HOST"])[os.environ["DB_NAME"]]
coordinates_collection = db[os.environ["COORDINATES_COLLECTION"]]
stations_fn = "metar-stations.txt"


def get_file():
    if os.path.exists(stations_fn):
        logging.debug("File already exists.")
        return

    response = requests.get(URL)
    if response.status_code == 200:
        with open(stations_fn, "w") as file:
            file.write(response.text)
        logging.debug("File downloaded successfully.")
        return
    raise Exception(f"Failed to download the file. Status code: {response.status_code}")


def convert_to_decimal_degrees(coord_string):
    degrees, minutes = coord_string.split()
    hemisphere = minutes[-1]
    minutes = minutes[:-1]

    decimal_degrees = float(degrees) + float(minutes) / 60

    if hemisphere in ("S", "W"):
        decimal_degrees *= -1

    return decimal_degrees


def parse_coordinates():
    get_file()
    with open(stations_fn, "r") as file:
        lines = file.readlines()[44:]

    bulk_ops = []
    for line in lines:
        icao = line[19:25].strip()
        lat = line[38:46].strip()
        long = line[46:55].strip()
        if not (len(icao) == 4 and len(lat) == 6 and len(long) == 7):
            logging.debug(
                f"Couldn't parse: icao: '{icao}', lat: '{lat}', long: '{long}'"
            )
            continue
        lat = convert_to_decimal_degrees(lat)
        long = convert_to_decimal_degrees(long)
        doc = {"name": icao, "lat": lat, "long": long}
        bulk_ops.append(
            pymongo.UpdateOne(
                {"name": icao},
                {"$setOnInsert": doc},
                upsert=True,
            )
        )
    coordinates_collection.bulk_write(bulk_ops)


if __name__ == "__main__":
    parse_coordinates()
    logging.info("Coordinates parse complete.")
