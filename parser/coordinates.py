import logging
import os

import pymongo
import requests

logging.basicConfig(level=logging.INFO)
URL = "http://www.moratech.com/aviation/metar-stations.txt"

logging.info("Trying to connect to db in coordinates...")

db = pymongo.MongoClient("mongo")["db"]
coordinates_collection = db["coordinates"]

logging.info("Connected to the db!")


def get_file():
    if os.path.exists("metar-stations.txt"):
        logging.debug("File already exists.")
        return

    response = requests.get(URL)

    if response.status_code == 200:
        text_content = response.text
        with open("metar-stations.txt", "w") as file:
            file.write(text_content)
        logging.debug("File downloaded successfully.")
    else:
        raise Exception(
            f"Failed to download the file. Status code: {response.status_code}"
        )


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
    with open("metar-stations.txt", "r") as file:
        lines = file.readlines()[44:]
        bulk_ops = []
        for line in lines:
            icao = line[19:25].strip()
            lat = line[38:46].strip()
            long = line[46:55].strip()
            if len(icao) == 4 and len(lat) == 6 and len(long) == 7:
                lat = convert_to_decimal_degrees(lat)
                long = convert_to_decimal_degrees(long)
                doc = {"icao": icao, "lat": lat, "long": long}
                bulk_ops.append(
                    pymongo.UpdateOne(
                        {"icao": icao},
                        {"$setOnInsert": doc},
                        upsert=True,
                    )
                )
            else:
                logging.debug(
                    f"Couldn't parse: icao: '{icao}', lat: '{lat}', long: '{long}'"
                )

        coordinates_collection.bulk_write(bulk_ops)


if __name__ == "__main__":
    parse_coordinates()
    print("Done.")
