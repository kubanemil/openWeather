import logging

import pymongo
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from geopy.distance import distance

logging.basicConfig(level=logging.DEBUG)
db = pymongo.MongoClient("mongo")["db"]
last_report_collection = db["last_report"]
weather_data = db["weather_data"]
coordinates_collection = db["coordinates"]

app = FastAPI()


def find_bounding_coords(center_lat, center_lon, radius):
    dist = distance(kilometers=radius / 1000)

    min_lat = dist.destination((center_lat, center_lon), 180).latitude
    max_lat = dist.destination((center_lat, center_lon), 0).latitude
    min_lon = dist.destination((center_lat, center_lon), 270).longitude
    max_lon = dist.destination((center_lat, center_lon), 90).longitude

    return min_lat, max_lat, min_lon, max_lon


@app.get("/id")
async def get_reports(
    name: str,
    start: int = Query(None, description="Start timestamp"),
    end: int = Query(None, description="End timestamp"),
):
    query = {"name": name}
    if start and end:
        query["last_modified_timestamp"] = {"$gte": start, "$lte": end}

    return list(weather_data.find(query, {"_id": 0}))


@app.get("/geo")
async def get_metar_by_geo(
    lat: float = Query(..., description="Latitude of the center"),
    lon: float = Query(..., description="Longitude of the center"),
    rad: int = Query(..., ge=0, description="Radius in meters"),
    start: int = Query(None, description="Start timestamp"),
    end: int = Query(None, description="End timestamp"),
):
    try:
        min_lat, max_lat, min_lon, max_lon = find_bounding_coords(lat, lon, rad)

        matching_stations = coordinates_collection.find(
            {
                "lat": {"$gte": min_lat, "$lte": max_lat},
                "long": {"$gte": min_lon, "$lte": max_lon},
            },
            projection={"name": 1, "_id": 0},
        )

        icao_codes = [station["name"] for station in matching_stations]
        query = {"name": {"$in": icao_codes}}
        if start and end:
            query["last_modified_timestamp"] = {"$gte": start, "$lte": end}

        return list(weather_data.find(query, {"_id": 0}))

    except pymongo.errors.OperationFailure as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/count_reports")
async def get_reports_count():
    """Count of all reports in the database."""
    count = weather_data.count_documents({})
    return {"count": count}


@app.get("/coordinates")
async def get_coordinates(name: str):
    results = list(coordinates_collection.find({"icao": name}, {"_id": 0}))
    return results


@app.get("/last_report")
async def get_last_report():
    last_report = last_report_collection.find_one()
    logging.info(f"Last report: {last_report}")
    last_report.pop("_id")
    return last_report


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_level="debug",
        log_config=None,
        reload=True,
    )
