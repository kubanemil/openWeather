import os

import pydantic
import pymongo
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from geopy.distance import distance

default_start = 1000000000
default_end = 1800000000
db = pymongo.MongoClient(os.environ["DB_HOST"])[os.environ["DB_NAME"]]
weather_data = db[os.environ["REPORTS_COLLECTION"]]
coordinates_collection = db[os.environ["COORDINATES_COLLECTION"]]

app = FastAPI()


class Report(pydantic.BaseModel):
    name: str
    timestamp: int
    last_modified_timestamp: int
    lat_lon: tuple[float, float]
    temperature: str
    pressure: str
    wind: str


class ReportGeo(Report):
    distance: float


@app.get("/id", response_model=list[Report])
async def get_reports(
    name: str,
    start: int = Query(default_start, ge=0, description="Start timestamp"),
    end: int = Query(default_end, ge=0, description="End timestamp"),
):
    query = {"name": name, "timestamp": {"$gte": start, "$lte": end}}

    reports = list(weather_data.find(query, {"_id": 0}))
    for report in reports:
        coords = coordinates_collection.find_one({"name": report["name"]}, {"_id": 0})
        report["lat_lon"] = (round(coords["lat"], 3), round(coords["long"], 3))
    return reports


@app.get("/geo", response_model=list[ReportGeo])
async def get_metar_by_geo(
    lat: float = Query(..., description="Latitude of the center"),
    lon: float = Query(..., description="Longitude of the center"),
    rad: int = Query(100_000, ge=0, description="Radius in meters"),
    start: int = Query(default_start, ge=0, description="Start timestamp"),
    end: int = Query(default_end, ge=0, description="End timestamp"),
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
        query = {"name": {"$in": icao_codes}, "timestamp": {"$gte": start, "$lte": end}}

        reports = list(weather_data.find(query, {"_id": 0}))
        filtered_reports = []
        for report in reports:
            coords = coordinates_collection.find_one(
                {"name": report["name"]}, {"_id": 0}
            )
            lat_lon = (round(coords["lat"], 3), round(coords["long"], 3))
            report["distance"] = estimate_distance((lat, lon), lat_lon)
            report["lat_lon"] = lat_lon
            if report["distance"] <= rad:
                filtered_reports.append(report)

        return filtered_reports

    except pymongo.errors.OperationFailure as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/count_reports")
async def get_reports_count():
    """Count of all reports in the database."""
    count = weather_data.count_documents({})
    return {"count": count}


@app.get("/coordinates")
async def get_coordinates(name: str):
    """Returns ICAO's coordinates"""
    results = list(coordinates_collection.find({"name": name}, {"_id": 0}))
    return results


@app.get("/last_report")
async def get_last_report():
    last_report = db["last_report"].find_one()
    last_report.pop("_id")
    return last_report


def find_bounding_coords(center_lat, center_lon, radius):
    dist = distance(kilometers=radius / 1000)

    min_lat = dist.destination((center_lat, center_lon), 180).latitude
    max_lat = dist.destination((center_lat, center_lon), 0).latitude
    min_lon = dist.destination((center_lat, center_lon), 270).longitude
    max_lon = dist.destination((center_lat, center_lon), 90).longitude

    return min_lat, max_lat, min_lon, max_lon


def estimate_distance(city1_coords, city2_coords):
    return distance(city1_coords, city2_coords).meters


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_level="debug",
        log_config=None,
        reload=True,
    )
