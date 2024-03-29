import pymongo
import requests
from bs4 import BeautifulSoup

URL = 'https://tgftp.nws.noaa.gov/data/observations/metar/decoded/?C=M;O=D'
client = pymongo.MongoClient("mongodb://localhost:27017/")  
db = client["db"]  
weather_data = db["reports"] 
weather_data.create_index("icao_code", unique=True)


def get_new_rows():
    latest_dt = db.get_latest_dt()
    dt_sorted_webpage = requests.get('https://tgftp.nws.noaa.gov/data/observations/metar/decoded/?C=M;O=D')
    new_rows = []
    for row in dt_sorted_webpage:
        if row.last_dt <= latest_dt: # what if new row with row.last_dt == latest_dt?
            break 
        new_rows.append(row)
    
    return new_rows


def parse_table(latest_station, url: str) -> list:
    stations = []
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        
        if not table:
            raise Exception("No table found on the page.")

        rows = table.find_all('tr')

        for row in rows[:50]:
            station = [col.text.strip() for col in row.find_all('td')[:2]]
            if len(station) > 0 and 'txt' in station[0].lower():
                stations.append(station)
                print(station)
                if station == latest_station:
                    break # parsed all the new stations at this point
        return stations
    raise Exception("Failed to retrieve the webpage.")


if __name__ == "__main__":
    parse_table(['KLYH.TXT', '29-Mar-2024 12:55', '385'], URL)