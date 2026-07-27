from dotenv import load_dotenv
from pathlib import Path

import requests
import logging
import json
import time
import os

load_dotenv()

# ========== GLOBAL VARS ==========
API_KEY = os.getenv("OPENAQ_API_KEY")
LOCATION_LOG_PATH = Path(os.getenv("LOCATION_LOG_PATH"))
LOCATION_PATH = Path(os.getenv("LOCATION_PATH"))
LOCATION_ERROR_PATH = Path(os.getenv("LOCATION_ERROR PATH"))
LOCATIONS_URL = os.getenv("LOCATIONS_URL")
COUNTRY_PATH = Path(os.getenv("COUNTRY_PATH"))
MAX_RETRIES = 6     #?  x5 retries + 1 to account for Python indexing 
MAX_LIMIT = 1000
CONNECTION_TIMEOUT = 3
READ_TIMEOUT = 15
BROKEN_KEYS = []

# ========== LOGGING ==========
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")
formatter = logging.Formatter(
    "{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M"
)

console_handler = logging.StreamHandler()
console_handler.setLevel("DEBUG")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(LOCATION_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel("INFO")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========== SCRIPT ==========
def extract_locations(country_id):
    
    for retry in range(1, MAX_RETRIES):
            
        try:
            logger.debug(f"PROCESS: Country #{country_id} extraction trial {retry} of {MAX_RETRIES - 1} ")
            
            #   requesting location data per country from OpenAQ API
            response = requests.get(url=LOCATIONS_URL,
                                   params={"order_by": "id",
                                           "countries_id":country_id,
                                           "limit": MAX_LIMIT},
                                   headers={"X-API-Key": API_KEY},
                                   timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT * retry))
            
            #   raising error if requests is unsuccessful
            response.raise_for_status()
            
            #   logging how much requests is left at a given time
            used = int(response.headers.get('X-ratelimit-used', 0))
            limit = int(response.headers.get('X-ratelimit-limit', 0))
            reset = int(response.headers.get('X-ratelimit-reset', 0))
            
            #   slowing down as the API reaches its limit
            if used > 55:
                time.sleep(10)
            
            logger.debug(f"CHECKPOINT: Requesting location data ({used}/{limit} resets in {reset})")
            
            #   converting request output to JSON
            response_json = response.json()

            #   checking queried results amount
            response_num = response_json.get('meta', {}).get('found')
            logger.debug(f"CHECKPOINT: Total number of locations retrieved: {response_num}")

            #   extracting country-level query results 
            results = response_json.get('results', [])

            for result in results:
                
                logger.debug(f"PROCESS: Extracting {result.get('name', None)}")
                
                location = {}
            
                location["loc_id"] = result.get("id")
                location["loc_name"] = result.get("name")
                location["country_id"] = result.get("country", {}).get("id")
                location["country_name"] = result.get("country", {}).get("name")
                location["latitude"] = result.get("coordinates", {}).get("latitude")
                location["longitude"] = result.get("coordinates", {}).get("longitude")
                location["bounds"] = result.get("bounds")
                        
                yield location
            
            break
        
        except requests.exceptions.HTTPError as http_err:
            logger.warning(f"PROCESS HALTED: HTTP error occurred: {http_err}")
            time.sleep(2 ** retry)
            
            if retry == 5:
                logger.warning(f"PROCESS CHECKPOINT: Unable to extract {country_id}. Saving to {LOCATION_ERROR_PATH}")
                BROKEN_KEYS.append(country_id)
            
        except requests.exceptions.ConnectionError as conn_err:
            logger.warning(f"PROCESS HALTED: Connection error occurred: {conn_err}")
            time.sleep(2 ** retry)
            
        except requests.exceptions.Timeout as timeout_err:
            logger.warning(f"PROCESS HALTED: Timeout error occurred: {timeout_err}")
            time.sleep(2 ** retry)
            
        except requests.exceptions.RequestException as err:
            logger.warning(f"PROCESS HALTED An error occurred: {err}")
            time.sleep(2 ** retry)


def main():
    logger.info(f"PROCESS STARTED: Location data extraction initiated.")
    START_TIME = time.perf_counter()
    
    if not LOCATION_PATH.exists():
        LOCATION_PATH.touch()
    
    #   extracting existing location ID to avoid writing duplicate entries
    with open(LOCATION_PATH, "r") as existing:
        
        reference = set()
        
        for row in existing:
            if row.strip():
                row_dict = json.loads(row)
                reference.add(row_dict.get("loc_id", 0))
    #   collecting country IDs as reference
    with open(COUNTRY_PATH, "r") as keys:
        
        country_keys = set()
        
        for key in keys:
            if key.strip():
                key_dict = json.loads(key)
                country_keys.add(key_dict.get("id", 0))
    
    #   extracting location data from each country
    with open(LOCATION_PATH, "a") as file:
        
        EXTRACT_COUNT = 0
        WRITE_COUNT = 0
        
        for country_key in country_keys:  
            locations = extract_locations(country_key)

            for location in locations:
                
                location_id = location.get("loc_id", 0)
                
                if location_id not in reference:

                    logger.debug(f"PROCESS: Writing {location.get('loc_name', None)} into {LOCATION_PATH}")
                    json.dump(location, file)
                    file.write("\n")
                
                    WRITE_COUNT += 1
                
                EXTRACT_COUNT += 1
    
    #   writing broken location links to a file
    with open(LOCATION_ERROR_PATH, "w") as error:
        keys_str = '\n'.join((str(key) for key in BROKEN_KEYS))
        error.write(keys_str)
    
    END_TIME = time.perf_counter()
    ELAPSED_TIME = END_TIME - START_TIME
    
    logger.info(f"PROCESS SUMMARY: {EXTRACT_COUNT} extracted locations")
    logger.info(f"PROCESS SUMMARY: {len(BROKEN_KEYS)} countries unable to be extracted")
    logger.info(f"PROCESS SUMMARY: Extraction process took {ELAPSED_TIME} seconds")
    
    if not WRITE_COUNT:
        logger.info(f"PROCESS TERMINATED: No new locations written")
    else:
        logger.info(f"PROCESS TERMINATED: {WRITE_COUNT} locations extracted and written")
    
# ========== MAIN ==========

if __name__ == "__main__":
    main()