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
SENSOR_LOG_PATH = Path(os.getenv("SENSOR_LOG_PATH"))
SENSOR_PATH = Path(os.getenv("SENSOR_PATH"))
SENSOR_ERROR_PATH = Path(os.getenv("SENSOR_ERROR_PATH"))
LOCATION_PATH = Path(os.getenv("LOCATION_PATH"))
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

file_handler = logging.FileHandler(SENSOR_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel("INFO")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========== SCRIPT ==========        
def extract_sensors(location_id):
    
    for retry in range(1, MAX_RETRIES):
            
        try:
            logger.debug(f"PROCESS: Location #{location_id} extraction trial {retry} of {MAX_RETRIES - 1} ")
            
            #   requesting location data per country from OpenAQ API
            SENSORS_URL = f"https://api.openaq.org/v3/locations/{location_id}/sensors"
            response = requests.get(url=SENSORS_URL,
                                   params={"order_by": "id",
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
            
            logger.debug(f"CHECKPOINT: Requesting sensor data ({used}/{limit} resets in {reset})")
            
            #   converting request output to JSON
            response_json = response.json()

            #   checking queried results amount
            response_num = response_json.get('meta', {}).get('found')
            logger.info(f"CHECKPOINT: Total number of sensors retrieved: {response_num}")

            #   extracting country-level query results 
            results = response_json.get('results', [])

            for result in results:
                
                logger.info(f"PROCESS: Extracting {result.get('name', None)}")
                
                sensor = {}
                
                sensor["loc_id"] = location_id
                sensor["sensor_id"] =  result.get("id", None)
                sensor["sensor_name"] = result.get("name", None)
                sensor["param_name"] = result.get("parameter", {}).get("name", None)
                sensor["param_units"] = result.get("parameter", {}).get("units", None)
                
                try:
                    sensor["datetimeFirst"] = result.get("datetimeFirst", {}).get("utc", None)
                    sensor["datetimeLast"] = result.get("datetimeLast", {}).get("utc", None)
                    
                except AttributeError:
                    sensor["datetimeFirst"] = None
                    sensor["datetimeLast"] = None

                yield sensor
            
            break
        
        except requests.exceptions.HTTPError as http_err:
            logger.warning(f"PROCESS HALTED: HTTP error occurred: {http_err}")
            time.sleep(2 ** retry)
            
            if retry == 5:
                logger.warning(f"PROCESS CHECKPOINT: Unable to extract {location_id}. Saving to {SENSOR_ERROR_PATH}")
                BROKEN_KEYS.append(location_id)    
            
        except requests.exceptions.ConnectionError as conn_err:
            logger.warning(f"PROCESS HALTED: Connection error occurred: {conn_err}")
            time.sleep(2 ** retry)
            
        except requests.exceptions.Timeout as timeout_err:
            logger.warning(f"PROCESS HALTED: Timeout error occurred: {timeout_err}")
            time.sleep(2 ** retry)
            
        except requests.exceptions.RequestException as err:
            logger.warning(f"PROCESS HALTED An error occurred: {err}")
            time.sleep(2 ** retry)
    
    return None
            

def main():
    logger.info(f"PROCESS STARTED: Sensor data extraction initiated.")
    START_TIME = time.perf_counter()
    
    if not SENSOR_PATH.exists():
        SENSOR_PATH.touch()
    
    #   extracting existing sensor ID to avoid writing duplicate entries
    with open(SENSOR_PATH, "r") as existing:
        
        reference = set()
        
        for row in existing:
            if row.strip():
                row_dict = json.loads(row)
                reference.add((row_dict.get("sensor_id", 0), row_dict.get("datetimeFirst", None)))
    
    #   collecting location IDs as reference
    with open(LOCATION_PATH, "r") as keys:
        
        location_keys = set()
        
        for key in keys:
            if key.strip():
                key_dict = json.loads(key)
                location_keys.add(key_dict.get("loc_id", 0))
    
    #   extracting seensor data from each location
    with open(SENSOR_PATH, "a") as file:
        
        EXTRACT_COUNT = 0
        WRITE_COUNT = 0
        
        for location_key in location_keys:  
            sensors = extract_sensors(location_key)

            for sensor in sensors:
                
                sensor_key = (sensor.get("sensor_id", 0), sensor.get("datetimeFirst", None))
                
                if sensor_key not in reference:

                    logger.info(f"PROCESS: Writing {sensor.get('sensor_name', None)} into {SENSOR_PATH}")
                    json.dump(sensor, file)
                    file.write("\n")
                
                    WRITE_COUNT += 1
                
                EXTRACT_COUNT += 1
    
    #   writing broken location links to a file
    with open(SENSOR_ERROR_PATH, "w") as error:
        keys_str = '\n'.join((str(key) for key in BROKEN_KEYS))
        error.write(keys_str)
    
    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 60
    
    logger.info(f"PROCESS SUMMARY: {EXTRACT_COUNT} extracted sensors")
    logger.info(f"PROCESS SUMMARY: {len(BROKEN_KEYS)} locations unable to be extracted")
    logger.info(f"PROCESS SUMMARY: Extraction process took {ELAPSED_TIME} minutes")
    
    if not WRITE_COUNT:
        logger.info(f"PROCESS TERMINATED: No new sensors written")
    else:
        logger.info(f"PROCESS TERMINATED: {WRITE_COUNT} sensors extracted and written")
    
# ========== MAIN ==========

if __name__ == "__main__":
    main()