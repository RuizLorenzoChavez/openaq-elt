from dotenv import load_dotenv
from pathlib import Path

import requests
import logging
import json
import math
import time
import os

load_dotenv()

# ========== GLOBAL VARS ==========
API_KEY = os.getenv("OPENAQ_API_KEY")
MEASUREMENT_LOG_PATH = Path(os.getenv("MEASUREMENT_LOG_PATH"))
MEASUREMENT_PATH = Path(os.getenv("MEASUREMENT_PATH"))
MEASUREMENT_ERROR_PATH = Path(os.getenv("MEASUREMENT_ERROR_PATH"))
SENSOR_PATH = Path(os.getenv("SENSOR_PATH"))
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

file_handler = logging.FileHandler(MEASUREMENT_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel("INFO")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========== SCRIPT ==========
def extract_measurements(sensor_id, datetime_first):
                
    #   requesting measurement data per sensor from OpenAQ API
    MEASUREMENTS_URL = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/daily"
    
    # REVISION: Safe metadata fetch with timeout and exception guard
    try:
        response = requests.get(url=MEASUREMENTS_URL,
                                params={"datetime_from": datetime_first, "limit": 1},
                                headers={"X-API-Key": API_KEY},
                                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT))
        response.raise_for_status()
        
        # REVISION: Check response body before calling .json()
        if not response.text or not response.text.strip():
            logger.warning(f"PROCESS HALTED: Blank response body from sensor #{sensor_id}")
            BROKEN_KEYS.append((sensor_id, 0))
            return None
            
        initial_json = response.json()
    except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as err:
        logger.warning(f"PROCESS HALTED: Unable to fetch metadata for sensor #{sensor_id}: {err}")
        logger.warning(f"PROCESS CHECKPOINT: Unable to extract {sensor_id}. Saving to {MEASUREMENT_ERROR_PATH}")
        BROKEN_KEYS.append((sensor_id, 0))
        return None
         
    #   identifying number of pages
    responses = initial_json.get("meta", {}).get("found", 0)
    
    if responses == 0:
        return None

    pages = math.ceil(responses / 1000)
    logger.debug(f"CHECKPOINT: Total number of responses retrieved: {responses} ({pages} pages)")
    
    for page in range(1, pages+1):
        
        logger.debug(f"CHECKPOINT: Extracting page {page}/{pages} of sensor #{sensor_id}")
    
        for retry in range(1, MAX_RETRIES):
            
            logger.debug(f"PROCESS: Sensor #{sensor_id} extraction trial {retry} of {MAX_RETRIES - 1}")
            
            try:
                result = requests.get(url=MEASUREMENTS_URL,
                            params={"datetime_from": datetime_first,
                                    "limit": 1000,
                                    "page": page},
                            headers={"X-API-Key": API_KEY},
                            timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT * retry))
                
                # REVISION: Handle explicit rate limit (HTTP 429) before raising
                if result.status_code == 429:
                    sleep_time = int(result.headers.get('Retry-After', 15))
                    logger.debug(f"RATE LIMIT REACHED (429): Sleeping for {sleep_time}s")
                    time.sleep(sleep_time)
                    continue

                #   raising error if requests is unsuccessful
                result.raise_for_status()
                
                #   logging how much requests is left at a given time
                used = int(result.headers.get('X-ratelimit-used', 0))
                limit = int(result.headers.get('X-ratelimit-limit', 0))
                reset = int(result.headers.get('X-ratelimit-reset', 0))
                
                #   slowing down as the API reaches its limit
                if used > 30:
                    time.sleep(15)
                
                logger.debug(f"CHECKPOINT: Requesting measurement data ({used}/{limit} resets in {reset})")
                
                # REVISION: Guard against empty string body prior to .json()
                if not result.text or not result.text.strip():
                    raise json.decoder.JSONDecodeError("Empty body", result.text, 0)

                #   converting request output to JSON
                result_json = result.json()

                #   checking queried results amount
                response_num = result_json.get('meta', {}).get('found')
                logger.debug(f"CHECKPOINT: Total number of sensors retrieved: {response_num}")

                #   extracting measurement-level query results 
                results = result_json.get('results', [])

                for item in results:
                    
                    logger.debug(f"PROCESS: Extracting {sensor_id} {item.get('period', {}).get('datetimeFrom', {}).get('utc', None)}")
                    
                    hourly_data = {}
                    
                    hourly_data["datetimeFrom"] = item.get("period", {}).get("datetimeFrom", {}).get("utc")
                    hourly_data["datetimeTo"] = item.get("period", {}).get("datetimeTo", {}).get("utc")
                    hourly_data["sensor_id"] = sensor_id
                    hourly_data["value"] = item.get("value")
                    hourly_data["param_name"] = item.get("parameter",{}).get("name")
                    hourly_data["param_units"] = item.get("parameter", {}).get("units")
                    
                    yield hourly_data
                
                break
            
            except requests.exceptions.HTTPError as http_err:
                logger.warning(f"PROCESS HALTED: HTTP error occurred: {http_err}")
                time.sleep(2 ** retry)
                
                if retry == 5:
                    logger.warning(f"PROCESS CHECKPOINT: Unable to extract {sensor_id}. Saving to {MEASUREMENT_ERROR_PATH}")
                    BROKEN_KEYS.append((sensor_id, page))    
                
            except requests.exceptions.ConnectionError as conn_err:
                logger.warning(f"PROCESS HALTED: Connection error occurred: {conn_err}")
                time.sleep(2 ** retry)
                
            except requests.exceptions.Timeout as timeout_err:
                logger.warning(f"PROCESS HALTED: Timeout error occurred: {timeout_err}")
                time.sleep(2 ** retry)
                
            # REVISION: Catch JSONDecodeError in inner retry loop
            except json.decoder.JSONDecodeError as json_err:
                logger.warning(f"PROCESS HALTED: JSON decode error occurred: {json_err}")
                time.sleep(2 ** retry)

            except requests.exceptions.RequestException as err:
                logger.warning(f"PROCESS HALTED An error occurred: {err}")
                time.sleep(2 ** retry)
        
    return None
            

def main():
    logger.info(f"PROCESS STARTED: Daily measurement data extraction initiated.")
    START_TIME = time.perf_counter()
    
    if not MEASUREMENT_PATH.exists():
        MEASUREMENT_PATH.touch()
    
    #   extracting existing measurements to avoid writing duplicate entries
    with open(MEASUREMENT_PATH, "r") as existing:
        
        reference = set()
        
        for row in existing:
            if row.strip():
                try:
                    row_dict = json.loads(row)
                    reference.add((row_dict.get("sensor_id", 0), row_dict.get("datetimeFrom", None)))
                except json.JSONDecodeError:
                    continue
    
    #   collecting sensor IDs as reference
    with open(SENSOR_PATH, "r") as keys:
        
        sensor_keys = set()
        
        for key in keys:
            if key.strip():
                try:
                    key_dict = json.loads(key)
                    sensor_keys.add((key_dict.get("sensor_id", 0), key_dict.get("datetimeFrom", None)))
                except json.JSONDecodeError:
                    continue
    
    #   extracting measurment data from each sensor
    with open(MEASUREMENT_PATH, "a") as file:
        
        EXTRACT_COUNT = 0
        WRITE_COUNT = 0
        
        for sensor_key in sensor_keys:  
            measurements = extract_measurements(sensor_key[0], sensor_key[1])

            if measurements is None:
                continue

            for measurement in measurements:
                
                measurement_key = (measurement.get("sensor_id", 0), measurement.get("datetimeFrom", None))
                
                if measurement_key not in reference:

                    logger.debug(f"PROCESS: Writing {measurement.get('sensor_id', None)} into {MEASUREMENT_PATH}")
                    json.dump(measurement, file)
                    file.write("\n")
                    
                    #   Update memory reference to prevent duplicates in current batch
                    reference.add(measurement_key)
                
                    WRITE_COUNT += 1
                
                EXTRACT_COUNT += 1
    
    #   writing broken location links to a file
    with open(MEASUREMENT_ERROR_PATH, "w") as error:
        keys_str = '\n'.join((str(key) for key in BROKEN_KEYS))
        error.write(keys_str)
    
    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 60
    
    logger.info(f"PROCESS SUMMARY: {EXTRACT_COUNT} extracted measurements")
    logger.info(f"PROCESS SUMMARY: {len(BROKEN_KEYS)} locations unable to be extracted")
    logger.info(f"PROCESS SUMMARY: Extraction process took {ELAPSED_TIME} minutes")
    
    if not WRITE_COUNT:
        logger.info(f"PROCESS TERMINATED: No new measurement written")
    else:
        logger.info(f"PROCESS TERMINATED: {WRITE_COUNT} measurements extracted and written")
    
# ========== MAIN ==========

if __name__ == "__main__":
    main()