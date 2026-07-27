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
MEASUREMENT_LOG_PATH = Path(os.getenv("MEASUREMENT_LOG_PATH", "logs/measurement.log"))
MEASUREMENT_PATH = Path(os.getenv("MEASUREMENT_PATH", "data/measurements"))
SENSOR_PATH = Path(os.getenv("SENSOR_PATH", "sensors.jsonl"))

MAX_RETRIES = 6 
CONNECTION_TIMEOUT = 3
READ_TIMEOUT = 15

# Ensure target folders exist prior to attached handlers
MEASUREMENT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
MEASUREMENT_PATH.mkdir(parents=True, exist_ok=True)

# ========== LOGGING ==========
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(MEASUREMENT_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# ========== SCRIPT ==========
def extract_measurements(sensor_id, datetime_first, datetime_last):
    MEASUREMENTS_URL = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/hourly"
    headers = {"X-API-Key": API_KEY}
    params = {}
    if datetime_first:
        params["datetime_from"] = datetime_first
    if datetime_last:
        params["datetime_to"] = datetime_last

    try:
        response = requests.get(
            url=MEASUREMENTS_URL,
            params={**params, "limit": 1},
            headers=headers,
            timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT)
        )
        response.raise_for_status()
        
        if not response.text or not response.text.strip():
            logger.warning(f"PROCESS HALTED: Blank response body from sensor #{sensor_id}")
            return  # Early exit stops execution on blank response
            
        initial_json = response.json()
        
    except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as err:
        logger.warning(f"PROCESS HALTED: Unable to fetch metadata for sensor #{sensor_id}: {err}")
        return  # Early exit prevents UnboundLocalError on initial_json

    responses = initial_json.get("meta", {}).get("found", 0)
    if responses == 0:
        logger.info(f"No measurements found for sensor #{sensor_id}")
        return

    pages = math.ceil(responses / 1000)
    logger.debug(f"CHECKPOINT: Total responses for sensor #{sensor_id}: {responses} ({pages} pages)")
    
    for page in range(1, pages + 1):
        logger.debug(f"CHECKPOINT: Extracting page {page}/{pages} of sensor #{sensor_id}")
    
        for retry in range(1, MAX_RETRIES):
            logger.debug(f"PROCESS: Sensor #{sensor_id} trial {retry} of {MAX_RETRIES - 1}")
            
            try:
                result = requests.get(
                    url=MEASUREMENTS_URL,
                    params={**params, "limit": 1000, "page": page},
                    headers=headers,
                    timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT * retry)
                )
                
                if result.status_code == 429:
                    sleep_time = int(result.headers.get('Retry-After', 15))
                    logger.warning(f"RATE LIMIT REACHED (429): Sleeping for {sleep_time}s")
                    time.sleep(sleep_time)
                    continue

                result.raise_for_status()
                
                remaining = int(result.headers.get('X-ratelimit-remaining', 100))
                if remaining < 5:
                    reset_sec = int(result.headers.get('X-ratelimit-reset', 15))
                    logger.info(f"Rate limit reaching capacity. Waiting {reset_sec}s...")
                    time.sleep(reset_sec)

                if not result.text or not result.text.strip():
                    raise json.decoder.JSONDecodeError("Empty body", result.text, 0)

                result_json = result.json()
                results = result_json.get('results', [])

                for item in results:
                    period = item.get("period", {})
                    parameter = item.get("parameter", {})
                    
                    yield {
                        "datetimeFrom": period.get("datetimeFrom", {}).get("utc"),
                        "datetimeTo": period.get("datetimeTo", {}).get("utc"),
                        "sensor_id": sensor_id,
                        "value": item.get("value"),
                        "param_name": parameter.get("name"),
                        "param_units": parameter.get("units")
                    }
                break  # Successful page extraction
            
            except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as err:
                logger.warning(f"PROCESS HALTED on sensor #{sensor_id} (Attempt {retry}): {err}")
                time.sleep(2 ** retry)


def main():
    logger.info("PROCESS STARTED: Hourly measurement data extraction initiated.")
    START_TIME = time.perf_counter()
    
    if not SENSOR_PATH.exists():
        logger.error(f"Sensor file missing at {SENSOR_PATH}")
        return

    sensor_keys = set()
    with open(SENSOR_PATH, "r", encoding="utf-8") as keys:
        for key in keys:
            if key.strip():
                try:
                    key_dict = json.loads(key)
                    sensor_keys.add((
                        key_dict.get("sensor_id"),
                        key_dict.get("datetimeFirst"),
                        key_dict.get("datetimeLast")
                    ))
                except json.JSONDecodeError:
                    continue
    
    WRITE_COUNT = 0
    
    for sensor_id, dt_first, dt_last in sensor_keys:
        if not sensor_id:
            continue
            
        final_file = MEASUREMENT_PATH / f"{sensor_id}.jsonl"
        temp_file = MEASUREMENT_PATH / f"{sensor_id}.jsonl.tmp"
        
        extracted_sensor_count = 0
        
        # IDEMPOTENCY FIX: Write to temp file first
        with open(temp_file, "w", encoding="utf-8") as file:
            for measurement in extract_measurements(sensor_id, dt_first, dt_last):
                json.dump(measurement, file)
                file.write("\n")
                extracted_sensor_count += 1

        # Only replace existing file if data was successfully extracted
        if extracted_sensor_count > 0:
            temp_file.replace(final_file)
            WRITE_COUNT += extracted_sensor_count
        else:
            if temp_file.exists():
                temp_file.unlink()  # Clean up empty temp file

    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 60
    
    # Calculate actual cumulative file size across directory
    TOTAL_BYTES = sum(f.stat().st_size for f in MEASUREMENT_PATH.glob("*.jsonl"))
    
    logger.info(f"PROCESS SUMMARY: Extraction took {ELAPSED_TIME:.2f} minutes")
    logger.info(f"PROCESS SUMMARY: Total volume on disk: {TOTAL_BYTES} bytes")
    logger.info(f"PROCESS TERMINATED: {WRITE_COUNT} measurements written")


if __name__ == "__main__":
    main()