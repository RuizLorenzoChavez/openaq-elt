import asyncio
import aiohttp
import logging
import json
import time
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ========== GLOBAL VARS ==========
API_KEY = os.getenv("OPENAQ_API_KEY")
MEASUREMENT_LOG_PATH = Path(os.getenv("MEASUREMENT_LOG_PATH", "logs/measurement.log"))
MEASUREMENT_PATH = Path(os.getenv("MEASUREMENT_PATH", "data/measurements"))
SENSOR_REF_PATH = Path(os.getenv("SENSOR_REF_PATH", "log/sensor_id.jsonl"))

MAX_RETRIES = 5 
MAX_LIMIT = 1000
MAX_CONCURRENT_REQUESTS = 5  # Control concurrency level
RATE_LIMIT_DELAY = 60 / 60   # 1 request per second to stay under 60 req/min

# Ensure target folders exist prior to attached handlers
MEASUREMENT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
MEASUREMENT_PATH.mkdir(parents=True, exist_ok=True)

# ========== LOGGING ==========
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(MEASUREMENT_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========== ASYNC WORKERS ==========

async def fetch_page(session, sensor_id, dt_first, dt_last, page, semaphore, queue):
    """Fetches a single page of measurements for a given sensor."""
    logger.debug(f"Attempting to acquire semaphore for Sensor #{sensor_id} (Page {page})...")
    async with semaphore:
        logger.debug(f"Semaphore acquired for Sensor #{sensor_id} (Page {page}).")
        
        for retry in range(1, MAX_RETRIES + 1):
            try:
                logger.debug(f"PROCESS: Sensor #{sensor_id} Page {page} trial {retry} of {MAX_RETRIES}")
                
                logger.debug(f"Applying rate limit delay of {RATE_LIMIT_DELAY}s...")
                await asyncio.sleep(RATE_LIMIT_DELAY) 

                headers = {"X-API-Key": API_KEY} if API_KEY else {}
                params = {
                    "limit": MAX_LIMIT,
                    "page": page
                }
                if dt_first:
                    params["datetime_from"] = dt_first
                if dt_last:
                    params["datetime_to"] = dt_last
                
                measurements_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/hourly"

                logger.debug(f"Sending GET request to {measurements_url} (Page {page})...")
                async with session.get(measurements_url, params=params, headers=headers, timeout=15) as response:
                    logger.debug(f"Received response for Sensor #{sensor_id} (Page {page}) with status {response.status}.")
                    
                    used = int(response.headers.get('X-ratelimit-used', 0))
                    reset = int(response.headers.get('X-ratelimit-reset', 0))
                    logger.debug(f"Rate limit stats - Used: {used}, Reset in: {reset}s.")
                    
                    if response.status == 429 or used >= 55:
                        wait_time = reset if reset > 0 else 60
                        logger.warning(f"Rate limit hit or nearing limit (used: {used}). Waiting {wait_time}s.")
                        await asyncio.sleep(wait_time)
                        continue 

                    response.raise_for_status()
                    logger.debug(f"Parsing JSON response for Sensor #{sensor_id} (Page {page})...")
                    
                    response_text = await response.text()
                    if not response_text or not response_text.strip():
                        raise json.JSONDecodeError("Empty body", response_text, 0)
                        
                    response_json = json.loads(response_text)

                    logger.debug(f"Putting data for Sensor #{sensor_id} (Page {page}) into the writer queue...")
                    await queue.put({
                        "sensor_id": sensor_id,
                        "data": response_json,
                        "page": page
                    })
                    logger.debug(f"Successfully queued data for Sensor #{sensor_id} (Page {page}).")
                    return 

            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as err:
                logger.warning(f"PROCESS HALTED on Sensor #{sensor_id} Page {page} (Attempt {retry}): {err}")
                await asyncio.sleep(2 ** retry)

        logger.error(f"Failed to extract page #{page} for Sensor #{sensor_id} after {MAX_RETRIES} retries.")


async def process_sensor(session, sensor_id, dt_first, dt_last, semaphore, queue):
    """Determines how many pages a sensor has and schedules their extraction."""
    logger.debug(f"Starting process_sensor task for Sensor #{sensor_id}.")
    page = 1
    
    logger.debug(f"Attempting to acquire semaphore for Sensor #{sensor_id} initial metadata request...")
    async with semaphore:
        logger.debug(f"Semaphore acquired for Sensor #{sensor_id} initial request.")
        logger.debug(f"Applying rate limit delay of {RATE_LIMIT_DELAY}s...")
        await asyncio.sleep(RATE_LIMIT_DELAY)
        
        headers = {"X-API-Key": API_KEY} if API_KEY else {}
        params = {
            "limit": MAX_LIMIT,
            "page": page
        }
        if dt_first:
            params["datetime_from"] = dt_first
        if dt_last:
            params["datetime_to"] = dt_last
        
        measurements_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/hourly"
        
        try:
            logger.debug(f"Sending GET request for initial metadata: Sensor #{sensor_id}...")
            async with session.get(measurements_url, params=params, headers=headers, timeout=15) as response:
                 
                used = int(response.headers.get('X-ratelimit-used', 0))
                reset = int(response.headers.get('X-ratelimit-reset', 0))
                
                if response.status == 429 or used >= 55:
                    wait_time = reset if reset > 0 else 60
                    logger.warning(f"Rate limit hit on initial request. Waiting {wait_time}s.")
                    await asyncio.sleep(wait_time)
                    return 
                    
                response.raise_for_status()
                
                response_text = await response.text()
                if not response_text or not response_text.strip():
                    raise json.JSONDecodeError("Empty body", response_text, 0)
                response_json = json.loads(response_text)
                
                found = response_json.get('meta', {}).get('found', 0)
                if not isinstance(found, int):
                    found = 0
                
                logger.debug(f"Metadata received for Sensor #{sensor_id}: {found} total measurements found.")
                
                if found == 0:
                    logger.debug(f"No measurements found for Sensor #{sensor_id}. Skipping.")
                    return

                logger.debug(f"Putting initial page data for Sensor #{sensor_id} into queue...")
                await queue.put({
                    "sensor_id": sensor_id,
                    "data": response_json,
                    "page": page
                })

                total_pages = (found // MAX_LIMIT) + (1 if found % MAX_LIMIT > 0 else 0)
                logger.debug(f"Calculated {total_pages} total pages for Sensor #{sensor_id}.")
                
                tasks = []
                for p in range(2, total_pages + 1):
                    logger.debug(f"Scheduling task for Sensor #{sensor_id} (Page {p})...")
                    tasks.append(fetch_page(session, sensor_id, dt_first, dt_last, p, semaphore, queue))
                
                if tasks:
                    logger.debug(f"Awaiting {len(tasks)} sub-tasks for Sensor #{sensor_id}...")
                    await asyncio.gather(*tasks)
                    logger.debug(f"All sub-tasks for Sensor #{sensor_id} completed.")
                    
        except Exception as e:
            logger.error(f"Failed to fetch initial data for Sensor #{sensor_id}: {e}")


async def writer_worker(queue):
    """Reads from the queue and writes data to disk sequentially."""
    logger.debug("Writer worker initialized and waiting for items in queue.")
    write_count = 0
    files_touched = set()
    
    while True:
        item = await queue.get()
        if item is None: # Poison pill to stop the worker
            logger.debug("Writer worker received poison pill (None). Stopping queue processing.")
            queue.task_done()
            break
            
        sensor_id = item['sensor_id']
        page_num = item.get('page', 'Unknown')
        logger.debug(f"Writer worker pulled data for Sensor #{sensor_id} (Page {page_num}) from queue.")
        
        data = item['data']
        results = data.get('results', [])
        temp_file_path = MEASUREMENT_PATH / f"{sensor_id}.jsonl.tmp"
        
        files_touched.add(sensor_id)

        # Open, write chunk, and close immediately to prevent OS "too many open files" errors
        logger.debug(f"Opening temporary file for appending: {temp_file_path}")
        with open(temp_file_path, "a", encoding="utf-8") as file:
            logger.debug(f"Writing {len(results)} records for Sensor #{sensor_id}...")
            
            for result in results:
                period = result.get("period", {})
                parameter = result.get("parameter", {})
                
                measurement = {
                    "datetimeFrom": period.get("datetimeFrom", {}).get("utc"),
                    "datetimeTo": period.get("datetimeTo", {}).get("utc"),
                    "sensor_id": sensor_id,
                    "value": result.get("value"),
                    "param_name": parameter.get("name"),
                    "param_units": parameter.get("units")
                }
                
                json.dump(measurement, file)
                file.write("\n")
                write_count += 1
            
        logger.debug(f"Finished writing {len(results)} records for Sensor #{sensor_id} (Page {page_num}).")
        queue.task_done()

    logger.debug("Writer worker loop exited. Proceeding to finalize files.")
    for sid in files_touched:
        temp_path = MEASUREMENT_PATH / f"{sid}.jsonl.tmp"
        final_path = MEASUREMENT_PATH / f"{sid}.jsonl"
        if temp_path.exists():
            logger.debug(f"Renaming {temp_path} -> {final_path}")
            temp_path.replace(final_path)

    logger.debug(f"Writer worker completely finished. Total records written: {write_count}.")
    return write_count


async def run_extraction():
    logger.info("PROCESS STARTED: Async hourly measurement data extraction initiated.")
    START_TIME = time.perf_counter()

    # Collect sensor IDs and datetimes
    sensor_keys = set()
    logger.debug(f"Reading sensor keys from {SENSOR_REF_PATH}...")
    try:
        with open(SENSOR_REF_PATH, "r", encoding="utf-8") as keys:
            for key in keys:
                if key.strip():
                    try:
                        key_dict = json.loads(key)
                        # Check for both camelCase and snake_case depending on how the earlier file saved them
                        dt_first = key_dict.get("datetimeFirst") or key_dict.get("datetime_first")
                        dt_last = key_dict.get("datetimeLast") or key_dict.get("datetime_last")
                        
                        sensor_keys.add((
                            key_dict.get("sensor_id"),
                            dt_first,
                            dt_last
                        ))
                    except json.JSONDecodeError:
                        logger.debug(f"Skipping malformed line in reference file.")
                        continue
        logger.debug(f"Loaded {len(sensor_keys)} valid sensors from file.")
    except FileNotFoundError:
        logger.error(f"Sensor reference file not found at {SENSOR_REF_PATH}. Exiting.")
        return

    queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    logger.debug("Starting writer_worker task...")
    writer_task = asyncio.create_task(writer_worker(queue))

    logger.debug("Initializing aiohttp ClientSession...")
    async with aiohttp.ClientSession() as session:
        tasks = []
        for sensor_id, dt_first, dt_last in sensor_keys:
            if not sensor_id:
                logger.debug("Skipping invalid sensor entry (No ID).")
                continue
            
            # Ensure any old tmp files are removed before starting
            tmp_file = MEASUREMENT_PATH / f"{sensor_id}.jsonl.tmp"
            if tmp_file.exists():
                logger.debug(f"Found orphaned tmp file {tmp_file}. Removing...")
                tmp_file.unlink()
                
            tasks.append(process_sensor(session, sensor_id, dt_first, dt_last, semaphore, queue))
        
        logger.debug(f"Awaiting all {len(tasks)} primary sensor processing tasks...")
        await asyncio.gather(*tasks)
        logger.debug("All primary sensor processing tasks finished.")

    logger.debug("Waiting for the queue to be fully processed (queue.join())...")
    await queue.join()
    logger.debug("Queue is empty and fully processed.")
    
    logger.debug("Sending poison pill to writer worker...")
    await queue.put(None)
    write_count = await writer_task
    logger.debug("Writer worker returned successfully.")

    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 60 / 60 
    
    logger.debug("Calculating total volume on disk...")
    try:
        TOTAL_BYTES = sum(f.stat().st_size for f in MEASUREMENT_PATH.glob("*.jsonl")) / (1024 ** 3)
    except Exception as e:
        logger.debug(f"Error calculating disk volume: {e}")
        TOTAL_BYTES = 0

    logger.info(f"PROCESS SUMMARY: Extraction took {ELAPSED_TIME:.4f} hours")
    logger.info(f"PROCESS SUMMARY: Total volume on disk: {TOTAL_BYTES:.4f} GB")
    logger.info(f"PROCESS TERMINATED: {write_count} measurements written")


# ========== MAIN ==========
if __name__ == "__main__":
    asyncio.run(run_extraction())