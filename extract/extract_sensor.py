import asyncio
import aiohttp
import json
import logging
import time
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ========== GLOBAL VARS ==========
API_KEY = os.getenv("OPENAQ_API_KEY")
SENSOR_LOG_PATH = Path(os.getenv("SENSOR_LOG_PATH", "sensors.log"))
SENSOR_PATH = Path(os.getenv("SENSOR_PATH", "sensors"))
SENSOR_REF_PATH = Path(os.getenv("SENSOR_REF_PATH", "sensor_ref.jsonl"))
LOCATION_REF_PATH = Path(os.getenv("LOCATION_REF_PATH", "location_ref.jsonl"))

MAX_RETRIES = 6
MAX_LIMIT = 1000
MAX_CONCURRENT_REQUESTS = 5  # Control concurrency level
RATE_LIMIT_DELAY = 60 / 60   # 1 request per second to stay under 60 req/min

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

file_handler = logging.FileHandler(SENSOR_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========== ASYNC WORKERS ==========

async def process_location(session, location_id, semaphore, queue):
    """Fetches all pages of sensors for a given location sequentially using metadata-driven termination."""
    logger.debug(f"Starting process_location task for Location #{location_id}.")
    page = 1
    extracted = 0
    
    while True:
        success = False
        response_json = {}
        
        # Retry loop for this specific page
        for retry in range(1, MAX_RETRIES):
            logger.debug(f"PROCESS: Location #{location_id} page #{page} trial {retry} of {MAX_RETRIES - 1}")
            rate_limit_wait = 0
            
            try:
                # 1. Acquire semaphore just for the request
                async with semaphore:
                    await asyncio.sleep(RATE_LIMIT_DELAY) 

                    headers = {"X-API-Key": API_KEY} if API_KEY else {}
                    params = {
                        "order_by": "id",
                        "limit": MAX_LIMIT,
                        "page": page
                    }
                    sensors_url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"

                    logger.debug(f"Fetching Location #{location_id} Page {page}...")
                    async with session.get(sensors_url, params=params, headers=headers, timeout=15) as response:
                        
                        if response.status == 429:
                            sleep_time = int(response.headers.get('X-ratelimit-reset', 60))
                            logger.warning(f"RATE LIMIT REACHED (429) for Location #{location_id}: Sleeping for {sleep_time}s")
                            rate_limit_wait = sleep_time
                        else:
                            response.raise_for_status()
                            
                            remaining = int(response.headers.get('X-ratelimit-remaining', 100))
                            if remaining < 5:
                                reset_sec = int(response.headers.get('X-ratelimit-reset', 15))
                                logger.info(f"Rate limit reaching capacity for Location #{location_id}. Waiting {reset_sec}s...")
                                rate_limit_wait = reset_sec

                            response_text = await response.text()
                            if not response_text or not response_text.strip():
                                raise json.JSONDecodeError("Empty body", response_text, 0)
                                
                            response_json = json.loads(response_text)
                            success = True
                            break # Success, break out of retry loop
                
                # 2. Handle rate limit outside semaphore if triggered
                if rate_limit_wait > 0:
                    await asyncio.sleep(rate_limit_wait)
                    continue

            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as err:
                logger.warning(f"PROCESS HALTED on Location #{location_id} Page {page} (Attempt {retry}): {err}")
                await asyncio.sleep(2 ** retry)

        if not success:
            logger.error(f"Failed to extract page #{page} for Location #{location_id} after {MAX_RETRIES - 1} retries. Aborting.")
            break

        found = response_json.get('meta', {}).get('found', 0)
        results = response_json.get('results', [])
        
        if not results:
            logger.debug(f"No results returned for Location #{location_id} on page #{page}. Stopping.")
            break

        # Queue valid items and track count
        extracted += len(results)
        await queue.put({
            "location_id": location_id,
            "data": response_json,
            "page": page
        })

        if found and extracted >= found:
            logger.debug(f"Extracted all {extracted}/{found} records for Location #{location_id}.")
            break
            
        page += 1

async def writer_worker(queue, sensor_ids_list):
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
            
        location_id = item['location_id']
        page_num = item.get('page', 'Unknown')
        logger.debug(f"Writer worker pulled data for Location #{location_id} (Page {page_num}) from queue.")
        
        data = item['data']
        results = data.get('results', [])
        temp_file_path = SENSOR_PATH / f"sensor{location_id}.jsonl.tmp"
        
        files_touched.add(location_id)

        # Open, write chunk, and close immediately to prevent OS "too many open files" errors
        logger.debug(f"Opening temporary file for appending: {temp_file_path}")
        with open(temp_file_path, "a", encoding="utf-8") as file:
            logger.debug(f"Writing {len(results)} records for Location #{location_id}...")
            
            for result in results:
                sensor = {
                    "loc_id": location_id,
                    "sensor_id": result.get("id"),
                    "sensor_name": result.get("name"),
                    "param_name": result.get("parameter", {}).get("name"),
                    "param_units": result.get("parameter", {}).get("units"),
                    "datetimeFirst": (result.get("datetimeFirst") or {}).get("utc"),
                    "datetimeLast": (result.get("datetimeLast") or {}).get("utc")
                }
                
                json.dump(sensor, file)
                file.write("\n")
                sensor_ids_list.append({
                    "sensor_id": sensor["sensor_id"],
                    "datetime_first": sensor["datetimeFirst"],
                    "datetime_last": sensor["datetimeLast"]
                })
                write_count += 1
            
        logger.debug(f"Finished writing {len(results)} records for Location #{location_id} (Page {page_num}).")
        queue.task_done()

    logger.debug("Writer worker loop exited. Proceeding to finalize files.")
    for loc_id in files_touched:
        temp_path = SENSOR_PATH / f"sensor{loc_id}.jsonl.tmp"
        final_path = SENSOR_PATH / f"sensor{loc_id}.jsonl"
        if temp_path.exists():
            logger.debug(f"Renaming {temp_path} -> {final_path}")
            temp_path.replace(final_path)

    logger.debug(f"Writer worker completely finished. Total records written: {write_count}.")
    return write_count

async def run_extraction():
    logger.info(f"PROCESS STARTED: Async sensor data extraction initiated.")
    START_TIME = time.perf_counter()

    logger.debug(f"Checking if SENSOR_PATH ({SENSOR_PATH}) exists...")
    if not SENSOR_PATH.exists():
        logger.debug(f"SENSOR_PATH missing. Creating directories...")
        SENSOR_PATH.mkdir(parents=True, exist_ok=True)

    # Collect location IDs
    location_keys = set()
    logger.debug(f"Reading location keys from {LOCATION_REF_PATH}...")
    try:
        with open(LOCATION_REF_PATH, "r", encoding="utf-8") as keys:
            for key in keys:
                if key.strip():
                    key_dict = json.loads(key)
                    location_keys.add(key_dict.get("loc_id", 0))
        logger.debug(f"Loaded {len(location_keys)} valid locations from file.")
    except FileNotFoundError:
        logger.error(f"Location file not found at {LOCATION_REF_PATH}. Exiting.")
        return

    SENSOR_IDS = []
    queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    logger.debug("Starting writer_worker task...")
    writer_task = asyncio.create_task(writer_worker(queue, SENSOR_IDS))

    logger.debug("Initializing aiohttp ClientSession...")
    async with aiohttp.ClientSession() as session:
        tasks = []
        for location_id in location_keys:
            if not location_id:
                logger.debug(f"Skipping invalid location entry: ID {location_id}")
                continue
            
            # Ensure any old tmp files are removed before starting
            tmp_file = SENSOR_PATH / f"sensor{location_id}.jsonl.tmp"
            if tmp_file.exists():
                logger.debug(f"Found orphaned tmp file {tmp_file}. Removing...")
                tmp_file.unlink()
                
            tasks.append(process_location(session, location_id, semaphore, queue))
        
        logger.debug(f"Awaiting all {len(tasks)} primary location processing tasks...")
        await asyncio.gather(*tasks)
        logger.debug("All primary location processing tasks finished.")

    logger.debug("Waiting for the queue to be fully processed (queue.join())...")
    await queue.join()
    logger.debug("Queue is empty and fully processed.")
    
    logger.debug("Sending poison pill to writer worker...")
    await queue.put(None)
    write_count = await writer_task
    logger.debug("Writer worker returned successfully.")

    logger.debug(f"Writing reference file to {SENSOR_REF_PATH}...")
    with open(SENSOR_REF_PATH, "w", encoding="utf-8") as ref:
        for sensor_dict in SENSOR_IDS:
            json.dump(sensor_dict, ref)
            ref.write("\n")
    logger.debug("Reference file created successfully.")

    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 60 / 60
    
    logger.debug("Calculating total volume on disk...")
    try:
        TOTAL_BYTES = sum(f.stat().st_size for f in SENSOR_PATH.glob("*.jsonl")) / (1024 ** 3)
    except Exception as e:
        logger.debug(f"Error calculating disk volume: {e}")
        TOTAL_BYTES = 0

    logger.info(f"PROCESS SUMMARY: Extraction took {ELAPSED_TIME:.4f} hours")
    logger.info(f"PROCESS SUMMARY: Total volume on disk: {TOTAL_BYTES:.4f} GB")
    logger.info(f"PROCESS TERMINATED: {write_count} sensors written")

# ========== MAIN ==========
if __name__ == "__main__":
    asyncio.run(run_extraction())