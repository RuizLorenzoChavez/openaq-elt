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

MAX_RETRIES = 5
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

async def fetch_page(session, location_id, page, semaphore, queue):
    """Fetches a single page of sensors for a given location."""
    logger.debug(f"Attempting to acquire semaphore for Location #{location_id} (Page {page})...")
    async with semaphore:
        logger.debug(f"Semaphore acquired for Location #{location_id} (Page {page}).")
        
        for retry in range(1, MAX_RETRIES + 1):
            try:
                logger.debug(f"PROCESS: Location #{location_id} Page {page} trial {retry} of {MAX_RETRIES}")
                
                logger.debug(f"Applying rate limit delay of {RATE_LIMIT_DELAY}s...")
                await asyncio.sleep(RATE_LIMIT_DELAY) 

                headers = {"X-API-Key": API_KEY} if API_KEY else {}
                params = {
                    "order_by": "id",
                    "limit": MAX_LIMIT,
                    "page": page
                }
                
                sensors_url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"

                logger.debug(f"Sending GET request to {sensors_url} (Page {page})...")
                async with session.get(sensors_url, params=params, headers=headers, timeout=15) as response:
                    logger.debug(f"Received response for Location #{location_id} (Page {page}) with status {response.status}.")
                    
                    used = int(response.headers.get('X-ratelimit-used', 0))
                    reset = int(response.headers.get('X-ratelimit-reset', 0))
                    logger.debug(f"Rate limit stats - Used: {used}, Reset in: {reset}s.")
                    
                    if response.status == 429 or used >= 55:
                        wait_time = reset if reset > 0 else 60
                        logger.warning(f"Rate limit hit or nearing limit (used: {used}). Waiting {wait_time}s.")
                        await asyncio.sleep(wait_time)
                        continue 

                    response.raise_for_status()
                    logger.debug(f"Parsing JSON response for Location #{location_id} (Page {page})...")
                    response_json = await response.json()

                    logger.debug(f"Putting data for Location #{location_id} (Page {page}) into the writer queue...")
                    await queue.put({
                        "location_id": location_id,
                        "data": response_json,
                        "page": page
                    })
                    logger.debug(f"Successfully queued data for Location #{location_id} (Page {page}).")
                    return 

            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as err:
                logger.warning(f"PROCESS HALTED on Location #{location_id} Page {page} (Attempt {retry}): {err}")
                await asyncio.sleep(2 ** retry)

        logger.error(f"Failed to extract page #{page} for Location #{location_id} after {MAX_RETRIES} retries.")

async def process_location(session, location_id, semaphore, queue):
    """Determines how many pages a location has and schedules their extraction."""
    logger.debug(f"Starting process_location task for Location #{location_id}.")
    page = 1
    
    logger.debug(f"Attempting to acquire semaphore for Location #{location_id} initial metadata request...")
    async with semaphore:
        logger.debug(f"Semaphore acquired for Location #{location_id} initial request.")
        logger.debug(f"Applying rate limit delay of {RATE_LIMIT_DELAY}s...")
        await asyncio.sleep(RATE_LIMIT_DELAY)
        
        headers = {"X-API-Key": API_KEY} if API_KEY else {}
        params = {
            "order_by": "id",
            "limit": MAX_LIMIT,
            "page": page
        }
        
        sensors_url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"
        
        try:
            logger.debug(f"Sending GET request for initial metadata: Location #{location_id}...")
            async with session.get(sensors_url, params=params, headers=headers, timeout=15) as response:
                 
                used = int(response.headers.get('X-ratelimit-used', 0))
                reset = int(response.headers.get('X-ratelimit-reset', 0))
                
                if response.status == 429 or used >= 55:
                    wait_time = reset if reset > 0 else 60
                    logger.warning(f"Rate limit hit on initial request. Waiting {wait_time}s.")
                    await asyncio.sleep(wait_time)
                    return 
                    
                response.raise_for_status()
                response_json = await response.json()
                
                found = response_json.get('meta', {}).get('found', 0)
                if not isinstance(found, int):
                    found = 0
                
                logger.debug(f"Metadata received for Location #{location_id}: {found} total sensors found.")
                
                if found == 0:
                    logger.debug(f"No sensors found for Location #{location_id}. Skipping.")
                    return

                logger.debug(f"Putting initial page data for Location #{location_id} into queue...")
                await queue.put({
                    "location_id": location_id,
                    "data": response_json,
                    "page": page
                })

                total_pages = (found // MAX_LIMIT) + (1 if found % MAX_LIMIT > 0 else 0)
                logger.debug(f"Calculated {total_pages} total pages for Location #{location_id}.")
                
                tasks = []
                for p in range(2, total_pages + 1):
                    logger.debug(f"Scheduling task for Location #{location_id} (Page {p})...")
                    tasks.append(fetch_page(session, location_id, p, semaphore, queue))
                
                if tasks:
                    logger.debug(f"Awaiting {len(tasks)} sub-tasks for Location #{location_id}...")
                    await asyncio.gather(*tasks)
                    logger.debug(f"All sub-tasks for Location #{location_id} completed.")
                    
        except Exception as e:
            logger.error(f"Failed to fetch initial data for Location #{location_id}: {e}")

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
        SENSOR_PATH.mkdir(parents=True)

    # Collect location IDs
    location_keys = set()
    logger.debug(f"Reading location keys from {LOCATION_REF_PATH}...")
    try:
        with open(LOCATION_REF_PATH, "r") as keys:
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