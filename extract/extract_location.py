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
LOCATION_LOG_PATH = Path(os.getenv("LOCATION_LOG_PATH", "locations.log"))
LOCATION_PATH = Path(os.getenv("LOCATION_PATH", "locations"))
LOCATION_REF_PATH = Path(os.getenv("LOCATION_REF_PATH", "location_ref.jsonl"))
LOCATIONS_URL = os.getenv("LOCATIONS_URL", "https://api.openaq.org/v3/locations")
COUNTRY_PATH = Path(os.getenv("COUNTRY_PATH", "countries.jsonl"))

MAX_RETRIES = 5
MAX_LIMIT = 1000
MAX_CONCURRENT_REQUESTS = 5  # Adjust this to control concurrency level
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

file_handler = logging.FileHandler(LOCATION_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========== ASYNC WORKERS ==========

async def fetch_page(session, country_id, country_name, page, semaphore, queue):
    """Fetches a single page of locations for a given country."""
    logger.debug(f"Attempting to acquire semaphore for {country_name} (Page {page})...")
    async with semaphore:
        logger.debug(f"Semaphore acquired for {country_name} (Page {page}).")
        
        for retry in range(1, MAX_RETRIES + 1):
            try:
                logger.debug(f"PROCESS: Country #{country_id} Page {page} trial {retry} of {MAX_RETRIES}")
                
                # Respect rate limit manually if needed
                logger.debug(f"Applying rate limit delay of {RATE_LIMIT_DELAY}s...")
                await asyncio.sleep(RATE_LIMIT_DELAY) 

                headers = {"X-API-Key": API_KEY} if API_KEY else {}
                params = {
                    "order_by": "id",
                    "countries_id": country_id,
                    "limit": MAX_LIMIT,
                    "page": page
                }

                logger.debug(f"Sending GET request to {LOCATIONS_URL} for {country_name} (Page {page})...")
                async with session.get(LOCATIONS_URL, params=params, headers=headers, timeout=15) as response:
                    logger.debug(f"Received response for {country_name} (Page {page}) with status {response.status}.")
                    
                    # Check Rate Limits Headers
                    used = int(response.headers.get('X-ratelimit-used', 0))
                    reset = int(response.headers.get('X-ratelimit-reset', 0))
                    logger.debug(f"Rate limit stats - Used: {used}, Reset in: {reset}s.")
                    
                    if response.status == 429 or used >= 55:
                        wait_time = reset if reset > 0 else 60
                        logger.warning(f"Rate limit hit or nearing limit (used: {used}). Waiting {wait_time}s.")
                        await asyncio.sleep(wait_time)
                        continue # Retry after waiting

                    response.raise_for_status()
                    logger.debug(f"Parsing JSON response for {country_name} (Page {page})...")
                    response_json = await response.json()

                    # Put data in queue to be written
                    logger.debug(f"Putting data for {country_name} (Page {page}) into the writer queue...")
                    await queue.put({
                        "country_name": country_name,
                        "data": response_json,
                        "page": page
                    })
                    logger.debug(f"Successfully queued data for {country_name} (Page {page}).")
                    return # Success

            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as err:
                logger.warning(f"PROCESS HALTED on country #{country_id} Page {page} (Attempt {retry}): {err}")
                await asyncio.sleep(2 ** retry)

        logger.error(f"Failed to extract page #{page} for country #{country_id} after {MAX_RETRIES} retries.")

async def process_country(session, country_id, country_name, semaphore, queue):
    """Determines how many pages a country has and schedules their extraction."""
    logger.debug(f"Starting process_country task for {country_name} (ID: {country_id}).")
    page = 1
    
    logger.debug(f"Attempting to acquire semaphore for {country_name} initial metadata request...")
    async with semaphore:
        logger.debug(f"Semaphore acquired for {country_name} initial request.")
        logger.debug(f"Applying rate limit delay of {RATE_LIMIT_DELAY}s...")
        await asyncio.sleep(RATE_LIMIT_DELAY)
        
        headers = {"X-API-Key": API_KEY} if API_KEY else {}
        params = {
            "order_by": "id",
            "countries_id": country_id,
            "limit": MAX_LIMIT,
            "page": page
        }
        
        try:
            logger.debug(f"Sending GET request for initial metadata: {country_name}...")
            async with session.get(LOCATIONS_URL, params=params, headers=headers, timeout=15) as response:
                 
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
                
                logger.debug(f"Metadata received for {country_name}: {found} total locations found.")
                
                # Queue the first page's data immediately since we already fetched it
                logger.debug(f"Putting initial page data for {country_name} into queue...")
                await queue.put({
                    "country_name": country_name,
                    "data": response_json,
                    "page": page
                })

                # Calculate remaining pages and schedule tasks for them
                total_pages = (found // MAX_LIMIT) + (1 if found % MAX_LIMIT > 0 else 0)
                logger.debug(f"Calculated {total_pages} total pages for {country_name}.")
                
                tasks = []
                for p in range(2, total_pages + 1):
                    logger.debug(f"Scheduling task for {country_name} (Page {p})...")
                    tasks.append(fetch_page(session, country_id, country_name, p, semaphore, queue))
                
                if tasks:
                    logger.debug(f"Awaiting {len(tasks)} sub-tasks for {country_name}...")
                    await asyncio.gather(*tasks)
                    logger.debug(f"All sub-tasks for {country_name} completed.")
                    
        except Exception as e:
            logger.error(f"Failed to fetch initial data for country {country_name}: {e}")

async def writer_worker(queue, loc_ids_list):
    """Reads from the queue and writes data to disk."""
    logger.debug("Writer worker initialized and waiting for items in queue.")
    write_count = 0
    files = {} 
    
    while True:
        item = await queue.get()
        if item is None: # Poison pill to stop the worker
            logger.debug("Writer worker received poison pill (None). Stopping queue processing.")
            queue.task_done()
            break
            
        country_name = item['country_name']
        page_num = item.get('page', 'Unknown')
        logger.debug(f"Writer worker pulled data for {country_name} (Page {page_num}) from queue.")
        
        data = item['data']
        results = data.get('results', [])
        temp_file_path = LOCATION_PATH / f"{country_name}.jsonl.tmp"
        
        if country_name not in files:
            logger.debug(f"Opening temporary file for appending: {temp_file_path}")
            files[country_name] = open(temp_file_path, "a", encoding="utf-8")
            
        file = files[country_name]

        logger.debug(f"Writing {len(results)} records for {country_name}...")
        for result in results:
            country = result.get("country", {})
            coords = result.get("coordinates", {})

            location = {
                "loc_id": result.get("id"),
                "loc_name": result.get("name"),
                "country_id": country.get("id"),
                "country_name": country.get("name"),
                "latitude": coords.get("latitude"),
                "longitude": coords.get("longitude"),
                "bounds": result.get("bounds")
            }
            
            json.dump(location, file)
            file.write("\n")
            loc_ids_list.append({"loc_id": location.get("loc_id")})
            write_count += 1
            
        logger.debug(f"Finished writing {len(results)} records for {country_name} (Page {page_num}).")
        queue.task_done()

    logger.debug("Writer worker loop exited. Proceeding to finalize files.")
    for name, file in files.items():
        logger.debug(f"Closing file for {name}...")
        file.close()
        temp_path = LOCATION_PATH / f"{name}.jsonl.tmp"
        final_path = LOCATION_PATH / f"{name}.jsonl"
        logger.debug(f"Renaming {temp_path} -> {final_path}")
        temp_path.replace(final_path)

    logger.debug(f"Writer worker completely finished. Total records written: {write_count}.")
    return write_count

async def run_extraction():
    logger.info(f"PROCESS STARTED: Async location data extraction initiated.")
    START_TIME = time.perf_counter()

    logger.debug(f"Checking if LOCATION_PATH ({LOCATION_PATH}) exists...")
    if not LOCATION_PATH.exists():
        logger.debug(f"LOCATION_PATH missing. Creating directories...")
        LOCATION_PATH.mkdir(parents=True)

    # Collect country IDs
    country_keys = set()
    logger.debug(f"Reading country keys from {COUNTRY_PATH}...")
    try:
        with open(COUNTRY_PATH, "r") as keys:
            for key in keys:
                if key.strip():
                    key_dict = json.loads(key)
                    country_keys.add((key_dict.get("id", 0), key_dict.get("code")))
        logger.debug(f"Loaded {len(country_keys)} valid countries from file.")
    except FileNotFoundError:
        logger.error(f"Country file not found at {COUNTRY_PATH}. Exiting.")
        return

    LOC_IDS = []
    queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    logger.debug("Starting writer_worker task...")
    writer_task = asyncio.create_task(writer_worker(queue, LOC_IDS))

    logger.debug("Initializing aiohttp ClientSession...")
    async with aiohttp.ClientSession() as session:
        tasks = []
        for country_id, country_name in country_keys:
            if not country_id or not country_name:
                logger.debug(f"Skipping invalid country entry: ID {country_id}, Name {country_name}")
                continue
            
            # Ensure any old tmp files are removed before starting
            tmp_file = LOCATION_PATH / f"{country_name}.jsonl.tmp"
            if tmp_file.exists():
                logger.debug(f"Found orphaned tmp file {tmp_file}. Removing...")
                tmp_file.unlink()
                
            tasks.append(process_country(session, country_id, country_name, semaphore, queue))
        
        logger.debug(f"Awaiting all {len(tasks)} primary country processing tasks...")
        await asyncio.gather(*tasks)
        logger.debug("All primary country processing tasks finished.")

    logger.debug("Waiting for the queue to be fully processed (queue.join())...")
    await queue.join()
    logger.debug("Queue is empty and fully processed.")
    
    logger.debug("Sending poison pill to writer worker...")
    await queue.put(None)
    write_count = await writer_task
    logger.debug("Writer worker returned successfully.")

    logger.debug(f"Writing reference file to {LOCATION_REF_PATH}...")
    with open(LOCATION_REF_PATH, "w", encoding="utf-8") as ref:
        for loc_id in LOC_IDS:
            json.dump(loc_id, ref)
            ref.write("\n")
    logger.debug("Reference file created successfully.")

    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 60 
    
    logger.debug("Calculating total volume on disk...")
    try:
        TOTAL_BYTES = sum(f.stat().st_size for f in LOCATION_PATH.glob("*.jsonl")) / (1024 ** 3)
    except Exception as e:
        logger.debug(f"Error calculating disk volume: {e}")
        TOTAL_BYTES = 0

    logger.info(f"PROCESS SUMMARY: Extraction took {ELAPSED_TIME:.4f} minutes")
    logger.info(f"PROCESS SUMMARY: Total volume on disk: {TOTAL_BYTES:.4f} GB")
    logger.info(f"PROCESS TERMINATED: {write_count} locations written")

# ========== MAIN ==========
if __name__ == "__main__":
    asyncio.run(run_extraction())