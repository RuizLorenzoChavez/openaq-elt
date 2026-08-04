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

async def process_country(session, country_id, country_name, semaphore, queue):
    """Fetches all pages for a given country sequentially until results are exhausted."""
    logger.debug(f"Starting process_country task for {country_name} (ID: {country_id}).")
    page = 1
    
    while True:
        success = False
        response_json = {}
        
        # Retry loop for this specific page
        for retry in range(1, MAX_RETRIES + 1):
            rate_limit_wait = 0
            try:
                # 1. Acquire semaphore just for the request
                async with semaphore:
                    await asyncio.sleep(RATE_LIMIT_DELAY) 

                    headers = {"X-API-Key": API_KEY} if API_KEY else {}
                    params = {
                        "order_by": "id",
                        "countries_id": country_id,
                        "limit": MAX_LIMIT,
                        "page": page
                    }

                    logger.debug(f"Fetching {country_name} Page {page}...")
                    async with session.get(LOCATIONS_URL, params=params, headers=headers, timeout=15) as response:
                        used = int(response.headers.get('X-ratelimit-used', 0))
                        reset = int(response.headers.get('X-ratelimit-reset', 0))
                        
                        # Check rate limits BEFORE parsing
                        if response.status == 429 or used >= 55:
                            rate_limit_wait = reset if reset > 0 else 60
                        else:
                            response.raise_for_status()
                            response_json = await response.json()
                            success = True
                            break # Success, break out of retry loop
                
                # 2. Release semaphore BEFORE sleeping if rate limited
                if rate_limit_wait > 0:
                    logger.warning(f"Rate limit hit/near for {country_name} Page {page}. Waiting {rate_limit_wait}s.")
                    await asyncio.sleep(rate_limit_wait)
                    continue

            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as err:
                logger.warning(f"PROCESS HALTED on country #{country_id} Page {page} (Attempt {retry}): {err}")
                await asyncio.sleep(2 ** retry)

        if not success:
            logger.error(f"Failed to extract page #{page} for country {country_name} after {MAX_RETRIES} retries. Aborting country.")
            break

        results = response_json.get('results', [])
        
        # Only queue if we actually have data
        if results:
            await queue.put({
                "country_name": country_name,
                "data": response_json,
                "page": page
            })

        # If the API returned fewer items than our limit, we have reached the last page
        if len(results) < MAX_LIMIT:
            logger.debug(f"Reached final page for {country_name}. Total pages extracted: {page}")
            break
            
        page += 1

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
        
        data = item['data']
        results = data.get('results', [])
        temp_file_path = LOCATION_PATH / f"{country_name}.jsonl.tmp"
        
        if country_name not in files:
            files[country_name] = open(temp_file_path, "a", encoding="utf-8")
            
        file = files[country_name]

        logger.debug(f"Writing {len(results)} records for {country_name} (Page {page_num})...")
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
            
        queue.task_done()

    logger.debug("Writer worker loop exited. Proceeding to finalize files.")
    for name, file in files.items():
        file.close()
        temp_path = LOCATION_PATH / f"{name}.jsonl.tmp"
        final_path = LOCATION_PATH / f"{name}.jsonl"
        if temp_path.exists():
            temp_path.replace(final_path)

    logger.debug(f"Writer worker completely finished. Total records written: {write_count}.")
    return write_count

async def run_extraction():
    logger.info(f"PROCESS STARTED: Async location data extraction initiated.")
    START_TIME = time.perf_counter()

    if not LOCATION_PATH.exists():
        LOCATION_PATH.mkdir(parents=True)

    # Collect country IDs
    country_keys = set()
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
    
    writer_task = asyncio.create_task(writer_worker(queue, LOC_IDS))

    async with aiohttp.ClientSession() as session:
        tasks = []
        for country_id, country_name in country_keys:
            if not country_id or not country_name:
                continue
            
            # Ensure any old tmp files are removed before starting
            tmp_file = LOCATION_PATH / f"{country_name}.jsonl.tmp"
            if tmp_file.exists():
                tmp_file.unlink()
                
            tasks.append(process_country(session, country_id, country_name, semaphore, queue))
        
        # This safely executes up to MAX_CONCURRENT_REQUESTS HTTP calls at a time
        await asyncio.gather(*tasks)

    # Wait for the queue to flush
    await queue.join()
    
    # Send poison pill to the worker to close out the files
    await queue.put(None)
    write_count = await writer_task

    with open(LOCATION_REF_PATH, "w", encoding="utf-8") as ref:
        for loc_id in LOC_IDS:
            json.dump(loc_id, ref)
            ref.write("\n")

    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 60 
    
    try:
        TOTAL_BYTES = sum(f.stat().st_size for f in LOCATION_PATH.glob("*.jsonl")) / (1024 ** 3)
    except Exception as e:
        TOTAL_BYTES = 0

    logger.info(f"PROCESS SUMMARY: Extraction took {ELAPSED_TIME:.4f} minutes")
    logger.info(f"PROCESS SUMMARY: Total volume on disk: {TOTAL_BYTES:.4f} GB")
    logger.info(f"PROCESS TERMINATED: {write_count} locations written")

if __name__ == "__main__":
    asyncio.run(run_extraction())