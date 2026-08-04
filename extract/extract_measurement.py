import asyncio
import aiohttp
import logging
import json
import time
import random
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ========== GLOBAL VARS ==========
API_KEY = os.getenv("OPENAQ_API_KEY")
MEASUREMENT_LOG_PATH = Path(os.getenv("MEASUREMENT_LOG_PATH", "logs/measurement.log"))
MEASUREMENT_PATH = Path(os.getenv("MEASUREMENT_PATH", "data/measurements"))
SENSOR_REF_PATH = Path(os.getenv("SENSOR_REF_PATH", "log/sensor_id.jsonl"))

MAX_RETRIES = 6
MAX_LIMIT = 100
MAX_CONCURRENT_REQUESTS = 15
BASE_RATE_LIMIT_DELAY = 3.0
RATE_LIMIT_JITTER = 1.0
QUEUE_MAX_SIZE = 50  # Memory backpressure limit to prevent RAM exhaustion

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

# ========== JITTERED RATE LIMITER ==========
class JitteredRateLimiter:
    """Ensures a paced delay with randomized jitter to prevent bot-detection sync pulses."""
    def __init__(self, base_delay: float, jitter: float):
        self.base_delay = base_delay
        self.jitter = jitter
        self.last_call = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_call
            current_delay = self.base_delay + random.uniform(0, self.jitter)
            if elapsed < current_delay:
                await asyncio.sleep(current_delay - elapsed)
            self.last_call = time.monotonic()

rate_limiter = JitteredRateLimiter(BASE_RATE_LIMIT_DELAY, RATE_LIMIT_JITTER)

# ========== SEMI-ANNUAL (HALF-YEAR) CHUNK GENERATOR ==========
def generate_semiannual_ranges(dt_first_str, dt_last_str):
    """Generates 6-month (semi-annual) start and end datetime string chunks."""
    if not dt_first_str or not dt_last_str:
        return [(None, None)]
        
    try:
        start_date = datetime.fromisoformat(dt_first_str.replace("Z", "+00:00"))
        end_date = datetime.fromisoformat(dt_last_str.replace("Z", "+00:00"))
    except ValueError:
        return [(dt_first_str, dt_last_str)]
        
    ranges = []
    curr = start_date
    while curr < end_date:
        # Determine the target month for a 6-month leap (H1 -> H2 or H2 -> Next Year H1)
        if curr.month <= 6:
            next_chunk = curr.replace(month=7, day=1)
        else:
            next_chunk = curr.replace(year=curr.year + 1, month=1, day=1)
            
        chunk_end = min(next_chunk, end_date)
        
        ranges.append((
            curr.isoformat(),
            chunk_end.isoformat()
        ))
        curr = chunk_end
        
    return ranges if ranges else [(dt_first_str, dt_last_str)]

# ========== ASYNC WORKERS ==========
async def process_sensor(session, sensor_id, dt_first, dt_last, semaphore, queue):
    """Fetches measurements for a given sensor partitioned by semi-annual time chunks."""
    logger.debug(f"[PROCESS_SENSOR START] Sensor #{sensor_id}")
    time_ranges = generate_semiannual_ranges(dt_first, dt_last)
    extracted_total = 0
    
    for chunk_idx, (chunk_start, chunk_end) in enumerate(time_ranges, 1):
        page = 1
        
        while True:
            success = False
            response_json = {}
            
            for retry in range(1, MAX_RETRIES):
                logger.debug(f"PROCESS: Sensor #{sensor_id} Chunk {chunk_idx}/{len(time_ranges)} (Page #{page}) trial {retry} of {MAX_RETRIES - 1}")
                
                try:
                    async with semaphore:
                        await rate_limiter.wait()

                        headers = {
                            "Accept": "application/json",
                            "User-Agent": "OpenAQ-Data-Extractor/1.0 (AsyncBot)",
                            "Connection": "close"
                        }
                        if API_KEY:
                            headers["X-API-Key"] = API_KEY
                            
                        params = {
                            "limit": MAX_LIMIT,
                            "page": page
                        }
                        if chunk_start:
                            params["datetime_from"] = chunk_start
                        if chunk_end:
                            params["datetime_to"] = chunk_end
                        
                        measurements_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/hourly"
                        logger.debug(f"Sending GET request to {measurements_url} with params {params}")
                        
                        timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
                        
                        async with session.get(measurements_url, params=params, headers=headers, timeout=timeout) as response:
                            
                            if response.status in (401, 403):
                                logger.critical(f"UNAUTHORIZED ({response.status}): API key is invalid or IP banned. Exiting sensor #{sensor_id}.")
                                return 

                            if response.status == 429:
                                sleep_time = int(response.headers.get('X-ratelimit-reset', 60))
                                logger.warning(f"RATE LIMIT REACHED (429): Sleeping globally for {sleep_time}s")
                                await asyncio.sleep(sleep_time + 1)
                                continue

                            if response.status in (408, 500, 502, 503, 504):
                                backoff = (15 * retry) + 5  
                                logger.warning(f"SERVER STRUGGLING ({response.status}) on sensor #{sensor_id} chunk #{chunk_idx}. Backing off for {backoff}s.")
                                await asyncio.sleep(backoff)
                                continue

                            response.raise_for_status()
                            
                            remaining = int(response.headers.get('X-ratelimit-remaining', 100))
                            if remaining < 5:
                                reset_sec = int(response.headers.get('X-ratelimit-reset', 15))
                                logger.debug(f"Rate limit reaching capacity. Waiting {reset_sec}s...")
                                await asyncio.sleep(reset_sec)

                            response_text = await response.text()
                            if not response_text or not response_text.strip():
                                raise json.JSONDecodeError("Empty body", response_text, 0)
                            
                            response_json = json.loads(response_text)
                            success = True
                            break  

                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as err:
                    logger.warning(f"PROCESS HALTED on sensor #{sensor_id} chunk #{chunk_idx} page #{page} (Attempt {retry}): {err}")
                    await asyncio.sleep(2 ** retry)
                    
            if not success:
                logger.error(f"Failed to extract chunk #{chunk_idx} page #{page} for sensor #{sensor_id} after retries. Skipping chunk.")
                break
                
            found = response_json.get('meta', {}).get('found', 0)
            results = response_json.get('results', [])
            
            if not results:
                break

            extracted_total += len(results)
            logger.debug(f"Putting data for Sensor #{sensor_id} Chunk {chunk_idx} Page {page} into writer queue...")
            
            # Memory Backpressure: Blocks if queue reaches QUEUE_MAX_SIZE
            await queue.put({
                "sensor_id": sensor_id,
                "data": response_json,
                "page": page
            })

            # Check if we've retrieved all items inside this particular time chunk
            if page * MAX_LIMIT >= found or len(results) < MAX_LIMIT:
                break
            
            page += 1
            await asyncio.sleep(random.uniform(2.0, 5.0))

    logger.debug(f"[PROCESS_SENSOR END] Completed all chunks for Sensor #{sensor_id}. Total records: {extracted_total}")

def write_measurements_to_disk(temp_file_path, results, sensor_id):
    """Synchronous file I/O isolated so it doesn't block the async event loop."""
    count = 0
    with open(temp_file_path, "a", encoding="utf-8") as file:
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
            count += 1
    return count

async def writer_worker(queue):
    """Reads from the queue and delegates writing to disk sequentially via a thread."""
    logger.debug("[WRITER_WORKER] Initialized and waiting for queue items...")
    write_count = 0
    files_touched = set()
    
    while True:
        item = await queue.get()
        if item is None: 
            logger.debug("[WRITER_WORKER] Received poison pill (None). Breaking write loop.")
            queue.task_done()
            break
            
        sensor_id = item['sensor_id']
        data = item['data']
        results = data.get('results', [])
        temp_file_path = MEASUREMENT_PATH / f"{sensor_id}.jsonl.tmp"
        
        files_touched.add(sensor_id)

        try:
            count = await asyncio.to_thread(
                write_measurements_to_disk, temp_file_path, results, sensor_id
            )
            write_count += count
        except Exception as write_err:
            logger.error(f"[WRITER_ERROR] Failed writing data for Sensor #{sensor_id}: {write_err}", exc_info=True)
            
        queue.task_done()

    logger.debug("[WRITER_WORKER] Finalizing temp files...")
    for sid in files_touched:
        temp_path = MEASUREMENT_PATH / f"{sid}.jsonl.tmp"
        final_path = MEASUREMENT_PATH / f"{sid}.jsonl"
        if temp_path.exists():
            temp_path.replace(final_path)

    logger.debug(f"[WRITER_WORKER] Finished completely. Total records written: {write_count}.")
    return write_count

async def run_extraction():
    logger.info("PROCESS STARTED: Semi-annual-chunked async hourly measurement extraction initiated.")
    START_TIME = time.perf_counter()

    sensor_keys = set()
    try:
        with open(SENSOR_REF_PATH, "r", encoding="utf-8") as keys:
            for line_no, key in enumerate(keys, 1):
                if key.strip():
                    try:
                        key_dict = json.loads(key)
                        dt_first = key_dict.get("datetimeFirst") or key_dict.get("datetime_first")
                        dt_last = key_dict.get("datetimeLast") or key_dict.get("datetime_last")
                        
                        sensor_keys.add((
                            key_dict.get("sensor_id"),
                            dt_first,
                            dt_last
                        ))
                    except json.JSONDecodeError:
                        continue
    except FileNotFoundError:
        logger.error(f"Sensor reference file not found at {SENSOR_REF_PATH}. Exiting.")
        return

    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    writer_task = asyncio.create_task(writer_worker(queue))

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS, 
        force_close=True,  
        enable_cleanup_closed=True
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        skipped_count = 0
        
        for sensor_id, dt_first, dt_last in sensor_keys:
            if not sensor_id or not dt_first:
                continue
            
            final_file = MEASUREMENT_PATH / f"{sensor_id}.jsonl"
            tmp_file = MEASUREMENT_PATH / f"{sensor_id}.jsonl.tmp"
            if tmp_file.exists():
                tmp_file.unlink()
                
            tasks.append(process_sensor(session, sensor_id, dt_first, dt_last, semaphore, queue))
        
        if skipped_count > 0:
            logger.info(f"CHECKPOINT SUMMARY: Skipped {skipped_count} sensors already present on disk.")

        gather_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for idx, res in enumerate(gather_results):
            if isinstance(res, Exception):
                logger.error(f"Primary process_sensor task exception caught at index {idx}: {res}", exc_info=True)

    await queue.join()
    await queue.put(None)
    write_count = await writer_task

    END_TIME = time.perf_counter()
    ELAPSED_TIME = (END_TIME - START_TIME) / 3600 
    
    try:
        TOTAL_BYTES = sum(f.stat().st_size for f in MEASUREMENT_PATH.glob("*.jsonl")) / (1024 ** 3)
    except Exception:
        TOTAL_BYTES = 0

    logger.info(f"PROCESS SUMMARY: Extraction took {ELAPSED_TIME:.4f} hours")
    logger.info(f"PROCESS SUMMARY: Total volume on disk: {TOTAL_BYTES:.4f} GB")
    logger.info(f"PROCESS TERMINATED: {write_count} measurements written")

if __name__ == "__main__":
    asyncio.run(run_extraction())