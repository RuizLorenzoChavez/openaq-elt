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
COUNTRY_LOG_PATH = Path(os.getenv("COUNTRY_LOG_PATH"))
COUNTRY_PATH = Path(os.getenv("COUNTRY_PATH"))
COUNTRIES_URL = os.getenv("COUNTRIES_URL")
MAX_RETRIES = 6     #?  x5 retries + 1 to account for Python indexing 
MAX_LIMIT = 300
CONNECTION_TIMEOUT = 3
READ_TIMEOUT = 15

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

file_handler = logging.FileHandler(COUNTRY_LOG_PATH, mode="a", encoding="utf-8")
file_handler.setLevel("INFO")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ========== SCRIPT ==========

def extract_countries():
    
    """Extract country-level data from OpenAQ API

    Yields:
        countries: Generated country-level data containing country ID, country code, and country name
    """
    
    for retry in range(1, MAX_RETRIES):
            
        try:
            logger.debug(f"CHECKPOINT: Extraction trial {retry} of {MAX_RETRIES - 1} ")
            
            #   requesting country-level data from OpenAQ API
            response = requests.get(url=COUNTRIES_URL,
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
            
            logger.debug(f"CHECKPOINT: Requesting country data ({used}/{limit} resets in {reset})")
            
            #   converting request output to JSON
            response_json = response.json()

            #   checking queried results amount
            response_num = response_json.get('meta', {}).get('found')
            logger.info(f"CHECKPOINT: Total number of countries retrieved: {response_num}")

            #   extracting country-level query results 
            results = response_json.get('results', [])

            for result in results:
                
                logger.debug(f"PROCESS: Extracting {result.get('name', None)}")
                
                country = {}
                
                country["id"] = result.get("id", None)
                country["code"] = result.get("code", None)
                country["name"] = result.get("name", None)
                        
                yield country
            
            break
        
        except requests.exceptions.HTTPError as http_err:
            logger.warning(f"PROCESS HALTED: HTTP error occurred: {http_err}")
            time.sleep(2 ** retry)
            
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
    logger.info(f"PROCESS STARTED: Country-level data extraction initiated.")
    START_TIME = time.perf_counter()
        
    if not COUNTRY_PATH.exists():
        COUNTRY_PATH.touch()
        
    with open(COUNTRY_PATH, "r") as existing:
        
        reference = set()
        
        for row in existing:
            if row.strip():
                row_dict = json.loads(row)
                reference.add(row_dict.get("id", 0))
                
    with open(COUNTRY_PATH, "a") as file:
        
        WRITE_COUNT = 0
        EXTRACT_COUNT = 0
        
        countries = extract_countries()

        for country in countries:
            
            country_id = country.get("id", 0)
            
            if country_id not in reference:
                logger.debug(f"PROCESS: Writing {country.get('name', None)} into {COUNTRY_PATH}")
                json.dump(country, file)
                file.write("\n")
            
                WRITE_COUNT += 1
            
            EXTRACT_COUNT += 1
    
    END_TIME = time.perf_counter()
    ELAPSED_TIME = END_TIME - START_TIME
    
    logger.info(f"PROCESS SUMMARY: {EXTRACT_COUNT} extracted countries")
    logger.info(f"PROCESS SUMMARY: Extraction process took {ELAPSED_TIME} seconds")
    
    if not WRITE_COUNT:
        logger.info(f"PROCESS TERMINATED: No new countries written")
    else:
        logger.info(f"PROCESS TERMINATED: {WRITE_COUNT} countries extracted and written")
    
# ========== MAIN ==========

if __name__ == "__main__":
    main()