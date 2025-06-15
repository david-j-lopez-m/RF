import logging
import os
from fetchers.noaa_fetcher import NOAAFetcher
from config import get_source_config

def run_all_sources():
    # Configure logging
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'etl.log')

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
)
    # Determine which sources are enabled
    enabled_sources = [
        source for source in ["noaa_swpc", "nasa_donki"]
        if get_source_config(source).get("enabled", False)
    ]
    logging.info(f"Enabled sources for this run: {', '.join(enabled_sources) if enabled_sources else 'None'}")

    if "noaa_swpc" in enabled_sources:
        logging.info(f"Downloading data from NOAA SWPC")
        noaa = NOAAFetcher()
        noaa.fetch()

    if "nasa_donki" in enabled_sources:
        logging.info(f"Downloading data from NASA DONKI")
        from fetchers.nasa_donki_fetcher import NASADONKIFetcher
        donki = NASADONKIFetcher()
        donki.fetch()
        

if __name__ == "__main__":
    run_all_sources()