import logging
import os
from fetchers.noaa_fetcher import NOAAFetcher
from fetchers.usgs_earthquake_fetcher import USGSEarthquakeFetcher
from fetchers.nasa_donki_fetcher import NASADONKIFetcher
from fetchers.aemet_fetcher import AEMETFetcher
from config import get_source_config, CONFIG

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
    # Determine which sources are enabled dynamically
    enabled_sources = [
        source for source, cfg in CONFIG.items()
        if isinstance(cfg, dict) and cfg.get("enabled", False)
    ]
    logging.info(f"Enabled sources for this run: {', '.join(enabled_sources) if enabled_sources else 'None'}")

    if "noaa_swpc" in enabled_sources:
        logging.info(f"Downloading data from NOAA SWPC")
        noaa = NOAAFetcher()
        noaa.fetch()

    if "nasa_donki" in enabled_sources:
        logging.info(f"Downloading data from NASA DONKI")
        donki = NASADONKIFetcher()
        donki.fetch()
        
    if "usgs_earthquakes" in enabled_sources:
        logging.info(f"Downloading data from USGS EARTHQUAKES")
        usgs = USGSEarthquakeFetcher()
        usgs.fetch()
        
    if "aemet" in enabled_sources:
        logging.info(f"Downloading data from AEMET")
        aemet =AEMETFetcher()
        aemet.fetch()

if __name__ == "__main__":
    run_all_sources()