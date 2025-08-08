import logging
import os
from fetchers.noaa_fetcher import NOAAFetcher
from fetchers.usgs_earthquake_fetcher import USGSEarthquakeFetcher
from fetchers.nasa_donki_fetcher import NASADONKIFetcher
from fetchers.aemet_fetcher import AEMETFetcher
from fetchers.gdacs_fetcher import GDACSFetcher
from fetchers.ign_fetcher import IGNFetcher
from fetchers.firms_fetcher import FIRMSFetcher
from config import CONFIG, get_incremental_flag

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

    enabled_sources = [
        source for source, cfg in CONFIG.items()
        if isinstance(cfg, dict) and cfg.get("enabled", False)
    ]
    logging.info(f"Enabled sources for this run: {', '.join(enabled_sources) if enabled_sources else 'None'}")

    for source in enabled_sources:
        logging.info(f"Downloading data from {source.upper()} (incremental={get_incremental_flag(source)})")
        
        if source == "noaa_swpc":
            NOAAFetcher().fetch(incremental=get_incremental_flag(source))
        elif source == "nasa_donki":
            NASADONKIFetcher().fetch(incremental=get_incremental_flag(source))
        elif source == "usgs_earthquakes":
            USGSEarthquakeFetcher().fetch(incremental=get_incremental_flag(source))
        elif source == "aemet":
            AEMETFetcher().fetch(incremental=get_incremental_flag(source))
        elif source == "gdacs":
            GDACSFetcher().fetch(incremental=get_incremental_flag(source))
        elif source == "ign":
            IGNFetcher().fetch(incremental=get_incremental_flag(source))
        elif source == "firms":
            FIRMSFetcher().fetch(incremental=get_incremental_flag(source))

if __name__ == "__main__":
    run_all_sources()