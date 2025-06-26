import logging
import os
from preprocessors.noaa_preprocessing import NOAAAlertPreprocessor
from preprocessors.aemet_preprocessing import AEMETAlertPreprocessor

def run_all_preprocessing():
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
    # NOAA Space Weather Alerts
    logging.info(f"Preprocessing data from NOAA SWPC")
    noaa = NOAAAlertPreprocessor()
    alerts = noaa.load_alerts()
    processed = noaa.process_alerts(alerts)
    noaa.save_alerts(processed)

    # AEMET Space Weather Alerts
    logging.info(f"Preprocessing data from AEMET")
    pre = AEMETAlertPreprocessor()
    raw_alerts = pre.load_alerts()
    processed = pre.process_alerts(raw_alerts)
    pre.save_alerts(processed)

if __name__ == "__main__":
    run_all_preprocessing()