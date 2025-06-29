import logging
import os
from preprocessors.noaa_preprocessing import NOAAAlertPreprocessor
from preprocessors.aemet_preprocessing import AEMETAlertPreprocessor
from preprocessors.firms_preprocessing import FIRMSAlertPreprocessor
from preprocessors.gdacs_preprocessing import GDACSAlertPreprocessor
from preprocessors.ign_alerts_preprocessing import IGNAlertPreprocessor

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
    raw_alerts = noaa.load_alerts()
    processed = noaa.process_alerts(raw_alerts)
    noaa.save_alerts(processed)

    # AEMET Weather Alerts
    logging.info(f"Preprocessing data from AEMET")
    pre = AEMETAlertPreprocessor()
    raw_alerts = pre.load_alerts()
    processed = pre.process_alerts(raw_alerts)
    pre.save_alerts(processed)

    # FIRMS MODIS Space Weather Alerts
    logging.info(f"Preprocessing data from FIRMS MODIS")
    pre = FIRMSAlertPreprocessor()
    raw_alerts = pre.load_alerts()
    processed = pre.process_alerts(raw_alerts)
    pre.save_alerts(processed)

    # GDACS Alerts
    logging.info(f"Preprocessing data from GDACS")
    pre = GDACSAlertPreprocessor()
    raw_alerts = pre.load_alerts()
    processed = pre.process_alerts(raw_alerts)
    pre.save_alerts(processed)

    # IGN Spain earthquakes Alerts
    logging.info(f"Preprocessing data from IGN")
    pre = IGNAlertPreprocessor()
    raw_alerts = pre.load_alerts()
    processed = pre.process_alerts(raw_alerts)
    pre.save_alerts(processed)


if __name__ == "__main__":
    run_all_preprocessing()