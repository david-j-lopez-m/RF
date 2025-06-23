from noaa_preprocessing import NOAAAlertPreprocessor

pre = NOAAAlertPreprocessor("data/alertas/2025-06-21/noaa_alerts.json", "LLM/Preprocessing/noaa_preprocessed.json")
alerts = pre.load_alerts()
processed = pre.process_alerts(alerts)
pre.save_alerts(processed)