import logging

logging.basicConfig(
    filename='logs/main_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True  
)

from etl.tourism_etl import tourism_mouvment
from etl.weather_etl import weather_etl
from etl.preprocess import preprocess

def run():
    logging.info("Starting tourism_etl ...")
    try:
        changed = tourism_mouvment()
    except Exception as e:
        logging.exception(f"Tourism ETL failed: {e}")
        changed = False

    if changed:
        logging.info("Tourism data updated — running downstream ETLs.")
        try:
            weather_etl()
            preprocess()
            logging.info("All ETLs completed successfully.")
        except Exception as e:
            logging.exception(f"Downstream ETL failed: {e}")
    else:
        logging.info("No new tourism data. Skipping weather and preprocessing.")


def main():
    logging.info("="*40)
    logging.info("MAIN ETL PIPELINE STARTED")
    logging.info("="*40)
    run()
    logging.info("="*40)
    logging.info("MAIN ETL PIPELINE FINISHED")
    logging.info("="*40)

if __name__ == "__main__":
    main()
