import logging
from delta_to_land import delta_to_landing
from land_to_stg import landing_to_staging
from stg_to_final import staging_to_final

logging.basicConfig(level=logging.INF0, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s)')
logging.basicConfig(level=logging.INFO)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logging.info("Starting the process to load data from SQL to hive tables.")
    logging.info("Starting to load data from SQL to landing layer.")
    if delta_to_landing() == 0:
        logging.info("Successfully loaded data from SQL to landing layer.")
        logging.info("Starting to load data from landing layer to staging layer.")
        if landing_to_staging() == 0:
            logging.info("Successfully loaded data from landing layer to staging layer.")
            logging.info("Starting to load data from staging layer to final layer IPTV.")
            if staging_to_final() == 0:
                logging.info("Successfully loaded data from staging layer to final layer IPTV.")
            else:
                logging.info("Fail to load data from staging layer to final layer IPTV.")
        else:
            logging.info("Fail to load data from landing layer to staging layer.")
    else:
        logging.info("Fail to load data from SQL to landing layer.")