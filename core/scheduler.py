# import schedule
# import time
# import logging
# from core.logger import logger

# from database.connection import get_db_connection
# from core.fetch_job import run_fetch_job_for_config

# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger(__name__)


# #def load_and_schedule_configs():
# def load_and_schedule_configs(run_immediately=False):
#     logger.info("Loading active configs and scheduling jobs")
#     """
#     Reads all active configs from DB and registers a scheduled job
#     for each one using its own frequency (in minutes).
#     Also runs each config immediately on startup.
#     """
#     schedule.clear()

#     db = get_db_connection()
#     cur = db.cursor(dictionary=True)
#     cur.execute("SELECT * FROM keyword_configs WHERE is_active=1")
#     configs = cur.fetchall()
#     cur.close()
#     db.close()

#     if not configs:
#         logger.warning("No active configs found.")
#         return

#     for config in configs:
#         frequency = int(config.get("frequency") or 60)
#         config_id = config["id"]

#         logger.info(f"Scheduling config {config_id} every {frequency} minutes")

#         # Run immediately on startup
#         # run_fetch_job_for_config(config)
#         if run_immediately:
#             run_fetch_job_for_config(config)

#         # Use a default-arg closure to properly capture each config
#         def make_job(cfg=config):
#             def job():
#                 run_fetch_job_for_config(cfg)
#             return job

#         schedule.every(frequency).minutes.do(make_job())
#     logger.info(f"Scheduled {len(configs)} config(s).")

# def start_scheduler():

#     logger.info("Starting Social Media Monitoring Scheduler")

#     # Initial load
#     #load_and_schedule_configs()

#     # Reload configs every 5 minutes
#     #schedule.every(5).minutes.do(load_and_schedule_configs)

#     load_and_schedule_configs(run_immediately=True)
#     schedule.every(5).minutes.do(load_and_schedule_configs)

#     while True:
#         schedule.run_pending()
#         time.sleep(30)

import schedule
import time
import logging

from core.logger import logger
from database.connection import get_db_connection
from core.fetch_job import run_single_config_job, send_grouped_reports_by_frequency

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def load_and_schedule_configs():

    logger.info("Loading active configs and scheduling jobs")

    schedule.clear()

    db = get_db_connection()
    cur = db.cursor(dictionary=True)
    cur.execute("SELECT * FROM keyword_configs WHERE is_active=1")
    configs = cur.fetchall()
    cur.close()
    db.close()

    if not configs:
        logger.warning("No active configs found.")
        return

    for config in configs:

        frequency = int(config.get("frequency") or 60)
        config_id = config["id"]

        logger.info(f"Scheduling config {config_id} every {frequency} minutes")

        run_single_config_job(config)

        def make_job(cfg=config):
            def job():
                run_single_config_job(cfg)
            return job

        schedule.every(frequency).minutes.do(make_job())

    logger.info(f"Scheduled {len(configs)} config(s).")


def start_scheduler():

    logger.info("Starting Social Media Monitoring Scheduler")

    load_and_schedule_configs()

    # ADD THIS LINE (VERY IMPORTANT)
    send_grouped_reports_by_frequency()

    schedule.every(5).minutes.do(load_and_schedule_configs)
    schedule.every(5).minutes.do(send_grouped_reports_by_frequency)

    while True:
        schedule.run_pending()
        time.sleep(30)