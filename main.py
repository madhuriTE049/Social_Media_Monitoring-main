from core.logger import logger
from core.scheduler import start_scheduler

if __name__ == "__main__":
    logger.info("Starting Social Media Monitoring Scheduler")
    start_scheduler()