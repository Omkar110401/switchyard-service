import logging
import os

from scheduler import Scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    logger.info(f"Starting scheduler with Redis: {redis_url}")

    scheduler = Scheduler(redis_url=redis_url)
    scheduler.run(interval_seconds=5)
