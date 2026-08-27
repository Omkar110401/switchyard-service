import logging
import os
import sys

from worker.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    worker_id = os.getenv("WORKER_ID", None)

    logger.info(f"Starting worker with Redis: {redis_url}")

    try:
        worker = Worker(redis_url=redis_url, worker_id=worker_id)
        worker.run()
    except Exception as e:
        logger.error(f"Worker failed: {e}", exc_info=True)
        sys.exit(1)
