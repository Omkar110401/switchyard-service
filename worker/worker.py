import json
import logging
import os
import signal
import subprocess
import threading
import time
import uuid
from datetime import datetime
from typing import Optional, Dict

import redis
from sqlalchemy.orm import Session

from shared.models import TaskRun, TaskDefinition
from shared.db import get_session

logger = logging.getLogger(__name__)


class WorkerShutdownException(Exception):
    pass


class Worker:
    """Executes tasks from Redis Streams."""

    def __init__(self, redis_url: str = "redis://localhost:6379", worker_id: Optional[str] = None):
        self.worker_id = worker_id or str(uuid.uuid4())[:8]
        self.redis_url = redis_url
        self.stream_name = "workflow_tasks"
        self.consumer_group = "workers"
        self.shutdown_requested = False

        self._init_redis()
        self._register_signals()

        logger.info(f"Worker {self.worker_id} initialized")

    def _init_redis(self):
        self.redis_client = redis.from_url(self.redis_url)
        try:
            self.redis_client.xgroup_create(self.stream_name, self.consumer_group, id="0", mkstream=True)
            logger.info(f"Created consumer group {self.consumer_group}")
        except redis.ResponseError as e:
            if "already exists" in str(e):
                logger.info("Consumer group already exists")
            else:
                raise

    def _register_signals(self):
        def handle_sigterm(signum, frame):
            logger.info("Received SIGTERM, shutting down")
            self.shutdown_requested = True
            raise WorkerShutdownException()

        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigterm)

    def run(self, block_timeout_ms: int = 1000):
        """Run worker loop, blocking on Redis for messages."""
        logger.info(f"Worker {self.worker_id} starting")
        try:
            while not self.shutdown_requested:
                try:
                    self._process_one_message(block_timeout_ms)
                except WorkerShutdownException:
                    break
        finally:
            logger.info(f"Worker {self.worker_id} stopped")

    def _process_one_message(self, block_timeout_ms: int):
        """Read one message from Redis and execute it."""
        try:
            messages = self.redis_client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.worker_id,
                streams={self.stream_name: ">"},
                count=1,
                block=block_timeout_ms
            )

            if not messages:
                return

            stream, msgs = messages[0]
            message_id, data = msgs[0]

            self._handle_message(data)
            self.redis_client.xack(self.stream_name, self.consumer_group, message_id)

        except redis.ResponseError as e:
            logger.error(f"Redis error: {e}")
            time.sleep(1)

    def _handle_message(self, data: Dict[bytes, bytes]):
        """Decode and execute a task."""
        task_run_id = data.get(b"task_run_id").decode() if data.get(b"task_run_id") else None
        command = data.get(b"command").decode() if data.get(b"command") else None
        params = json.loads(data.get(b"params", b"{}").decode()) if data.get(b"params") else {}
        attempt = int(data.get(b"attempt_number", b"1").decode())

        if not task_run_id or not command:
            logger.error(f"Invalid message: {data}")
            return

        logger.info(f"Processing task {task_run_id} (attempt {attempt}): {command}")

        session = get_session()
        try:
            task_run = session.query(TaskRun).get(task_run_id)
            if not task_run:
                logger.error(f"Task run {task_run_id} not found")
                return

            task_def = session.query(TaskDefinition).get(task_run.task_definition_id)
            if not task_def:
                logger.error("Task definition not found")
                return

            task_run.status = "running"
            task_run.worker_id = self.worker_id
            task_run.started_at = datetime.utcnow()
            session.commit()

            output, error = self._execute_task(command, params, task_run_id)

            if error:
                logger.error(f"Task {task_run_id} failed: {error}")
                self._report_failure(task_run_id, error)
            else:
                logger.info(f"Task {task_run_id} succeeded")
                self._report_success(task_run_id, output)

        except Exception as e:
            logger.error(f"Failed to process task {task_run_id}: {e}", exc_info=True)
            self._report_failure(task_run_id, str(e))
        finally:
            session.close()

    def _execute_task(self, command: str, params: Dict, task_run_id: str) -> tuple:
        """Execute task in subprocess with heartbeat monitoring."""
        try:
            cmd = f"python {command}"
            env = os.environ.copy()
            env["TASK_RUN_ID"] = task_run_id
            env["TASK_PARAMS"] = json.dumps(params)

            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None
            )

            self._start_heartbeat(task_run_id)

            try:
                stdout, stderr = process.communicate(timeout=3600)
            except subprocess.TimeoutExpired:
                process.kill()
                return None, "Task timeout after 3600 seconds"

            if process.returncode != 0:
                return None, f"Task exited with code {process.returncode}: {stderr}"

            try:
                output = json.loads(stdout) if stdout else {}
            except json.JSONDecodeError:
                output = {"result": stdout}

            return output, None

        except Exception as e:
            return None, str(e)

    def _start_heartbeat(self, task_run_id: str):
        """Update heartbeat every 10s so scheduler knows task is alive."""
        def heartbeat_loop():
            while True:
                time.sleep(10)
                session = get_session()
                try:
                    task_run = session.query(TaskRun).get(task_run_id)
                    if task_run and task_run.status == "running":
                        task_run.heartbeat_at = datetime.utcnow()
                        session.commit()
                except Exception as e:
                    logger.error(f"Heartbeat update failed: {e}")
                finally:
                    session.close()

        thread = threading.Thread(target=heartbeat_loop, daemon=True)
        thread.start()

    def _report_success(self, task_run_id: str, output: Dict):
        """Update DB: task succeeded."""
        session = get_session()
        try:
            task_run = session.query(TaskRun).get(task_run_id)
            if task_run:
                task_run.status = "succeeded"
                task_run.outputs = output
                task_run.completed_at = datetime.utcnow()
                session.commit()
        except Exception as e:
            logger.error(f"Failed to report success: {e}")
        finally:
            session.close()

    def _report_failure(self, task_run_id: str, error: str):
        """Update DB: task failed."""
        session = get_session()
        try:
            task_run = session.query(TaskRun).get(task_run_id)
            if task_run:
                task_run.status = "failed"
                task_run.error_message = error
                task_run.completed_at = datetime.utcnow()
                session.commit()
        except Exception as e:
            logger.error(f"Failed to report failure: {e}")
        finally:
            session.close()
