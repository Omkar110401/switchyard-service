import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Set, Optional, Dict

from sqlalchemy.orm import Session

from shared.models import (
    WorkflowDefinition,
    WorkflowRun,
    TaskRun,
    TaskDefinition,
)
from shared.db import get_session
from shared.dag import DAGValidator, validate_workflow

logger = logging.getLogger(__name__)


class Scheduler:
    """Orchestrates workflow execution."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self._init_redis()

    def _init_redis(self):
        import redis
        self.redis_client = redis.from_url(self.redis_url)
        self.stream_name = "workflow_tasks"
        self.consumer_group = "workers"

        try:
            self.redis_client.xgroup_create(self.stream_name, self.consumer_group, id="0", mkstream=True)
            logger.info(f"Created consumer group {self.consumer_group}")
        except redis.ResponseError as e:
            if "already exists" in str(e):
                logger.info(f"Consumer group {self.consumer_group} already exists")
            else:
                raise

    def run_once(self):
        """Single scheduler iteration."""
        session = get_session()
        try:
            workflows = self._get_running_workflows(session)
            logger.info(f"Processing {len(workflows)} running workflows")

            for workflow_run in workflows:
                self._process_workflow(session, workflow_run)

            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Scheduler iteration failed: {e}", exc_info=True)
        finally:
            session.close()

    def _get_running_workflows(self, session: Session) -> List[WorkflowRun]:
        """Get all workflows with status 'pending' or 'running'."""
        return session.query(WorkflowRun).filter(
            WorkflowRun.status.in_(["pending", "running"])
        ).all()

    def _process_workflow(self, session: Session, workflow_run: WorkflowRun):
        """Process a single workflow."""
        workflow_def = session.query(WorkflowDefinition).get(workflow_run.workflow_definition_id)
        if not workflow_def:
            logger.error(f"Workflow definition {workflow_run.workflow_definition_id} not found")
            return

        workflow_dict = json.loads(workflow_def.dag_definition) if isinstance(workflow_def.dag_definition, str) else workflow_def.dag_definition

        try:
            validator = DAGValidator(workflow_dict)
        except Exception as e:
            logger.error(f"Failed to initialize DAG validator: {e}")
            workflow_run.status = "failed"
            return

        completed_tasks = self._get_completed_tasks(session, workflow_run.id)
        failed_tasks = self._get_failed_tasks(session, workflow_run.id)

        if workflow_run.status == "pending":
            workflow_run.status = "running"
            workflow_run.started_at = datetime.utcnow()

        ready_tasks = validator.get_ready_tasks(completed_tasks)
        logger.info(f"Workflow {workflow_run.id}: {len(ready_tasks)} tasks ready")

        for task_id in ready_tasks:
            self._schedule_task(session, workflow_run, task_id)

        if not self._has_pending_or_running_tasks(session, workflow_run.id):
            if failed_tasks:
                workflow_run.status = "failed"
                logger.info(f"Workflow {workflow_run.id} marked failed")
            else:
                workflow_run.status = "succeeded"
                workflow_run.completed_at = datetime.utcnow()
                logger.info(f"Workflow {workflow_run.id} completed successfully")

    def _get_completed_tasks(self, session: Session, workflow_run_id: str) -> Set[str]:
        """Get task_definition_ids that have completed successfully."""
        task_runs = session.query(TaskRun).filter(
            TaskRun.workflow_run_id == workflow_run_id,
            TaskRun.status == "succeeded"
        ).all()
        return {tr.task_definition_id for tr in task_runs}

    def _get_failed_tasks(self, session: Session, workflow_run_id: str) -> Set[str]:
        """Get task_definition_ids that have failed permanently."""
        task_runs = session.query(TaskRun).filter(
            TaskRun.workflow_run_id == workflow_run_id,
            TaskRun.status == "failed"
        ).all()
        return {tr.task_definition_id for tr in task_runs}

    def _schedule_task(self, session: Session, workflow_run: WorkflowRun, task_def_id: str):
        """Create task_run for a ready task and publish to Redis."""
        task_def = session.query(TaskDefinition).get(task_def_id)
        if not task_def:
            logger.error(f"Task definition {task_def_id} not found")
            return

        existing = session.query(TaskRun).filter(
            TaskRun.workflow_run_id == workflow_run.id,
            TaskRun.task_definition_id == task_def_id
        ).all()

        if existing:
            logger.debug(f"Task {task_def.task_key} already has runs, skipping")
            return

        task_run = TaskRun(
            id=uuid.uuid4(),
            workflow_run_id=workflow_run.id,
            task_definition_id=task_def_id,
            status="ready",
            attempt_number=1
        )
        session.add(task_run)
        session.flush()

        self._publish_to_redis(task_run, task_def)
        logger.info(f"Scheduled task {task_def.task_key} (run: {task_run.id})")

    def _publish_to_redis(self, task_run: TaskRun, task_def: TaskDefinition):
        """Publish task to Redis Streams."""
        message = {
            "task_run_id": str(task_run.id),
            "task_definition_id": str(task_def.id),
            "task_key": task_def.task_key,
            "command": task_def.command,
            "params": json.dumps(task_def.params or {}),
            "attempt_number": task_run.attempt_number,
            "max_attempts": task_def.retry_max_attempts,
        }
        self.redis_client.xadd(self.stream_name, message)

    def _has_pending_or_running_tasks(self, session: Session, workflow_run_id: str) -> bool:
        """Check if workflow has any pending or running tasks."""
        count = session.query(TaskRun).filter(
            TaskRun.workflow_run_id == workflow_run_id,
            TaskRun.status.in_(["pending", "ready", "running", "retrying"])
        ).count()
        return count > 0

    def check_heartbeats(self):
        """Check for expired heartbeats and mark tasks as failed."""
        session = get_session()
        try:
            now = datetime.utcnow()
            timeout_seconds = 120

            expired_tasks = session.query(TaskRun).filter(
                TaskRun.status == "running",
                TaskRun.heartbeat_at < (now - timedelta(seconds=timeout_seconds))
            ).all()

            for task_run in expired_tasks:
                logger.warning(f"Task {task_run.id} heartbeat expired, marking as failed")
                task_run.status = "failed"
                task_run.error_message = "Heartbeat timeout"
                task_run.completed_at = now

            if expired_tasks:
                session.commit()
                logger.info(f"Marked {len(expired_tasks)} tasks as failed due to heartbeat timeout")
        except Exception as e:
            logger.error(f"Heartbeat check failed: {e}", exc_info=True)
        finally:
            session.close()

    def handle_task_result(self, task_run_id: str, status: str, output: Optional[Dict] = None, error: Optional[str] = None):
        """Handle task completion from worker."""
        session = get_session()
        try:
            task_run = session.query(TaskRun).get(task_run_id)
            if not task_run:
                logger.error(f"Task run {task_run_id} not found")
                return

            task_def = session.query(TaskDefinition).get(task_run.task_definition_id)
            workflow_run = session.query(WorkflowRun).get(task_run.workflow_run_id)

            if status == "succeeded":
                task_run.status = "succeeded"
                task_run.outputs = output or {}
                task_run.completed_at = datetime.utcnow()
                logger.info(f"Task {task_run.id} succeeded")
            elif status == "failed":
                if task_run.attempt_number < task_def.retry_max_attempts:
                    task_run.status = "retrying"
                    task_run.attempt_number += 1
                    task_run.error_message = error
                    logger.info(f"Task {task_run.id} failed, retrying (attempt {task_run.attempt_number})")
                    self._publish_to_redis(task_run, task_def)
                else:
                    task_run.status = "failed"
                    task_run.error_message = error
                    task_run.completed_at = datetime.utcnow()
                    logger.warning(f"Task {task_run.id} failed after {task_def.retry_max_attempts} attempts")

                    if task_def.on_failure_task_key:
                        self._schedule_on_failure_task(session, workflow_run, task_def)

            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to handle task result: {e}", exc_info=True)
        finally:
            session.close()

    def _schedule_on_failure_task(self, session: Session, workflow_run: WorkflowRun, failed_task_def: TaskDefinition):
        """Schedule compensation task when a task fails."""
        if not failed_task_def.on_failure_task_key:
            return

        on_failure_task = session.query(TaskDefinition).filter(
            TaskDefinition.workflow_definition_id == workflow_run.workflow_definition_id,
            TaskDefinition.task_key == failed_task_def.on_failure_task_key
        ).first()

        if not on_failure_task:
            logger.warning(f"on_failure task '{failed_task_def.on_failure_task_key}' not found")
            return

        existing = session.query(TaskRun).filter(
            TaskRun.workflow_run_id == workflow_run.id,
            TaskRun.task_definition_id == on_failure_task.id
        ).first()

        if existing:
            logger.debug(f"Compensation task {on_failure_task.task_key} already exists")
            return

        task_run = TaskRun(
            id=uuid.uuid4(),
            workflow_run_id=workflow_run.id,
            task_definition_id=on_failure_task.id,
            status="ready",
            attempt_number=1
        )
        session.add(task_run)
        session.flush()
        self._publish_to_redis(task_run, on_failure_task)
        logger.info(f"Scheduled compensation task {on_failure_task.task_key}")

    def run(self, interval_seconds: int = 5):
        """Run scheduler in continuous loop."""
        logger.info(f"Scheduler started (interval: {interval_seconds}s)")
        import time

        try:
            while True:
                self.run_once()
                self.check_heartbeats()
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Scheduler stopped")
