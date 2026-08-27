import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.db import init_db, get_session
from shared.models import WorkflowDefinition, WorkflowRun, TaskDefinition, TaskDependency, TaskRun
from shared.dag import validate_workflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Switchyard API")


class WorkflowSubmission(BaseModel):
    name: str
    tasks: list


class TaskRunResponse(BaseModel):
    id: str
    key: str
    command: str
    status: str
    attempt_number: int
    max_attempts: int
    outputs: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    heartbeat_at: Optional[str] = None


class WorkflowSummaryResponse(BaseModel):
    id: str
    name: str
    version: int
    status: str
    progress: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class WorkflowDetailResponse(BaseModel):
    id: str
    name: str
    version: int
    status: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    tasks: List[TaskRunResponse]


@app.on_event("startup")
def startup_event():
    try:
        init_db()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        raise


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/workflows", response_model=List[WorkflowSummaryResponse])
def list_workflows():
    """Get all workflow runs with summary status."""
    session = get_session()
    try:
        workflow_runs = session.query(WorkflowRun).order_by(WorkflowRun.created_at.desc()).all()

        summaries = []
        for run in workflow_runs:
            completed_count = session.query(TaskRun).filter(
                TaskRun.workflow_run_id == run.id,
                TaskRun.status == "succeeded"
            ).count()
            total_count = session.query(TaskRun).filter(
                TaskRun.workflow_run_id == run.id
            ).count()

            progress = f"{completed_count}/{total_count}" if total_count > 0 else "0/0"

            summaries.append(WorkflowSummaryResponse(
                id=str(run.id),
                name=run.workflow_definition.name,
                version=run.workflow_definition.version,
                status=run.status,
                progress=progress,
                created_at=run.created_at.isoformat(),
                started_at=run.started_at.isoformat() if run.started_at else None,
                completed_at=run.completed_at.isoformat() if run.completed_at else None
            ))

        return summaries
    finally:
        session.close()


@app.get("/workflows/{workflow_run_id}", response_model=WorkflowDetailResponse)
def get_workflow(workflow_run_id: str):
    """Get full workflow execution state with all task details."""
    session = get_session()
    try:
        workflow_run = session.query(WorkflowRun).filter(
            WorkflowRun.id == workflow_run_id
        ).first()

        if not workflow_run:
            raise HTTPException(status_code=404, detail=f"Workflow run {workflow_run_id} not found")

        workflow_def = workflow_run.workflow_definition
        task_runs = session.query(TaskRun).filter(
            TaskRun.workflow_run_id == workflow_run.id
        ).all()

        task_run_by_def_id = {tr.task_definition_id: tr for tr in task_runs}

        tasks = []
        for task_def in workflow_def.task_definitions:
            task_run = task_run_by_def_id.get(task_def.id)

            status = task_run.status if task_run else "pending"
            attempt_number = task_run.attempt_number if task_run else 0
            outputs = task_run.outputs if task_run else None
            error_message = task_run.error_message if task_run else None
            created_at = task_run.created_at.isoformat() if task_run else None
            started_at = task_run.started_at.isoformat() if task_run and task_run.started_at else None
            completed_at = task_run.completed_at.isoformat() if task_run and task_run.completed_at else None
            heartbeat_at = task_run.heartbeat_at.isoformat() if task_run and task_run.heartbeat_at else None

            tasks.append(TaskRunResponse(
                id=str(task_run.id) if task_run else str(task_def.id),
                key=task_def.task_key,
                command=task_def.command,
                status=status,
                attempt_number=attempt_number,
                max_attempts=task_def.retry_max_attempts if task_def.retry_max_attempts > 0 else 1,
                outputs=outputs,
                error_message=error_message,
                created_at=created_at or datetime.utcnow().isoformat(),
                started_at=started_at,
                completed_at=completed_at,
                heartbeat_at=heartbeat_at
            ))

        return WorkflowDetailResponse(
            id=str(workflow_run.id),
            name=workflow_def.name,
            version=workflow_def.version,
            status=workflow_run.status,
            created_at=workflow_run.created_at.isoformat(),
            started_at=workflow_run.started_at.isoformat() if workflow_run.started_at else None,
            completed_at=workflow_run.completed_at.isoformat() if workflow_run.completed_at else None,
            tasks=tasks
        )
    finally:
        session.close()


@app.post("/workflows")
def submit_workflow(workflow: WorkflowSubmission):
    """Submit a workflow for execution."""
    try:
        workflow_dict = workflow.dict()

        result = validate_workflow(workflow_dict)
        if not result.is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid workflow: {result.errors}")

        session = get_session()
        try:
            workflow_def = session.query(WorkflowDefinition).filter(
                WorkflowDefinition.name == workflow_dict["name"]
            ).order_by(WorkflowDefinition.version.desc()).first()

            version = 1 if not workflow_def else workflow_def.version + 1

            workflow_def = WorkflowDefinition(
                id=uuid.uuid4(),
                name=workflow_dict["name"],
                version=version,
                dag_definition=json.dumps(workflow_dict),
                created_at=datetime.utcnow()
            )
            session.add(workflow_def)
            session.flush()

            task_key_to_id = {}
            for task in workflow_dict.get("tasks", []):
                task_def = TaskDefinition(
                    id=uuid.uuid4(),
                    workflow_definition_id=workflow_def.id,
                    task_key=task["id"],
                    command=task["command"],
                    params=task.get("params"),
                    retry_max_attempts=task.get("retry", {}).get("max_attempts", 0) if isinstance(task.get("retry"), dict) else 0,
                    retry_backoff_strategy=task.get("retry", {}).get("backoff", "linear") if isinstance(task.get("retry"), dict) else None,
                    on_failure_task_key=task.get("on_failure"),
                    created_at=datetime.utcnow()
                )
                session.add(task_def)
                task_key_to_id[task["id"]] = task_def.id
                session.flush()

            for task in workflow_dict.get("tasks", []):
                for dep in task.get("depends_on", []):
                    dep_rel = TaskDependency(
                        task_definition_id=task_key_to_id[task["id"]],
                        depends_on_task_definition_id=task_key_to_id[dep]
                    )
                    session.add(dep_rel)

            workflow_run = WorkflowRun(
                id=uuid.uuid4(),
                workflow_definition_id=workflow_def.id,
                status="pending",
                triggered_by="api",
                created_at=datetime.utcnow()
            )
            session.add(workflow_run)
            session.commit()

            logger.info(f"Workflow {workflow_dict['name']} (v{version}) submitted: {workflow_run.id}")

            return {
                "workflow_run_id": str(workflow_run.id),
                "workflow_name": workflow_dict["name"],
                "version": version,
                "status": "pending"
            }

        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to submit workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))