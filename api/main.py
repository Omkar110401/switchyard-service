import json
import logging
import uuid
from datetime import datetime

import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.db import init_db, get_session
from shared.models import WorkflowDefinition, WorkflowRun, TaskDefinition, TaskDependency
from shared.dag import validate_workflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Switchyard API")


class WorkflowSubmission(BaseModel):
    name: str
    tasks: list


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