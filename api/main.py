import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

import yaml
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel

from shared.db import init_db, get_session
from shared.models import WorkflowDefinition, WorkflowRun, TaskDefinition, TaskDependency, TaskRun, User, AuditLog
from shared.dag import validate_workflow
from shared.auth import hash_password, verify_password, create_token, verify_token

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

class SignupRequest(BaseModel):
    username: str
    password: str

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user_id: str

class UserResponse(BaseModel):
    id: str
    username: str
    created_at: str

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


def get_current_user(authorization: str = Header(None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Invalid or missing token")

    token = authorization.replace("Bearer ", "")
    user_id = verify_token(token)

    if not user_id:
        raise HTTPException(401, "Invalid or missing token")

    return user_id


@app.get("/workflows", response_model=List[WorkflowSummaryResponse])
def list_workflows(user_id: str = Depends(get_current_user)):
    """Get all workflow runs with summary status."""
    session = get_session()
    try:
        workflow_runs = session.query(WorkflowRun).filter(
            WorkflowRun.user_id == user_id
        ).order_by(WorkflowRun.created_at.desc()).all()

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
def get_workflow(workflow_run_id: str, user_id: str = Depends(get_current_user)):
    """Get full workflow execution state with all task details."""
    session = get_session()
    try:
        workflow_run = session.query(WorkflowRun).filter(
            WorkflowRun.id == workflow_run_id,
            WorkflowRun.user_id == user_id
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
def submit_workflow(workflow: WorkflowSubmission, user_id: str = Depends(get_current_user)):
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
                user_id=user_id,
                workflow_definition_id=workflow_def.id,
                status="pending",
                triggered_by="api",
                created_at=datetime.utcnow()
            )
            session.add(workflow_run)
            session.flush()

            audit = AuditLog(
                id=uuid.uuid4(),
                user_id=user_id,
                action="workflow_submitted",
                status="success",
                details=workflow_dict["name"],
                created_at=datetime.utcnow()
            )
            session.add(audit)
            session.commit()

            logger.info(f"Workflow {workflow_dict['name']} (v{version}) submitted by {user_id}: {workflow_run.id}")

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


@app.post("/auth/signup")
def signup(request: SignupRequest):
    if not request.username or len(request.username) < 3:
        raise HTTPException(400, "Username required, min 3 chars")
    if not request.password or len(request.password) < 8:
        raise HTTPException(400, "Password too short")

    session=get_session()
    try:
        existing=session.query(User).filter(User.username==request.username).first()
        if existing:
            audit=AuditLog(
                id=uuid.uuid4(),
                user_id=None,
                action="signup",
                status="failure",
                details="Username already exists.",
                created_at=datetime.utcnow()
            )
            session.add(audit)
            session.commit()
            raise HTTPException(400, "Username already exists.")

        pwd_truncated = request.password[:72]
        logger.info(f"Hashing password of length {len(pwd_truncated)}")
        user =User(
            id= uuid.uuid4(),
            username=request.username,
            password_hash=hash_password(pwd_truncated),
            created_at=datetime.utcnow()
        )
        session.add(user)
        session.flush()

        audit=AuditLog(
            id=uuid.uuid4(),
            user_id=user.id,
            action="signup",
            status="success",
            details=None,
            created_at=datetime.utcnow()
        )
        session.add(audit)
        session.commit()

        return{
            "id": str(user.id),
            "username": str(user.username),
            "created_at": user.created_at.isoformat()
        }
    except Exception as e:
        session.rollback()
        logger.error(f"Signup failed: {e}", exc_info=True)
        raise HTTPException(500, f"Signup failed: {str(e)}")
    finally:
        session.close()


@app.post("/auth/login", response_model=TokenResponse)
def login(request: LoginRequest):
    session = get_session()
    try:
        user = session.query(User).filter(User.username == request.username).first()

        if not user or not verify_password(request.password[:72], user.password_hash):
            audit = AuditLog(
                id=uuid.uuid4(),
                user_id=user.id if user else None,
                action="login_attempt",
                status="failure",
                details="Invalid credentials",
                created_at=datetime.utcnow()
            )
            session.add(audit)
            session.commit()
            raise HTTPException(401, "Invalid username or password")

        token = create_token(str(user.id))

        audit = AuditLog(
            id=uuid.uuid4(),
            user_id=user.id,
            action="login_attempt",
            status="success",
            details=None,
            created_at=datetime.utcnow()
        )
        session.add(audit)
        session.commit()

        return {
            "access_token": token,
            "token_type": "bearer",
            "user_id": str(user.id)
        }
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Login failed: {e}")
        raise HTTPException(500, "Login failed")
    finally:
        session.close()


@app.get("/auth/me", response_model=UserResponse)
def get_me(user_id: str = Depends(get_current_user)):
    session = get_session()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(404, "User not found")

        return {
            "id": str(user.id),
            "username": user.username,
            "created_at": user.created_at.isoformat()
        }
    finally:
        session.close()