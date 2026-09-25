import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

import yaml
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from shared.db import init_db, get_session
from shared.models import WorkflowDefinition, WorkflowRun, TaskDefinition, TaskDependency, TaskRun, User, AuditLog, WorkflowType
from shared.dag import validate_workflow
from shared.auth import hash_password, verify_password, create_token, verify_token

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Switchyard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    depends_on: List[str] = []
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
    progress: str
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


class TaskInfo(BaseModel):
    name: str
    command: str
    workflow_type: WorkflowType
    outputs: List[str]


class AvailableTasksResponse(BaseModel):
    workflow_types: List[str]
    tasks: List[TaskInfo]


class TaskSubmissionInfo(BaseModel):
    id: str
    command: str
    depends_on: List[str] = []
    outputs: Optional[List[str]] = None


class WorkflowSubmissionResponse(BaseModel):
    workflow_run_id: str
    workflow_name: str
    version: int
    status: str
    tasks: List[TaskSubmissionInfo]

class WorkflowDeleteResponse(BaseModel):
    workflow_run_id: str
    status: str
    error: bool


class AnalyticsSummary(BaseModel):
    total_workflows: int
    success_rate: float
    avg_execution_time_seconds: float
    total_failures: int


class TrendData(BaseModel):
    date: str
    success_rate: float


class AnalyticsTrends(BaseModel):
    trends: List[TrendData]


class ErrorData(BaseModel):
    error: str
    count: int


class AnalyticsErrors(BaseModel):
    errors: List[ErrorData]


class TaskStats(BaseModel):
    command: str
    total_runs: int
    successes: int
    failures: int
    avg_time_seconds: float


class AnalyticsTasks(BaseModel):
    tasks: List[TaskStats]

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


@app.get("/workflows/available-tasks", response_model=AvailableTasksResponse)
def list_available_tasks():
    """Return available task commands grouped by workflow type."""
    tasks_data = [
        # ML Pipeline
        TaskInfo(name="fetch_dataset", command="demos/tasks/ml_fetch_data.py", workflow_type=WorkflowType.ML, outputs=["data_path"]),
        TaskInfo(name="feature_engineering", command="demos/tasks/ml_feature_engineering.py", workflow_type=WorkflowType.ML, outputs=["features_path"]),
        TaskInfo(name="train_model", command="demos/tasks/ml_train_model.py", workflow_type=WorkflowType.ML, outputs=["model_path"]),
        TaskInfo(name="evaluate_model", command="demos/tasks/ml_evaluate_model.py", workflow_type=WorkflowType.ML, outputs=["metrics"]),
        TaskInfo(name="upload_to_registry", command="demos/tasks/ml_upload_registry.py", workflow_type=WorkflowType.ML, outputs=["registry_url"]),
        TaskInfo(name="cleanup_staging", command="demos/tasks/ml_cleanup_staging.py", workflow_type=WorkflowType.ML, outputs=["status"]),
        # ETL Pipeline
        TaskInfo(name="fetch_data", command="demos/tasks/fetch_data.py", workflow_type=WorkflowType.ETL, outputs=["data_file"]),
        TaskInfo(name="transform", command="demos/tasks/transform.py", workflow_type=WorkflowType.ETL, outputs=["transformed_data"]),
        TaskInfo(name="validate", command="demos/tasks/validate.py", workflow_type=WorkflowType.ETL, outputs=["validation_result"]),
        TaskInfo(name="load_to_db", command="demos/tasks/load_to_db.py", workflow_type=WorkflowType.ETL, outputs=["rows_loaded"]),
        TaskInfo(name="notify", command="demos/tasks/notify.py", workflow_type=WorkflowType.ETL, outputs=["notification_id"]),
        # E-commerce
        TaskInfo(name="place_order", command="demos/tasks/place_order.py", workflow_type=WorkflowType.ECOMMERCE, outputs=["order_id"]),
        TaskInfo(name="reserve_inventory", command="demos/tasks/reserve_inventory.py", workflow_type=WorkflowType.ECOMMERCE, outputs=["reservation_id"]),
        TaskInfo(name="process_payment", command="demos/tasks/process_payment.py", workflow_type=WorkflowType.ECOMMERCE, outputs=["transaction_id"]),
        TaskInfo(name="confirm_order", command="demos/tasks/confirm_order.py", workflow_type=WorkflowType.ECOMMERCE, outputs=["confirmation"]),
        TaskInfo(name="release_inventory", command="demos/tasks/release_inventory.py", workflow_type=WorkflowType.ECOMMERCE, outputs=["status"]),
        TaskInfo(name="cancel_order", command="demos/tasks/cancel_order.py", workflow_type=WorkflowType.ECOMMERCE, outputs=["status"]),
        # CI/CD Pipeline
        TaskInfo(name="run_unit_tests", command="demos/tasks/run_unit_tests.py", workflow_type=WorkflowType.CICD, outputs=["test_results"]),
        TaskInfo(name="build_artifact", command="demos/tasks/build_artifact.py", workflow_type=WorkflowType.CICD, outputs=["artifact_path"]),
        TaskInfo(name="deploy_staging", command="demos/tasks/deploy_staging.py", workflow_type=WorkflowType.CICD, outputs=["deployment_id"]),
        TaskInfo(name="run_smoke_tests", command="demos/tasks/run_smoke_tests.py", workflow_type=WorkflowType.CICD, outputs=["test_results"]),
        TaskInfo(name="deploy_prod", command="demos/tasks/deploy_prod.py", workflow_type=WorkflowType.CICD, outputs=["deployment_id"]),
        TaskInfo(name="rollback_prod", command="demos/tasks/rollback_prod.py", workflow_type=WorkflowType.CICD, outputs=["status"]),
        # Media Processing
        TaskInfo(name="transcode_1080p", command="demos/tasks/transcode_1080p.py", workflow_type=WorkflowType.MEDIA, outputs=["output_path"]),
        TaskInfo(name="transcode_720p", command="demos/tasks/transcode_720p.py", workflow_type=WorkflowType.MEDIA, outputs=["output_path"]),
        TaskInfo(name="transcode_480p", command="demos/tasks/transcode_480p.py", workflow_type=WorkflowType.MEDIA, outputs=["output_path"]),
        TaskInfo(name="generate_thumbnail", command="demos/tasks/generate_thumbnail.py", workflow_type=WorkflowType.MEDIA, outputs=["thumbnail_path"]),
        TaskInfo(name="run_content_moderation", command="demos/tasks/run_content_moderation.py", workflow_type=WorkflowType.MEDIA, outputs=["approval"]),
        TaskInfo(name="publish_media", command="demos/tasks/publish_media.py", workflow_type=WorkflowType.MEDIA, outputs=["cdn_url"]),
        # Reporting
        TaskInfo(name="fetch_sales_data", command="demos/tasks/fetch_sales_data.py", workflow_type=WorkflowType.REPORTING, outputs=["data_path"]),
        TaskInfo(name="fetch_user_data", command="demos/tasks/fetch_user_data.py", workflow_type=WorkflowType.REPORTING, outputs=["data_path"]),
        TaskInfo(name="fetch_marketing_data", command="demos/tasks/fetch_marketing_data.py", workflow_type=WorkflowType.REPORTING, outputs=["data_path"]),
        TaskInfo(name="merge_datasets", command="demos/tasks/merge_datasets.py", workflow_type=WorkflowType.REPORTING, outputs=["merged_path"]),
        TaskInfo(name="generate_pdf_report", command="demos/tasks/generate_pdf_report.py", workflow_type=WorkflowType.REPORTING, outputs=["pdf_path"]),
        TaskInfo(name="upload_to_storage", command="demos/tasks/upload_to_storage.py", workflow_type=WorkflowType.REPORTING, outputs=["storage_url"]),
        TaskInfo(name="notify_stakeholders", command="demos/tasks/notify_stakeholders.py", workflow_type=WorkflowType.REPORTING, outputs=["status"]),
        # Retry Testing
        TaskInfo(name="flaky_operation", command="demos/tasks/flaky_task.py", workflow_type=WorkflowType.RETRY_TEST, outputs=["result"]),
    ]

    workflow_types = sorted(list(set(t.workflow_type.value for t in tasks_data)))

    return AvailableTasksResponse(workflow_types=workflow_types, tasks=tasks_data)


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
            WorkflowRun.user_id == user_id,
            WorkflowRun.is_deleted==False
        ).order_by(WorkflowRun.created_at.desc()).all()

        summaries = []
        for run in workflow_runs:
            workflow_def = run.workflow_definition
            total_tasks = len(workflow_def.task_definitions)

            completed_count = session.query(TaskRun).filter(
                TaskRun.workflow_run_id == run.id,
                TaskRun.status == "succeeded"
            ).count()

            progress = f"{completed_count}/{total_tasks}"

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
            WorkflowRun.is_deleted==False,
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

            depends_on = [str(dep.depends_on_task.id) for dep in task_def.dependencies]

            tasks.append(TaskRunResponse(
                id=str(task_def.id),
                key=task_def.task_key,
                command=task_def.command,
                status=status,
                attempt_number=attempt_number,
                max_attempts=task_def.retry_max_attempts if task_def.retry_max_attempts > 0 else 1,
                depends_on=depends_on,
                outputs=outputs,
                error_message=error_message,
                created_at=created_at or datetime.utcnow().isoformat(),
                started_at=started_at,
                completed_at=completed_at,
                heartbeat_at=heartbeat_at
            ))

        completed_count = sum(1 for t in tasks if t.status == "succeeded")
        total_count = len(tasks)
        progress = f"{completed_count}/{total_count}"

        return WorkflowDetailResponse(
            id=str(workflow_run.id),
            name=workflow_def.name,
            version=workflow_def.version,
            status=workflow_run.status,
            progress=progress,
            created_at=workflow_run.created_at.isoformat(),
            started_at=workflow_run.started_at.isoformat() if workflow_run.started_at else None,
            completed_at=workflow_run.completed_at.isoformat() if workflow_run.completed_at else None,
            tasks=tasks
        )
    finally:
        session.close()


VALID_COMMANDS = {
    # ML Pipeline
    "demos/tasks/ml_fetch_data.py",
    "demos/tasks/ml_feature_engineering.py",
    "demos/tasks/ml_train_model.py",
    "demos/tasks/ml_evaluate_model.py",
    "demos/tasks/ml_upload_registry.py",
    "demos/tasks/ml_cleanup_staging.py",
    # ETL Pipeline
    "demos/tasks/fetch_data.py",
    "demos/tasks/transform.py",
    "demos/tasks/validate.py",
    "demos/tasks/load_to_db.py",
    "demos/tasks/notify.py",
    # E-commerce
    "demos/tasks/place_order.py",
    "demos/tasks/reserve_inventory.py",
    "demos/tasks/process_payment.py",
    "demos/tasks/confirm_order.py",
    "demos/tasks/release_inventory.py",
    "demos/tasks/cancel_order.py",
    # CI/CD Pipeline
    "demos/tasks/run_unit_tests.py",
    "demos/tasks/build_artifact.py",
    "demos/tasks/deploy_staging.py",
    "demos/tasks/run_smoke_tests.py",
    "demos/tasks/deploy_prod.py",
    "demos/tasks/rollback_prod.py",
    # Media Processing
    "demos/tasks/transcode_1080p.py",
    "demos/tasks/transcode_720p.py",
    "demos/tasks/transcode_480p.py",
    "demos/tasks/generate_thumbnail.py",
    "demos/tasks/run_content_moderation.py",
    "demos/tasks/publish_media.py",
    # Reporting
    "demos/tasks/fetch_sales_data.py",
    "demos/tasks/fetch_user_data.py",
    "demos/tasks/fetch_marketing_data.py",
    "demos/tasks/merge_datasets.py",
    "demos/tasks/generate_pdf_report.py",
    "demos/tasks/upload_to_storage.py",
    "demos/tasks/notify_stakeholders.py",
    # Retry Testing
    "demos/tasks/flaky_task.py",
}


@app.post("/workflows", response_model=WorkflowSubmissionResponse)
def submit_workflow(workflow: WorkflowSubmission, user_id: str = Depends(get_current_user)):
    """Submit a workflow for execution."""
    try:
        workflow_dict = workflow.dict()

        result = validate_workflow(workflow_dict)
        if not result.is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid workflow: {result.errors}")

        for task in workflow_dict.get("tasks", []):
            if task["command"] not in VALID_COMMANDS:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown command: {task['command']}. Available commands: {', '.join(sorted(VALID_COMMANDS))}"
                )

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

            tasks_response = [
                TaskSubmissionInfo(
                    id=task["id"],
                    command=task["command"],
                    depends_on=task.get("depends_on", []),
                    outputs=task.get("outputs")
                )
                for task in workflow_dict.get("tasks", [])
            ]

            return WorkflowSubmissionResponse(
                workflow_run_id=str(workflow_run.id),
                workflow_name=workflow_dict["name"],
                version=version,
                status="pending",
                tasks=tasks_response
            )

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


@app.delete("/workflows/{workflow_run_id}", response_model=WorkflowDeleteResponse)
def delete_workflow(workflow_run_id: str, user_id: str=Depends(get_current_user)):
    session=get_session()
    try:
        workflow=session.query(WorkflowRun).filter(
            WorkflowRun.id==workflow_run_id,
            WorkflowRun.user_id==user_id
        ).first()

        if not workflow:
            raise HTTPException(404, "Workflow not found.")
        
        workflow.is_deleted=True
        audit = AuditLog(
            id=uuid.uuid4(),
            user_id=user_id,
            action="delete_workflow",
            status="success",
            details=workflow_run_id,
            created_at=datetime.utcnow()
        )
        session.add(audit)
        session.commit()

        return WorkflowDeleteResponse(
                workflow_run_id= workflow_run_id,
                status= "Workflow deleted successfully.",
                error= False
        )
    finally:
        session.close()


@app.get("/analytics/summary", response_model=AnalyticsSummary)
def get_analytics_summary(user_id: str = Depends(get_current_user)):
    session = get_session()
    try:
        workflows = session.query(WorkflowRun).filter(
            WorkflowRun.user_id == user_id,
            WorkflowRun.is_deleted == False
        ).all()

        total_workflows = len(workflows)
        succeeded = len([w for w in workflows if w.status == "succeeded"])
        failed = len([w for w in workflows if w.status == "failed"])

        success_rate = (succeeded / total_workflows * 100) if total_workflows > 0 else 0

        completed_workflows = [w for w in workflows if w.completed_at and w.started_at]
        avg_time = 0.0
        if completed_workflows:
            total_time = sum((w.completed_at - w.started_at).total_seconds() for w in completed_workflows)
            avg_time = total_time / len(completed_workflows)

        return AnalyticsSummary(
            total_workflows=total_workflows,
            success_rate=round(success_rate, 2),
            avg_execution_time_seconds=round(avg_time, 2),
            total_failures=failed
        )
    finally:
        session.close()


@app.get("/analytics/trends", response_model=AnalyticsTrends)
def get_analytics_trends(user_id: str = Depends(get_current_user)):
    from sqlalchemy import func
    session = get_session()
    try:
        workflows = session.query(WorkflowRun).filter(
            WorkflowRun.user_id == user_id,
            WorkflowRun.is_deleted == False
        ).all()

        trends_dict = {}
        for workflow in workflows:
            date_str = workflow.created_at.date().isoformat()
            if date_str not in trends_dict:
                trends_dict[date_str] = {"total": 0, "succeeded": 0}
            trends_dict[date_str]["total"] += 1
            if workflow.status == "succeeded":
                trends_dict[date_str]["succeeded"] += 1

        trends = [
            TrendData(
                date=date,
                success_rate=round((data["succeeded"] / data["total"] * 100) if data["total"] > 0 else 0, 2)
            )
            for date, data in sorted(trends_dict.items(), reverse=True)
        ]

        return AnalyticsTrends(trends=trends)
    finally:
        session.close()


@app.get("/analytics/errors", response_model=AnalyticsErrors)
def get_analytics_errors(user_id: str = Depends(get_current_user)):
    from sqlalchemy import func
    session = get_session()
    try:
        failed_workflows = session.query(WorkflowRun.id).filter(
            WorkflowRun.user_id == user_id,
            WorkflowRun.status == "failed",
            WorkflowRun.is_deleted == False
        ).all()

        failed_workflow_ids = [w.id for w in failed_workflows]

        if not failed_workflow_ids:
            return AnalyticsErrors(errors=[])

        error_data = session.query(
            TaskRun.error_message,
            func.count(TaskRun.id).label("count")
        ).filter(
            TaskRun.workflow_run_id.in_(failed_workflow_ids),
            TaskRun.error_message.isnot(None)
        ).group_by(TaskRun.error_message).order_by(
            func.count(TaskRun.id).desc()
        ).limit(10).all()

        errors = [
            ErrorData(error=row.error_message, count=row.count)
            for row in error_data
        ]

        return AnalyticsErrors(errors=errors)
    finally:
        session.close()


@app.get("/analytics/tasks", response_model=AnalyticsTasks)
def get_analytics_tasks(user_id: str = Depends(get_current_user)):
    from sqlalchemy import func, extract, case
    session = get_session()
    try:
        task_stats = session.query(
            TaskDefinition.command,
            func.count(TaskRun.id).label("total_runs"),
            func.count(case((TaskRun.status == "succeeded", 1))).label("successes"),
            func.count(case((TaskRun.status == "failed", 1))).label("failures"),
            func.avg(
                extract("epoch", TaskRun.completed_at - TaskRun.started_at)
            ).label("avg_time_seconds")
        ).join(
            TaskDefinition, TaskRun.task_definition_id == TaskDefinition.id
        ).join(
            WorkflowRun, TaskRun.workflow_run_id == WorkflowRun.id
        ).filter(
            WorkflowRun.user_id == user_id,
            WorkflowRun.is_deleted == False
        ).group_by(TaskDefinition.command).order_by(
            func.count(TaskRun.id).desc()
        ).all()

        tasks = [
            TaskStats(
                command=row.command,
                total_runs=row.total_runs,
                successes=row.successes or 0,
                failures=row.failures or 0,
                avg_time_seconds=round(float(row.avg_time_seconds) if row.avg_time_seconds else 0, 2)
            )
            for row in task_stats
        ]

        return AnalyticsTasks(tasks=tasks)
    finally:
        session.close()
    