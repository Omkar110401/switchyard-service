import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class WorkflowType(str, Enum):
    """Workflow types for task filtering."""
    ML = "ml-pipeline"
    ETL = "etl-pipeline"
    ECOMMERCE = "ecommerce"
    CICD = "cicd"
    MEDIA = "media"
    REPORTING = "reporting"
    RETRY_TEST = "retry-test"


class WorkflowDefinition(Base):
    __tablename__ = "workflow_definitions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    dag_definition = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    task_definitions = relationship(
        "TaskDefinition", back_populates="workflow_definition", cascade="all, delete-orphan"
    )
    workflow_runs = relationship(
        "WorkflowRun", back_populates="workflow_definition", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("name", "version", name="uq_workflow_name_version"),)


class TaskDefinition(Base):
    __tablename__ = "task_definitions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workflow_definition_id = Column(
        UUID(as_uuid=True), ForeignKey("workflow_definitions.id"), nullable=False
    )
    task_key = Column(String(255), nullable=False)
    command = Column(String(255), nullable=False)
    params = Column(JSONB, nullable=True)
    retry_max_attempts = Column(Integer, nullable=False, default=0)
    retry_backoff_strategy = Column(String(50), nullable=True)
    on_failure_task_key = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Relationships
    workflow_definition = relationship("WorkflowDefinition", back_populates="task_definitions")
    dependencies = relationship(
        "TaskDependency",
        foreign_keys="TaskDependency.task_definition_id",
        back_populates="task",
        cascade="all, delete-orphan",
    )
    dependents = relationship(
        "TaskDependency",
        foreign_keys="TaskDependency.depends_on_task_definition_id",
        back_populates="depends_on_task",
        cascade="all, delete-orphan",
    )
    task_runs = relationship("TaskRun", back_populates="task_definition", cascade="all, delete-orphan")


class TaskDependency(Base):
    __tablename__ = "task_dependencies"

    task_definition_id = Column(
        UUID(as_uuid=True), ForeignKey("task_definitions.id"), nullable=False
    )
    depends_on_task_definition_id = Column(
        UUID(as_uuid=True), ForeignKey("task_definitions.id"), nullable=False
    )

    # Relationships
    task = relationship(
        "TaskDefinition",
        foreign_keys=[task_definition_id],
        back_populates="dependencies",
    )
    depends_on_task = relationship(
        "TaskDefinition",
        foreign_keys=[depends_on_task_definition_id],
        back_populates="dependents",
    )

    __table_args__ = (
        PrimaryKeyConstraint("task_definition_id", "depends_on_task_definition_id"),
    )


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String(255), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    workflow_runs = relationship("WorkflowRun", back_populates="user", cascade="all, delete-orphan")
    audit_logs = relationship("AuditLog", back_populates="user", cascade="all, delete-orphan")


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    workflow_definition_id = Column(
        UUID(as_uuid=True), ForeignKey("workflow_definitions.id"), nullable=False
    )
    status = Column(String(20), nullable=False, default="pending")
    triggered_by = Column(String(50), nullable=False, default="manual")
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="workflow_runs")
    workflow_definition = relationship("WorkflowDefinition", back_populates="workflow_runs")
    task_runs = relationship("TaskRun", back_populates="workflow_run", cascade="all, delete-orphan")

    is_deleted = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_workflow_runs_user_deleted", "user_id", "is_deleted"),
    )


class TaskRun(Base):
    __tablename__ = "task_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workflow_run_id = Column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.id"), nullable=False
    )
    task_definition_id = Column(
        UUID(as_uuid=True), ForeignKey("task_definitions.id"), nullable=False
    )
    status = Column(String(20), nullable=False, default="pending")
    attempt_number = Column(Integer, nullable=False, default=1)
    worker_id = Column(String(255), nullable=True)
    outputs = Column(JSONB, nullable=True)
    error_message = Column(String, nullable=True)
    heartbeat_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Relationships
    workflow_run = relationship("WorkflowRun", back_populates="task_runs")
    task_definition = relationship("TaskDefinition", back_populates="task_runs")

    __table_args__ = (
        Index("ix_task_runs_workflow_status", "workflow_run_id", "status"),
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    action = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False)
    details = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("User", back_populates="audit_logs")

    __table_args__ = (
        Index("ix_audit_logs_user_created", "user_id", "created_at"),
    )
