from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional
from enum import Enum


class ValidationError(Enum):
    CYCLE_DETECTED = "cycle_detected"
    MISSING_DEPENDENCY = "missing_dependency"
    SELF_LOOP = "self_loop"
    INVALID_TASK = "invalid_task"


@dataclass
class DAGValidationResult:
    is_valid: bool
    errors: List[tuple] = field(default_factory=list)
    topological_order: Optional[List[str]] = None


class DAGValidator:
    """Validates and analyzes workflow DAGs."""

    def __init__(self, workflow_definition: dict):
        self.workflow_definition = workflow_definition
        self.tasks = {task["id"]: task for task in workflow_definition.get("tasks", [])}
        self.task_ids = set(self.tasks.keys())

        self.adjacency_list = {}
        self.reverse_adjacency = {}
        for task_id in self.task_ids:
            self.adjacency_list[task_id] = []
            self.reverse_adjacency[task_id] = []

        for task_id, task in self.tasks.items():
            deps = task.get("depends_on", [])
            if isinstance(deps, str):
                deps = [deps]
            for dep in deps:
                if dep in self.task_ids:
                    self.adjacency_list[dep].append(task_id)
                    self.reverse_adjacency[task_id].append(dep)

    def validate(self) -> DAGValidationResult:
        """Run all validations and return result."""
        errors = []

        for task_id, task in self.tasks.items():
            deps = task.get("depends_on", [])
            if isinstance(deps, str):
                deps = [deps]

            if task_id in deps:
                errors.append((ValidationError.SELF_LOOP, task_id, "task depends on itself"))

            for dep in deps:
                if dep not in self.task_ids:
                    errors.append((ValidationError.MISSING_DEPENDENCY, task_id, f"depends on missing task '{dep}'"))

        if self._has_cycle():
            errors.append((ValidationError.CYCLE_DETECTED, None, "workflow contains circular dependency"))

        if errors:
            return DAGValidationResult(is_valid=False, errors=errors)

        topo_order = self._topological_sort()
        return DAGValidationResult(is_valid=True, errors=[], topological_order=topo_order)

    def _has_cycle(self) -> bool:
        """Detect cycles using DFS with white/gray/black coloring."""
        colors = {task_id: "white" for task_id in self.task_ids}

        def dfs(node):
            if colors[node] == "gray":
                return True
            if colors[node] == "black":
                return False

            colors[node] = "gray"
            for neighbor in self.adjacency_list[node]:
                if dfs(neighbor):
                    return True
            colors[node] = "black"
            return False

        for task_id in self.task_ids:
            if colors[task_id] == "white":
                if dfs(task_id):
                    return True

        return False

    def _topological_sort(self) -> List[str]:
        """Return tasks in topological order using DFS post-order."""
        visited = set()
        order = []

        def dfs(node):
            if node in visited:
                return
            visited.add(node)
            for neighbor in self.adjacency_list[node]:
                dfs(neighbor)
            order.append(node)

        for task_id in self.task_ids:
            if task_id not in visited:
                dfs(task_id)

        return order[::-1]

    def get_ready_tasks(self, completed_tasks: Set[str]) -> List[str]:
        """Find tasks that can run now (all deps done, not yet started)."""
        ready = []
        for task_id in self.task_ids:
            if task_id in completed_tasks:
                continue

            deps = self.reverse_adjacency[task_id]
            if all(dep in completed_tasks for dep in deps):
                ready.append(task_id)

        return ready

    def get_dependents(self, task_id: str) -> List[str]:
        """Return all tasks that depend on this task."""
        return self.adjacency_list.get(task_id, [])

    def get_dependencies(self, task_id: str) -> List[str]:
        """Return all tasks this task depends on."""
        return self.reverse_adjacency.get(task_id, [])


def parse_workflow_yaml(yaml_dict: dict) -> Dict:
    """Parse and validate workflow structure."""
    if not isinstance(yaml_dict, dict):
        raise ValueError("Workflow must be a dict")

    if "name" not in yaml_dict:
        raise ValueError("Workflow must have 'name'")

    if "tasks" not in yaml_dict:
        raise ValueError("Workflow must have 'tasks'")

    if not isinstance(yaml_dict["tasks"], list):
        raise ValueError("'tasks' must be a list")

    for task in yaml_dict["tasks"]:
        if not isinstance(task, dict):
            raise ValueError("Each task must be a dict")

        if "id" not in task:
            raise ValueError("Each task must have 'id'")

        if "command" not in task:
            raise ValueError(f"Task '{task['id']}' must have 'command'")

        if "depends_on" in task and not isinstance(task["depends_on"], (list, str)):
            raise ValueError(f"Task '{task['id']}' depends_on must be list or string")

        if "depends_on" in task and isinstance(task["depends_on"], str):
            task["depends_on"] = [task["depends_on"]]

        if "depends_on" not in task:
            task["depends_on"] = []

    return yaml_dict


def validate_workflow(workflow_definition: dict) -> DAGValidationResult:
    """High-level validation: parse and validate a workflow."""
    try:
        normalized = parse_workflow_yaml(workflow_definition)
        validator = DAGValidator(normalized)
        return validator.validate()
    except ValueError as e:
        return DAGValidationResult(
            is_valid=False,
            errors=[(ValidationError.INVALID_TASK, None, str(e))]
        )