"""DAG (Directed Acyclic Graph) implementation."""

from datetime import datetime
from typing import List, Dict, Optional, Any, Union
from ..operators.base_operator import BaseOperator
from enum import Enum

class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

class DAGNode:
    """Base class for DAG nodes."""
    
    def __init__(self, node_id: str, name: str = None):
        self.node_id = node_id
        self.name = name or node_id
        self.status = TaskStatus.PENDING
        self.created_at = datetime.now()
    
    def get_info(self) -> Dict[str, Any]:
        """Get node information."""
        return {
            "id": self.node_id,
            "name": self.name,
            "status": self.status.value,
            "created_at": self.created_at.isoformat()
        }
    
    def reset_status(self):
        """Reset node status to pending."""
        self.status = TaskStatus.PENDING

class DAG(DAGNode):
    """Directed Acyclic Graph for workflow management."""
    
    def __init__(self, dag_id: str, description: str = "", schedule_interval: str = None):
        super().__init__(dag_id, dag_id)
        self.description = description
        self.schedule_interval = schedule_interval
        self.tasks: List[Union[DAGNode, BaseOperator]] = []
        self.dependencies: Dict[str, List[str]] = {}
        self._is_paused = False
    
    def add_task(self, task: DAGNode):
        """Add a task to the DAG."""
        if task.node_id not in [t.node_id for t in self.tasks]:
            self.tasks.append(task)
            self.dependencies[task.node_id] = []
    
    def add_dependency(self, upstream_task_id: str, downstream_task_id: str):
        """Add dependency between tasks."""
        if downstream_task_id in self.dependencies:
            if upstream_task_id not in self.dependencies[downstream_task_id]:
                self.dependencies[downstream_task_id].append(upstream_task_id)
    
    def get_task_count(self) -> int:
        """Get total number of tasks."""
        return len(self.tasks)
    
    def pause(self):
        """Pause the DAG."""
        self._is_paused = True
    
    def unpause(self):
        """Unpause the DAG."""
        self._is_paused = False
    
    def is_paused(self) -> bool:
        """Check if DAG is paused."""
        return self._is_paused
    
    def validate_structure(self) -> bool:
        """Validate DAG structure for cycles."""
        validator = DAGValidator(self)
        return validator.is_valid()
        
    def to_visualization_data(self) -> Dict[str, Any]:
        """Convert DAG to frontend visualization format."""
        from ..utils.helpers import generate_dag_visualization_data
        return generate_dag_visualization_data(self)  # 添加调用关系

class DAGValidator:
    """Validator for DAG structure."""
    
    def __init__(self, dag: DAG):
        self.dag = dag
        self._visited = set()
        self._recursion_stack = set()
    
    def is_valid(self) -> bool:
        """Check if DAG is valid (no cycles)."""
        self._visited.clear()
        self._recursion_stack.clear()
        
        for task in self.dag.tasks:
            if task.node_id not in self._visited:
                if self._has_cycle(task.node_id):
                    return False
        return True
    
    def _has_cycle(self, task_id: str) -> bool:
        """Check for cycles using DFS."""
        self._visited.add(task_id)
        self._recursion_stack.add(task_id)
        
        for dependency in self.dag.dependencies.get(task_id, []):
            if dependency not in self._visited:
                if self._has_cycle(dependency):
                    return True
            elif dependency in self._recursion_stack:
                return True
        
        self._recursion_stack.remove(task_id)
        return False
    
    def get_validation_report(self) -> Dict[str, Any]:
        """Get detailed validation report."""
        is_valid = self.is_valid()
        return {
            "is_valid": is_valid,
            "task_count": len(self.dag.tasks),
            "dependency_count": sum(len(deps) for deps in self.dag.dependencies.values()),
            "validation_time": datetime.now().isoformat()
        }

def create_dag(dag_id: str, description: str = "", tasks: List[str] = None) -> DAG:
    """Factory function to create a DAG with basic tasks."""
    dag = DAG(dag_id, description)
    
    if tasks:
        for task_id in tasks:
            task = DAGNode(task_id)
            dag.add_task(task)
    
    return dag

# Global constants
DEFAULT_DAG_ARGS = {
    "retries": 1,
    "retry_delay": 300,
    "email_on_failure": False,
    "email_on_retry": False
}

MAX_TASK_INSTANCES = 100
