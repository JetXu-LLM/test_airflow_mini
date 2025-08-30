"""DAG (Directed Acyclic Graph) implementation."""

from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union, Set
from enum import Enum
import json

class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"  # 新增状态
    UP_FOR_RETRY = "up_for_retry"       # 新增状态
    QUEUED = "queued"                   # 新增状态

class DAGState(Enum):
    """DAG state enumeration."""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PAUSED = "paused"

class DAGNode:
    """Base class for DAG nodes."""
    
    def __init__(self, node_id: str, name: str = None):
        self.node_id = node_id
        self.name = name or node_id
        self.status = TaskStatus.PENDING
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.metadata: Dict[str, Any] = {}
        self.tags: Set[str] = set()
    
    def get_info(self) -> Dict[str, Any]:
        """Get node information."""
        return {
            "id": self.node_id,
            "name": self.name,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata,
            "tags": list(self.tags)
        }
    
    def reset_status(self):
        """Reset node status to pending."""
        self.status = TaskStatus.PENDING
        self.updated_at = datetime.now()
    
    def add_tag(self, tag: str):
        """Add a tag to the node."""
        self.tags.add(tag)
        self.updated_at = datetime.now()
    
    def remove_tag(self, tag: str):
        """Remove a tag from the node."""
        self.tags.discard(tag)
        self.updated_at = datetime.now()
    
    def set_metadata(self, key: str, value: Any):
        """Set metadata for the node."""
        self.metadata[key] = value
        self.updated_at = datetime.now()

# 新增：DAG模板类
class DAGTemplate:
    """Template for creating standardized DAGs."""
    
    def __init__(self, template_id: str, name: str, description: str = ""):
        self.template_id = template_id
        self.name = name
        self.description = description
        self.default_args: Dict[str, Any] = {}
        self.task_templates: List[Dict[str, Any]] = []
        self.created_at = datetime.now()
    
    def add_task_template(self, task_type: str, task_config: Dict[str, Any]):
        """Add a task template."""
        self.task_templates.append({
            "type": task_type,
            "config": task_config,
            "order": len(self.task_templates)
        })
    
    def create_dag_from_template(self, dag_id: str, custom_args: Dict[str, Any] = None) -> 'DAG':
        """Create a DAG instance from this template."""
        merged_args = {**self.default_args, **(custom_args or {})}
        dag = DAG(dag_id, self.description, **merged_args)
        
        # Add tasks based on template
        for task_template in self.task_templates:
            # This would create actual tasks based on template
            # Implementation would depend on task types
            pass
        
        return dag

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..operators.base_operator import BaseOperator

class DAG(DAGNode):
    """Directed Acyclic Graph for workflow management."""
    
    def __init__(self, dag_id: str, description: str = "", schedule_interval: str = None,
                 start_date: datetime = None, end_date: datetime = None, 
                 catchup: bool = True, max_active_runs: int = 16):
        super().__init__(dag_id, dag_id)
        self.description = description
        self.schedule_interval = schedule_interval
        self.start_date = start_date or datetime.now()
        self.end_date = end_date
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.tasks: List[Union[DAGNode, 'BaseOperator']] = []
        self.dependencies: Dict[str, List[str]] = {}
        self._is_paused = False
        self.state = DAGState.RUNNING
        self.owner = "airflow"
        self.email: List[str] = []
        self.email_on_failure = True
        self.email_on_retry = True
        self.retries = 1
        self.retry_delay = timedelta(minutes=5)
        self.doc_md: Optional[str] = None
        
        # 新增属性
        self.concurrency = 16
        self.dagrun_timeout: Optional[timedelta] = None
        self.sla_miss_callback: Optional[callable] = None
        self.default_view = "graph"
        self.orientation = "LR"
        self.is_subdag = False
        self.template_searchpath: Optional[List[str]] = None
    
    def add_task(self, task: DAGNode):
        """Add a task to the DAG."""
        if task.node_id not in [t.node_id for t in self.tasks]:
            self.tasks.append(task)
            self.dependencies[task.node_id] = []
            # 设置任务的DAG引用
            if hasattr(task, 'dag'):
                task.dag = self
    
    def remove_task(self, task_id: str):
        """Remove a task from the DAG."""
        self.tasks = [t for t in self.tasks if t.node_id != task_id]
        if task_id in self.dependencies:
            del self.dependencies[task_id]
        
        # Remove from other tasks' dependencies
        for deps in self.dependencies.values():
            if task_id in deps:
                deps.remove(task_id)
    
    def add_dependency(self, upstream_task_id: str, downstream_task_id: str):
        """Add dependency between tasks."""
        if downstream_task_id in self.dependencies:
            if upstream_task_id not in self.dependencies[downstream_task_id]:
                self.dependencies[downstream_task_id].append(upstream_task_id)
    
    def get_task_count(self) -> int:
        """Get total number of tasks."""
        return len(self.tasks)
    
    def get_task_by_id(self, task_id: str) -> Optional[DAGNode]:
        """Get task by ID."""
        for task in self.tasks:
            if task.node_id == task_id:
                return task
        return None
    
    def pause(self):
        """Pause the DAG."""
        self._is_paused = True
        self.state = DAGState.PAUSED
    
    def unpause(self):
        """Unpause the DAG."""
        self._is_paused = False
        self.state = DAGState.RUNNING
    
    def is_paused(self) -> bool:
        """Check if DAG is paused."""
        return self._is_paused
    
    def validate_structure(self) -> bool:
        """Validate DAG structure for cycles."""
        validator = DAGValidator(self)
        return validator.is_valid()
    
    # 新增方法：获取根任务
    def get_root_tasks(self) -> List[DAGNode]:
        """Get tasks with no upstream dependencies."""
        root_tasks = []
        for task in self.tasks:
            if not self.dependencies.get(task.node_id):
                root_tasks.append(task)
        return root_tasks
    
    # 新增方法：获取叶子任务
    def get_leaf_tasks(self) -> List[DAGNode]:
        """Get tasks with no downstream dependencies."""
        downstream_tasks = set()
        for deps in self.dependencies.values():
            downstream_tasks.update(deps)
        
        leaf_tasks = []
        for task in self.tasks:
            if task.node_id not in downstream_tasks:
                leaf_tasks.append(task)
        return leaf_tasks
    
    # 新增方法：获取任务的直接下游
    def get_task_downstream(self, task_id: str) -> List[str]:
        """Get direct downstream tasks of a given task."""
        downstream = []
        for task_id_check, deps in self.dependencies.items():
            if task_id in deps:
                downstream.append(task_id_check)
        return downstream
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert DAG to dictionary representation."""
        return {
            "dag_id": self.node_id,
            "description": self.description,
            "schedule_interval": self.schedule_interval,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "is_paused": self._is_paused,
            "state": self.state.value,
            "task_count": len(self.tasks),
            "owner": self.owner,
            "tags": list(self.tags),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
    
    def to_visualization_data(self) -> Dict[str, Any]:
        """Convert DAG to frontend visualization format."""
        from ..utils.helpers import generate_dag_visualization_data
        return generate_dag_visualization_data(self)
    
    # 新增方法：从JSON创建DAG
    @classmethod
    def from_dict(cls, dag_dict: Dict[str, Any]) -> 'DAG':
        """Create DAG from dictionary representation."""
        dag = cls(
            dag_id=dag_dict["dag_id"],
            description=dag_dict.get("description", ""),
            schedule_interval=dag_dict.get("schedule_interval")
        )
        
        if dag_dict.get("start_date"):
            dag.start_date = datetime.fromisoformat(dag_dict["start_date"])
        if dag_dict.get("end_date"):
            dag.end_date = datetime.fromisoformat(dag_dict["end_date"])
        
        dag._is_paused = dag_dict.get("is_paused", False)
        dag.owner = dag_dict.get("owner", "airflow")
        
        return dag

class DAGValidator:
    """Validator for DAG structure."""
    
    def __init__(self, dag: DAG):
        self.dag = dag
        self._visited = set()
        self._recursion_stack = set()
        self.validation_errors: List[str] = []
    
    def is_valid(self) -> bool:
        """Check if DAG is valid (no cycles)."""
        self._visited.clear()
        self._recursion_stack.clear()
        self.validation_errors.clear()
        
        # Check for cycles
        for task in self.dag.tasks:
            if task.node_id not in self._visited:
                if self._has_cycle(task.node_id):
                    self.validation_errors.append(f"Cycle detected involving task {task.node_id}")
                    return False
        
        # Additional validations
        self._validate_task_dependencies()
        self._validate_dag_structure()
        
        return len(self.validation_errors) == 0
    
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
    
    def _validate_task_dependencies(self):
        """Validate that all task dependencies exist."""
        task_ids = {task.node_id for task in self.dag.tasks}
        
        for task_id, deps in self.dag.dependencies.items():
            if task_id not in task_ids:
                self.validation_errors.append(f"Task {task_id} in dependencies but not in tasks")
            
            for dep in deps:
                if dep not in task_ids:
                    self.validation_errors.append(f"Dependency {dep} for task {task_id} does not exist")
    
    def _validate_dag_structure(self):
        """Validate overall DAG structure."""
        if not self.dag.tasks:
            self.validation_errors.append("DAG has no tasks")
        
        # Check for orphaned tasks (no upstream or downstream)
        connected_tasks = set()
        for task_id, deps in self.dag.dependencies.items():
            connected_tasks.add(task_id)
            connected_tasks.update(deps)
        
        for task in self.dag.tasks:
            if task.node_id not in connected_tasks and len(self.dag.tasks) > 1:
                self.validation_errors.append(f"Task {task.node_id} is not connected to any other task")
    
    def get_validation_report(self) -> Dict[str, Any]:
        """Get detailed validation report."""
        is_valid = self.is_valid()
        return {
            "is_valid": is_valid,
            "task_count": len(self.dag.tasks),
            "dependency_count": sum(len(deps) for deps in self.dag.dependencies.values()),
            "validation_time": datetime.now().isoformat(),
            "errors": self.validation_errors,
            "root_tasks": len(self.dag.get_root_tasks()),
            "leaf_tasks": len(self.dag.get_leaf_tasks())
        }

# 新增：DAG工厂类
class DAGFactory:
    """Factory for creating different types of DAGs."""
    
    @staticmethod
    def create_etl_dag(dag_id: str, source_config: Dict[str, Any], 
                      target_config: Dict[str, Any]) -> DAG:
        """Create an ETL (Extract, Transform, Load) DAG."""
        dag = DAG(
            dag_id=dag_id,
            description=f"ETL DAG: {source_config.get('name', 'Unknown')} to {target_config.get('name', 'Unknown')}",
            schedule_interval="@daily"
        )
        
        # Add ETL-specific tags
        dag.add_tag("etl")
        dag.add_tag("data-pipeline")
        
        return dag
    
    @staticmethod
    def create_ml_training_dag(dag_id: str, model_config: Dict[str, Any]) -> DAG:
        """Create a machine learning training DAG."""
        dag = DAG(
            dag_id=dag_id,
            description=f"ML Training DAG for {model_config.get('model_name', 'Unknown Model')}",
            schedule_interval="@weekly"
        )
        
        dag.add_tag("ml")
        dag.add_tag("training")
        dag.add_tag(model_config.get('model_type', 'unknown'))
        
        return dag

def create_dag(dag_id: str, description: str = "", tasks: List[str] = None, **kwargs) -> DAG:
    """Factory function to create a DAG with basic tasks."""
    dag = DAG(dag_id, description, **kwargs)
    
    if tasks:
        for task_id in tasks:
            task = DAGNode(task_id)
            dag.add_task(task)
    
    return dag

# 修改：扩展全局常量
DEFAULT_DAG_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": datetime(2023, 1, 1),
    "catchup": False
}

MAX_TASK_INSTANCES = 100
DEFAULT_POOL_SIZE = 128
DEFAULT_QUEUE = "default"

# 新增：DAG序列化工具
class DAGSerializer:
    """Utility for serializing and deserializing DAGs."""
    
    @staticmethod
    def serialize_dag(dag: DAG) -> str:
        """Serialize DAG to JSON string."""
        dag_dict = dag.to_dict()
        
        # Add tasks information
        dag_dict["tasks"] = []
        for task in dag.tasks:
            task_info = task.get_info()
            if hasattr(task, 'operator_type'):
                task_info["operator_type"] = task.operator_type.value
            dag_dict["tasks"].append(task_info)
        
        # Add dependencies
        dag_dict["dependencies"] = dag.dependencies
        
        return json.dumps(dag_dict, indent=2, default=str)
    
    @staticmethod
    def deserialize_dag(json_str: str) -> DAG:
        """Deserialize DAG from JSON string."""
        dag_dict = json.loads(json_str)
        dag = DAG.from_dict(dag_dict)
        
        # Recreate tasks (simplified - would need more complex logic for operators)
        for task_dict in dag_dict.get("tasks", []):
            task = DAGNode(task_dict["id"], task_dict["name"])
            dag.add_task(task)
        
        # Recreate dependencies
        dag.dependencies = dag_dict.get("dependencies", {})
        
        return dag