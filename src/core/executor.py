"""Executor implementations for task execution."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import threading
from .dag import DAG, DAGNode, TaskStatus

class BaseExecutor(ABC):
    """Abstract base class for executors."""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.running_tasks: Dict[str, Any] = {}
        self.completed_tasks: List[str] = []
        self.failed_tasks: List[str] = []
    
    @abstractmethod
    def execute_task(self, task: DAGNode) -> bool:
        """Execute a single task."""
        pass
    
    @abstractmethod
    def execute_dag(self, dag: DAG) -> Dict[str, Any]:
        """Execute an entire DAG."""
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        return {
            "max_workers": self.max_workers,
            "running_tasks": len(self.running_tasks),
            "completed_tasks": len(self.completed_tasks),
            "failed_tasks": len(self.failed_tasks),
            "total_processed": len(self.completed_tasks) + len(self.failed_tasks)
        }
    
    def reset_stats(self):
        """Reset execution statistics."""
        self.running_tasks.clear()
        self.completed_tasks.clear()
        self.failed_tasks.clear()

class LocalExecutor(BaseExecutor):
    """Local executor for running tasks in the same process."""
    
    def __init__(self, max_workers: int = 4, use_threading: bool = True):
        super().__init__(max_workers)
        self.use_threading = use_threading
        self.thread_pool: List[threading.Thread] = []
    
    def execute_task(self, task: DAGNode) -> bool:
        """Execute a single task locally."""
        try:
            task.status = TaskStatus.RUNNING
            self.running_tasks[task.node_id] = {
                "task": task,
                "start_time": datetime.now()
            }
            
            # Simulate task execution
            result = self._run_task_logic(task)
            
            if result:
                task.status = TaskStatus.SUCCESS
                self.completed_tasks.append(task.node_id)
            else:
                task.status = TaskStatus.FAILED
                self.failed_tasks.append(task.node_id)
            
            self.running_tasks.pop(task.node_id, None)
            return result
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            self.failed_tasks.append(task.node_id)
            self.running_tasks.pop(task.node_id, None)
            return False
    
    def execute_dag(self, dag: DAG) -> Dict[str, Any]:
        """Execute an entire DAG locally."""
        # 使用 DAGValidator 进行验证
        validator = DAGValidator(dag)  # 添加这个调用关系
        if not validator.is_valid():
            validation_report = validator.get_validation_report()  # 添加调用
            return {
                "success": False, 
                "error": "Invalid DAG structure",
                "validation_report": validation_report
            }
        
        execution_start = datetime.now()
        task_results = {}
        
        # 使用 TaskRunner 执行任务
        task_runner = TaskRunner(self)  # 添加这个关系
        
        for task in dag.tasks:
            if not dag.is_paused():
                # 如果是 BaseOperator，使用其 execute 方法
                if hasattr(task, 'execute'):
                    try:
                        task.pre_execute({})  # 添加调用关系
                        context = {"execution_date": execution_start, "dag": dag}
                        result = task.execute(context)
                        task.post_execute(context, result)  # 添加调用关系
                        task_results[task.node_id] = True
                    except Exception as e:
                        task_results[task.node_id] = False
                else:
                    # 普通 DAGNode 使用 TaskRunner
                    result = task_runner.run_with_retry(task)  # 添加调用关系
                    task_results[task.node_id] = result
        
        execution_end = datetime.now()
        
        return {
            "success": len(self.failed_tasks) == 0,
            "execution_time": (execution_end - execution_start).total_seconds(),
            "task_results": task_results,
            "stats": self.get_stats()
        }
    
    def _run_task_logic(self, task: DAGNode) -> bool:
        """Run the actual task logic."""
        # Simulate some work
        import time
        time.sleep(0.1)  # Simulate task execution time
        
        # Simple success condition for testing
        return len(task.node_id) > 0

class TaskRunner:
    """Helper class for running individual tasks."""
    
    def __init__(self, executor: BaseExecutor):
        self.executor = executor
        self.retry_count = 3
        self.retry_delay = 1.0
    
    def run_with_retry(self, task: DAGNode) -> bool:
        """Run task with retry logic."""
        for attempt in range(self.retry_count):
            if self.executor.execute_task(task):
                return True
            
            if attempt < self.retry_count - 1:
                import time
                time.sleep(self.retry_delay)
                task.reset_status()  # Reset for retry
        
        return False
    
    def set_retry_config(self, count: int, delay: float):
        """Configure retry behavior."""
        self.retry_count = max(1, count)
        self.retry_delay = max(0.1, delay)

# Factory function
def create_executor(executor_type: str = "local", **kwargs) -> BaseExecutor:
    """Factory function to create executors."""
    if executor_type.lower() == "local":
        return LocalExecutor(**kwargs)
    else:
        raise ValueError(f"Unknown executor type: {executor_type}")
