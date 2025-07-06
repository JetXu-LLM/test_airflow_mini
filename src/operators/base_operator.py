"""Base operator classes for task execution."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Callable, Optional
from enum import Enum
from datetime import datetime
from ..core.dag import DAGNode, TaskStatus

class OperatorType(Enum):
    """Types of operators."""
    PYTHON = "python"
    BASH = "bash"
    SQL = "sql"
    HTTP = "http"
    EMAIL = "email"

class BaseOperator(DAGNode, ABC):
    """Base class for all operators."""
    
    def __init__(self, task_id: str, operator_type: OperatorType, 
                 retries: int = 0, retry_delay: int = 300):
        super().__init__(task_id, task_id)
        self.operator_type = operator_type
        self.retries = retries
        self.retry_delay = retry_delay
        self.execution_context: Dict[str, Any] = {}
        self.upstream_tasks: List[str] = []
        self.downstream_tasks: List[str] = []
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the operator logic."""
        pass
    
    def pre_execute(self, context: Dict[str, Any]):
        """Pre-execution hook."""
        self.execution_context = context.copy()
        self.status = TaskStatus.RUNNING
    
    def post_execute(self, context: Dict[str, Any], result: Any):
        """Post-execution hook."""
        if result is not None:
            self.status = TaskStatus.SUCCESS
        else:
            self.status = TaskStatus.FAILED
    
    def set_upstream(self, task_id: str):
        """Set upstream task dependency."""
        if task_id not in self.upstream_tasks:
            self.upstream_tasks.append(task_id)
    
    def set_downstream(self, task_id: str):
        """Set downstream task dependency."""
        if task_id not in self.downstream_tasks:
            self.downstream_tasks.append(task_id)
    
    def get_operator_info(self) -> Dict[str, Any]:
        """Get operator information."""
        base_info = self.get_info()
        base_info.update({
            "operator_type": self.operator_type.value,
            "retries": self.retries,
            "retry_delay": self.retry_delay,
            "upstream_count": len(self.upstream_tasks),
            "downstream_count": len(self.downstream_tasks)
        })
        return base_info

class PythonOperator(BaseOperator):
    """Operator for executing Python functions."""
    
    def __init__(self, task_id: str, python_callable: Callable = None,
                 op_args: tuple = None, op_kwargs: Dict[str, Any] = None, **kwargs):
        super().__init__(task_id, OperatorType.PYTHON, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.return_value: Any = None
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the Python callable."""
        if not self.python_callable:
            raise ValueError("python_callable is required")
        
        try:
            # Merge context with op_kwargs
            kwargs = {**self.op_kwargs, **context}
            
            # Execute the callable
            result = self.python_callable(*self.op_args, **kwargs)
            self.return_value = result
            return result
            
        except Exception as e:
            self.status = TaskStatus.FAILED
            raise e
    
    def set_callable(self, callable_func: Callable, args: tuple = None, 
                    kwargs: Dict[str, Any] = None):
        """Set the Python callable and its arguments."""
        self.python_callable = callable_func
        if args is not None:
            self.op_args = args
        if kwargs is not None:
            self.op_kwargs = kwargs

class BashOperator(BaseOperator):
    """Operator for executing bash commands."""
    
    def __init__(self, task_id: str, bash_command: str = "", **kwargs):
        super().__init__(task_id, OperatorType.BASH, **kwargs)
        self.bash_command = bash_command
        self.exit_code: Optional[int] = None
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the bash command."""
        if not self.bash_command:
            raise ValueError("bash_command is required")
        
        # Simulate bash execution
        import subprocess
        try:
            result = subprocess.run(
                self.bash_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300
            )
            self.exit_code = result.returncode
            
            if result.returncode == 0:
                return result.stdout
            else:
                raise RuntimeError(f"Command failed with exit code {result.returncode}: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            raise RuntimeError("Command timed out")

# Factory functions
def create_python_operator(task_id: str, python_callable: Callable, **kwargs) -> PythonOperator:
    """Factory function to create Python operators."""
    return PythonOperator(task_id, python_callable, **kwargs)

def create_bash_operator(task_id: str, bash_command: str, **kwargs) -> BashOperator:
    """Factory function to create Bash operators."""
    return BashOperator(task_id, bash_command, **kwargs)
