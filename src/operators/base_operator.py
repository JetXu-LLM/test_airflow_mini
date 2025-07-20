"""Base operator classes for task execution."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Callable, Optional, List, Union
from enum import Enum
from datetime import datetime, timedelta
from ..core.dag import DAGNode, TaskStatus

class OperatorType(Enum):
    """Types of operators."""
    PYTHON = "python"
    BASH = "bash"
    SQL = "sql"
    HTTP = "http"
    EMAIL = "email"
    DOCKER = "docker"  # 新增
    KUBERNETES = "kubernetes"  # 新增
    SENSOR = "sensor"  # 新增

class OperatorState(Enum):  # 新增枚举
    """Operator execution states."""
    NONE = "none"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    SHUTDOWN = "shutdown"
    RESTARTING = "restarting"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    SENSING = "sensing"
    DEFERRED = "deferred"
    REMOVED = "removed"

class BaseOperator(DAGNode, ABC):
    """Base class for all operators."""
    
    def __init__(self, task_id: str, operator_type: OperatorType, 
                 retries: int = 0, retry_delay: Union[int, timedelta] = 300,
                 retry_exponential_backoff: bool = False,
                 max_retry_delay: Optional[timedelta] = None,
                 **kwargs):
        super().__init__(task_id, task_id)
        self.operator_type = operator_type
        self.retries = retries
        
        # Handle retry_delay as both int (seconds) and timedelta
        if isinstance(retry_delay, int):
            self.retry_delay = timedelta(seconds=retry_delay)
        else:
            self.retry_delay = retry_delay
            
        self.retry_exponential_backoff = retry_exponential_backoff
        self.max_retry_delay = max_retry_delay or timedelta(hours=24)
        
        self.execution_context: Dict[str, Any] = {}
        self.upstream_tasks: List[str] = []
        self.downstream_tasks: List[str] = []
        
        # 新增属性
        self.pool: str = kwargs.get('pool', 'default_pool')
        self.pool_slots: int = kwargs.get('pool_slots', 1)
        self.queue: str = kwargs.get('queue', 'default')
        self.priority_weight: int = kwargs.get('priority_weight', 1)
        self.weight_rule: str = kwargs.get('weight_rule', 'downstream')
        
        # SLA and timeout settings
        self.sla: Optional[timedelta] = kwargs.get('sla')
        self.execution_timeout: Optional[timedelta] = kwargs.get('execution_timeout')
        
        # Email settings
        self.email: Optional[Union[str, List[str]]] = kwargs.get('email')
        self.email_on_retry: bool = kwargs.get('email_on_retry', True)
        self.email_on_failure: bool = kwargs.get('email_on_failure', True)
        
        # Trigger rule
        self.trigger_rule: str = kwargs.get('trigger_rule', 'all_success')
        
        # State tracking
        self.operator_state = OperatorState.NONE
        self.try_number: int = 1
        self.max_tries: int = self.retries + 1
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the operator logic."""
        pass
    
    def pre_execute(self, context: Dict[str, Any]):
        """Pre-execution hook."""
        self.execution_context = context.copy()
        self.status = TaskStatus.RUNNING
        self.operator_state = OperatorState.RUNNING
        self.try_number += 1
    
    def post_execute(self, context: Dict[str, Any], result: Any):
        """Post-execution hook."""
        if result is not None:
            self.status = TaskStatus.SUCCESS
            self.operator_state = OperatorState.SUCCESS
        else:
            self.status = TaskStatus.FAILED
            self.operator_state = OperatorState.FAILED
    
    def on_kill(self):
        """Called when the task instance is killed."""
        self.operator_state = OperatorState.SHUTDOWN
    
    def set_upstream(self, task_or_task_list: Union[str, List[str], 'BaseOperator', List['BaseOperator']]):
        """Set upstream task dependency."""
        if isinstance(task_or_task_list, (str, BaseOperator)):
            task_or_task_list = [task_or_task_list]
        
        for task in task_or_task_list:
            task_id = task if isinstance(task, str) else task.task_id
            if task_id not in self.upstream_tasks:
                self.upstream_tasks.append(task_id)
    
    def set_downstream(self, task_or_task_list: Union[str, List[str], 'BaseOperator', List['BaseOperator']]):
        """Set downstream task dependency."""
        if isinstance(task_or_task_list, (str, BaseOperator)):
            task_or_task_list = [task_or_task_list]
        
        for task in task_or_task_list:
            task_id = task if isinstance(task, str) else task.task_id
            if task_id not in self.downstream_tasks:
                self.downstream_tasks.append(task_id)
    
    def __rshift__(self, other: 'BaseOperator') -> 'BaseOperator':
        """Implement >> operator for setting downstream."""
        self.set_downstream(other)
        other.set_upstream(self)
        return other
    
    def __lshift__(self, other: 'BaseOperator') -> 'BaseOperator':
        """Implement << operator for setting upstream."""
        self.set_upstream(other)
        other.set_downstream(self)
        return self
    
    def get_operator_info(self) -> Dict[str, Any]:
        """Get operator information."""
        base_info = self.get_info()
        base_info.update({
            "operator_type": self.operator_type.value,
            "operator_state": self.operator_state.value,
            "retries": self.retries,
            "retry_delay": self.retry_delay.total_seconds(),
            "upstream_count": len(self.upstream_tasks),
            "downstream_count": len(self.downstream_tasks),
            "pool": self.pool,
            "queue": self.queue,
            "priority_weight": self.priority_weight,
            "try_number": self.try_number,
            "max_tries": self.max_tries
        })
        return base_info
    
    def clear_xcom_data(self):
        """Clear XCom data for this task."""
        # Placeholder for XCom functionality
        pass
    
    def render_template_fields(self, context: Dict[str, Any]):
        """Render template fields with context."""
        # Placeholder for template rendering
        pass

class PythonOperator(BaseOperator):
    """Operator for executing Python functions."""
    
    def __init__(self, task_id: str, python_callable: Callable = None,
                 op_args: tuple = None, op_kwargs: Dict[str, Any] = None, 
                 provide_context: bool = False, **kwargs):
        super().__init__(task_id, OperatorType.PYTHON, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context
        self.return_value: Any = None
        
        # Template fields for rendering
        self.template_fields = ['op_args', 'op_kwargs']
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the Python callable."""
        if not self.python_callable:
            raise ValueError("python_callable is required")
        
        try:
            # Prepare arguments
            if self.provide_context:
                kwargs = {**self.op_kwargs, **context}
            else:
                kwargs = self.op_kwargs.copy()
            
            # Execute the callable
            result = self.python_callable(*self.op_args, **kwargs)
            self.return_value = result
            return result
            
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.operator_state = OperatorState.FAILED
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
    
    def __init__(self, task_id: str, bash_command: str = "", 
                 env: Dict[str, str] = None, cwd: str = None,
                 output_encoding: str = 'utf-8', **kwargs):
        super().__init__(task_id, OperatorType.BASH, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.cwd = cwd
        self.output_encoding = output_encoding
        self.exit_code: Optional[int] = None
        self.command_output: str = ""
        
        # Template fields
        self.template_fields = ['bash_command', 'env']
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the bash command."""
        if not self.bash_command:
            raise ValueError("bash_command is required")
        
        import subprocess
        import os
        
        try:
            # Prepare environment
            env = os.environ.copy()
            if self.env:
                env.update(self.env)
            
            # Execute command
            result = subprocess.run(
                self.bash_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=self.cwd,
                env=env,
                timeout=self.execution_timeout.total_seconds() if self.execution_timeout else None,
                encoding=self.output_encoding
            )
            
            self.exit_code = result.returncode
            self.command_output = result.stdout
            
            if result.returncode == 0:
                return result.stdout
            else:
                error_msg = f"Command failed with exit code {result.returncode}: {result.stderr}"
                raise RuntimeError(error_msg)
                
        except subprocess.TimeoutExpired:
            raise RuntimeError("Command timed out")
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.operator_state = OperatorState.FAILED
            raise e

# 重命名：EmailOperator (原来没有，现在新增)
class EmailOperator(BaseOperator):
    """Operator for sending emails."""
    
    def __init__(self, task_id: str, to: Union[str, List[str]], 
                 subject: str, html_content: str = None, 
                 files: List[str] = None, cc: Union[str, List[str]] = None,
                 bcc: Union[str, List[str]] = None, **kwargs):
        super().__init__(task_id, OperatorType.EMAIL, **kwargs)
        self.to = to if isinstance(to, list) else [to]
        self.subject = subject
        self.html_content = html_content or ""
        self.files = files or []
        self.cc = cc if isinstance(cc, list) else ([cc] if cc else [])
        self.bcc = bcc if isinstance(bcc, list) else ([bcc] if bcc else [])
        
        # Template fields
        self.template_fields = ['to', 'subject', 'html_content', 'cc', 'bcc']
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Send email."""
        try:
            # Mock email sending
            recipients = self.to + self.cc + self.bcc
            self.logger.info(f"Sending email to {recipients}")
            self.logger.info(f"Subject: {self.subject}")
            self.logger.info(f"Content length: {len(self.html_content)}")
            self.logger.info(f"Attachments: {len(self.files)}")
            
            # Simulate email sending
            import time
            time.sleep(0.1)
            
            return f"Email sent successfully to {len(recipients)} recipients"
            
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.operator_state = OperatorState.FAILED
            raise e

# 新增：SensorOperator基类
class SensorOperator(BaseOperator):
    """Base class for sensor operators."""
    
    def __init__(self, task_id: str, poke_interval: int = 60, 
                 timeout: int = 60 * 60 * 24 * 7, soft_fail: bool = False,
                 mode: str = 'poke', **kwargs):
        super().__init__(task_id, OperatorType.SENSOR, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.soft_fail = soft_fail
        self.mode = mode  # 'poke' or 'reschedule'
        
        if self.mode not in ['poke', 'reschedule']:
            raise ValueError("Mode must be 'poke' or 'reschedule'")
    
    @abstractmethod
    def poke(self, context: Dict[str, Any]) -> bool:
        """Check if the condition is met."""
        pass
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the sensor logic."""
        import time
        start_time = time.time()
        
        while True:
            if self.poke(context):
                self.logger.info(f"Sensor {self.task_id} condition met")
                return True
            
            # Check timeout
            if time.time() - start_time > self.timeout:
                if self.soft_fail:
                    self.status = TaskStatus.SKIPPED
                    self.operator_state = OperatorState.SKIPPED
                    return False
                else:
                    raise RuntimeError(f"Sensor {self.task_id} timed out")
            
            if self.mode == 'poke':
                time.sleep(self.poke_interval)
            else:
                # In reschedule mode, we would reschedule the task
                # For now, just sleep
                time.sleep(self.poke_interval)

# 新增：FileSensor
class FileSensor(SensorOperator):
    """Sensor that waits for a file to exist."""
    
    def __init__(self, task_id: str, filepath: str, **kwargs):
        super().__init__(task_id, **kwargs)
        self.filepath = filepath
        self.template_fields = ['filepath']
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """Check if file exists."""
        import os
        exists = os.path.exists(self.filepath)
        if exists:
            self.logger.info(f"File {self.filepath} found")
        else:
            self.logger.info(f"File {self.filepath} not found")
        return exists

# Factory functions
def create_python_operator(task_id: str, python_callable: Callable, **kwargs) -> PythonOperator:
    """Factory function to create Python operators."""
    return PythonOperator(task_id, python_callable, **kwargs)

def create_bash_operator(task_id: str, bash_command: str, **kwargs) -> BashOperator:
    """Factory function to create Bash operators."""
    return BashOperator(task_id, bash_command, **kwargs)

def create_email_operator(task_id: str, to: Union[str, List[str]], 
                         subject: str, **kwargs) -> EmailOperator:
    """Factory function to create Email operators."""
    return EmailOperator(task_id, to, subject, **kwargs)

def create_file_sensor(task_id: str, filepath: str, **kwargs) -> FileSensor:
    """Factory function to create File sensors."""
    return FileSensor(task_id, filepath, **kwargs)

# 新增：操作符注册表
class OperatorRegistry:
    """Registry for operator types and their factories."""
    
    def __init__(self):
        self._operators: Dict[str, type] = {}
        self._factories: Dict[str, Callable] = {}
        
        # Register built-in operators
        self.register_operator('python', PythonOperator)
        self.register_operator('bash', BashOperator)
        self.register_operator('email', EmailOperator)
        self.register_operator('file_sensor', FileSensor)
        
        # Register factories
        self.register_factory('python', create_python_operator)
        self.register_factory('bash', create_bash_operator)
        self.register_factory('email', create_email_operator)
        self.register_factory('file_sensor', create_file_sensor)
    
    def register_operator(self, name: str, operator_class: type):
        """Register an operator class."""
        self._operators[name] = operator_class
    
    def register_factory(self, name: str, factory_func: Callable):
        """Register a factory function."""
        self._factories[name] = factory_func
    
    def get_operator_class(self, name: str) -> Optional[type]:
        """Get operator class by name."""
        return self._operators.get(name)
    
    def create_operator(self, operator_type: str, **kwargs) -> BaseOperator:
        """Create operator using registered factory."""
        if operator_type in self._factories:
            return self._factories[operator_type](**kwargs)
        elif operator_type in self._operators:
            return self._operators[operator_type](**kwargs)
        else:
            raise ValueError(f"Unknown operator type: {operator_type}")
    
    def list_operators(self) -> List[str]:
        """List all registered operator types."""
        return list(self._operators.keys())

# Global operator registry
OPERATOR_REGISTRY = OperatorRegistry()
