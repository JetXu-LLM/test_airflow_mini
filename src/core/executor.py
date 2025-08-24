"""Executor implementations for task execution."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from .dag import DAG, DAGNode, TaskStatus, DAGValidator
from ..utils.helpers import DEFAULT_CONFIG, DEFAULT_LOGGER, retry_decorator

class ExecutorType(Enum):
    """Types of executors."""
    LOCAL = "local"
    CELERY = "celery"
    KUBERNETES = "kubernetes"
    SEQUENTIAL = "sequential"

class BaseExecutor(ABC):
    """Abstract base class for executors."""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = DEFAULT_CONFIG.get("max_workers", max_workers)
        self.timeout = DEFAULT_CONFIG.get("timeout", 300)
        self.running_tasks: Dict[str, Any] = {}
        self.completed_tasks: List[str] = []
        self.failed_tasks: List[str] = []
        self.queued_tasks: List[str] = []  # 新增队列任务列表
        self.logger = DEFAULT_LOGGER
        self.executor_type = ExecutorType.LOCAL
        
        # 新增执行器状态
        self.is_running = False
        self.heartbeat_interval = 5
        self._heartbeat_thread: Optional[threading.Thread] = None
    
    @abstractmethod
    def execute_task(self, task: DAGNode) -> bool:
        """Execute a single task."""
        self.logger.info(f"Starting execution of task: {task.node_id}")
        # ... 执行逻辑
        self.logger.info(f"Completed execution of task: {task.node_id}")
    
    @abstractmethod
    def execute_dag(self, dag: DAG) -> Dict[str, Any]:
        """Execute an entire DAG."""
        pass
    
    def start(self):
        """Start the executor."""
        self.is_running = True
        self._start_heartbeat()
        self.logger.info(f"{self.__class__.__name__} started")
    
    def stop(self):
        """Stop the executor."""
        self.is_running = False
        self._stop_heartbeat()
        self.logger.info(f"{self.__class__.__name__} stopped")
    
    def _start_heartbeat(self):
        """Start heartbeat thread."""
        if not self._heartbeat_thread:
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()
    
    def _stop_heartbeat(self):
        """Stop heartbeat thread."""
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=1)
            self._heartbeat_thread = None
    
    def _heartbeat_loop(self):
        """Heartbeat loop for monitoring."""
        import time
        while self.is_running:
            self._heartbeat()
            time.sleep(self.heartbeat_interval)
    
    def _heartbeat(self):
        """Perform heartbeat operations."""
        # Check for timed out tasks
        current_time = datetime.now()
        timed_out_tasks = []
        
        for task_id, task_info in self.running_tasks.items():
            if "start_time" in task_info:
                elapsed = (current_time - task_info["start_time"]).total_seconds()
                if elapsed > self.timeout:
                    timed_out_tasks.append(task_id)
        
        # Handle timed out tasks
        for task_id in timed_out_tasks:
            self.logger.warning(f"Task {task_id} timed out")
            self._handle_task_timeout(task_id)
    
    def _handle_task_timeout(self, task_id: str):
        """Handle task timeout."""
        if task_id in self.running_tasks:
            task_info = self.running_tasks.pop(task_id)
            task = task_info.get("task")
            if task:
                task.status = TaskStatus.FAILED
            self.failed_tasks.append(task_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        return {
            "executor_type": self.executor_type.value,
            "max_workers": self.max_workers,
            "running_tasks": len(self.running_tasks),
            "completed_tasks": len(self.completed_tasks),
            "failed_tasks": len(self.failed_tasks),
            "queued_tasks": len(self.queued_tasks),
            "total_processed": len(self.completed_tasks) + len(self.failed_tasks),
            "is_running": self.is_running
        }
    
    def reset_stats(self):
        """Reset execution statistics."""
        self.running_tasks.clear()
        self.completed_tasks.clear()
        self.failed_tasks.clear()
        self.queued_tasks.clear()

class LocalExecutor(BaseExecutor):
    """Local executor for running tasks in the same process."""
    
    def __init__(self, max_workers: int = 4, use_threading: bool = True, 
                 use_multiprocessing: bool = False):
        super().__init__(max_workers)
        self.use_threading = use_threading
        self.use_multiprocessing = use_multiprocessing
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        self.process_pool: Optional[ProcessPoolExecutor] = None
        self.executor_type = ExecutorType.LOCAL
        
        if use_multiprocessing:
            self.process_pool = ProcessPoolExecutor(max_workers=max_workers)
        elif use_threading:
            self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
    
    def start(self):
        """Start the local executor."""
        super().start()
        if self.use_multiprocessing and not self.process_pool:
            self.process_pool = ProcessPoolExecutor(max_workers=self.max_workers)
        elif self.use_threading and not self.thread_pool:
            self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
    
    def stop(self):
        """Stop the local executor."""
        super().stop()
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        if self.process_pool:
            self.process_pool.shutdown(wait=True)
    
    def execute_task(self, task: DAGNode) -> bool:
        """Execute a single task locally."""
        try:
            task.status = TaskStatus.RUNNING
            self.running_tasks[task.node_id] = {
                "task": task,
                "start_time": datetime.now()
            }
            
            # Execute based on configuration
            if self.use_multiprocessing and self.process_pool:
                future = self.process_pool.submit(self._run_task_logic, task)
                result = future.result(timeout=self.timeout)
            elif self.use_threading and self.thread_pool:
                future = self.thread_pool.submit(self._run_task_logic, task)
                result = future.result(timeout=self.timeout)
            else:
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
            self.logger.error(f"Error executing task {task.node_id}: {e}")
            task.status = TaskStatus.FAILED
            self.failed_tasks.append(task.node_id)
            self.running_tasks.pop(task.node_id, None)
            return False
    
    def execute_dag(self, dag: DAG) -> Dict[str, Any]:
        """Execute an entire DAG locally."""
        validator = DAGValidator(dag)
        if not validator.is_valid():
            validation_report = validator.get_validation_report()
            return {
                "success": False, 
                "error": "Invalid DAG structure",
                "validation_report": validation_report
            }
        
        execution_start = datetime.now()
        task_results = {}
        
        # 使用改进的任务执行器
        task_executor = TaskExecutionEngine(self)  # 重命名的类
        
        for task in dag.tasks:
            if not dag.is_paused():
                if hasattr(task, 'execute'):
                    try:
                        task.pre_execute({})
                        context = {"execution_date": execution_start, "dag": dag}
                        result = task.execute(context)
                        task.post_execute(context, result)
                        task_results[task.node_id] = True
                    except Exception as e:
                        self.logger.error(f"Task {task.node_id} failed: {e}")
                        task_results[task.node_id] = False
                else:
                    result = task_executor.execute_with_retry(task)  # 重命名的方法
                    task_results[task.node_id] = result
        
        execution_end = datetime.now()
        
        return {
            "success": len(self.failed_tasks) == 0,
            "execution_time": (execution_end - execution_start).total_seconds(),
            "task_results": task_results,
            "stats": self.get_stats()
        }
    
    @retry_decorator(max_retries=3, delay=0.5)
    def _run_task_logic(self, task: DAGNode) -> bool:
        """Run the actual task logic with retry capability."""
        timeout = DEFAULT_CONFIG.get("timeout", 300)
        
        import time
        time.sleep(0.1)
        
        from ..utils.helpers import validate_task_id
        if not validate_task_id(task.node_id):
            raise ValueError(f"Invalid task ID: {task.node_id}")
        
        return len(task.node_id) > 0

# 新增：异步执行器
class AsyncExecutor(BaseExecutor):
    """Asynchronous executor using asyncio."""
    
    def __init__(self, max_workers: int = 4):
        super().__init__(max_workers)
        self.executor_type = ExecutorType.LOCAL
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
    
    def start(self):
        """Start the async executor."""
        super().start()
        self.loop = asyncio.new_event_loop()
        self.semaphore = asyncio.Semaphore(self.max_workers)
    
    async def execute_task_async(self, task: DAGNode) -> bool:
        """Execute a task asynchronously."""
        async with self.semaphore:
            try:
                task.status = TaskStatus.RUNNING
                self.running_tasks[task.node_id] = {
                    "task": task,
                    "start_time": datetime.now()
                }
                
                # Simulate async task execution
                await asyncio.sleep(0.1)
                result = await self._run_task_async(task)
                
                if result:
                    task.status = TaskStatus.SUCCESS
                    self.completed_tasks.append(task.node_id)
                else:
                    task.status = TaskStatus.FAILED
                    self.failed_tasks.append(task.node_id)
                
                self.running_tasks.pop(task.node_id, None)
                return result
                
            except Exception as e:
                self.logger.error(f"Async task {task.node_id} failed: {e}")
                task.status = TaskStatus.FAILED
                self.failed_tasks.append(task.node_id)
                self.running_tasks.pop(task.node_id, None)
                return False
    
    async def _run_task_async(self, task: DAGNode) -> bool:
        """Run task logic asynchronously."""
        # Simulate async work
        await asyncio.sleep(0.05)
        return len(task.node_id) > 0
    
    def execute_task(self, task: DAGNode) -> bool:
        """Execute a single task (sync wrapper)."""
        if not self.loop:
            self.start()
        
        return self.loop.run_until_complete(self.execute_task_async(task))
    
    def execute_dag(self, dag: DAG) -> Dict[str, Any]:
        """Execute DAG asynchronously."""
        if not self.loop:
            self.start()
        
        return self.loop.run_until_complete(self._execute_dag_async(dag))
    
    async def _execute_dag_async(self, dag: DAG) -> Dict[str, Any]:
        """Execute DAG asynchronously."""
        execution_start = datetime.now()
        
        # Execute all tasks concurrently
        tasks = [self.execute_task_async(task) for task in dag.tasks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        task_results = {}
        for i, result in enumerate(results):
            task_id = dag.tasks[i].node_id
            task_results[task_id] = not isinstance(result, Exception) and result
        
        execution_end = datetime.now()
        
        return {
            "success": all(task_results.values()),
            "execution_time": (execution_end - execution_start).total_seconds(),
            "task_results": task_results,
            "stats": self.get_stats()
        }

# 重命名：TaskRunner -> TaskExecutionEngine
class TaskExecutionEngine:
    """Engine for executing individual tasks with advanced features."""
    
    def __init__(self, executor: BaseExecutor):
        self.executor = executor
        self.retry_count = 3
        self.retry_delay = 1.0
        self.task_callbacks: Dict[str, List[Callable]] = {}
        self.execution_history: List[Dict[str, Any]] = []
    
    def execute_with_retry(self, task: DAGNode) -> bool:  # 重命名的方法
        """Execute task with retry logic and callbacks."""
        execution_record = {
            "task_id": task.node_id,
            "start_time": datetime.now(),
            "attempts": 0,
            "success": False
        }
        
        for attempt in range(self.retry_count):
            execution_record["attempts"] += 1
            
            # Execute pre-task callbacks
            self._execute_callbacks(task.node_id, "pre_execute")
            
            if self.executor.execute_task(task):
                execution_record["success"] = True
                execution_record["end_time"] = datetime.now()
                
                # Execute success callbacks
                self._execute_callbacks(task.node_id, "on_success")
                
                self.execution_history.append(execution_record)
                return True
            
            if attempt < self.retry_count - 1:
                import time
                time.sleep(self.retry_delay)
                task.reset_status()
                
                # Execute retry callbacks
                self._execute_callbacks(task.node_id, "on_retry")
        
        # Execute failure callbacks
        self._execute_callbacks(task.node_id, "on_failure")
        
        execution_record["end_time"] = datetime.now()
        self.execution_history.append(execution_record)
        return False
    
    def add_callback(self, task_id: str, callback_type: str, callback: Callable):
        """Add callback for task execution events."""
        if task_id not in self.task_callbacks:
            self.task_callbacks[task_id] = {}
        if callback_type not in self.task_callbacks[task_id]:
            self.task_callbacks[task_id][callback_type] = []
        
        self.task_callbacks[task_id][callback_type].append(callback)
    
    def _execute_callbacks(self, task_id: str, callback_type: str):
        """Execute callbacks for a specific event."""
        if task_id in self.task_callbacks:
            callbacks = self.task_callbacks[task_id].get(callback_type, [])
            for callback in callbacks:
                try:
                    callback(task_id)
                except Exception as e:
                    self.executor.logger.error(f"Callback error for {task_id}: {e}")
    
    def set_retry_config(self, count: int, delay: float):
        """Configure retry behavior."""
        self.retry_count = max(1, count)
        self.retry_delay = max(0.1, delay)
    
    def get_execution_history(self, task_id: str = None) -> List[Dict[str, Any]]:
        """Get execution history."""
        if task_id:
            return [record for record in self.execution_history if record["task_id"] == task_id]
        return self.execution_history

# Factory function with more options
def create_executor(executor_type: str = "local", **kwargs) -> BaseExecutor:
    """Factory function to create executors."""
    executor_type = executor_type.lower()
    
    if executor_type == "local":
        return LocalExecutor(**kwargs)
    elif executor_type == "async":
        return AsyncExecutor(**kwargs)
    else:
        raise ValueError(f"Unknown executor type: {executor_type}")

# 新增：执行器管理器
class ExecutorManager:
    """Manager for multiple executors."""
    
    def __init__(self):
        self.executors: Dict[str, BaseExecutor] = {}
        self.default_executor = "local"
    
    def register_executor(self, name: str, executor: BaseExecutor):
        """Register an executor."""
        self.executors[name] = executor
    
    def get_executor(self, name: str = None) -> BaseExecutor:
        """Get executor by name."""
        executor_name = name or self.default_executor
        if executor_name not in self.executors:
            # Create default executor if not exists
            self.executors[executor_name] = create_executor(executor_name)
        
        return self.executors[executor_name]
    
    def start_all(self):
        """Start all registered executors."""
        for executor in self.executors.values():
            executor.start()
    
    def stop_all(self):
        """Stop all registered executors."""
        for executor in self.executors.values():
            executor.stop()
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get stats from all executors."""
        return {name: executor.get_stats() for name, executor in self.executors.items()}
