"""DAG scheduling functionality."""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from enum import Enum
import threading
import time

from ..core.dag import DAG, TaskStatus
from ..utils.helpers import DEFAULT_LOGGER, format_datetime, retry_decorator

class ScheduleType(Enum):
    """Types of scheduling intervals."""
    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    HOURLY = "hourly"
    CRON = "cron"

class SchedulerStatus(Enum):
    """Scheduler status."""
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"

class SchedulerConfig:
    """Configuration for scheduler."""
    
    def __init__(self, max_concurrent_dags: int = 10, 
                 check_interval: int = 60, enable_catchup: bool = True):
        self.max_concurrent_dags = max_concurrent_dags
        self.check_interval = check_interval
        self.enable_catchup = enable_catchup
        self.timezone = "UTC"
        self.max_dag_runs_per_dag = 16
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "max_concurrent_dags": self.max_concurrent_dags,
            "check_interval": self.check_interval,
            "enable_catchup": self.enable_catchup,
            "timezone": self.timezone,
            "max_dag_runs_per_dag": self.max_dag_runs_per_dag
        }

class DAGRun:
    """Represents a single DAG execution run."""
    
    def __init__(self, dag_id: str, run_id: str, execution_date: datetime):
        self.dag_id = dag_id
        self.run_id = run_id
        self.execution_date = execution_date
        self.start_date: Optional[datetime] = None
        self.end_date: Optional[datetime] = None
        self.status = TaskStatus.PENDING
        self.external_trigger = False
        self.conf: Dict[str, Any] = {}
    
    def start_run(self):
        """Start the DAG run."""
        self.start_date = datetime.now()
        self.status = TaskStatus.RUNNING
        DEFAULT_LOGGER.info(f"Starting DAG run {self.run_id} for DAG {self.dag_id}")
    
    def complete_run(self, success: bool = True):
        """Complete the DAG run."""
        self.end_date = datetime.now()
        self.status = TaskStatus.SUCCESS if success else TaskStatus.FAILED
        DEFAULT_LOGGER.info(f"Completed DAG run {self.run_id} with status {self.status.value}")
    
    def get_duration(self) -> Optional[float]:
        """Get run duration in seconds."""
        if self.start_date and self.end_date:
            return (self.end_date - self.start_date).total_seconds()
        return None

class DAGScheduler:
    """Main scheduler for managing DAG executions."""
    
    def __init__(self, config: SchedulerConfig = None):
        self.config = config or SchedulerConfig()
        self.status = SchedulerStatus.STOPPED
        self.registered_dags: Dict[str, DAG] = {}
        self.active_runs: Dict[str, DAGRun] = {}
        self.completed_runs: List[DAGRun] = []
        self.scheduler_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.logger = DEFAULT_LOGGER
    
    def register_dag(self, dag: DAG, schedule_type: ScheduleType = ScheduleType.DAILY,
                    schedule_interval: Union[str, timedelta] = None):
        """Register a DAG for scheduling."""
        self.registered_dags[dag.node_id] = dag
        dag.schedule_type = schedule_type
        dag.schedule_interval = schedule_interval
        self.logger.info(f"Registered DAG {dag.node_id} with schedule {schedule_type.value}")
    
    def unregister_dag(self, dag_id: str):
        """Unregister a DAG from scheduling."""
        if dag_id in self.registered_dags:
            del self.registered_dags[dag_id]
            self.logger.info(f"Unregistered DAG {dag_id}")
    
    def start(self):
        """Start the scheduler."""
        if self.status == SchedulerStatus.RUNNING:
            self.logger.warning("Scheduler is already running")
            return
        
        self.status = SchedulerStatus.RUNNING
        self._stop_event.clear()
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        self.logger.info("DAG Scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        if self.status != SchedulerStatus.RUNNING:
            return
        
        self.status = SchedulerStatus.STOPPED
        self._stop_event.set()
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        self.logger.info("DAG Scheduler stopped")
    
    def pause(self):
        """Pause the scheduler."""
        if self.status == SchedulerStatus.RUNNING:
            self.status = SchedulerStatus.PAUSED
            self.logger.info("DAG Scheduler paused")
    
    def resume(self):
        """Resume the scheduler."""
        if self.status == SchedulerStatus.PAUSED:
            self.status = SchedulerStatus.RUNNING
            self.logger.info("DAG Scheduler resumed")
    
    @retry_decorator(max_retries=3, delay=1.0)
    def _scheduler_loop(self):
        """Main scheduler loop."""
        while not self._stop_event.is_set():
            try:
                if self.status == SchedulerStatus.RUNNING:
                    self._check_and_schedule_dags()
                    self._cleanup_completed_runs()
                
                time.sleep(self.config.check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                self.status = SchedulerStatus.ERROR
                time.sleep(self.config.check_interval)
    
    def _check_and_schedule_dags(self):
        """Check which DAGs need to be scheduled."""
        current_time = datetime.now()
        
        for dag_id, dag in self.registered_dags.items():
            if dag.is_paused():
                continue
            
            if self._should_schedule_dag(dag, current_time):
                self._create_dag_run(dag, current_time)
    
    def _should_schedule_dag(self, dag: DAG, current_time: datetime) -> bool:
        """Determine if a DAG should be scheduled."""
        # Simple scheduling logic - in real implementation would be more complex
        if hasattr(dag, 'last_scheduled'):
            time_since_last = current_time - dag.last_scheduled
            if hasattr(dag, 'schedule_type'):
                if dag.schedule_type == ScheduleType.HOURLY:
                    return time_since_last >= timedelta(hours=1)
                elif dag.schedule_type == ScheduleType.DAILY:
                    return time_since_last >= timedelta(days=1)
        else:
            # First time scheduling
            return True
        
        return False
    
    def _create_dag_run(self, dag: DAG, execution_date: datetime):
        """Create a new DAG run."""
        if len(self.active_runs) >= self.config.max_concurrent_dags:
            self.logger.warning("Maximum concurrent DAGs reached, skipping scheduling")
            return
        
        run_id = f"{dag.node_id}_{execution_date.strftime('%Y%m%d_%H%M%S')}"
        dag_run = DAGRun(dag.node_id, run_id, execution_date)
        
        self.active_runs[run_id] = dag_run
        dag.last_scheduled = execution_date
        
        # Start the DAG run in a separate thread
        run_thread = threading.Thread(
            target=self._execute_dag_run, 
            args=(dag, dag_run), 
            daemon=True
        )
        run_thread.start()
    
    def _execute_dag_run(self, dag: DAG, dag_run: DAGRun):
        """Execute a DAG run."""
        try:
            dag_run.start_run()
            
            # Import here to avoid circular imports
            from ..core.executor import LocalExecutor
            executor = LocalExecutor()
            result = executor.execute_dag(dag)
            
            dag_run.complete_run(result.get("success", False))
            
        except Exception as e:
            self.logger.error(f"Error executing DAG run {dag_run.run_id}: {e}")
            dag_run.complete_run(False)
        
        finally:
            # Move from active to completed
            if dag_run.run_id in self.active_runs:
                del self.active_runs[dag_run.run_id]
            self.completed_runs.append(dag_run)
    
    def _cleanup_completed_runs(self):
        """Clean up old completed runs."""
        if len(self.completed_runs) > 1000:  # Keep last 1000 runs
            self.completed_runs = self.completed_runs[-1000:]
    
    def get_dag_runs(self, dag_id: str = None, limit: int = 100) -> List[DAGRun]:
        """Get DAG runs, optionally filtered by DAG ID."""
        runs = self.completed_runs + list(self.active_runs.values())
        
        if dag_id:
            runs = [run for run in runs if run.dag_id == dag_id]
        
        return sorted(runs, key=lambda x: x.execution_date, reverse=True)[:limit]
    
    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "status": self.status.value,
            "registered_dags": len(self.registered_dags),
            "active_runs": len(self.active_runs),
            "completed_runs": len(self.completed_runs),
            "config": self.config.to_dict(),
            "uptime": format_datetime(datetime.now())
        }
    
    def trigger_dag(self, dag_id: str, execution_date: datetime = None) -> Optional[DAGRun]:
        """Manually trigger a DAG run."""
        if dag_id not in self.registered_dags:
            self.logger.error(f"DAG {dag_id} not registered")
            return None
        
        dag = self.registered_dags[dag_id]
        execution_date = execution_date or datetime.now()
        
        run_id = f"{dag_id}_manual_{execution_date.strftime('%Y%m%d_%H%M%S')}"
        dag_run = DAGRun(dag_id, run_id, execution_date)
        dag_run.external_trigger = True
        
        self.active_runs[run_id] = dag_run
        
        # Execute in separate thread
        run_thread = threading.Thread(
            target=self._execute_dag_run,
            args=(dag, dag_run),
            daemon=True
        )
        run_thread.start()
        
        return dag_run

def create_scheduler(config: Dict[str, Any] = None) -> DAGScheduler:
    """Factory function to create a scheduler."""
    if config:
        scheduler_config = SchedulerConfig(**config)
    else:
        scheduler_config = SchedulerConfig()
    
    return DAGScheduler(scheduler_config)
