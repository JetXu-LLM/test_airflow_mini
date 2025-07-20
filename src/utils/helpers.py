"""Helper utilities and decorators."""

import time
import functools
from datetime import datetime, timedelta
from typing import Any, Dict, Callable, Optional, Union, List
import json
import os
import hashlib

def generate_dag_visualization_data(dag: 'DAG') -> Dict[str, Any]:
    """Generate data for frontend DAG visualization."""
    from ..core.dag import DAG, TaskStatus
    
    nodes = []
    edges = []
    
    # 转换任务为前端节点格式
    for i, task in enumerate(dag.tasks):
        node_data = {
            "id": task.node_id,
            "name": task.name,
            "status": task.status.value if hasattr(task.status, 'value') else 'pending',
            "type": 'generic',
            "position": {"x": 100 + i * 200, "y": 100}
        }
        
        # 添加操作符类型信息
        if hasattr(task, 'operator_type'):
            node_data["type"] = task.operator_type.value if hasattr(task.operator_type, 'value') else 'generic'
        
        nodes.append(node_data)
    
    # 转换依赖为边
    edge_id = 0
    for task_id, dependencies in dag.dependencies.items():
        for dep_id in dependencies:
            edges.append({
                "id": f"edge_{edge_id}",
                "source": dep_id,
                "target": task_id,
                "type": "dependency"
            })
            edge_id += 1
    
    return {
        "dagId": dag.node_id,
        "nodes": nodes,
        "edges": edges,
        "width": 800,
        "height": 400,
        "interactive": True
    }

def retry_decorator(max_retries: int = 3, delay: float = 1.0, 
                   backoff_factor: float = 2.0, exceptions: tuple = None):
    """Enhanced decorator for retrying function calls."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Check if exception should trigger retry
                    if exceptions and not isinstance(e, exceptions):
                        raise e
                    
                    last_exception = e
                    if attempt < max_retries:
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        raise last_exception
            
            raise last_exception
        return wrapper
    return decorator

def format_datetime(dt: datetime, format_string: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime object to string."""
    if not isinstance(dt, datetime):
        raise TypeError("Expected datetime object")
    return dt.strftime(format_string)

def parse_datetime(dt_string: str, format_string: str = "%Y-%m-%d %H:%M:%S") -> datetime:
    """Parse datetime string to datetime object."""
    try:
        return datetime.strptime(dt_string, format_string)
    except ValueError as e:
        raise ValueError(f"Unable to parse datetime string '{dt_string}': {e}")

def calculate_duration(start_time: datetime, end_time: datetime) -> Dict[str, float]:
    """Calculate duration between two datetime objects."""
    if end_time < start_time:
        raise ValueError("End time must be after start time")
    
    delta = end_time - start_time
    return {
        "total_seconds": delta.total_seconds(),
        "minutes": delta.total_seconds() / 60,
        "hours": delta.total_seconds() / 3600,
        "days": delta.days
    }

# 新增：文件工具函数
def calculate_file_hash(filepath: str, algorithm: str = 'md5') -> str:
    """Calculate hash of a file."""
    hash_func = getattr(hashlib, algorithm)()
    
    try:
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}")
    except Exception as e:
        raise RuntimeError(f"Error calculating hash for {filepath}: {e}")

def ensure_directory(directory: str):
    """Ensure directory exists, create if not."""
    os.makedirs(directory, exist_ok=True)

def read_file_safe(filepath: str, encoding: str = 'utf-8') -> Optional[str]:
    """Safely read file content."""
    try:
        with open(filepath, 'r', encoding=encoding) as f:
            return f.read()
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return None

def write_file_safe(filepath: str, content: str, encoding: str = 'utf-8') -> bool:
    """Safely write content to file."""
    try:
        # Ensure directory exists
        directory = os.path.dirname(filepath)
        if directory:
            ensure_directory(directory)
        
        with open(filepath, 'w', encoding=encoding) as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"Error writing file {filepath}: {e}")
        return False

# 简化的ConfigManager（原来的移动到config模块）
class SimpleConfig:
    """Simplified configuration management."""
    
    def __init__(self):
        self._config = {
            "max_workers": 4,
            "timeout": 300,
            "retry_count": 3,
            "log_level": "INFO"
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value."""
        self._config[key] = value
    
    def update(self, config_dict: Dict[str, Any]):
        """Update configuration."""
        self._config.update(config_dict)

class LogManager:
    """Enhanced logging utility."""
    
    def __init__(self, log_level: str = "INFO", log_format: str = None):
        self.log_level = log_level.upper()
        self.log_levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
        self.log_format = log_format or "[{level}] {timestamp}: {message}"
        self.log_history: List[Dict[str, Any]] = []
        self.max_history = 1000
    
    def _should_log(self, level: str) -> bool:
        """Check if message should be logged."""
        return self.log_levels.get(level.upper(), 20) >= self.log_levels.get(self.log_level, 20)
    
    def _log(self, level: str, message: str, extra_data: Dict[str, Any] = None):
        """Internal logging method."""
        if self._should_log(level):
            timestamp = datetime.now().isoformat()
            formatted_message = self.log_format.format(
                level=level,
                timestamp=timestamp,
                message=message
            )
            print(formatted_message)
            
            # Store in history
            log_entry = {
                "level": level,
                "message": message,
                "timestamp": timestamp,
                "extra_data": extra_data or {}
            }
            self.log_history.append(log_entry)
            
            # Trim history if needed
            if len(self.log_history) > self.max_history:
                self.log_history = self.log_history[-self.max_history:]
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self._log("DEBUG", message, kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log("INFO", message, kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self._log("WARNING", message, kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message."""
        self._log("ERROR", message, kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message."""
        self._log("CRITICAL", message, kwargs)
    
    def get_recent_logs(self, count: int = 100, level: str = None) -> List[Dict[str, Any]]:
        """Get recent log entries."""
        logs = self.log_history[-count:] if count else self.log_history
        if level:
            logs = [log for log in logs if log["level"] == level.upper()]
        return logs

# Global constants and utilities - 扩展
CONSTANTS = {
    "DEFAULT_TIMEOUT": 300,
    "MAX_RETRIES": 3,
    "SUPPORTED_FORMATS": [".py", ".sql", ".json", ".yaml", ".txt", ".md"],
    "DATE_FORMAT": "%Y-%m-%d %H:%M:%S",
    "LOG_LEVELS": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    "DEFAULT_POOLS": ["default_pool", "high_priority", "low_priority"],
    "OPERATOR_COLORS": {
        "python": "#3776ab",
        "bash": "#4EAA25", 
        "sql": "#336791",
        "email": "#EA4335",
        "sensor": "#FF9800"
    }
}

DEFAULT_CONFIG = SimpleConfig()
DEFAULT_LOGGER = LogManager()

def get_current_timestamp() -> str:
    """Get current timestamp as formatted string."""
    return format_datetime(datetime.now())

def validate_task_id(task_id: str) -> bool:
    """Enhanced task ID validation."""
    if not task_id or not isinstance(task_id, str):
        return False
    
    # Task ID should be alphanumeric with underscores and hyphens
    # Length should be between 1 and 250 characters
    if len(task_id) < 1 or len(task_id) > 250:
        return False
    
    import re
    pattern = r'^[a-zA-Z0-9_-]+$'
    return bool(re.match(pattern, task_id))

def validate_dag_id(dag_id: str) -> bool:
    """Validate DAG ID format."""
    return validate_task_id(dag_id)  # Same rules as task ID

def generate_task_id(prefix: str = "task", suffix: str = None) -> str:
    """Generate a unique task ID."""
    import uuid
    unique_part = str(uuid.uuid4())[:8]
    
    if suffix:
        return f"{prefix}_{unique_part}_{suffix}"
    else:
        return f"{prefix}_{unique_part}"

# 新增：性能监控工具
class PerformanceMonitor:
    """Simple performance monitoring utility."""
    
    def __init__(self):
        self.metrics: Dict[str, List[float]] = {}
        self.start_times: Dict[str, float] = {}
    
    def start_timer(self, name: str):
        """Start timing an operation."""
        self.start_times[name] = time.time()
    
    def end_timer(self, name: str) -> float:
        """End timing and record duration."""
        if name not in self.start_times:
            raise ValueError(f"Timer {name} was not started")
        
        duration = time.time() - self.start_times[name]
        
        if name not in self.metrics:
            self.metrics[name] = []
        
        self.metrics[name].append(duration)
        del self.start_times[name]
        
        return duration
    
    def get_stats(self, name: str) -> Dict[str, float]:
        """Get statistics for a metric."""
        if name not in self.metrics:
            return {}
        
        values = self.metrics[name]
        return {
            "count": len(values),
            "total": sum(values),
            "average": sum(values) / len(values),
            "min": min(values),
            "max": max(values)
        }
    
    def reset_metrics(self, name: str = None):
        """Reset metrics."""
        if name:
            self.metrics.pop(name, None)
        else:
            self.metrics.clear()

# Global performance monitor
PERFORMANCE_MONITOR = PerformanceMonitor()

# 新增：上下文管理器用于性能监控
class timer:
    """Context manager for timing operations."""
    
    def __init__(self, name: str, monitor: PerformanceMonitor = None):
        self.name = name
        self.monitor = monitor or PERFORMANCE_MONITOR
        self.duration = 0
    
    def __enter__(self):
        self.monitor.start_timer(self.name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration = self.monitor.end_timer(self.name)
