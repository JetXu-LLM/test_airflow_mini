"""Helper utilities and decorators."""

import time
import functools
from datetime import datetime, timedelta
from typing import Any, Dict, Callable, Optional, Union
import json
import os

def retry_decorator(max_retries: int = 3, delay: float = 1.0, 
                   backoff_factor: float = 2.0):
    """Decorator for retrying function calls."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
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

class ConfigManager:
    """Configuration management utility."""
    
    def __init__(self, config_file: str = None):
        self.config_file = config_file
        self._config: Dict[str, Any] = {}
        self._defaults: Dict[str, Any] = {
            "max_workers": 4,
            "timeout": 300,
            "retry_count": 3,
            "log_level": "INFO"
        }
        
        if config_file and os.path.exists(config_file):
            self.load_config()
    
    def load_config(self):
        """Load configuration from file."""
        if not self.config_file:
            return
        
        try:
            with open(self.config_file, 'r') as f:
                self._config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Warning: Could not load config file: {e}")
            self._config = {}
    
    def save_config(self):
        """Save configuration to file."""
        if not self.config_file:
            return
        
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self._config, f, indent=2)
        except Exception as e:
            print(f"Error saving config file: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, self._defaults.get(key, default))
    
    def set(self, key: str, value: Any):
        """Set configuration value."""
        self._config[key] = value
    
    def update(self, config_dict: Dict[str, Any]):
        """Update configuration with dictionary."""
        self._config.update(config_dict)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values."""
        result = self._defaults.copy()
        result.update(self._config)
        return result

class LogManager:
    """Simple logging utility."""
    
    def __init__(self, log_level: str = "INFO"):
        self.log_level = log_level.upper()
        self.log_levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    
    def _should_log(self, level: str) -> bool:
        """Check if message should be logged."""
        return self.log_levels.get(level.upper(), 20) >= self.log_levels.get(self.log_level, 20)
    
    def debug(self, message: str):
        """Log debug message."""
        if self._should_log("DEBUG"):
            print(f"[DEBUG] {datetime.now().isoformat()}: {message}")
    
    def info(self, message: str):
        """Log info message."""
        if self._should_log("INFO"):
            print(f"[INFO] {datetime.now().isoformat()}: {message}")
    
    def warning(self, message: str):
        """Log warning message."""
        if self._should_log("WARNING"):
            print(f"[WARNING] {datetime.now().isoformat()}: {message}")
    
    def error(self, message: str):
        """Log error message."""
        if self._should_log("ERROR"):
            print(f"[ERROR] {datetime.now().isoformat()}: {message}")

# Global constants and utilities
CONSTANTS = {
    "DEFAULT_TIMEOUT": 300,
    "MAX_RETRIES": 3,
    "SUPPORTED_FORMATS": [".py", ".sql", ".json", ".yaml"],
    "DATE_FORMAT": "%Y-%m-%d %H:%M:%S",
    "LOG_LEVELS": ["DEBUG", "INFO", "WARNING", "ERROR"]
}

DEFAULT_CONFIG = ConfigManager()
DEFAULT_LOGGER = LogManager()

def get_current_timestamp() -> str:
    """Get current timestamp as formatted string."""
    return format_datetime(datetime.now())

def validate_task_id(task_id: str) -> bool:
    """Validate task ID format."""
    if not task_id or not isinstance(task_id, str):
        return False
    
    # Task ID should be alphanumeric with underscores and hyphens
    import re
    pattern = r'^[a-zA-Z0-9_-]+$'
    return bool(re.match(pattern, task_id))
