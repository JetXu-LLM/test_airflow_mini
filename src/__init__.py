"""Test Airflow Mini package."""

from .core.dag import DAG, DAGNode
from .core.executor import LocalExecutor
from .operators.base_operator import BaseOperator

__version__ = "1.0.0"
__all__ = ["DAG", "DAGNode", "LocalExecutor", "BaseOperator"]

# Package level constants
DEFAULT_TIMEOUT = 300
MAX_RETRIES = 3
