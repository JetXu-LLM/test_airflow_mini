"""SQL operator for database operations."""

from typing import Any, Dict, List, Optional, Union
from ..operators.base_operator import BaseOperator, OperatorType
from ..utils.helpers import DEFAULT_LOGGER, retry_decorator

class SQLOperator(BaseOperator):
    """Operator for executing SQL queries."""
    
    def __init__(self, task_id: str, sql: Union[str, List[str]], 
                 database: str = "default", autocommit: bool = False,
                 parameters: Dict[str, Any] = None, **kwargs):
        super().__init__(task_id, OperatorType.SQL, **kwargs)
        self.sql = sql if isinstance(sql, list) else [sql]
        self.database = database
        self.autocommit = autocommit
        self.parameters = parameters or {}
        self.query_results: List[Any] = []
    
    @retry_decorator(max_retries=2, delay=1.0)
    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute SQL queries."""
        DEFAULT_LOGGER.info(f"Executing SQL operator {self.task_id}")
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            for query in self.sql:
                processed_query = self._process_query(query, context)
                DEFAULT_LOGGER.debug(f"Executing query: {processed_query}")
                
                cursor.execute(processed_query, self.parameters)
                
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    self.query_results.append(results)
                    DEFAULT_LOGGER.info(f"Query returned {len(results)} rows")
                else:
                    affected_rows = cursor.rowcount
                    self.query_results.append(affected_rows)
                    DEFAULT_LOGGER.info(f"Query affected {affected_rows} rows")
            
            if self.autocommit:
                connection.commit()
            
            cursor.close()
            connection.close()
            
            return self.query_results
            
        except Exception as e:
            DEFAULT_LOGGER.error(f"SQL execution failed: {e}")
            raise e
    
    def _get_connection(self):
        """Get database connection (mock implementation)."""
        # In real implementation, this would return actual DB connection
        class MockConnection:
            def cursor(self):
                return MockCursor()
            def commit(self):
                pass
            def close(self):
                pass
        
        class MockCursor:
            def __init__(self):
                self.rowcount = 1
            
            def execute(self, query, params=None):
                # Mock execution
                pass
            
            def fetchall(self):
                # Mock results
                return [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
            
            def close(self):
                pass
        
        return MockConnection()
    
    def _process_query(self, query: str, context: Dict[str, Any]) -> str:
        """Process query with context variables."""
        # Simple template processing
        processed = query
        for key, value in context.items():
            placeholder = f"{{{key}}}"
            if placeholder in processed:
                processed = processed.replace(placeholder, str(value))
        
        return processed
    
    def get_query_results(self) -> List[Any]:
        """Get results from executed queries."""
        return self.query_results
    
    def set_sql(self, sql: Union[str, List[str]]):
        """Set SQL queries to execute."""
        self.sql = sql if isinstance(sql, list) else [sql]
    
    def add_parameter(self, key: str, value: Any):
        """Add query parameter."""
        self.parameters[key] = value

class PostgreSQLOperator(SQLOperator):
    """PostgreSQL specific operator."""
    
    def __init__(self, task_id: str, sql: Union[str, List[str]], 
                 postgres_conn_id: str = "postgres_default", **kwargs):
        super().__init__(task_id, sql, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.database_type = "postgresql"
    
    def _get_connection(self):
        """Get PostgreSQL connection."""
        # Mock PostgreSQL connection
        DEFAULT_LOGGER.info(f"Connecting to PostgreSQL: {self.postgres_conn_id}")
        return super()._get_connection()

class MySQLOperator(SQLOperator):
    """MySQL specific operator."""
    
    def __init__(self, task_id: str, sql: Union[str, List[str]], 
                 mysql_conn_id: str = "mysql_default", **kwargs):
        super().__init__(task_id, sql, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.database_type = "mysql"
    
    def _get_connection(self):
        """Get MySQL connection."""
        # Mock MySQL connection
        DEFAULT_LOGGER.info(f"Connecting to MySQL: {self.mysql_conn_id}")
        return super()._get_connection()

# Factory functions
def create_sql_operator(task_id: str, sql: Union[str, List[str]], 
                       database_type: str = "generic", **kwargs) -> SQLOperator:
    """Factory function to create SQL operators."""
    if database_type.lower() == "postgresql":
        return PostgreSQLOperator(task_id, sql, **kwargs)
    elif database_type.lower() == "mysql":
        return MySQLOperator(task_id, sql, **kwargs)
    else:
        return SQLOperator(task_id, sql, **kwargs)
