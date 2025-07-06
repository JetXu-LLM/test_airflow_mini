"""Test cases for DAG functionality."""

import unittest
from datetime import datetime, timedelta
from src.core.dag import DAG, DAGNode, DAGValidator, TaskStatus, create_dag, DEFAULT_DAG_ARGS
from src.core.executor import LocalExecutor, BaseExecutor, TaskRunner, create_executor
from src.operators.base_operator import PythonOperator, BashOperator, OperatorType
from src.utils.helpers import retry_decorator, format_datetime, ConfigManager, validate_task_id

class TestDAGNode(unittest.TestCase):
    """Test cases for DAGNode class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.node = DAGNode("test_node", "Test Node")
    
    def test_node_creation(self):
        """Test basic node creation."""
        self.assertEqual(self.node.node_id, "test_node")
        self.assertEqual(self.node.name, "Test Node")
        self.assertEqual(self.node.status, TaskStatus.PENDING)
        self.assertIsInstance(self.node.created_at, datetime)
    
    def test_node_info(self):
        """Test node information retrieval."""
        info = self.node.get_info()
        self.assertIn("id", info)
        self.assertIn("name", info)
        self.assertIn("status", info)
        self.assertIn("created_at", info)
        self.assertEqual(info["id"], "test_node")
    
    def test_reset_status(self):
        """Test status reset functionality."""
        self.node.status = TaskStatus.SUCCESS
        self.node.reset_status()
        self.assertEqual(self.node.status, TaskStatus.PENDING)

class TestDAG(unittest.TestCase):
    """Test cases for DAG class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.dag = DAG("test_dag", "Test DAG for unit testing")
        self.task1 = DAGNode("task1")
        self.task2 = DAGNode("task2")
        self.task3 = DAGNode("task3")
    
    def test_dag_creation(self):
        """Test basic DAG creation."""
        self.assertEqual(self.dag.node_id, "test_dag")
        self.assertEqual(self.dag.description, "Test DAG for unit testing")
        self.assertEqual(len(self.dag.tasks), 0)
        self.assertFalse(self.dag.is_paused())
    
    def test_add_task(self):
        """Test adding tasks to DAG."""
        self.dag.add_task(self.task1)
        self.assertEqual(len(self.dag.tasks), 1)
        self.assertEqual(self.dag.get_task_count(), 1)
        
        # Test duplicate task addition
        self.dag.add_task(self.task1)
        self.assertEqual(len(self.dag.tasks), 1)  # Should not add duplicate
    
    def test_add_dependency(self):
        """Test adding dependencies between tasks."""
        self.dag.add_task(self.task1)
        self.dag.add_task(self.task2)
        
        self.dag.add_dependency("task1", "task2")
        self.assertIn("task1", self.dag.dependencies["task2"])
    
    def test_pause_unpause(self):
        """Test DAG pause/unpause functionality."""
        self.assertFalse(self.dag.is_paused())
        
        self.dag.pause()
        self.assertTrue(self.dag.is_paused())
        
        self.dag.unpause()
        self.assertFalse(self.dag.is_paused())
    
    def test_dag_validation(self):
        """Test DAG structure validation."""
        # Valid DAG
        self.dag.add_task(self.task1)
        self.dag.add_task(self.task2)
        self.dag.add_dependency("task1", "task2")
        
        self.assertTrue(self.dag.validate_structure())
        
        # Create cycle to test invalid DAG
        self.dag.add_dependency("task2", "task1")
        self.assertFalse(self.dag.validate_structure())

class TestDAGValidator(unittest.TestCase):
    """Test cases for DAGValidator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.dag = DAG("validator_test_dag")
        self.task1 = DAGNode("val_task1")
        self.task2 = DAGNode("val_task2")
        self.task3 = DAGNode("val_task3")
        
        self.dag.add_task(self.task1)
        self.dag.add_task(self.task2)
        self.dag.add_task(self.task3)
    
    def test_valid_dag(self):
        """Test validation of valid DAG."""
        self.dag.add_dependency("val_task1", "val_task2")
        self.dag.add_dependency("val_task2", "val_task3")
        
        validator = DAGValidator(self.dag)
        self.assertTrue(validator.is_valid())
    
    def test_cyclic_dag(self):
        """Test detection of cyclic DAG."""
        self.dag.add_dependency("val_task1", "val_task2")
        self.dag.add_dependency("val_task2", "val_task3")
        self.dag.add_dependency("val_task3", "val_task1")  # Creates cycle
        
        validator = DAGValidator(self.dag)
        self.assertFalse(validator.is_valid())
    
    def test_validation_report(self):
        """Test detailed validation report."""
        self.dag.add_dependency("val_task1", "val_task2")
        
        validator = DAGValidator(self.dag)
        report = validator.get_validation_report()
        
        self.assertIn("is_valid", report)
        self.assertIn("task_count", report)
        self.assertIn("dependency_count", report)
        self.assertIn("validation_time", report)
        self.assertEqual(report["task_count"], 3)

class TestLocalExecutor(unittest.TestCase):
    """Test cases for LocalExecutor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.executor = LocalExecutor(max_workers=2)
        self.task = DAGNode("executor_test_task")
        self.dag = DAG("executor_test_dag")
        self.dag.add_task(self.task)
    
    def test_executor_creation(self):
        """Test executor creation."""
        self.assertEqual(self.executor.max_workers, 2)
        self.assertEqual(len(self.executor.running_tasks), 0)
        self.assertEqual(len(self.executor.completed_tasks), 0)
    
    def test_execute_task(self):
        """Test single task execution."""
        result = self.executor.execute_task(self.task)
        self.assertTrue(result)
        self.assertEqual(self.task.status, TaskStatus.SUCCESS)
        self.assertIn(self.task.node_id, self.executor.completed_tasks)
    
    def test_execute_dag(self):
        """Test DAG execution."""
        result = self.executor.execute_dag(self.dag)
        
        self.assertIn("success", result)
        self.assertIn("execution_time", result)
        self.assertIn("task_results", result)
        self.assertIn("stats", result)
    
    def test_get_stats(self):
        """Test execution statistics."""
        stats = self.executor.get_stats()
        
        self.assertIn("max_workers", stats)
        self.assertIn("running_tasks", stats)
        self.assertIn("completed_tasks", stats)
        self.assertIn("failed_tasks", stats)
        self.assertIn("total_processed", stats)
    
    def test_reset_stats(self):
        """Test statistics reset."""
        self.executor.execute_task(self.task)
        self.executor.reset_stats()
        
        self.assertEqual(len(self.executor.running_tasks), 0)
        self.assertEqual(len(self.executor.completed_tasks), 0)
        self.assertEqual(len(self.executor.failed_tasks), 0)

class TestPythonOperator(unittest.TestCase):
    """Test cases for PythonOperator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        def sample_function(x, y=10):
            return x + y
        
        self.operator = PythonOperator(
            task_id="python_test_task",
            python_callable=sample_function,
            op_args=(5,),
            op_kwargs={"y": 15}
        )
    
    def test_operator_creation(self):
        """Test operator creation."""
        self.assertEqual(self.operator.task_id, "python_test_task")
        self.assertEqual(self.operator.operator_type, OperatorType.PYTHON)
        self.assertIsNotNone(self.operator.python_callable)
    
    def test_execute_operator(self):
        """Test operator execution."""
        context = {"execution_date": datetime.now()}
        result = self.operator.execute(context)
        
        self.assertEqual(result, 20)  # 5 + 15
        self.assertEqual(self.operator.return_value, 20)
    
    def test_operator_info(self):
        """Test operator information."""
        info = self.operator.get_operator_info()
        
        self.assertIn("operator_type", info)
        self.assertIn("retries", info)
        self.assertIn("upstream_count", info)
        self.assertIn("downstream_count", info)
        self.assertEqual(info["operator_type"], "python")
    
    def test_set_callable(self):
        """Test setting callable function."""
        def new_function(a, b):
            return a * b
        
        self.operator.set_callable(new_function, (3, 4))
        context = {}
        result = self.operator.execute(context)
        
        self.assertEqual(result, 12)

class TestTaskRunner(unittest.TestCase):
    """Test cases for TaskRunner class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.executor = LocalExecutor()
        self.runner = TaskRunner(self.executor)
        self.task = DAGNode("runner_test_task")
    
    def test_runner_creation(self):
        """Test runner creation."""
        self.assertEqual(self.runner.retry_count, 3)
        self.assertEqual(self.runner.retry_delay, 1.0)
    
    def test_run_with_retry(self):
        """Test task execution with retry."""
        result = self.runner.run_with_retry(self.task)
        self.assertTrue(result)
    
    def test_set_retry_config(self):
        """Test retry configuration."""
        self.runner.set_retry_config(5, 2.0)
        self.assertEqual(self.runner.retry_count, 5)
        self.assertEqual(self.runner.retry_delay, 2.0)

class TestHelperFunctions(unittest.TestCase):
    """Test cases for helper functions."""
    
    def test_create_dag_function(self):
        """Test DAG creation helper function."""
        dag = create_dag("helper_test_dag", "Helper Test", ["task1", "task2"])
        
        self.assertEqual(dag.node_id, "helper_test_dag")
        self.assertEqual(dag.description, "Helper Test")
        self.assertEqual(len(dag.tasks), 2)
    
    def test_create_executor_function(self):
        """Test executor creation helper function."""
        executor = create_executor("local", max_workers=8)
        
        self.assertIsInstance(executor, LocalExecutor)
        self.assertEqual(executor.max_workers, 8)
    
    def test_format_datetime(self):
        """Test datetime formatting."""
        dt = datetime(2023, 12, 25, 10, 30, 45)
        formatted = format_datetime(dt)
        
        self.assertEqual(formatted, "2023-12-25 10:30:45")
    
    def test_validate_task_id(self):
        """Test task ID validation."""
        self.assertTrue(validate_task_id("valid_task_id"))
        self.assertTrue(validate_task_id("task-123"))
        self.assertFalse(validate_task_id("invalid task id"))  # Contains space
        self.assertFalse(validate_task_id(""))  # Empty string
        self.assertFalse(validate_task_id(None))  # None value

class TestConfigManager(unittest.TestCase):
    """Test cases for ConfigManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config_manager = ConfigManager()
    
    def test_config_creation(self):
        """Test config manager creation."""
        self.assertIsInstance(self.config_manager._defaults, dict)
        self.assertIn("max_workers", self.config_manager._defaults)
    
    def test_get_set_config(self):
        """Test getting and setting configuration."""
        self.config_manager.set("test_key", "test_value")
        value = self.config_manager.get("test_key")
        
        self.assertEqual(value, "test_value")
    
    def test_get_default_value(self):
        """Test getting default values."""
        max_workers = self.config_manager.get("max_workers")
        self.assertEqual(max_workers, 4)  # Default value
    
    def test_update_config(self):
        """Test updating configuration with dictionary."""
        updates = {"new_key": "new_value", "max_workers": 8}
        self.config_manager.update(updates)
        
        self.assertEqual(self.config_manager.get("new_key"), "new_value")
        self.assertEqual(self.config_manager.get("max_workers"), 8)
    
    def test_get_all_config(self):
        """Test getting all configuration values."""
        all_config = self.config_manager.get_all()
        
        self.assertIsInstance(all_config, dict)
        self.assertIn("max_workers", all_config)

class TestRetryDecorator(unittest.TestCase):
    """Test cases for retry decorator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.call_count = 0
    
    def test_successful_execution(self):
        """Test decorator with successful function."""
        @retry_decorator(max_retries=3)
        def successful_function():
            self.call_count += 1
            return "success"
        
        result = successful_function()
        self.assertEqual(result, "success")
        self.assertEqual(self.call_count, 1)
    
    def test_retry_on_failure(self):
        """Test decorator with failing function."""
        @retry_decorator(max_retries=2, delay=0.1)
        def failing_function():
            self.call_count += 1
            if self.call_count < 3:
                raise ValueError("Test error")
            return "success after retries"
        
        result = failing_function()
        self.assertEqual(result, "success after retries")
        self.assertEqual(self.call_count, 3)  # Initial + 2 retries
    
    def test_max_retries_exceeded(self):
        """Test decorator when max retries exceeded."""
        @retry_decorator(max_retries=2, delay=0.1)
        def always_failing_function():
            self.call_count += 1
            raise ValueError("Always fails")
        
        with self.assertRaises(ValueError):
            always_failing_function()
        
        self.assertEqual(self.call_count, 3)  # Initial + 2 retries

class TestIntegration(unittest.TestCase):
    """Integration test cases."""
    
    def test_complete_workflow(self):
        """Test complete workflow from DAG creation to execution."""
        # Create DAG with tasks
        dag = create_dag("integration_test_dag", "Integration Test DAG")
        
        # Create Python operator
        def sample_task():
            return "Task completed successfully"
        
        python_op = PythonOperator(
            task_id="python_task",
            python_callable=sample_task
        )
        
        # Add operator to DAG
        dag.add_task(python_op)
        
        # Create executor and run
        executor = create_executor("local")
        result = executor.execute_dag(dag)
        
        # Verify results
        self.assertTrue(result["success"])
        self.assertIn("python_task", result["task_results"])
        self.assertTrue(result["task_results"]["python_task"])
    
    def test_dag_with_dependencies(self):
        """Test DAG execution with task dependencies."""
        dag = DAG("dependency_test_dag")
        
        task1 = DAGNode("first_task")
        task2 = DAGNode("second_task")
        task3 = DAGNode("third_task")
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        # Create dependency chain: task1 -> task2 -> task3
        dag.add_dependency("first_task", "second_task")
        dag.add_dependency("second_task", "third_task")
        
        # Validate structure
        self.assertTrue(dag.validate_structure())
        
        # Execute DAG
        executor = LocalExecutor()
        result = executor.execute_dag(dag)
        
        self.assertTrue(result["success"])
        self.assertEqual(len(result["task_results"]), 3)

if __name__ == "__main__":
    # Run all tests
    unittest.main(verbosity=2)
