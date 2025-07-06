"""Test cases for DAG functionality."""

import unittest
from datetime import datetime, timedelta
from src.core.dag import DAG, DAGNode, DAGValidator, TaskStatus, create_dag, DEFAULT_DAG_ARGS, DAGTemplate, DAGFactory
from src.core.executor import LocalExecutor, BaseExecutor, TaskExecutionEngine, create_executor, AsyncExecutor
from src.operators.base_operator import PythonOperator, BashOperator, OperatorType, EmailOperator, FileSensor, OPERATOR_REGISTRY
from src.operators.sql_operator import SQLOperator, PostgreSQLOperator, create_sql_operator
from src.schedulers.scheduler import DAGScheduler, ScheduleType, SchedulerConfig, DAGRun
from src.utils.helpers import retry_decorator, format_datetime, SimpleConfig, validate_task_id, timer

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
    
    def test_tags_and_metadata(self):
        """Test tags and metadata functionality."""
        self.node.add_tag("test")
        self.node.add_tag("important")
        self.assertIn("test", self.node.tags)
        self.assertIn("important", self.node.tags)
        
        self.node.set_metadata("priority", "high")
        self.node.set_metadata("owner", "test_user")
        
        self.assertEqual(self.node.metadata["priority"], "high")
        self.assertEqual(self.node.metadata["owner"], "test_user")
        
        self.node.remove_tag("test")
        self.assertNotIn("test", self.node.tags)
        self.assertIn("important", self.node.tags)

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
    
    def test_add_remove_task(self):
        """Test adding and removing tasks."""
        self.dag.add_task(self.task1)
        self.assertEqual(len(self.dag.tasks), 1)
        self.assertEqual(self.dag.get_task_count(), 1)
        
        # Test duplicate task addition
        self.dag.add_task(self.task1)
        self.assertEqual(len(self.dag.tasks), 1)  # Should not add duplicate
        
        # Test task removal
        self.dag.add_task(self.task2)
        self.assertEqual(len(self.dag.tasks), 2)
        
        self.dag.remove_task("task1")
        self.assertEqual(len(self.dag.tasks), 1)
        self.assertIsNone(self.dag.get_task_by_id("task1"))
    
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
    
    def test_root_and_leaf_tasks(self):
        """Test getting root and leaf tasks."""
        self.dag.add_task(self.task1)
        self.dag.add_task(self.task2)
        self.dag.add_task(self.task3)
        
        # task1 -> task2 -> task3
        self.dag.add_dependency("task1", "task2")
        self.dag.add_dependency("task2", "task3")
        
        root_tasks = self.dag.get_root_tasks()
        leaf_tasks = self.dag.get_leaf_tasks()
        
        self.assertEqual(len(root_tasks), 1)
        self.assertEqual(root_tasks[0].node_id, "task1")
        
        self.assertEqual(len(leaf_tasks), 1)
        self.assertEqual(leaf_tasks[0].node_id, "task3")
    
    def test_dag_serialization(self):
        """Test DAG serialization and deserialization."""
        from src.core.dag import DAGSerializer
        
        self.dag.add_task(self.task1)
        self.dag.add_task(self.task2)
        self.dag.add_dependency("task1", "task2")
        
        # Serialize
        serialized = DAGSerializer.serialize_dag(self.dag)
        self.assertIsInstance(serialized, str)
        
        # Deserialize
        deserialized_dag = DAGSerializer.deserialize_dag(serialized)
        self.assertEqual(deserialized_dag.node_id, self.dag.node_id)
        self.assertEqual(deserialized_dag.description, self.dag.description)

class TestDAGTemplate(unittest.TestCase):
    """Test cases for DAGTemplate class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.template = DAGTemplate("etl_template", "ETL Template", "Template for ETL workflows")
    
    def test_template_creation(self):
        """Test template creation."""
        self.assertEqual(self.template.template_id, "etl_template")
        self.assertEqual(self.template.name, "ETL Template")
        self.assertEqual(self.template.description, "Template for ETL workflows")
    
    def test_add_task_template(self):
        """Test adding task templates."""
        self.template.add_task_template("python", {"callable": "extract_data"})
        self.template.add_task_template("sql", {"query": "SELECT * FROM source"})
        
        self.assertEqual(len(self.template.task_templates), 2)
        self.assertEqual(self.template.task_templates[0]["type"], "python")

class TestDAGFactory(unittest.TestCase):
    """Test cases for DAGFactory class."""
    
    def test_create_etl_dag(self):
        """Test ETL DAG creation."""
        source_config = {"name": "PostgreSQL", "type": "postgres"}
        target_config = {"name": "BigQuery", "type": "bigquery"}
        
        dag = DAGFactory.create_etl_dag("test_etl", source_config, target_config)
        
        self.assertEqual(dag.node_id, "test_etl")
        self.assertIn("etl", dag.tags)
        self.assertIn("data-pipeline", dag.tags)
    
    def test_create_ml_training_dag(self):
        """Test ML training DAG creation."""
        model_config = {"model_name": "fraud_detection", "model_type": "xgboost"}
        
        dag = DAGFactory.create_ml_training_dag("ml_training", model_config)
        
        self.assertEqual(dag.node_id, "ml_training")
        self.assertIn("ml", dag.tags)
        self.assertIn("training", dag.tags)
        self.assertIn("xgboost", dag.tags)

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
    
    def test_executor_start_stop(self):
        """Test executor start and stop."""
        self.assertFalse(self.executor.is_running)
        
        self.executor.start()
        self.assertTrue(self.executor.is_running)
        
        self.executor.stop()
        self.assertFalse(self.executor.is_running)
    
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

class TestAsyncExecutor(unittest.TestCase):
    """Test cases for AsyncExecutor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.executor = AsyncExecutor(max_workers=2)
        self.task = DAGNode("async_test_task")
        self.dag = DAG("async_test_dag")
        self.dag.add_task(self.task)
    
    def test_async_executor_creation(self):
        """Test async executor creation."""
        self.assertEqual(self.executor.max_workers, 2)
        self.assertFalse(self.executor.is_running)
    
    def test_async_execute_task(self):
        """Test async task execution."""
        result = self.executor.execute_task(self.task)
        self.assertTrue(result)
        self.assertEqual(self.task.status, TaskStatus.SUCCESS)
    
    def test_async_execute_dag(self):
        """Test async DAG execution."""
        result = self.executor.execute_dag(self.dag)
        
        self.assertIn("success", result)
        self.assertIn("execution_time", result)
        self.assertTrue(result["success"])

class TestTaskExecutionEngine(unittest.TestCase):
    """Test cases for TaskExecutionEngine class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.executor = LocalExecutor()
        self.engine = TaskExecutionEngine(self.executor)
        self.task = DAGNode("engine_test_task")
    
    def test_engine_creation(self):
        """Test engine creation."""
        self.assertEqual(self.engine.retry_count, 3)
        self.assertEqual(self.engine.retry_delay, 1.0)
    
    def test_execute_with_retry(self):
        """Test task execution with retry."""
        result = self.engine.execute_with_retry(self.task)
        self.assertTrue(result)
    
    def test_callbacks(self):
        """Test callback functionality."""
        callback_called = []
        
        def test_callback(task_id):
            callback_called.append(task_id)
        
        self.engine.add_callback(self.task.node_id, "on_success", test_callback)
        self.engine.execute_with_retry(self.task)
        
        self.assertIn(self.task.node_id, callback_called)
    
    def test_execution_history(self):
        """Test execution history tracking."""
        self.engine.execute_with_retry(self.task)
        
        history = self.engine.get_execution_history()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["task_id"], self.task.node_id)

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
    
    def test_operator_chaining(self):
        """Test operator chaining with >> and << operators."""
        def dummy_func():
            return "dummy"
        
        op1 = PythonOperator(task_id="op1", python_callable=dummy_func)
        op2 = PythonOperator(task_id="op2", python_callable=dummy_func)
        
        # Test >> operator
        op1 >> op2
        self.assertIn("op1", op2.upstream_tasks)
        self.assertIn("op2", op1.downstream_tasks)

class TestBashOperator(unittest.TestCase):
    """Test cases for BashOperator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.operator = BashOperator(
            task_id="bash_test_task",
            bash_command="echo 'Hello World'"
        )
    
    def test_bash_operator_creation(self):
        """Test bash operator creation."""
        self.assertEqual(self.operator.task_id, "bash_test_task")
        self.assertEqual(self.operator.operator_type, OperatorType.BASH)
        self.assertEqual(self.operator.bash_command, "echo 'Hello World'")
    
    def test_bash_execute(self):
        """Test bash command execution."""
        context = {"execution_date": datetime.now()}
        result = self.operator.execute(context)
        
        self.assertIsInstance(result, str)
        self.assertEqual(self.operator.exit_code, 0)

class TestEmailOperator(unittest.TestCase):
    """Test cases for EmailOperator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.operator = EmailOperator(
            task_id="email_test_task",
            to="test@example.com",
            subject="Test Email",
            html_content="<h1>Test</h1>"
        )
    
    def test_email_operator_creation(self):
        """Test email operator creation."""
        self.assertEqual(self.operator.task_id, "email_test_task")
        self.assertEqual(self.operator.operator_type, OperatorType.EMAIL)
        self.assertIn("test@example.com", self.operator.to)
    
    def test_email_execute(self):
        """Test email sending."""
        context = {"execution_date": datetime.now()}
        result = self.operator.execute(context)
        
        self.assertIn("Email sent successfully", result)

class TestSQLOperator(unittest.TestCase):
    """Test cases for SQL operators."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sql_operator = SQLOperator(
            task_id="sql_test_task",
            sql="SELECT * FROM test_table"
        )
        
        self.postgres_operator = PostgreSQLOperator(
            task_id="postgres_test_task",
            sql="SELECT COUNT(*) FROM users"
        )
    
    def test_sql_operator_creation(self):
        """Test SQL operator creation."""
        self.assertEqual(self.sql_operator.task_id, "sql_test_task")
        self.assertEqual(self.sql_operator.operator_type, OperatorType.SQL)
        self.assertEqual(len(self.sql_operator.sql), 1)
    
    def test_postgres_operator_creation(self):
        """Test PostgreSQL operator creation."""
        self.assertEqual(self.postgres_operator.task_id, "postgres_test_task")
        self.assertEqual(self.postgres_operator.database_type, "postgresql")
    
    def test_sql_execute(self):
        """Test SQL execution."""
        context = {"execution_date": datetime.now()}
        result = self.sql_operator.execute(context)
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)  # One query result
    
    def test_create_sql_operator_factory(self):
        """Test SQL operator factory."""
        operator = create_sql_operator(
            task_id="factory_test",
            sql="SELECT 1",
            database_type="postgresql"
        )
        
        self.assertIsInstance(operator, PostgreSQLOperator)

class TestFileSensor(unittest.TestCase):
    """Test cases for FileSensor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sensor = FileSensor(
            task_id="file_sensor_test",
            filepath="/tmp/test_file.txt",
            poke_interval=1,
            timeout=10
        )
    
    def test_file_sensor_creation(self):
        """Test file sensor creation."""
        self.assertEqual(self.sensor.task_id, "file_sensor_test")
        self.assertEqual(self.sensor.operator_type, OperatorType.SENSOR)
        self.assertEqual(self.sensor.filepath, "/tmp/test_file.txt")
    
    def test_file_sensor_poke(self):
        """Test file sensor poke method."""
        # Test with non-existent file
        result = self.sensor.poke({})
        self.assertFalse(result)

class TestDAGScheduler(unittest.TestCase):
    """Test cases for DAGScheduler."""
    
    def setUp(self):
        """Set up test fixtures."""
        config = SchedulerConfig(max_concurrent_dags=2, check_interval=1)
        self.scheduler = DAGScheduler(config)
        self.dag = create_dag("test_scheduled_dag", "Test DAG for scheduling")
    
    def test_scheduler_creation(self):
        """Test scheduler creation."""
        self.assertEqual(self.scheduler.config.max_concurrent_dags, 2)
        self.assertEqual(len(self.scheduler.registered_dags), 0)
    
    def test_register_dag(self):
        """Test DAG registration."""
        self.scheduler.register_dag(self.dag, ScheduleType.DAILY)
        
        self.assertIn(self.dag.node_id, self.scheduler.registered_dags)
        self.assertEqual(self.dag.schedule_type, ScheduleType.DAILY)
    
    def test_scheduler_start_stop(self):
        """Test scheduler start and stop."""
        from src.schedulers.scheduler import SchedulerStatus
        
        self.assertEqual(self.scheduler.status, SchedulerStatus.STOPPED)
        
        self.scheduler.start()
        self.assertEqual(self.scheduler.status, SchedulerStatus.RUNNING)
        
        self.scheduler.stop()
        self.assertEqual(self.scheduler.status, SchedulerStatus.STOPPED)
    
    def test_trigger_dag(self):
        """Test manual DAG triggering."""
        self.scheduler.register_dag(self.dag)
        
        dag_run = self.scheduler.trigger_dag(self.dag.node_id)
        
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag_run.dag_id, self.dag.node_id)
        self.assertTrue(dag_run.external_trigger)

class TestDAGRun(unittest.TestCase):
    """Test cases for DAGRun."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.dag_run = DAGRun("test_dag", "test_run_1", datetime.now())
    
    def test_dag_run_creation(self):
        """Test DAG run creation."""
        self.assertEqual(self.dag_run.dag_id, "test_dag")
        self.assertEqual(self.dag_run.run_id, "test_run_1")
        self.assertEqual(self.dag_run.status, TaskStatus.PENDING)
    
    def test_dag_run_lifecycle(self):
        """Test DAG run start and completion."""
        self.dag_run.start_run()
        self.assertEqual(self.dag_run.status, TaskStatus.RUNNING)
        self.assertIsNotNone(self.dag_run.start_date)
        
        self.dag_run.complete_run(True)
        self.assertEqual(self.dag_run.status, TaskStatus.SUCCESS)
        self.assertIsNotNone(self.dag_run.end_date)
        
        duration = self.dag_run.get_duration()
        self.assertIsNotNone(duration)
        self.assertGreaterEqual(duration, 0)

class TestOperatorRegistry(unittest.TestCase):
    """Test cases for OperatorRegistry."""
    
    def test_registry_operations(self):
        """Test operator registry operations."""
        registry = OPERATOR_REGISTRY
        
        # Test listing operators
        operators = registry.list_operators()
        self.assertIn('python', operators)
        self.assertIn('bash', operators)
        
        # Test creating operators
        python_op = registry.create_operator('python', task_id='test', python_callable=lambda: None)
        self.assertIsInstance(python_op, PythonOperator)
        
        bash_op = registry.create_operator('bash', task_id='test', bash_command='echo test')
        self.assertIsInstance(bash_op, BashOperator)

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
        
        async_executor = create_executor("async", max_workers=4)
        self.assertIsInstance(async_executor, AsyncExecutor)
    
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
    
    def test_performance_timer(self):
        """Test performance timing context manager."""
        with timer("test_operation") as t:
            import time
            time.sleep(0.01)
        
        self.assertGreater(t.duration, 0)

class TestSimpleConfig(unittest.TestCase):
    """Test cases for SimpleConfig class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = SimpleConfig()
    
    def test_config_creation(self):
        """Test config creation."""
        self.assertEqual(self.config.get("max_workers"), 4)
    
    def test_get_set_config(self):
        """Test getting and setting configuration."""
        self.config.set("test_key", "test_value")
        value = self.config.get("test_key")
        
        self.assertEqual(value, "test_value")
    
    def test_update_config(self):
        """Test updating configuration with dictionary."""
        updates = {"new_key": "new_value", "max_workers": 8}
        self.config.update(updates)
        
        self.assertEqual(self.config.get("new_key"), "new_value")
        self.assertEqual(self.config.get("max_workers"), 8)

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
        @retry_decorator(max_retries=2, delay=0.01)
        def failing_function():
            self.call_count += 1
            if self.call_count < 3:
                raise ValueError("Test error")
            return "success after retries"
        
        result = failing_function()
        self.assertEqual(result, "success after retries")
        self.assertEqual(self.call_count, 3)  # Initial + 2 retries
    
    def test_specific_exceptions(self):
        """Test decorator with specific exception types."""
        @retry_decorator(max_retries=2, delay=0.01, exceptions=(ValueError,))
        def specific_exception_function():
            self.call_count += 1
            if self.call_count == 1:
                raise ValueError("Retryable error")
            elif self.call_count == 2:
                raise TypeError("Non-retryable error")
            return "success"
        
        with self.assertRaises(TypeError):
            specific_exception_function()
        
        self.assertEqual(self.call_count, 2)

class TestIntegrationScenarios(unittest.TestCase):
    """Integration test cases for complex scenarios."""

    def test_complete_etl_workflow(self):
        """Test complete ETL workflow with multiple operators."""
        # Create ETL DAG
        dag = DAGFactory.create_etl_dag(
            "integration_etl_dag",
            {"name": "source_db", "type": "postgres"},
            {"name": "target_db", "type": "bigquery"}
        )
        
        # Add extraction task
        def extract_data():
            return {"records": 100, "status": "extracted"}
        
        extract_task = PythonOperator(
            task_id="extract_data",
            python_callable=extract_data
        )
        
        # Add transformation task
        transform_task = SQLOperator(
            task_id="transform_data",
            sql="SELECT * FROM staging WHERE status = 'active'"
        )
        
        # Add loading task
        def load_data():
            return {"loaded_records": 100, "status": "completed"}
        
        load_task = PythonOperator(
            task_id="load_data",
            python_callable=load_data
        )
        
        # Add email notification
        notify_task = EmailOperator(
            task_id="notify_completion",
            to="admin@example.com",
            subject="ETL Job Completed",
            html_content="<h1>ETL job completed successfully</h1>"
        )
        
        # Build DAG
        dag.add_task(extract_task)
        dag.add_task(transform_task)
        dag.add_task(load_task)
        dag.add_task(notify_task)
        
        # Set dependencies: extract -> transform -> load -> notify
        dag.add_dependency("extract_data", "transform_data")
        dag.add_dependency("transform_data", "load_data")
        dag.add_dependency("load_data", "notify_completion")
        
        # Validate DAG
        self.assertTrue(dag.validate_structure())
        
        # Execute DAG
        executor = LocalExecutor()
        result = executor.execute_dag(dag)
        
        self.assertTrue(result["success"])
        self.assertEqual(len(result["task_results"]), 4)

    def test_scheduled_dag_with_sensors(self):
        """Test scheduled DAG with sensor dependencies."""
        # Create DAG
        dag = create_dag("sensor_workflow_dag", "Workflow with sensors")
        
        # Add file sensor
        file_sensor = FileSensor(
            task_id="wait_for_file",
            filepath="/tmp/trigger_file.txt",
            poke_interval=1,
            timeout=30
        )
        
        # Add processing task
        def process_file():
            return {"processed": True, "timestamp": datetime.now().isoformat()}
        
        process_task = PythonOperator(
            task_id="process_file",
            python_callable=process_file
        )
        
        # Add cleanup task
        cleanup_task = BashOperator(
            task_id="cleanup",
            bash_command="echo 'Cleaning up temporary files'"
        )
        
        # Build DAG
        dag.add_task(file_sensor)
        dag.add_task(process_task)
        dag.add_task(cleanup_task)
        
        # Set dependencies
        dag.add_dependency("wait_for_file", "process_file")
        dag.add_dependency("process_file", "cleanup")
        
        # Test with scheduler
        config = SchedulerConfig(max_concurrent_dags=1, check_interval=1)
        scheduler = DAGScheduler(config)
        scheduler.register_dag(dag, ScheduleType.HOURLY)
        
        self.assertIn(dag.node_id, scheduler.registered_dags)
        
        # Trigger DAG manually
        dag_run = scheduler.trigger_dag(dag.node_id)
        self.assertIsNotNone(dag_run)

    def test_complex_dependency_graph(self):
        """Test complex DAG with multiple dependency patterns."""
        dag = DAG("complex_dependency_dag", "Complex dependency patterns")
        
        # Create tasks
        start_task = PythonOperator(task_id="start", python_callable=lambda: "started")
        
        # Parallel branch 1
        branch1_task1 = PythonOperator(task_id="branch1_task1", python_callable=lambda: "branch1_1")
        branch1_task2 = PythonOperator(task_id="branch1_task2", python_callable=lambda: "branch1_2")
        
        # Parallel branch 2
        branch2_task1 = PythonOperator(task_id="branch2_task1", python_callable=lambda: "branch2_1")
        branch2_task2 = PythonOperator(task_id="branch2_task2", python_callable=lambda: "branch2_2")
        
        # Join task
        join_task = PythonOperator(task_id="join", python_callable=lambda: "joined")
        
        # End task
        end_task = PythonOperator(task_id="end", python_callable=lambda: "completed")
        
        # Add tasks to DAG
        for task in [start_task, branch1_task1, branch1_task2, branch2_task1, branch2_task2, join_task, end_task]:
            dag.add_task(task)
        
        # Set up complex dependencies
        # start -> branch1_task1 -> branch1_task2 -> join
        # start -> branch2_task1 -> branch2_task2 -> join
        # join -> end
        dag.add_dependency("start", "branch1_task1")
        dag.add_dependency("start", "branch2_task1")
        dag.add_dependency("branch1_task1", "branch1_task2")
        dag.add_dependency("branch2_task1", "branch2_task2")
        dag.add_dependency("branch1_task2", "join")
        dag.add_dependency("branch2_task2", "join")
        dag.add_dependency("join", "end")
        
        # Validate structure
        self.assertTrue(dag.validate_structure())
        
        # Check root and leaf tasks
        root_tasks = dag.get_root_tasks()
        leaf_tasks = dag.get_leaf_tasks()
        
        self.assertEqual(len(root_tasks), 1)
        self.assertEqual(root_tasks[0].node_id, "start")
        self.assertEqual(len(leaf_tasks), 1)
        self.assertEqual(leaf_tasks[0].node_id, "end")
        
        # Execute DAG
        executor = LocalExecutor()
        result = executor.execute_dag(dag)
        
        self.assertTrue(result["success"])
        self.assertEqual(len(result["task_results"]), 7)

    def test_error_handling_and_retries(self):
        """Test error handling and retry mechanisms."""
        dag = DAG("error_handling_dag", "Test error handling")
        
        # Task that fails initially then succeeds
        self.attempt_count = 0
        def flaky_function():
            self.attempt_count += 1
            if self.attempt_count < 3:
                raise ValueError(f"Attempt {self.attempt_count} failed")
            return f"Success on attempt {self.attempt_count}"
        
        flaky_task = PythonOperator(
            task_id="flaky_task",
            python_callable=flaky_function,
            retries=3,
            retry_delay=timedelta(seconds=1)
        )
        
        # Task that always succeeds
        def reliable_function():
            return "Always works"
        
        reliable_task = PythonOperator(
            task_id="reliable_task",
            python_callable=reliable_function
        )
        
        dag.add_task(flaky_task)
        dag.add_task(reliable_task)
        dag.add_dependency("flaky_task", "reliable_task")
        
        # Execute with retry engine
        executor = LocalExecutor()
        engine = TaskExecutionEngine(executor)
        
        # Test retry mechanism
        result = engine.execute_with_retry(flaky_task)
        self.assertTrue(result)
        self.assertEqual(self.attempt_count, 3)
        
        # Check execution history
        history = engine.get_execution_history("flaky_task")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["attempts"], 3)
        self.assertTrue(history[0]["success"])

    def test_operator_chaining_and_relationships(self):
        """Test operator chaining using >> and << operators."""
        def dummy_func(name):
            return f"Executed {name}"
        
        # Create operators
        op1 = PythonOperator(task_id="op1", python_callable=dummy_func, op_args=("task1",))
        op2 = PythonOperator(task_id="op2", python_callable=dummy_func, op_args=("task2",))
        op3 = PythonOperator(task_id="op3", python_callable=dummy_func, op_args=("task3",))
        op4 = PythonOperator(task_id="op4", python_callable=dummy_func, op_args=("task4",))
        
        # Test chaining: op1 >> op2 >> op3
        op1 >> op2 >> op3
        
        # Test multiple downstream: op1 >> [op2, op4]
        op1 >> op4
        
        # Verify relationships
        self.assertIn("op1", op2.upstream_tasks)
        self.assertIn("op2", op1.downstream_tasks)
        self.assertIn("op1", op4.upstream_tasks)
        self.assertIn("op4", op1.downstream_tasks)
        
        # Test << operator
        op5 = PythonOperator(task_id="op5", python_callable=dummy_func, op_args=("task5",))
        op5 << op3  # op3 >> op5
        
        self.assertIn("op3", op5.upstream_tasks)
        self.assertIn("op5", op3.downstream_tasks)

    def test_dag_serialization_with_complex_structure(self):
        """Test DAG serialization with complex operators and relationships."""
        from src.core.dag import DAGSerializer
        
        dag = DAG("serialization_test_dag", "Test serialization")
        
        # Add various operator types
        python_op = PythonOperator(
            task_id="python_task",
            python_callable=lambda: "python result"
        )
        
        bash_op = BashOperator(
            task_id="bash_task",
            bash_command="echo 'bash result'"
        )
        
        sql_op = SQLOperator(
            task_id="sql_task",
            sql="SELECT 1 as result"
        )
        
        email_op = EmailOperator(
            task_id="email_task",
            to="test@example.com",
            subject="Test"
        )
        
        # Add to DAG
        for op in [python_op, bash_op, sql_op, email_op]:
            dag.add_task(op)
        
        # Set dependencies
        dag.add_dependency("python_task", "bash_task")
        dag.add_dependency("bash_task", "sql_task")
        dag.add_dependency("sql_task", "email_task")
        
        # Serialize
        serialized = DAGSerializer.serialize_dag(dag)
        self.assertIsInstance(serialized, str)
        self.assertIn("serialization_test_dag", serialized)
        
        # Deserialize
        deserialized_dag = DAGSerializer.deserialize_dag(serialized)
        self.assertEqual(deserialized_dag.node_id, dag.node_id)
        self.assertEqual(len(deserialized_dag.tasks), len(dag.tasks))

    def test_performance_monitoring_integration(self):
        """Test performance monitoring with DAG execution."""
        from src.utils.helpers import PERFORMANCE_MONITOR, timer
        
        dag = DAG("performance_test_dag", "Performance monitoring test")
        
        def timed_function():
            import time
            time.sleep(0.01)  # Simulate work
            return "completed"
        
        task = PythonOperator(
            task_id="timed_task",
            python_callable=timed_function
        )
        
        dag.add_task(task)
        
        # Execute with timing
        with timer("dag_execution"):
            executor = LocalExecutor()
            result = executor.execute_dag(dag)
        
        self.assertTrue(result["success"])
        
        # Check performance stats
        stats = PERFORMANCE_MONITOR.get_stats("dag_execution")
        self.assertIn("count", stats)
        self.assertIn("average", stats)
        self.assertGreater(stats["average"], 0)

class TestConfigurationIntegration(unittest.TestCase):
    """Test configuration integration across components."""
    
    def test_config_file_loading(self):
        """Test loading configuration from JSON file."""
        import tempfile
        import json
        
        config_data = {
            "scheduler": {
                "max_concurrent_dags": 5,
                "check_interval": 30
            },
            "executor": {
                "type": "local",
                "max_workers": 8
            }
        }
        
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_file = f.name
        
        try:
            # Test config loading (would need actual ConfigManager implementation)
            # This is a placeholder for configuration integration
            self.assertTrue(True)  # Placeholder assertion
        finally:
            import os
            os.unlink(config_file)

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""
    
    def test_empty_dag(self):
        """Test behavior with empty DAG."""
        dag = DAG("empty_dag", "Empty DAG")
        
        # Validate empty DAG
        validator = DAGValidator(dag)
        report = validator.get_validation_report()
        
        self.assertFalse(report["is_valid"])
        self.assertIn("DAG has no tasks", validator.validation_errors)
    
    def test_single_task_dag(self):
        """Test DAG with single task."""
        dag = DAG("single_task_dag", "Single task DAG")
        
        task = PythonOperator(
            task_id="only_task",
            python_callable=lambda: "single task result"
        )
        
        dag.add_task(task)
        
        # Should be valid
        self.assertTrue(dag.validate_structure())
        
        # Execute
        executor = LocalExecutor()
        result = executor.execute_dag(dag)
        
        self.assertTrue(result["success"])
        self.assertEqual(len(result["task_results"]), 1)
    
    def test_invalid_task_ids(self):
        """Test handling of invalid task IDs."""
        # Test invalid characters
        with self.assertRaises(Exception):
            PythonOperator(
                task_id="invalid task id",  # Contains space
                python_callable=lambda: None
            )
    
    def test_circular_dependencies(self):
        """Test detection of circular dependencies."""
        dag = DAG("circular_dag", "Circular dependency test")
        
        task1 = PythonOperator(task_id="task1", python_callable=lambda: None)
        task2 = PythonOperator(task_id="task2", python_callable=lambda: None)
        task3 = PythonOperator(task_id="task3", python_callable=lambda: None)
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        # Create circular dependency: task1 -> task2 -> task3 -> task1
        dag.add_dependency("task1", "task2")
        dag.add_dependency("task2", "task3")
        dag.add_dependency("task3", "task1")
        
        # Should detect cycle
        self.assertFalse(dag.validate_structure())
        
        validator = DAGValidator(dag)
        validator.is_valid()
        self.assertTrue(any("Cycle detected" in error for error in validator.validation_errors))
    
    def test_orphaned_tasks(self):
        """Test detection of orphaned tasks."""
        dag = DAG("orphaned_dag", "Orphaned task test")
        
        connected_task1 = PythonOperator(task_id="connected1", python_callable=lambda: None)
        connected_task2 = PythonOperator(task_id="connected2", python_callable=lambda: None)
        orphaned_task = PythonOperator(task_id="orphaned", python_callable=lambda: None)
        
        dag.add_task(connected_task1)
        dag.add_task(connected_task2)
        dag.add_task(orphaned_task)
        
        # Connect only two tasks
        dag.add_dependency("connected1", "connected2")
        
        # Should detect orphaned task
        validator = DAGValidator(dag)
        self.assertFalse(validator.is_valid())
        self.assertTrue(any("not connected" in error for error in validator.validation_errors))

class TestCleanupAndTeardown(unittest.TestCase):
    """Test cleanup and resource management."""
    
    def test_executor_cleanup(self):
        """Test executor resource cleanup."""
        executor = LocalExecutor(max_workers=2, use_threading=True)
        
        # Start executor
        executor.start()
        self.assertTrue(executor.is_running)
        self.assertIsNotNone(executor.thread_pool)
        
        # Stop executor
        executor.stop()
        self.assertFalse(executor.is_running)
    
    def test_scheduler_cleanup(self):
        """Test scheduler resource cleanup."""
        config = SchedulerConfig(check_interval=1)
        scheduler = DAGScheduler(config)
        
        # Start scheduler
        scheduler.start()
        from src.schedulers.scheduler import SchedulerStatus
        self.assertEqual(scheduler.status, SchedulerStatus.RUNNING)
        
        # Stop scheduler
        scheduler.stop()
        self.assertEqual(scheduler.status, SchedulerStatus.STOPPED)
    
    def test_performance_monitor_cleanup(self):
        """Test performance monitor cleanup."""
        from src.utils.helpers import PERFORMANCE_MONITOR
        
        # Add some metrics
        PERFORMANCE_MONITOR.start_timer("test_metric")
        import time
        time.sleep(0.01)
        PERFORMANCE_MONITOR.end_timer("test_metric")
        
        # Verify metrics exist
        stats = PERFORMANCE_MONITOR.get_stats("test_metric")
        self.assertGreater(stats["count"], 0)
        
        # Reset metrics
        PERFORMANCE_MONITOR.reset_metrics("test_metric")
        stats = PERFORMANCE_MONITOR.get_stats("test_metric")
        self.assertEqual(len(stats), 0)

if __name__ == "__main__":
    # Configure test runner
    unittest.TestLoader.sortTestMethodsUsing = None  # Preserve test order
    
    # Run all tests with detailed output
    unittest.main(verbosity=2, buffer=True)
