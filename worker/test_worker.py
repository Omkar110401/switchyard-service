import json
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from worker import Worker


class TestWorkerTaskExecution(unittest.TestCase):
    """Test task execution via subprocess."""

    def setUp(self):
        self.worker = Worker(redis_url="redis://localhost:6379", worker_id="test-worker")

    def test_execute_simple_task(self):
        """Test executing a simple task that succeeds."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('import json\nprint(json.dumps({"result": "success"}))\n')
            f.flush()
            script = f.name

        output, error = self.worker._execute_task(script, {}, "test-run-1")

        self.assertIsNone(error)
        self.assertEqual(output["result"], "success")

    def test_execute_task_with_params(self):
        """Test task receives params via environment."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('''
import json
import os
params = json.loads(os.environ.get("TASK_PARAMS", "{}"))
print(json.dumps({"received_param": params.get("test_key")}))
''')
            f.flush()
            script = f.name

        output, error = self.worker._execute_task(script, {"test_key": "test_value"}, "test-run-2")

        self.assertIsNone(error)
        self.assertEqual(output["received_param"], "test_value")

    def test_execute_task_failure(self):
        """Test task execution failure."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('import sys\nprint("error message", file=sys.stderr)\nsys.exit(1)\n')
            f.flush()
            script = f.name

        output, error = self.worker._execute_task(script, {}, "test-run-3")

        self.assertIsNotNone(error)
        self.assertIn("exited with code 1", error)

    def test_execute_task_timeout(self):
        """Test task timeout handling."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('import time\ntime.sleep(3700)\n')
            f.flush()
            script = f.name

        output, error = self.worker._execute_task(script, {}, "test-run-4")

        self.assertIsNotNone(error)
        self.assertIn("timeout", error.lower())

    def test_execute_task_invalid_json_output(self):
        """Test task with non-JSON output."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('print("plain text output")\n')
            f.flush()
            script = f.name

        output, error = self.worker._execute_task(script, {}, "test-run-5")

        self.assertIsNone(error)
        self.assertEqual(output["result"], "plain text output")


class TestWorkerMessageHandling(unittest.TestCase):
    """Test message handling from Redis."""

    def setUp(self):
        self.worker = Worker(redis_url="redis://localhost:6379", worker_id="test-worker")

    @patch('worker.get_session')
    def test_decode_message(self, mock_get_session):
        """Test decoding message from Redis."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_task_run = MagicMock()
        mock_task_run.status = "pending"
        mock_task_run.task_definition_id = "task-123"

        mock_task_def = MagicMock()
        mock_task_def.task_key = "test_task"
        mock_task_def.retry_max_attempts = 3

        mock_session.query.return_value.get.side_effect = [mock_task_run, mock_task_def]

        data = {
            b"task_run_id": b"run-123",
            b"command": b"echo.py",
            b"params": b'{"key": "value"}',
            b"attempt_number": b"1"
        }

        with patch.object(self.worker, '_execute_task', return_value=({"result": "ok"}, None)):
            self.worker._handle_message(data)

        self.assertEqual(mock_task_run.status, "running")
        self.assertEqual(mock_task_run.worker_id, self.worker.worker_id)

    @patch('worker.get_session')
    def test_handle_invalid_message(self, mock_get_session):
        """Test handling invalid message."""
        data = {
            b"task_run_id": None,
            b"command": b"test.py"
        }

        with patch('worker.logger') as mock_logger:
            self.worker._handle_message(data)
            mock_logger.error.assert_called()


class TestWorkerHeartbeat(unittest.TestCase):
    """Test heartbeat mechanism."""

    def setUp(self):
        self.worker = Worker(redis_url="redis://localhost:6379", worker_id="test-worker")

    @patch('worker.get_session')
    @patch('worker.time.sleep')
    def test_heartbeat_updates_db(self, mock_sleep, mock_get_session):
        """Test heartbeat updates task_run in DB."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_task_run = MagicMock()
        mock_task_run.status = "running"
        mock_session.query.return_value.get.return_value = mock_task_run

        with patch('worker.threading.Thread'):
            self.worker._start_heartbeat("task-run-123")

        self.assertTrue(hasattr(mock_task_run, 'heartbeat_at'))


class TestWorkerResultReporting(unittest.TestCase):
    """Test result reporting to DB."""

    def setUp(self):
        self.worker = Worker(redis_url="redis://localhost:6379", worker_id="test-worker")

    @patch('worker.get_session')
    def test_report_success(self, mock_get_session):
        """Test reporting task success."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_task_run = MagicMock()
        mock_session.query.return_value.get.return_value = mock_task_run

        output = {"result": "done"}
        self.worker._report_success("task-run-123", output)

        self.assertEqual(mock_task_run.status, "succeeded")
        self.assertEqual(mock_task_run.outputs, output)
        self.assertIsNotNone(mock_task_run.completed_at)
        mock_session.commit.assert_called()

    @patch('worker.get_session')
    def test_report_failure(self, mock_get_session):
        """Test reporting task failure."""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        mock_task_run = MagicMock()
        mock_session.query.return_value.get.return_value = mock_task_run

        error_msg = "Task failed due to timeout"
        self.worker._report_failure("task-run-123", error_msg)

        self.assertEqual(mock_task_run.status, "failed")
        self.assertEqual(mock_task_run.error_message, error_msg)
        self.assertIsNotNone(mock_task_run.completed_at)
        mock_session.commit.assert_called()


class TestWorkerIntegration(unittest.TestCase):
    """Integration tests."""

    def setUp(self):
        self.worker = Worker(redis_url="redis://localhost:6379", worker_id="test-worker")

    def test_worker_initialization(self):
        """Test worker initializes correctly."""
        self.assertEqual(self.worker.stream_name, "workflow_tasks")
        self.assertEqual(self.worker.consumer_group, "workers")
        self.assertEqual(self.worker.shutdown_requested, False)

    @patch('worker.signal.signal')
    def test_signal_registration(self, mock_signal):
        """Test signal handlers are registered."""
        Worker(redis_url="redis://localhost:6379")
        self.assertEqual(mock_signal.call_count, 2)


if __name__ == '__main__':
    unittest.main()
