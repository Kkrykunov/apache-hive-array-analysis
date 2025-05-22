import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
from pathlib import Path
import subprocess

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from utils import (
    validate_hadoop_environment,
    execute_hive_command,
    format_array_for_hive,
    validate_array_data,
    check_hdfs_path,
    get_performance_metrics
)


class TestUtils(unittest.TestCase):
    """Test cases for utility functions."""
    
    def test_format_array_for_hive(self):
        """Test array formatting for Hive."""
        test_array = [1.5, 2.7, 3.9]
        
        formatted = format_array_for_hive(test_array)
        
        self.assertEqual(formatted, "ARRAY(1.5,2.7,3.9)")
    
    def test_format_array_for_hive_empty(self):
        """Test empty array formatting."""
        test_array = []
        
        formatted = format_array_for_hive(test_array)
        
        self.assertEqual(formatted, "ARRAY()")
    
    def test_validate_array_data_valid(self):
        """Test valid array data validation."""
        valid_data = [
            {'id': 1, 'values': [1.0, 2.0]},
            {'id': 2, 'values': [3.0, 4.0]}
        ]
        
        is_valid, message = validate_array_data(valid_data)
        
        self.assertTrue(is_valid)
        self.assertEqual(message, "Data validation successful")
    
    def test_validate_array_data_invalid_structure(self):
        """Test invalid array data structure."""
        invalid_data = [
            {'wrong_key': 1, 'values': [1.0, 2.0]}
        ]
        
        is_valid, message = validate_array_data(invalid_data)
        
        self.assertFalse(is_valid)
        self.assertIn("Missing required key", message)
    
    def test_validate_array_data_invalid_values(self):
        """Test invalid array values."""
        invalid_data = [
            {'id': 1, 'values': 'not_an_array'}
        ]
        
        is_valid, message = validate_array_data(invalid_data)
        
        self.assertFalse(is_valid)
        self.assertIn("values must be a list", message)
    
    @patch.dict(os.environ, {
        'HADOOP_HOME': '/opt/hadoop',
        'HIVE_HOME': '/opt/hive',
        'JAVA_HOME': '/opt/java'
    })
    @patch('utils.Path.exists')
    def test_validate_hadoop_environment_success(self, mock_exists):
        """Test successful Hadoop environment validation."""
        mock_exists.return_value = True
        
        is_valid, message = validate_hadoop_environment()
        
        self.assertTrue(is_valid)
        self.assertEqual(message, "Hadoop environment validation successful")
    
    @patch.dict(os.environ, {}, clear=True)
    def test_validate_hadoop_environment_missing_vars(self):
        """Test missing environment variables."""
        is_valid, message = validate_hadoop_environment()
        
        self.assertFalse(is_valid)
        self.assertIn("Missing environment variable", message)
    
    @patch('utils.subprocess.run')
    def test_execute_hive_command_success(self, mock_run):
        """Test successful Hive command execution."""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout = "Query result"
        mock_process.stderr = ""
        mock_run.return_value = mock_process
        
        with patch.dict(os.environ, {'HIVE_HOME': '/opt/hive'}):
            success, stdout, stderr = execute_hive_command("SHOW TABLES")
        
        self.assertTrue(success)
        self.assertEqual(stdout, "Query result")
        self.assertEqual(stderr, "")
    
    @patch('utils.subprocess.run')
    def test_execute_hive_command_failure(self, mock_run):
        """Test failed Hive command execution."""
        mock_process = Mock()
        mock_process.returncode = 1
        mock_process.stdout = ""
        mock_process.stderr = "Command failed"
        mock_run.return_value = mock_process
        
        with patch.dict(os.environ, {'HIVE_HOME': '/opt/hive'}):
            success, stdout, stderr = execute_hive_command("INVALID QUERY")
        
        self.assertFalse(success)
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "Command failed")
    
    @patch('utils.subprocess.run')
    def test_execute_hive_command_timeout(self, mock_run):
        """Test Hive command timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("hive", 10)
        
        with patch.dict(os.environ, {'HIVE_HOME': '/opt/hive'}):
            success, stdout, stderr = execute_hive_command("LONG RUNNING QUERY", timeout=1)
        
        self.assertFalse(success)
        self.assertIn("timeout", stderr.lower())
    
    @patch('utils.subprocess.run')
    def test_check_hdfs_path_exists(self, mock_run):
        """Test HDFS path existence check."""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_run.return_value = mock_process
        
        with patch.dict(os.environ, {'HADOOP_HOME': '/opt/hadoop'}):
            exists = check_hdfs_path('/hdfs/test/path')
        
        self.assertTrue(exists)
    
    @patch('utils.subprocess.run')
    def test_check_hdfs_path_not_exists(self, mock_run):
        """Test HDFS path non-existence check."""
        mock_process = Mock()
        mock_process.returncode = 1
        mock_run.return_value = mock_process
        
        with patch.dict(os.environ, {'HADOOP_HOME': '/opt/hadoop'}):
            exists = check_hdfs_path('/hdfs/nonexistent/path')
        
        self.assertFalse(exists)
    
    def test_get_performance_metrics(self):
        """Test performance metrics calculation."""
        start_time = 1000.0
        end_time = 1010.5
        
        metrics = get_performance_metrics(start_time, end_time, 100)
        
        self.assertEqual(metrics['execution_time'], 10.5)
        self.assertEqual(metrics['records_processed'], 100)
        self.assertAlmostEqual(metrics['records_per_second'], 9.52, places=2)


if __name__ == '__main__':
    unittest.main()