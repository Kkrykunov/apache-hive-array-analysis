import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from main import HiveArrayAnalyzer


class TestHiveArrayAnalyzer(unittest.TestCase):
    """Test cases for HiveArrayAnalyzer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.analyzer = HiveArrayAnalyzer()
    
    @patch('main.validate_hadoop_environment')
    def test_setup_environment_success(self, mock_validate):
        """Test successful environment setup."""
        mock_validate.return_value = (True, "Environment valid")
        
        result = self.analyzer.setup_environment()
        
        self.assertTrue(result)
        mock_validate.assert_called_once()
    
    @patch('main.validate_hadoop_environment')
    def test_setup_environment_failure(self, mock_validate):
        """Test environment setup failure."""
        mock_validate.return_value = (False, "Environment invalid")
        
        result = self.analyzer.setup_environment()
        
        self.assertFalse(result)
        mock_validate.assert_called_once()
    
    def test_generate_sample_data(self):
        """Test sample data generation."""
        data = self.analyzer.generate_sample_data(size=5)
        
        self.assertEqual(len(data), 5)
        for row in data:
            self.assertIsInstance(row, dict)
            self.assertIn('id', row)
            self.assertIn('values', row)
            self.assertIsInstance(row['values'], list)
    
    def test_create_hive_table_script(self):
        """Test HQL table creation script generation."""
        script = self.analyzer.create_hive_table_script('test_table')
        
        self.assertIn('CREATE TABLE test_table', script)
        self.assertIn('ARRAY<DOUBLE>', script)
        self.assertIn('STORED AS TEXTFILE', script)
    
    def test_generate_array_operations_hql(self):
        """Test array operations HQL generation."""
        hql = self.analyzer.generate_array_operations_hql('test_table')
        
        self.assertIn('SELECT', hql)
        self.assertIn('test_table', hql)
        self.assertIn('LATERAL VIEW', hql)
        self.assertIn('posexplode', hql)
    
    @patch('main.execute_hive_command')
    def test_execute_analysis_success(self, mock_execute):
        """Test successful analysis execution."""
        mock_execute.return_value = (True, "Query executed", "")
        
        result = self.analyzer.execute_analysis(['test query'])
        
        self.assertTrue(result)
        mock_execute.assert_called()
    
    @patch('main.execute_hive_command')
    def test_execute_analysis_failure(self, mock_execute):
        """Test failed analysis execution."""
        mock_execute.return_value = (False, "", "Error occurred")
        
        result = self.analyzer.execute_analysis(['test query'])
        
        self.assertFalse(result)
        mock_execute.assert_called()
    
    def test_calculate_statistics(self):
        """Test statistics calculation."""
        data = [
            {'id': 1, 'values': [1.0, 2.0, 3.0]},
            {'id': 2, 'values': [4.0, 5.0, 6.0]}
        ]
        
        stats = self.analyzer.calculate_statistics(data)
        
        self.assertIn('total_arrays', stats)
        self.assertIn('avg_array_length', stats)
        self.assertIn('total_elements', stats)
        self.assertEqual(stats['total_arrays'], 2)
        self.assertEqual(stats['avg_array_length'], 3.0)
        self.assertEqual(stats['total_elements'], 6)
    
    @patch('main.execute_hive_command')
    def test_export_to_hdfs_success(self, mock_execute):
        """Test successful HDFS export."""
        mock_execute.return_value = (True, "Export completed", "")
        
        result = self.analyzer.export_to_hdfs('test_table', '/hdfs/path')
        
        self.assertTrue(result)
        mock_execute.assert_called()
    
    def test_format_results(self):
        """Test results formatting."""
        results = "1\t[1.0,2.0,3.0]\n2\t[4.0,5.0,6.0]"
        
        formatted = self.analyzer.format_results(results)
        
        self.assertIsInstance(formatted, list)
        self.assertEqual(len(formatted), 2)
        for item in formatted:
            self.assertIn('id', item)
            self.assertIn('processed_values', item)


if __name__ == '__main__':
    unittest.main()