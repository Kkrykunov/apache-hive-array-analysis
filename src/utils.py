#!/usr/bin/env python3
"""
Utility Functions for Hive Array Analysis
========================================

Supporting functions for Apache Hive and Hadoop array analysis including
HQL script generation, data validation, and result processing utilities.

Author: Big Data Engineering Portfolio
License: MIT
"""

import os
import subprocess
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def validate_hadoop_environment() -> bool:
    """
    Validate that Hadoop environment is properly configured.
    
    Returns:
        bool: True if environment is valid, False otherwise
    """
    required_vars = ['JAVA_HOME', 'HADOOP_HOME', 'HIVE_HOME']
    
    for var in required_vars:
        if not os.environ.get(var):
            logger.error(f"Required environment variable {var} not set")
            return False
    
    # Check if Hadoop binaries exist
    hadoop_bin = Path(os.environ['HADOOP_HOME']) / 'bin' / 'hadoop'
    hive_bin = Path(os.environ['HIVE_HOME']) / 'bin' / 'hive'
    
    if not hadoop_bin.exists():
        logger.error(f"Hadoop binary not found at: {hadoop_bin}")
        return False
        
    if not hive_bin.exists():
        logger.error(f"Hive binary not found at: {hive_bin}")
        return False
    
    logger.info("Hadoop environment validation successful")
    return True


def check_hdfs_status() -> bool:
    """
    Check if HDFS is running and accessible.
    
    Returns:
        bool: True if HDFS is accessible, False otherwise
    """
    try:
        hadoop_bin = Path(os.environ['HADOOP_HOME']) / 'bin' / 'hdfs'
        result = subprocess.run([str(hadoop_bin), 'dfsadmin', '-report'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("HDFS is running and accessible")
            return True
        else:
            logger.error("HDFS is not accessible")
            return False
            
    except Exception as e:
        logger.error(f"Error checking HDFS status: {str(e)}")
        return False


def create_hql_script(script_name: str, content: str, output_dir: str = "output") -> str:
    """
    Create HQL script file with given content.
    
    Args:
        script_name: Name of the script file
        content: HQL script content
        output_dir: Output directory path
        
    Returns:
        str: Path to created script file
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    script_path = output_path / f"{script_name}.hql"
    
    with open(script_path, 'w') as f:
        f.write(content)
    
    logger.info(f"Created HQL script: {script_path}")
    return str(script_path)


def execute_hive_command(command: str, timeout: int = 300) -> Tuple[bool, str, str]:
    """
    Execute Hive command and return results.
    
    Args:
        command: Hive command to execute
        timeout: Command timeout in seconds
        
    Returns:
        Tuple of (success, stdout, stderr)
    """
    try:
        hive_bin = Path(os.environ['HIVE_HOME']) / 'bin' / 'hive'
        cmd = [str(hive_bin), '-e', command]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        
        success = result.returncode == 0
        return success, result.stdout, result.stderr
        
    except subprocess.TimeoutExpired:
        logger.error(f"Hive command timed out after {timeout} seconds")
        return False, "", "Command timeout"
    except Exception as e:
        logger.error(f"Error executing Hive command: {str(e)}")
        return False, "", str(e)


def parse_hive_output(output: str) -> List[Dict]:
    """
    Parse Hive query output into structured data.
    
    Args:
        output: Raw Hive query output
        
    Returns:
        List of dictionaries representing rows
    """
    lines = output.strip().split('\n')
    
    # Filter out Hive system messages and warnings
    data_lines = []
    for line in lines:
        if (not line.startswith('SLF4J:') and 
            not line.startswith('Hive Session ID') and
            not line.startswith('Time taken:') and
            not line.startswith('OK') and
            line.strip()):
            data_lines.append(line.strip())
    
    # Parse tabular data
    if not data_lines:
        return []
    
    # First line might be headers, data starts from second line
    parsed_data = []
    for line in data_lines:
        if '\t' in line:
            values = line.split('\t')
            parsed_data.append({'values': values})
    
    return parsed_data


def validate_array_data(data: List[Union[int, float]]) -> bool:
    """
    Validate array data for Hive processing.
    
    Args:
        data: Array data to validate
        
    Returns:
        bool: True if data is valid, False otherwise
    """
    if not data:
        logger.warning("Empty array data")
        return False
    
    # Check for None values
    if any(x is None for x in data):
        logger.warning("Array contains None values")
        return False
    
    # Check for infinite or NaN values
    try:
        numeric_data = np.array(data, dtype=float)
        if np.any(np.isinf(numeric_data)) or np.any(np.isnan(numeric_data)):
            logger.warning("Array contains infinite or NaN values")
            return False
    except (ValueError, TypeError):
        logger.warning("Array contains non-numeric values")
        return False
    
    return True


def format_array_for_hive(data: List[Union[int, float]]) -> str:
    """
    Format array data for Hive ARRAY() function.
    
    Args:
        data: Array data to format
        
    Returns:
        str: Formatted array string for Hive
    """
    if not validate_array_data(data):
        raise ValueError("Invalid array data")
    
    # Convert to strings with proper casting
    formatted_values = []
    for value in data:
        if isinstance(value, int):
            formatted_values.append(f"CAST({value} AS DOUBLE)")
        else:
            formatted_values.append(f"CAST({float(value)} AS DOUBLE)")
    
    return f"ARRAY({', '.join(formatted_values)})"


def generate_test_data(num_arrays: int = 3, array_size: int = 10, 
                      data_type: str = 'sequential') -> Dict[str, List]:
    """
    Generate test data for array analysis.
    
    Args:
        num_arrays: Number of arrays to generate
        array_size: Size of each array
        data_type: Type of data ('sequential', 'random', 'mathematical')
        
    Returns:
        Dictionary mapping array names to data
    """
    arrays = {}
    
    for i in range(num_arrays):
        array_name = f"array_{i+1}"
        
        if data_type == 'sequential':
            # Simple sequential data
            arrays[array_name] = list(range(1, array_size + 1))
            
        elif data_type == 'random':
            # Random data with normal distribution
            np.random.seed(42 + i)  # For reproducibility
            arrays[array_name] = np.random.normal(50, 15, array_size).tolist()
            
        elif data_type == 'mathematical':
            # Mathematical sequences
            if i == 0:
                # Linear sequence
                arrays[array_name] = np.linspace(1, array_size, array_size).tolist()
            elif i == 1:
                # Exponential sequence
                arrays[array_name] = [2**j for j in range(array_size)]
            else:
                # Polynomial sequence
                arrays[array_name] = [j**2 for j in range(1, array_size + 1)]
    
    logger.info(f"Generated {num_arrays} test arrays of size {array_size}")
    return arrays


def create_hive_table_ddl(table_name: str, columns: Dict[str, str], 
                         storage_format: str = 'TEXTFILE') -> str:
    """
    Generate Hive DDL for creating table.
    
    Args:
        table_name: Name of the table
        columns: Dictionary mapping column names to data types
        storage_format: Storage format (TEXTFILE, PARQUET, etc.)
        
    Returns:
        str: DDL statement
    """
    column_definitions = []
    for col_name, col_type in columns.items():
        column_definitions.append(f"    {col_name} {col_type}")
    
    ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{',\n'.join(column_definitions)}
)
STORED AS {storage_format};"""
    
    return ddl


def export_hdfs_to_local(hdfs_path: str, local_path: str) -> bool:
    """
    Export data from HDFS to local filesystem.
    
    Args:
        hdfs_path: HDFS path to export from
        local_path: Local path to export to
        
    Returns:
        bool: Success status
    """
    try:
        hadoop_bin = Path(os.environ['HADOOP_HOME']) / 'bin' / 'hdfs'
        cmd = [str(hadoop_bin), 'dfs', '-get', hdfs_path, local_path]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Successfully exported {hdfs_path} to {local_path}")
            return True
        else:
            logger.error(f"Failed to export HDFS data: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error exporting HDFS data: {str(e)}")
        return False


def upload_local_to_hdfs(local_path: str, hdfs_path: str) -> bool:
    """
    Upload local file to HDFS.
    
    Args:
        local_path: Local file path
        hdfs_path: HDFS destination path
        
    Returns:
        bool: Success status
    """
    try:
        hadoop_bin = Path(os.environ['HADOOP_HOME']) / 'bin' / 'hdfs'
        
        # Create directory if it doesn't exist
        hdfs_dir = str(Path(hdfs_path).parent)
        mkdir_cmd = [str(hadoop_bin), 'dfs', '-mkdir', '-p', hdfs_dir]
        subprocess.run(mkdir_cmd, capture_output=True)
        
        # Upload file
        put_cmd = [str(hadoop_bin), 'dfs', '-put', local_path, hdfs_path]
        result = subprocess.run(put_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Successfully uploaded {local_path} to {hdfs_path}")
            return True
        else:
            logger.error(f"Failed to upload to HDFS: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error uploading to HDFS: {str(e)}")
        return False


def create_performance_report(query_results: List[Dict], 
                             execution_times: List[float]) -> str:
    """
    Create performance analysis report.
    
    Args:
        query_results: List of query results
        execution_times: List of execution times in seconds
        
    Returns:
        str: Performance report
    """
    if not execution_times:
        return "No performance data available"
    
    total_time = sum(execution_times)
    avg_time = np.mean(execution_times)
    max_time = max(execution_times)
    min_time = min(execution_times)
    
    report = f"""
Performance Analysis Report
==========================

Execution Statistics:
- Total Queries: {len(execution_times)}
- Total Execution Time: {total_time:.2f} seconds
- Average Query Time: {avg_time:.2f} seconds
- Maximum Query Time: {max_time:.2f} seconds  
- Minimum Query Time: {min_time:.2f} seconds

Query Performance:
"""
    
    for i, time_taken in enumerate(execution_times):
        report += f"- Query {i+1}: {time_taken:.2f}s\n"
    
    if query_results:
        report += f"\nData Processing:\n"
        report += f"- Total Records Processed: {len(query_results)}\n"
        report += f"- Records per Second: {len(query_results)/total_time:.2f}\n"
    
    return report


def validate_hive_result(result: str, expected_columns: int = None) -> bool:
    """
    Validate Hive query result format.
    
    Args:
        result: Raw Hive result string
        expected_columns: Expected number of columns
        
    Returns:
        bool: True if result format is valid
    """
    if not result or not result.strip():
        logger.warning("Empty result")
        return False
    
    lines = result.strip().split('\n')
    data_lines = [line for line in lines if '\t' in line and not line.startswith('SLF4J:')]
    
    if not data_lines:
        logger.warning("No data lines found in result")
        return False
    
    if expected_columns:
        for line in data_lines:
            columns = line.split('\t')
            if len(columns) != expected_columns:
                logger.warning(f"Expected {expected_columns} columns, got {len(columns)}")
                return False
    
    return True


def get_project_info() -> Dict[str, str]:
    """Get project information."""
    return {
        'name': 'Apache Hive Array Analysis System',
        'version': '1.0.0',
        'description': 'Big Data array processing using Hive and Hadoop ecosystem',
        'technologies': 'Apache Hive, Hadoop HDFS, HQL, Python',
        'author': 'Big Data Engineering Portfolio'
    }


def setup_logging(log_level: str = 'INFO') -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('hive_analysis.log'),
            logging.StreamHandler()
        ]
    )
    
    # Reduce verbosity of some external libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)