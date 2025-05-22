#!/usr/bin/env python3
"""
Apache Hive Array Analysis System
================================

A comprehensive big data analysis system demonstrating advanced array operations
using Apache Hive and Hadoop ecosystem for large-scale data processing.

Features:
- Apache Hive SQL queries for array manipulation
- Hadoop HDFS distributed storage
- Complex mathematical operations on array data
- Multi-dimensional array analysis with statistical functions
- Production-ready HQL script generation

Author: Big Data Engineering Portfolio
License: MIT
"""

import logging
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ArrayOperation:
    """Data structure for array operation results."""
    position: int
    array1_value: float
    array2_value: float
    array3_value: float
    operation_result: float


@dataclass
class HiveConfiguration:
    """Hive environment configuration."""
    java_home: str
    hadoop_home: str
    hive_home: str
    hdfs_namenode_url: str
    warehouse_dir: str


class HiveArrayAnalyzer:
    """
    Advanced array data analyzer using Apache Hive and Hadoop.
    
    Provides comprehensive array operations including element-wise calculations,
    statistical aggregations, and complex mathematical transformations using
    distributed computing frameworks.
    """
    
    def __init__(self, output_dir: str = "output"):
        """Initialize the Hive array analyzer."""
        self.output_path = Path(output_dir)
        self.output_path.mkdir(exist_ok=True)
        
        # Hive configuration
        self.config = HiveConfiguration(
            java_home="/usr/lib/jvm/java-8-openjdk-amd64",
            hadoop_home="/content/hadoop",
            hive_home="/content/hive", 
            hdfs_namenode_url="hdfs://localhost:9000",
            warehouse_dir="/user/hive/warehouse"
        )
        
        # Set environment variables
        self._setup_environment()
        
        logger.info("Hive Array Analyzer initialized")
    
    def _setup_environment(self) -> None:
        """Setup Hadoop and Hive environment variables."""
        env_vars = {
            "JAVA_HOME": self.config.java_home,
            "HADOOP_HOME": self.config.hadoop_home,
            "HIVE_HOME": self.config.hive_home,
            "PATH": f"{self.config.hive_home}/bin:{self.config.hadoop_home}/bin:{os.environ.get('PATH', '')}"
        }
        
        for key, value in env_vars.items():
            os.environ[key] = value
        
        logger.info("Environment variables configured")
    
    def execute_hql_script(self, script_path: str) -> bool:
        """
        Execute HQL script using Hive CLI.
        
        Args:
            script_path: Path to HQL script file
            
        Returns:
            bool: Success status
        """
        try:
            cmd = [f"{self.config.hive_home}/bin/hive", "-f", script_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Successfully executed HQL script: {script_path}")
                return True
            else:
                logger.error(f"HQL script failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error executing HQL script: {str(e)}")
            return False
    
    def create_array_tables_script(self) -> str:
        """
        Generate HQL script for creating array tables.
        
        Returns:
            str: Path to generated script
        """
        script_content = """
-- Create database for array analysis
CREATE DATABASE IF NOT EXISTS array_analysis;
USE array_analysis;

-- Table for storing test arrays
CREATE TABLE IF NOT EXISTS test_arrays (
    array1 ARRAY<INT>,
    array2 ARRAY<INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Insert sample data for basic operations
INSERT INTO TABLE test_arrays
SELECT
    array(1,2,3,4,5),
    array(6,7,8,9,10);

-- Table for advanced array operations with double precision
CREATE TABLE IF NOT EXISTS large_arrays (
    linear_array ARRAY<DOUBLE>,
    exponential_array ARRAY<DOUBLE>,
    polynomial_array ARRAY<DOUBLE>
)
STORED AS TEXTFILE;

-- Insert complex mathematical sequences
INSERT INTO TABLE large_arrays
SELECT
    ARRAY(
        CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE), CAST(3.0 AS DOUBLE), 
        CAST(4.0 AS DOUBLE), CAST(5.0 AS DOUBLE), CAST(6.0 AS DOUBLE),
        CAST(7.0 AS DOUBLE), CAST(8.0 AS DOUBLE), CAST(9.0 AS DOUBLE), 
        CAST(10.0 AS DOUBLE)
    ),
    ARRAY(
        CAST(2.0 AS DOUBLE), CAST(4.0 AS DOUBLE), CAST(8.0 AS DOUBLE),
        CAST(16.0 AS DOUBLE), CAST(32.0 AS DOUBLE), CAST(64.0 AS DOUBLE),
        CAST(128.0 AS DOUBLE), CAST(256.0 AS DOUBLE), CAST(512.0 AS DOUBLE),
        CAST(1024.0 AS DOUBLE)
    ),
    ARRAY(
        CAST(1.0 AS DOUBLE), CAST(4.0 AS DOUBLE), CAST(9.0 AS DOUBLE),
        CAST(16.0 AS DOUBLE), CAST(25.0 AS DOUBLE), CAST(36.0 AS DOUBLE),
        CAST(49.0 AS DOUBLE), CAST(64.0 AS DOUBLE), CAST(81.0 AS DOUBLE),
        CAST(100.0 AS DOUBLE)
    );

-- Table for storing array operation results
CREATE TABLE IF NOT EXISTS array_results (
    position INT,
    linear_value DOUBLE,
    exponential_value DOUBLE,
    polynomial_value DOUBLE,
    log_exp_value DOUBLE,
    sqrt_poly_value DOUBLE,
    combined_result DOUBLE
)
STORED AS TEXTFILE;
"""
        
        script_path = self.output_path / "create_tables.hql"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        logger.info(f"Created table creation script: {script_path}")
        return str(script_path)
    
    def create_array_operations_script(self) -> str:
        """
        Generate HQL script for complex array operations.
        
        Returns:
            str: Path to generated script
        """
        script_content = """
USE array_analysis;

-- Basic array element access and arithmetic
SELECT 
    'Basic Array Operations' as operation_type,
    array1[0] as first_elem_array1,
    array2[0] as first_elem_array2,
    size(array1) as array1_size,
    array1[1] + array2[1] as sum_second_elements
FROM test_arrays;

-- Element-wise multiplication using LATERAL VIEW
SELECT
    'Element-wise Operations' as operation_type,
    pe1.pos as position,
    pe1.val as array1_element,
    pe2.val as array2_element,
    pe1.val * pe2.val as multiplication
FROM test_arrays
LATERAL VIEW posexplode(array1) pe1 AS pos, val
LATERAL VIEW posexplode(array2) pe2 AS pos2, val2
WHERE pe1.pos = pe2.pos2;

-- Advanced mathematical operations on large arrays
INSERT INTO array_results
SELECT
    pe1.pos AS position,
    pe1.linear AS linear_value,
    pe2.exp AS exponential_value,
    pe3.poly AS polynomial_value,
    ln(pe2.exp) AS log_exp_value,
    sqrt(pe3.poly) AS sqrt_poly_value,
    -- Complex formula: log(exp) * sqrt(poly) + linear^2
    ln(pe2.exp) * sqrt(pe3.poly) + pow(pe1.linear, 2) AS combined_result
FROM large_arrays
LATERAL VIEW posexplode(linear_array) pe1 AS pos, linear
LATERAL VIEW posexplode(exponential_array) pe2 AS pos2, exp
LATERAL VIEW posexplode(polynomial_array) pe3 AS pos3, poly
WHERE pe1.pos = pe2.pos2 AND pe1.pos = pe3.pos3;

-- Statistical aggregations
SELECT
    'Statistical Summary' as analysis_type,
    avg(linear_value) as avg_linear,
    avg(exponential_value) as avg_exponential,
    avg(polynomial_value) as avg_polynomial,
    stddev(combined_result) as stddev_combined,
    min(combined_result) as min_result,
    max(combined_result) as max_result,
    percentile_approx(combined_result, 0.5) as median_result
FROM array_results;
"""
        
        script_path = self.output_path / "array_operations.hql"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        logger.info(f"Created array operations script: {script_path}")
        return str(script_path)
    
    def create_export_script(self) -> str:
        """
        Generate HQL script for exporting results to HDFS.
        
        Returns:
            str: Path to generated script
        """
        script_content = """
USE array_analysis;

-- Export array operation results to HDFS
INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/array_analysis_results'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\\n'
SELECT
    position,
    linear_value,
    exponential_value, 
    polynomial_value,
    log_exp_value,
    sqrt_poly_value,
    combined_result
FROM array_results
ORDER BY position;

-- Export statistical summary
INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/array_statistics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\\n'
SELECT
    'linear_stats' as metric_type,
    avg(linear_value) as average,
    stddev(linear_value) as std_deviation,
    min(linear_value) as minimum,
    max(linear_value) as maximum
FROM array_results
UNION ALL
SELECT
    'exponential_stats' as metric_type,
    avg(exponential_value) as average,
    stddev(exponential_value) as std_deviation,
    min(exponential_value) as minimum,
    max(exponential_value) as maximum
FROM array_results
UNION ALL
SELECT
    'polynomial_stats' as metric_type,
    avg(polynomial_value) as average,
    stddev(polynomial_value) as std_deviation,
    min(polynomial_value) as minimum,
    max(polynomial_value) as maximum
FROM array_results;
"""
        
        script_path = self.output_path / "export_results.hql"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        logger.info(f"Created export script: {script_path}")
        return str(script_path)
    
    def generate_sample_data(self, size: int = 100) -> str:
        """
        Generate sample CSV data for analysis.
        
        Args:
            size: Number of data points to generate
            
        Returns:
            str: Path to generated CSV file
        """
        try:
            # Generate timeline data
            time_range = pd.date_range(start='2024-01-01', periods=size, freq='D')
            
            # Generate arrays with different mathematical patterns
            linear_data = np.linspace(1, size, size)
            exponential_data = np.exp(np.linspace(0, 5, size))
            polynomial_data = np.power(np.linspace(1, size, size), 2)
            random_data = np.random.normal(50, 15, size)
            
            # Create DataFrame
            df = pd.DataFrame({
                'timestamp': time_range,
                'linear_sequence': linear_data,
                'exponential_sequence': exponential_data,
                'polynomial_sequence': polynomial_data,
                'random_values': random_data
            })
            
            # Save to CSV
            csv_path = self.output_path / "sample_array_data.csv"
            df.to_csv(csv_path, index=False)
            
            logger.info(f"Generated sample data: {csv_path}")
            return str(csv_path)
            
        except Exception as e:
            logger.error(f"Error generating sample data: {str(e)}")
            return ""
    
    def run_complete_analysis(self) -> Dict[str, Any]:
        """
        Execute complete Hive array analysis pipeline.
        
        Returns:
            Dictionary containing analysis results and file paths
        """
        logger.info("Starting complete Hive array analysis")
        
        try:
            results = {
                'status': 'success',
                'scripts_generated': [],
                'data_files': [],
                'analysis_steps': []
            }
            
            # Generate sample data
            sample_data_path = self.generate_sample_data()
            if sample_data_path:
                results['data_files'].append(sample_data_path)
                results['analysis_steps'].append("Sample data generation completed")
            
            # Create HQL scripts
            table_script = self.create_array_tables_script()
            operations_script = self.create_array_operations_script()
            export_script = self.create_export_script()
            
            results['scripts_generated'].extend([
                table_script, operations_script, export_script
            ])
            
            results['analysis_steps'].extend([
                "Table creation script generated",
                "Array operations script generated", 
                "Export script generated"
            ])
            
            # Create execution instructions
            instructions = self._create_execution_instructions()
            instructions_path = self.output_path / "execution_instructions.md"
            with open(instructions_path, 'w') as f:
                f.write(instructions)
            
            results['data_files'].append(str(instructions_path))
            results['analysis_steps'].append("Execution instructions created")
            
            logger.info("Complete Hive array analysis pipeline generated")
            return results
            
        except Exception as e:
            logger.error(f"Analysis pipeline failed: {str(e)}")
            return {'status': 'failed', 'error': str(e)}
    
    def _create_execution_instructions(self) -> str:
        """Create detailed execution instructions."""
        return """# Hive Array Analysis Execution Guide

## Prerequisites
1. Java 8 installed and JAVA_HOME configured
2. Apache Hadoop 3.3.6+ downloaded and configured
3. Apache Hive 3.1.3+ downloaded and configured
4. HDFS namenode and datanode running

## Setup Steps

### 1. Environment Configuration
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/path/to/hadoop
export HIVE_HOME=/path/to/hive
export PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH
```

### 2. Start Hadoop Services
```bash
# Format namenode (first time only)
$HADOOP_HOME/bin/hdfs namenode -format

# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# Create Hive warehouse directory
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /user/hive/warehouse
```

### 3. Initialize Hive Metastore
```bash
$HIVE_HOME/bin/schematool -dbType derby -initSchema
```

## Execution Sequence

### Step 1: Create Tables and Load Data
```bash
hive -f create_tables.hql
```

### Step 2: Execute Array Operations
```bash
hive -f array_operations.hql
```

### Step 3: Export Results
```bash
hive -f export_results.hql
```

### Step 4: Retrieve Results from HDFS
```bash
# Copy results to local filesystem
hdfs dfs -get /user/hive/warehouse/array_analysis_results ./results/
hdfs dfs -get /user/hive/warehouse/array_statistics ./statistics/
```

## Expected Outputs

1. **Array Operation Results**: Element-wise calculations and transformations
2. **Statistical Summary**: Aggregated metrics across array dimensions
3. **Performance Metrics**: Query execution times and resource usage

## Troubleshooting

### Common Issues:
- **HDFS Connection**: Ensure namenode is running on port 9000
- **Hive Metastore**: Initialize Derby database if not done
- **Java Version**: Use Java 8 for compatibility
- **Memory Settings**: Adjust heap size for large datasets

### Log Locations:
- Hadoop logs: `$HADOOP_HOME/logs/`
- Hive logs: `$HIVE_HOME/logs/`
- Application logs: `./output/`
"""


def main():
    """Main execution function for Hive array analysis."""
    print("=" * 80)
    print("APACHE HIVE ARRAY ANALYSIS SYSTEM")
    print("=" * 80)
    print("Big Data array processing using Hive and Hadoop ecosystem")
    print()
    
    try:
        # Initialize analyzer
        analyzer = HiveArrayAnalyzer()
        
        # Run complete analysis
        results = analyzer.run_complete_analysis()
        
        if results.get('status') == 'success':
            print("✅ Hive Array Analysis Pipeline Generated Successfully!")
            print(f"📜 HQL scripts created: {len(results['scripts_generated'])}")
            print(f"📊 Data files generated: {len(results['data_files'])}")
            print()
            
            print("📋 Analysis Steps Completed:")
            for step in results['analysis_steps']:
                print(f"   • {step}")
            
            print()
            print("🗂️ Generated Files:")
            all_files = results['scripts_generated'] + results['data_files']
            for file_path in all_files:
                print(f"   • {file_path}")
            
            print()
            print("🚀 Next Steps:")
            print("   1. Set up Hadoop and Hive environment")
            print("   2. Start HDFS and initialize Hive metastore")
            print("   3. Execute HQL scripts in sequence")
            print("   4. Analyze results from HDFS export")
            
        else:
            print(f"❌ Analysis pipeline failed: {results.get('error', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        print(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    main()