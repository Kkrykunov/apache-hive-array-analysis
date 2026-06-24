# Apache Hive Array Analysis

Element-wise array operations on large datasets using Apache Hive + HDFS. Academic project exploring distributed array manipulation at scale.

## Use Case

Computes element-wise multiplication and aggregation over columnar array data stored in Hive tables. Designed for datasets too large for single-node processing.

## Stack

- Apache Hive 3.x
- Hadoop / HDFS
- HQL (Hive Query Language)

## Setup

Requires a running Hive/HDFS cluster.

```bash
# Start Hive CLI
hive

# Run analysis
hive -f analysis.hql
```

## Queries Covered

- Element-wise array multiplication via `posexplode()`
- Aggregate functions over nested arrays
- HDFS-backed external table creation

## Output

Aggregated results written back to HDFS as delimited text.
