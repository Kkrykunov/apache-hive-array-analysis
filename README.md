# Спочатку перевіримо та створимо необхідні HDFS директорії

## Description

!/content/hive/bin/hive --service metastore & !/content/hive/bin/hive -e "CREATE DATABASE IF NOT EXISTS testdb; USE testdb; SHOW TABLES;" !/content/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse Перший запит показав наші масиви: Поелементне множення елементів масивів (кожен елемент array1 по...

## Project Structure

- `notebooks/`: Jupyter notebooks containing the analysis
- `src/`: Source code extracted from notebooks
- `data/`: Data files used in the project

## Setup and Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/Untitled5_clean_clean_clean.git
cd Untitled5_clean_clean_clean

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

You can run the Jupyter notebooks to see the full analysis:

```bash
jupyter notebook notebooks/
```

Or you can use the extracted Python modules:

```bash
python src/main.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
