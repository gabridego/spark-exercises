# Data Engineering Coding Challenge

Gabriele Degola, June 2022

---

## Introduction

This project simulates concrete data engineering scenario, regarding generation of business intelligence reports and delivery of data insights, and applied machine learning.

## Dependencies

Solutions are developed using the Python language on top of [Apache Spark](https://spark.apache.org/), leveraging the RDD API, the DataFrame API and the MLlib library.
To download and install Spark, refer to the [official documentation](https://spark.apache.org/docs/latest/).

[Task 2.5](./src/task_2_5.py) is solved through [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start/index.html).

## Project organization

This git repo is organized as follows:

```
.
..
data/
src/
out/
README.md
```

- [`data/`](./data/) directory contains the datasets used in the different exercises.
- [`src/`](./src/) directory contains the source code files, named as `task_x_y.py` (solution of part `x`, task `y`). Solutions are described in the associated [`README` file](./src/README.md).
- [`out/`](./out/) directory contains output files, named following the same convention.

### Data

Three datasets are used in total, one for each part of the challenge:

- [`groceries.csv`](https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv): shopping transactions, in `csv` format
- [`sf-airbnb-clean.parquet`](https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet): small version of the AirBnB dataset, in `parquet` format
- [`iris.csv`](https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data): the classic iris dataset, in `csv` format

## Run the solutions

All solutions are designed to be run through the `spark-submit` command on a local Spark cluster with a single worker thread.

```
spark-submit task_x_y.py path/to/input/file.txt path/to/output/file.txt
```

Specific instructions are returned and contained in each Python script.
