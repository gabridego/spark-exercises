# Part 2: Spark Dataframe API
## Task 5

import datetime

import pendulum

from airflow import DAG
# DummyOperator is deprecated, use EmptyOperator instead
from airflow.operators.empty import EmptyOperator

with DAG(
	dag_id='task_2_5',
	schedule_interval=None,
	start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
	catchup=False,
	tags=['dummy'],
) as dag:
	# instantiate the six dummy tasks
	task1 = EmptyOperator(task_id='task1')
	task2 = EmptyOperator(task_id='task2')
	task3 = EmptyOperator(task_id='task3')
	task4 = EmptyOperator(task_id='task4')
	task5 = EmptyOperator(task_id='task5')
	task6 = EmptyOperator(task_id='task6')

	# task1 followed by task2 and task3 in parallel
	task1 >> [task2, task3]
	# task2 and task3 followed by task4, task5 and task6 in parallel
	[task2, task3] >> [task4, task5, task6]
