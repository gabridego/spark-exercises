# Part 3: Applied Machine Learning
## Task 1

import sys

from pyspark.sql import SparkSession

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml import Pipeline

# spark-submit src/task_3_1.py data/iris.csv models/LRModel

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: task_3_1 <inputfile> <modeldir>", file=sys.stderr)
		sys.exit(-1)

	spark = SparkSession \
		.builder \
		.appName("Task 3:1") \
		.getOrCreate()

	# load csv from memory, inferring datatypes and defining header
	df = spark.read.csv(sys.argv[1], inferSchema=True).toDF("sepal_length", "sepal_width", "petal_length", "petal_width", "class")
	df.show(5)

	"""
	+------------+-----------+------------+-----------+-----------+
	|sepal_length|sepal_width|petal_length|petal_width|      class|
	+------------+-----------+------------+-----------+-----------+
	|         5.1|        3.5|         1.4|        0.2|Iris-setosa|
	|         4.9|        3.0|         1.4|        0.2|Iris-setosa|
	|         4.7|        3.2|         1.3|        0.2|Iris-setosa|
	|         4.6|        3.1|         1.5|        0.2|Iris-setosa|
	|         5.0|        3.6|         1.4|        0.2|Iris-setosa|
	+------------+-----------+------------+-----------+-----------+
	"""

	# convert classes to numerical value
	indexer = StringIndexer(inputCol="class", outputCol="label").fit(df)
	df = indexer.transform(df)

	# put together predictive features as required by MLlib
	assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features")
	# define logistic regression model (C=1e5)
	lr = LogisticRegression(regParam=1/1e5)
	# revert numerical prediction to class
	converter = IndexToString(inputCol="prediction", outputCol="predictedClass", labels=indexer.labels)

	# define pipeline for assembler, logistic regression and prediction converter
	pipeline = Pipeline().setStages([assembler, lr, converter])

	# fit the model on training data
	model = pipeline.fit(df)


	# create dataframe for test
	test_df = spark.createDataFrame([(5.1, 3.5, 1.4, 0.2, "Iris-setosa"), (6.2, 3.4, 5.4, 2.3, "Iris-virginica")]) \
		.toDF("sepal_length", "sepal_width", "petal_length", "petal_width", "class")
	test_df.show()

	"""
	+------------+-----------+------------+-----------+--------------+
	|sepal_length|sepal_width|petal_length|petal_width|         class|
	+------------+-----------+------------+-----------+--------------+
	|         5.1|        3.5|         1.4|        0.2|   Iris-setosa|
	|         6.2|        3.4|         5.4|        2.3|Iris-virginica|
	+------------+-----------+------------+-----------+--------------+
	"""

	# predict on test data
	preds = model.transform(test_df)
	preds.select("predictedClass").show()

	"""
	+--------------+
	|predictedClass|
	+--------------+
	|   Iris-setosa|
	|Iris-virginica|
	+--------------+
	"""

	# save the pipeline for future use
	model.write().overwrite().save(sys.argv[2])

	spark.stop()
