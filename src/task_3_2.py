# Part 3: Applied Machine Learning
## Task 2

import sys

from pyspark.sql import SparkSession

from pyspark.ml import PipelineModel

# spark-submit src/task_3_2.py models/LRModel out/out_3_2.txt

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: task_3_2 <inputfile> <modeldir>", file=sys.stderr)
		sys.exit(-1)

	spark = SparkSession \
		.builder \
		.appName("Task 3:2") \
		.getOrCreate()

	# create dataframe for test
	pred_data = spark.createDataFrame([(5.1, 3.5, 1.4, 0.2), (6.2, 3.4, 5.4, 2.3)],
		["sepal_length", "sepal_width", "petal_length", "petal_width"])

	# load the trained model from memory
	# pipeline composed of feature assembler, logistic regression and prediction converter to string
	model = PipelineModel.load(sys.argv[1])

	# do and store predictions
	predictions = model.transform(pred_data).select("predictedClass").collect()
	
	with open(sys.argv[2], 'w') as f:
		print("class", file=f)
		print("\n".join([p[0] for p in predictions]), file=f)

	spark.stop()
