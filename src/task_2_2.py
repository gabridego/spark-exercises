# Part 2: Spark Dataframe API
## Task 2

import sys
import csv

from pyspark.sql import SparkSession

# spark-submit src/task_2_2.py data/sf-airbnb-clean.parquet out/out_2_2.txt

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: task_2_2 <inputdir> <output>", file=sys.stderr)
		sys.exit(-1)

	spark = SparkSession \
		.builder \
		.appName("Task 2:2") \
		.getOrCreate()

	airbnbDF = spark.read.parquet(sys.argv[1])

	# take minimum price, maximum price, number of rows
	min_price = airbnbDF.agg({"price": "min"}).collect()[0][0]
	max_price = airbnbDF.agg({"price": "max"}).collect()[0][0]
	row_count = airbnbDF.count()

	# save as csv with custom header
	with open(sys.argv[2], 'w', newline='') as f:
		writer = csv.writer(f)

		writer.writerow(["min_price", "max_price", "row_count"])
		writer.writerow([min_price, max_price, row_count])

	spark.stop()
