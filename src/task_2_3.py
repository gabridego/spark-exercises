# Part 2: Spark Dataframe API
## Task 3

import sys
import csv

from pyspark.sql import SparkSession

# spark-submit src/task_2_3.py data/sf-airbnb-clean.parquet out/out_2_3.txt

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: task_2_3 <inputdir> <output>", file=sys.stderr)
		sys.exit(-1)

	spark = SparkSession \
		.builder \
		.appName("Task 2:3") \
		.getOrCreate()

	airbnbDF = spark.read.parquet(sys.argv[1])

	filteredDF = airbnbDF.filter("price>5000 AND review_scores_value=10")

	# filteredDF.show(10)

	# take average number of bathrooms and bedrooms
	avg_bathrooms = filteredDF.agg({"bathrooms": "avg"}).collect()[0][0]
	avg_bedrooms = filteredDF.agg({"bedrooms": "avg"}).collect()[0][0]

	# save as csv with custom header
	with open(sys.argv[2], 'w', newline='') as f:
		writer = csv.writer(f)

		writer.writerow(["avg_bathrooms", "avg_bedrooms"])
		writer.writerow([avg_bathrooms, avg_bedrooms])

	spark.stop()
