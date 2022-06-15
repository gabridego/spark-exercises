# Part 2: Spark Dataframe API
## Task 4

import sys
import csv

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import min, max, desc

# spark-submit src/task_2_4.py data/sf-airbnb-clean.parquet out/out_2_4.txt

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: task_2_4 <inputdir> <output>", file=sys.stderr)
		sys.exit(-1)

	spark = SparkSession \
		.builder \
		.appName("Task 2:4") \
		.getOrCreate()

	airbnbDF = spark.read.parquet(sys.argv[1])

	# add columns containing lowest price and highest review scores rating
	testDF = airbnbDF.withColumn("min_price", min('price').over(Window.orderBy("price"))) \
		.withColumn("max_rating", max('review_scores_rating').over(Window.orderBy(desc("review_scores_rating"))))

	# keep only the property with lowest price and highest rating
	testDF = testDF.filter("price=min_price AND review_scores_rating=max_rating")

	with open(sys.argv[2], 'w') as f:
		print(int(testDF.select("accommodates").collect()[0][0]), file=f)

	spark.stop()
