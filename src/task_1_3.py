# Part 1: Spark RDD API
## Task 3

import sys

from pyspark import SparkContext, SparkConf

# spark-submit src/task_1_3.py data/groceries.csv out/out_1_3.txt

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: task_1_3 <inputfile> <outputfile>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("Task 1:3").setMaster("local")
    sc = SparkContext(conf=conf)

    linesRDD = sc.textFile(sys.argv[1])

    productsRDD = linesRDD.flatMap(lambda l: l.split(','))
    # map every purchased product to key-value pair, value 1
    productsRDD = productsRDD.map(lambda x: (x.strip(), 1))
    # sum values of each key, total number of purchases per product
    purchasesRDD = productsRDD.reduceByKey(lambda v1, v2: v1 + v2)
    # take 5 most purchased products
    top_purchases = purchasesRDD.takeOrdered(5, lambda x: -x[1])

    with open(sys.argv[2], 'w') as f:
        print('\n'.join([str(t) for t in top_purchases]), file=f)

    sc.stop()
