# Part 1: Spark RDD API
## Task 2

import sys

from pyspark import SparkContext, SparkConf

# spark-submit src/task_1_2.py data/groceries.csv out/out_1_2a.txt out/out_1_2b.txt

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: task1_2 <inputfile> <outputfile1> <outputfile2>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("Task 1:2").setMaster("local")
    sc = SparkContext(conf=conf)

    linesRDD = sc.textFile(sys.argv[1])

    productsRDD = linesRDD.flatMap(lambda l: l.split(','))
    # remove trailing spaces
    # RDD is cached as accessed twice
    productsRDD = productsRDD.map(lambda x: x.strip()).cache()

    uniqueRDD = productsRDD.distinct()
    # non-optimal, requirement to generate a single file
    # better to use saveAsTextFile, but only output directory can be set, not the filename
    with open(sys.argv[2], 'w') as f:
        print("\n".join(uniqueRDD.collect()), file=f)

    # return the total number of purchased products
    count = productsRDD.count()
    with open(sys.argv[3], 'w') as f:
        print(f"Count:\n{count}", file=f)

    sc.stop()
