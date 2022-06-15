# Part 1: Spark RDD API
## Task 1

from pyspark import SparkContext, SparkConf, SparkFiles

# spark-submit src/task_1_1.py

if __name__ == "__main__":
    conf = SparkConf().setAppName("Task 1:1").setMaster("local")
    sc = SparkContext(conf=conf)

    # get csv file from url
    url = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"
    sc.addFile(url)
    linesRDD = sc.textFile(SparkFiles.get("groceries.csv"))

    # get all products in every line
    itemsRDD = linesRDD.map(lambda l: l.split(','))

    # take first five lines
    head = itemsRDD.take(5)
    print(head)

    sc.stop()
