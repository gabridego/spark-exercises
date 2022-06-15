# Data Engineering Coding Challenge

Gabriele Degola, June 2022

---

## Part 1: Spark RDD API
### Task 1

For this task, the input file `groceries.csv` is retrieved from the [remote url](https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv)
and loaded in Spark using the `SparkFile` component. However, the file is not persistently stored and is only accessible by the current `SparkContext`.

The products in each transaction are then isolated. The first five transactions look like this:

```
[['citrus fruit', 'semi-finished bread', 'margarine', 'ready soups'],
 ['tropical fruit', 'yogurt', 'coffee'],
 ['whole milk'],
 ['pip fruit', 'yogurt', 'cream cheese ', 'meat spreads'],
 ['other vegetables', 'whole milk', 'condensed milk', 'long life bakery product']]
```

### Task 2

From now on, input files are loaded from memory.

Products in each transaction are separated, removing possible tralining spaces, and transactions are collapsed in a single list. The RDD now contains the list of all purchased products
and is cached as is accessed multiple times. The list of all unique purchased products and the total number of purchased products are stored in two text files.

The most efficient way to write to file would be the `saveAsTextFile` action, but it generates multiple files (with logs) in a directory of given name.
As we want single text files of given name, insights of interest are locally collected in memory and stored using standard Python methods.

### Task 3

Every purchased product is mapped to a key-value pair (with the name as key and 1 as value), and all pairs are reduced summing the values for each key, obtaining the number of purchases for
each product. Five most purchased products are then retrieved and stored.


## Part 2: Spark Dataframe API
### Task 1

The parquet input file is loaded from memory and the first five entries are shown:

```
+-----------------+--------------------+----------------+-------------------------+----------------------+--------+----------+-------------+---------------+------------+---------+--------+----+--------+--------------+-----------------+--------------------+----------------------+-------------------------+---------------------+---------------------------+----------------------+-------------------+-----+-----------+------------+-------+-----------------------+-------------------------+----------------------------+------------------------+------------------------------+-------------------------+----------------------+
|host_is_superhost| cancellation_policy|instant_bookable|host_total_listings_count|neighbourhood_cleansed|latitude| longitude|property_type|      room_type|accommodates|bathrooms|bedrooms|beds|bed_type|minimum_nights|number_of_reviews|review_scores_rating|review_scores_accuracy|review_scores_cleanliness|review_scores_checkin|review_scores_communication|review_scores_location|review_scores_value|price|bedrooms_na|bathrooms_na|beds_na|review_scores_rating_na|review_scores_accuracy_na|review_scores_cleanliness_na|review_scores_checkin_na|review_scores_communication_na|review_scores_location_na|review_scores_value_na|
+-----------------+--------------------+----------------+-------------------------+----------------------+--------+----------+-------------+---------------+------------+---------+--------+----+--------+--------------+-----------------+--------------------+----------------------+-------------------------+---------------------+---------------------------+----------------------+-------------------+-----+-----------+------------+-------+-----------------------+-------------------------+----------------------------+------------------------+------------------------------+-------------------------+----------------------+
|                t|            moderate|               t|                      1.0|      Western Addition|37.76931|-122.43386|    Apartment|Entire home/apt|         3.0|      1.0|     1.0| 2.0|Real Bed|           1.0|            180.0|                97.0|                  10.0|                     10.0|                 10.0|                       10.0|                  10.0|               10.0|170.0|        0.0|         0.0|    0.0|                    0.0|                      0.0|                         0.0|                     0.0|                           0.0|                      0.0|                   0.0|
|                f|strict_14_with_gr...|               f|                      2.0|        Bernal Heights|37.74511|-122.42102|    Apartment|Entire home/apt|         5.0|      1.0|     2.0| 3.0|Real Bed|          30.0|            111.0|                98.0|                  10.0|                     10.0|                 10.0|                       10.0|                  10.0|                9.0|235.0|        0.0|         0.0|    0.0|                    0.0|                      0.0|                         0.0|                     0.0|                           0.0|                      0.0|                   0.0|
|                f|strict_14_with_gr...|               f|                     10.0|        Haight Ashbury|37.76669| -122.4525|    Apartment|   Private room|         2.0|      4.0|     1.0| 1.0|Real Bed|          32.0|             17.0|                85.0|                   8.0|                      8.0|                  9.0|                        9.0|                   9.0|                8.0| 65.0|        0.0|         0.0|    0.0|                    0.0|                      0.0|                         0.0|                     0.0|                           0.0|                      0.0|                   0.0|
|                f|strict_14_with_gr...|               f|                     10.0|        Haight Ashbury|37.76487|-122.45183|    Apartment|   Private room|         2.0|      4.0|     1.0| 1.0|Real Bed|          32.0|              8.0|                93.0|                   9.0|                      9.0|                 10.0|                       10.0|                   9.0|                9.0| 65.0|        0.0|         0.0|    0.0|                    0.0|                      0.0|                         0.0|                     0.0|                           0.0|                      0.0|                   0.0|
|                f|strict_14_with_gr...|               f|                      2.0|      Western Addition|37.77525|-122.43637|        House|Entire home/apt|         5.0|      1.5|     2.0| 2.0|Real Bed|           7.0|             27.0|                97.0|                  10.0|                     10.0|                 10.0|                       10.0|                  10.0|                9.0|785.0|        0.0|         0.0|    0.0|                    0.0|                      0.0|                         0.0|                     0.0|                           0.0|                      0.0|                   0.0|
+-----------------+--------------------+----------------+-------------------------+----------------------+--------+----------+-------------+---------------+------------+---------+--------+----+--------+--------------+-----------------+--------------------+----------------------+-------------------------+---------------------+---------------------------+----------------------+-------------------+-----+-----------+------------+-------+-----------------------+-------------------------+----------------------------+------------------------+------------------------------+-------------------------+----------------------+
```

### Task 2

Minimum and maximum price over all apartments are retrieved using the DataFrame's `agg` method, with `min` and `max` option respectively, as well as the total number of apartments which is simply obtained with `count`. These insights are then stored in a `csv` file with custom header.

### Task 3

Apartments of interest are filtered and, similarly as before, the average number of bathrooms and bedrooms are computed and stored.

### Task 4

In order to obtain the property with lowest price and highest rating (considering the feature `review_scores_rating`), two columns containing the values are added to the DataFrame. This is done exploiting two SQL windows. Only the row containing the lowest price value and the highest rating is retained, and the number of accommodated people is stored.

### Task 5

The following DAG is reproduced using Apache Airflow:

```
			 Task 2		  Task 4
Task 1  -->			 -->  Task 5
			 Task 4		  Task 6
```

Each task is associated to an [`EmptyOperator`](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/empty/index.html#airflow.operators.empty.EmptyOperator) (as the commonly used `DummyOperator` is [deprecated](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/dummy/index.html#airflow.operators.dummy.DummyOperator)).


## Part 3: Applied Machine Learning
### Task 1

In this task, a pipeline associated to a logistic regression model is defined and trained on the famous iris dataset:

```
+------------+-----------+------------+-----------+-----------+
|sepal_length|sepal_width|petal_length|petal_width|      class|
+------------+-----------+------------+-----------+-----------+
|         5.1|        3.5|         1.4|        0.2|Iris-setosa|
|         4.9|        3.0|         1.4|        0.2|Iris-setosa|
|         4.7|        3.2|         1.3|        0.2|Iris-setosa|
|         4.6|        3.1|         1.5|        0.2|Iris-setosa|
|         5.0|        3.6|         1.4|        0.2|Iris-setosa|
+------------+-----------+------------+-----------+-----------+
```

To replicate the provided `sklearn` code, the regularization factor is set to `1/1e5`, as in MLlib it is equal to `1/C` (`C` as used in `sklearn`).

The model return correct predictions on the two training examples:

```
+------------+-----------+------------+-----------+--------------+-----------------+--------------------+--------------------+----------+--------------+
|sepal_length|sepal_width|petal_length|petal_width|         class|         features|       rawPrediction|         probability|prediction|predictedClass|
+------------+-----------+------------+-----------+--------------+-----------------+--------------------+--------------------+----------+--------------+
|         5.1|        3.5|         1.4|        0.2|   Iris-setosa|[5.1,3.5,1.4,0.2]|[26.2934838719734...|[0.99998748569720...|       0.0|   Iris-setosa|
|         6.2|        3.4|         5.4|        2.3|Iris-virginica|[6.2,3.4,5.4,2.3]|[-15.603626132716...|[2.37447441373521...|       2.0|Iris-virginica|
+------------+-----------+------------+-----------+--------------+-----------------+--------------------+--------------------+----------+--------------+
```

The pipeline is then locally stored in order to be used again.

### Task 2

The logistic regression model trained in the previous task is loaded and tested on the same examples as before. Predictions are stored in a text file.
