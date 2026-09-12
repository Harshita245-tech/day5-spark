# Day 5 - Transformations and Actions

## Objective

To understand Spark transformations and actions using Scala and Apache Spark RDDs.

This assignment demonstrates how Spark transforms data, performs actions, uses lazy evaluation, and analyzes log messages.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Core
- SBT
- RDD

## Topics Covered

1. map
2. filter
3. flatMap
4. distinct
5. union
6. count
7. collect
8. first
9. take
10. reduce
11. Transformations vs Actions
12. Lazy Evaluation
13. Log Analyzer

## Topic Explanation

### 1. map

`map` is a transformation that applies a function to every element of an RDD and returns a new RDD.

Example:
val doubled = numbers.map(_ * 2)

It multiplies every number by 2.

### 2. filter

`filter` is a transformation that selects only the elements that satisfy a given condition.

Example:
val evenNumbers = numbers.filter(_ % 2 == 0)

It selects only even numbers.

### 3. flatMap

`flatMap` applies a function to every element and then flattens the results into a single RDD.

Example:
val words = sentences.flatMap(_.split(" "))

It splits sentences into individual words.

### 4. distinct

`distinct` is a transformation that removes duplicate elements from an RDD.

Example:
val distinctNumbers = duplicateNumbers.distinct()

It returns only unique values. The order of the elements may vary because RDDs are distributed.

### 5. union

`union` combines two RDDs into one RDD.

Example:
val unionRDD = firstRDD.union(secondRDD)

It combines the elements of both RDDs.

### 6. count

`count` is an action that returns the number of elements in an RDD.

Example:
numbers.count()

Output:
Number of Elements: 10

### 7. collect

`collect` is an action that brings all elements of an RDD to the Driver.

Example:
numbers.collect()

It should be used carefully with large datasets because all data is brought to the Driver.

### 8. first

`first` is an action that returns the first element of an RDD.

Example:
numbers.first()

Output:
First Number: 1

### 9. take

`take(n)` is an action that returns the first n elements from an RDD.

Example:
numbers.take(5)

Output:
1, 2, 3, 4, 5

### 10. reduce

`reduce` is an action that combines all elements of an RDD using a specified operation.

Example:
val total = numbers.reduce(_ + _)

For numbers from 1 to 10, the result is 55.

Output:
Sum using REDUCE: 55

## Transformations vs Actions

### Transformations

The transformations used in this assignment are:

- map
- filter
- flatMap
- distinct
- union

Transformations are lazy. They create a new RDD but do not execute immediately.

### Actions

The actions used in this assignment are:

- count
- collect
- first
- take
- reduce

Actions trigger the actual execution of Spark operations and return a result.

## Lazy Evaluation

Spark uses lazy evaluation for transformations.

When a transformation such as map or filter is called, Spark does not immediately execute it. Spark builds the execution plan.

When an action such as count, collect, or reduce is called, Spark executes the required transformations.

This helps Spark optimize the execution before processing the data.

## Log Analyzer

A log analyzer was implemented using Spark RDD transformations and actions.

The log data contains INFO, WARN, and ERROR messages.

The filter transformation is used to select only messages containing ERROR.

Example:
val errorLogs = logs.filter(_.contains("ERROR"))

The count action is then used to find the total number of ERROR messages.

The distinct transformation is used to identify unique ERROR messages.

## Sample Output

===== DAY 5 - TRANSFORMATIONS AND ACTIONS =====

===== 1. MAP =====
Original Numbers:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

After MAP - Multiply by 2:
2, 4, 6, 8, 10, 12, 14, 16, 18, 20

===== 2. FILTER =====
Even Numbers:
2, 4, 6, 8, 10

===== 3. FLATMAP =====
Words:
Spark, is, fast, Scala, is, powerful, RDD, is, distributed

===== 4. DISTINCT =====
Original Values:
10, 20, 10, 30, 20, 40, 30, 50

Distinct Values:
50, 40, 30, 20, 10

===== 5. UNION =====
First RDD:
1, 2, 3, 4

Second RDD:
5, 6, 7, 8

After UNION:
1, 2, 3, 4, 5, 6, 7, 8

===== 6. COUNT =====
Number of Elements: 10

===== 7. COLLECT =====
Collected Numbers:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

===== 8. FIRST =====
First Number: 1

===== 9. TAKE =====
First 5 Numbers:
1, 2, 3, 4, 5

===== 10. REDUCE =====
Sum using REDUCE: 55

===== 11. TRANSFORMATIONS VS ACTIONS =====
Transformations:
- map
- filter
- flatMap
- distinct
- union

Actions:
- count
- collect
- first
- take
- reduce

Transformations are lazy.
Actions trigger execution of the Spark job.

===== 12. LOG ANALYZER - ERROR MESSAGES =====
All Log Messages:
INFO Application started
ERROR Database connection failed
INFO User logged in
ERROR File not found
WARN Memory usage is high
ERROR Timeout occurred
INFO Application completed
ERROR Database connection failed

ERROR Messages:
ERROR Database connection failed
ERROR File not found
ERROR Timeout occurred
ERROR Database connection failed

Total ERROR Messages: 4

===== 13. LOG ANALYZER SUMMARY =====
Unique ERROR Messages:
ERROR Timeout occurred
ERROR File not found
ERROR Database connection failed

Unique ERROR Count: 3

===== DAY 5 TRANSFORMATIONS AND ACTIONS COMPLETED =====

## How to Run

cd ~/day5-spark
sbt compile
sbt run

## Files

- Day5TransformationsActions.scala - Main Spark application
- build.sbt - SBT project configuration
- project/build.properties - SBT version
- .gitignore - Ignores generated files

## Result

Successfully practiced Spark transformations and actions and implemented a log analyzer to count ERROR messages and identify unique ERROR messages.

===== DAY 5 COMPLETED =====
