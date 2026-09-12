import org.apache.spark.{SparkConf, SparkContext}

object Day5TransformationsActions {

  def main(args: Array[String]): Unit = {

    println("\n===== DAY 5 - TRANSFORMATIONS AND ACTIONS =====")

    val conf = new SparkConf()
      .setAppName("Day5TransformationsActions")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // =========================================================
    // 1. MAP
    // =========================================================
    println("\n===== 1. MAP =====")

    val numbers = sc.parallelize(1 to 10)

    val doubled = numbers.map(_ * 2)

    println("Original Numbers:")
    println(numbers.collect().mkString(", "))

    println("After MAP - Multiply by 2:")
    println(doubled.collect().mkString(", "))


    // =========================================================
    // 2. FILTER
    // =========================================================
    println("\n===== 2. FILTER =====")

    val evenNumbers = numbers.filter(_ % 2 == 0)

    println("Even Numbers:")
    println(evenNumbers.collect().mkString(", "))


    // =========================================================
    // 3. FLATMAP
    // =========================================================
    println("\n===== 3. FLATMAP =====")

    val sentences = sc.parallelize(
      Seq(
        "Spark is fast",
        "Scala is powerful",
        "RDD is distributed"
      )
    )

    val words = sentences.flatMap(_.split(" "))

    println("Words:")
    println(words.collect().mkString(", "))


    // =========================================================
    // 4. DISTINCT
    // =========================================================
    println("\n===== 4. DISTINCT =====")

    val duplicateNumbers = sc.parallelize(
      Seq(10, 20, 10, 30, 20, 40, 30, 50)
    )

    val distinctNumbers = duplicateNumbers.distinct()

    println("Original Values:")
    println(duplicateNumbers.collect().mkString(", "))

    println("Distinct Values:")
    println(distinctNumbers.collect().mkString(", "))


    // =========================================================
    // 5. UNION
    // =========================================================
    println("\n===== 5. UNION =====")

    val firstRDD = sc.parallelize(Seq(1, 2, 3, 4))
    val secondRDD = sc.parallelize(Seq(5, 6, 7, 8))

    val unionRDD = firstRDD.union(secondRDD)

    println("First RDD:")
    println(firstRDD.collect().mkString(", "))

    println("Second RDD:")
    println(secondRDD.collect().mkString(", "))

    println("After UNION:")
    println(unionRDD.collect().mkString(", "))


    // =========================================================
    // 6. COUNT
    // =========================================================
    println("\n===== 6. COUNT =====")

    val count = numbers.count()

    println(s"Number of Elements: $count")


    // =========================================================
    // 7. COLLECT
    // =========================================================
    println("\n===== 7. COLLECT =====")

    val collectedNumbers = numbers.collect()

    println("Collected Numbers:")
    println(collectedNumbers.mkString(", "))


    // =========================================================
    // 8. FIRST
    // =========================================================
    println("\n===== 8. FIRST =====")

    val firstNumber = numbers.first()

    println(s"First Number: $firstNumber")


    // =========================================================
    // 9. TAKE
    // =========================================================
    println("\n===== 9. TAKE =====")

    val firstFive = numbers.take(5)

    println("First 5 Numbers:")
    println(firstFive.mkString(", "))


    // =========================================================
    // 10. REDUCE
    // =========================================================
    println("\n===== 10. REDUCE =====")

    val total = numbers.reduce(_ + _)

    println(s"Sum using REDUCE: $total")


    // =========================================================
    // 11. TRANSFORMATIONS VS ACTIONS
    // =========================================================
    println("\n===== 11. TRANSFORMATIONS VS ACTIONS =====")

    println("Transformations:")
    println("- map")
    println("- filter")
    println("- flatMap")
    println("- distinct")
    println("- union")

    println("\nActions:")
    println("- count")
    println("- collect")
    println("- first")
    println("- take")
    println("- reduce")

    println("\nTransformations are lazy.")
    println("Actions trigger execution of the Spark job.")


    // =========================================================
    // 12. LOG ANALYZER - ERROR MESSAGES
    // =========================================================
    println("\n===== 12. LOG ANALYZER - ERROR MESSAGES =====")

    val logs = sc.parallelize(
      Seq(
        "INFO Application started",
        "ERROR Database connection failed",
        "INFO User logged in",
        "ERROR File not found",
        "WARN Memory usage is high",
        "ERROR Timeout occurred",
        "INFO Application completed",
        "ERROR Database connection failed"
      )
    )

    println("All Log Messages:")
    logs.collect().foreach(println)

    val errorLogs = logs.filter(_.contains("ERROR"))

    println("\nERROR Messages:")
    errorLogs.collect().foreach(println)

    val errorCount = errorLogs.count()

    println(s"\nTotal ERROR Messages: $errorCount")


    // =========================================================
    // 13. LOG ANALYZER USING TRANSFORMATIONS
    // =========================================================
    println("\n===== 13. LOG ANALYZER SUMMARY =====")

    val uniqueErrors = errorLogs.distinct()

    println("Unique ERROR Messages:")
    uniqueErrors.collect().foreach(println)

    println(s"\nUnique ERROR Count: ${uniqueErrors.count()}")


    // =========================================================
    // FINAL
    // =========================================================
    println("\n===== DAY 5 TRANSFORMATIONS AND ACTIONS COMPLETED =====")

    sc.stop()
  }
}
