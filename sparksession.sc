import $ivy.`org.apache.spark::spark-sql:2.4.3` // Or use any other 2.x version here
import $ivy.`sh.almond::almond-spark:0.6.0`

import org.apache.spark._
import org.apache.spark.sql.{functions => func, _}
import org.apache.spark.sql.types._

import org.slf4j.LoggerFactory
import org.apache.log4j.{Level, Logger}

val spark = NotebookSparkSession
      .builder()
      .config("spark.sql.join.preferSortMergeJoin", false)
      .config("spark.sql.shuffle.partitions", 64)
      .master("local[*]")
      .getOrCreate()

import spark.implicits._

Logger.getRootLogger().setLevel(Level.ERROR)

def run[A](code: => A): A = {
    val start = System.currentTimeMillis()
    val res = code
    println(s"Took ${System.currentTimeMillis() - start}")
    res
}
