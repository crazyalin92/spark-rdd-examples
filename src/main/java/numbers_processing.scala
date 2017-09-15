/**
  * Created by ALINA on 15.09.2017.
  */

import org.apache.spark.{SparkConf, SparkContext}

object NumbersProcessing {
  def main(args: Array[String]): Unit = {

    //initialize spark configurations
    val conf = new SparkConf()
    conf.setAppName("numbers-rdd-example")
    conf.setMaster("local[2]")

    //SparkContext
    val sc = new SparkContext(conf)

    val inputFile = args(0);

    //read file
    val fileRDD = sc.textFile(inputFile)
    val rows = fileRDD.map(s => s.split(" "))

    //create the RDD of numbers
    val numbers = rows.map(s => s.map(t => t.toInt))

    println()
    println("Array of numbers:")
    numbers.map(s => s.mkString(" ")).take(20).foreach(print)

    println()
    println("Sum of numbers of each row:")
    val rowsSum = numbers.map(t => t.reduce((a, b) => a + b))
    rowsSum.map(s => s.toString + " ").take(20).foreach(print)

    println()
    println("Distinct numbers of each row:")
    val distinctRow = numbers.map(t => t.distinct)
    distinctRow.map(s => s.toString + " ").take(20).foreach(print)

    println()
    println("Max numbers of each row:")
    val maxInRow = distinctRow.map(t => t.max)
    maxInRow.map(s => s.toString + " ").take(20).foreach(print)
  }
}

