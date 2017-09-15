/**
  * Created by ALINA on 15.09.2017.
  */

import org.apache.spark.{SparkConf, SparkContext}

object WordsCount {
  def main(args: Array[String]): Unit = {

    //initialize spark configurations
    val conf = new SparkConf()
    conf.setAppName("words-count-example")
    conf.setMaster("local[2]")

    //SparkContext
    val sc = new SparkContext(conf)

    val inputFile = args(0);

    val fileRDD = sc.textFile(inputFile)
    val wordsTuplesRDD = fileRDD.flatMap(s => s.split(" ")).map(s => (s, 1))

    val wordsCount = wordsTuplesRDD.reduceByKey { case (a, b) => a + b }

    val wordFrq = wordsTuplesRDD.filter(s => s._1.contains("BARRIER")).count()
    println(wordFrq)
  }
}
