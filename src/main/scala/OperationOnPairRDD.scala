package org.sparkbyexamples.application

import org.apache.spark.sql.SparkSession

object OperationOnPairRDD {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("OperationOnPairRDD").master("local[3]").getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val rdd = spark.sparkContext.parallelize(List("Germany India USA", "USA India Russia", "India Brazil Canada China"))

    val wordsRDD = rdd.flatMap(x => x.split(" "))
    val pairRDD = wordsRDD.map(x=>(x,1))
    //wordsRDD.foreach(println)
    pairRDD.foreach(println)

    println("Distinct=>>")
    pairRDD.distinct().foreach(println)

    println("Sortby=>")
    val sortRDD = pairRDD.sortByKey()
    sortRDD.foreach(println)

    println("ReduceBy =>")
    val reduceRDD = pairRDD.reduceByKey(_+_)
    reduceRDD.foreach(println)

    def param1 = (accu:Int,v:Int) => accu +v
    def param2 = (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate by key==> wordcount")
    val wordcount2 = pairRDD.aggregateByKey(0)(param1,param2)
    wordcount2.foreach(println)

    println("Values")
    wordcount2.values.foreach(println)

    println("Keys")
    wordcount2.keys.foreach(println)

    println("CollectAsMap===>")
    wordcount2.collectAsMap().foreach(println)

    println("Count")
    println("count" +wordcount2.count())
  }
}