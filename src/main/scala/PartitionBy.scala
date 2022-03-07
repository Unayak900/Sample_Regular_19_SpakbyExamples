package org.sparkbyexamples.application

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PartitionBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("PartitionBy").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("file:///home/hduser/PRAC/zipcodes.csv")
    val rdd2 = rdd.map(x=>x.split(","))
    val rdd3 = rdd2.map(x=>(x(1),x.mkString(",")))
    rdd3.foreach(println)

    val rdd4 = rdd3.partitionBy(new HashPartitioner(3))

    println("Output of HashPartitioner")
    rdd4.saveAsTextFile("file:///home/hduser/PRAC/ZipcodesHashPatitioner")



  }
}
