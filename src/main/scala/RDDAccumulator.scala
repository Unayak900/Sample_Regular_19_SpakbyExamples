package org.sparkbyexamples.application

import org.apache.spark.sql.SparkSession

object RDDAccumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("RDDAccumulator").getOrCreate()

    val sc = spark.sparkContext

    val longacc = sc.longAccumulator("SumAccumator")
    val rdd = sc.parallelize(Array(1,2,3))
    rdd.foreach(x=> longacc.add(x))
    println(longacc.value)

  }
}
