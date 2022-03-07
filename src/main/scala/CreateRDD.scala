package org.sparkbyexamples.application

import org.apache.spark.sql.SparkSession

object CreateRDD  {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("CreateRDD").getOrCreate()
    val rdd = spark.sparkContext.parallelize(Seq(("Java",20000),("python",10000),("Scala",3000)) )
    rdd.foreach(println)

    val rdd2 = spark.sparkContext.wholeTextFiles("file:///home/hduser/PRAC/Textfile.txt")
    rdd2.foreach(x=>println("Filename:"+x._1+",filecontents:"+x._2))

    val rdd3 = rdd.map(x=>(x._1,x._2+100))
    rdd3.foreach(println)

    val myrdd2 = spark.range(20).toDF().rdd
    myrdd2.foreach(println)


  }

}
