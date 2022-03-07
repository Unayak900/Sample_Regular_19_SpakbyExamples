package org.sparkbyexamples.application

import org.apache.spark.sql.SparkSession

object RDDBroadcast {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("RDDBroadcast").getOrCreate()

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United states of America"),("IN","India"))

    val broadcastates = spark.sparkContext.broadcast(states)
    val broadcastcountries = spark.sparkContext.broadcast(countries)

    val data = Seq(("James","Smith","USA","CA"),("Michael","Rose","USA","NY"),
      ("Robert","Willams","USA","CA"),("Maria","Jones","USA","FL"))

    val rdd = spark.sparkContext.parallelize(data)

    val rdd2 = rdd.map(f=>{
      val country = f._3
      val state = f._4
      val fullcountry = broadcastcountries.value.get(country).get
      val fullstate = broadcastates.value.get(state).get
      (f._1,f._2,f._3,f._4,fullcountry,fullstate)

      println("here is the f._1 \t"+f._1)
      println("here is the f._2 \t"+f._2)
      println("here is the f._3 Country\t"+country)
      println("here is the f._4 State\t"+state)
    })

    println("Output=========>")
    println(rdd2.collect().mkString("\n"))

    /*(James,Smith,USA,CA,United states of America,California)
     (Michael,Rose,USA,NY,United states of America,New York)
     (Robert,Willams,USA,CA,United states of America,California)
     (Maria,Jones,USA,FL,United states of America,Florida)*/
  }

}
