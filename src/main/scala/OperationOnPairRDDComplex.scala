package org.sparkbyexamples.application

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OperationOnPairRDDComplex {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OperationOnPairComplex").master("local[1]").getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val keywithValueslist = Array("Foo=A", "Foo=A", "Foo=A", "Foo=A", "Foo=B", "bar=C", "bar=D", "bar=D")
    val data = spark.sparkContext.parallelize(keywithValueslist)
    val kv = data.map(x=>x.split("=")).map(x => (x(0), x(1))).cache()
    kv.foreach(println)

    val se = mutable.HashSet.empty[String]
    def param3 = (accu:mutable.HashSet[String],v:String) => accu + v
    def param4 = (accu1:mutable.HashSet[String],accu2:mutable.HashSet[String]) => accu1 ++= accu2
    kv.aggregateByKey(se)(param3,param4).foreach(println)
  /* Output
  (bar,D)
  (bar,D)
   (bar,Set(C, D))
   (Foo,Set(B, A))
   */
    
     def param5 = (accu:Int,v:String) => accu +1
     def param6 = (accu1:Int,accu2:Int ) => accu1 + accu2
     kv.aggregateByKey(0)(param5, param6).foreach(println)
 /* Output
 (bar,3)
(Foo,5)
  */

    val studentRDD = spark.sparkContext.parallelize(Array(("Joseph","Maths",83),("Joseph","Physics",74),("Joseph","chemistry",91),
      ("Joseph", "Biology", 82),("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
      ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
      ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
      ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)),3)


    val studentkey = studentRDD.map(f=>(f._1,(f._2,f._3)))
    studentkey.reduceByKey((accu,v)=>{(accu._1,if(accu._2 > v._2)accu._2 else v._2)}).foreach(println)

    val def1 = (accu:Int, v:(String,Int)) => if(accu > v._2) accu else v._2
    val def2 = (accu1:Int,accu2:Int)  => if(accu1 > accu2) accu1 else accu2
    studentkey.aggregateByKey(0)(def1,def2).foreach(println)

    // finding the highest score//

    val def5 = (accu:Int,v:(String,Int)) => accu + v._2
    val def6 = (accu1:Int,accu2:Int) => accu1 + accu2
    val studenttotals = studentkey.aggregateByKey(0)(def5,def6)
    studenttotals.take(1).foreach(println)

     val tot = studenttotals.max()(new Ordering[Tuple2[String,Int]] ()
     {
       override def compare(x: (String, Int), y: (String, Int)): Int = Ordering[Int].compare(x._2,y._2)
       })
    println("First class student:"+tot._1 + "="+tot._2)





  }
  }
