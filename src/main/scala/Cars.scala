package org.sparkbyexamples.application

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

object Cars {

  def main(args: Array[String]): Unit = {


    case class Input(val1:Int,val2:Int,val3:Int,val4:Int)

    val conf = new SparkConf().setAppName("Cars").setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "MyRegistrator")
    conf.set("spark.kryo.registrationRequired","true")

    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val dataPath = "/home/hduser/PRAC/Input.txt"


    val file = sc.textFile(dataPath)

    val rdd = file.map(x=>x.split("\t")).map(i=>Input(i(0).toInt,i(1).toInt,i(2).toInt,i(3).toInt))

    rdd.collect.foreach(x=>println(x))

    /*case  class cars(make:String, Model:String,MPG:String,Cylinders:Integer,Engine_Disp:Integer,
                     Horsepower:Integer,Weight:Float,Accelerate:Float,Year:Integer,Origin:String)

    /* val conf = new SparkConf().setMaster("local[*]").setAppName("Cars")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true").registerKryoClasses(Array(classOf[cars],classOf[Array[cars]],
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage")))*/

    val conf = new SparkConf().setAppName("Cars").setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "MyRegistrator")
    conf.set("spark.kryo.registrationRequired","true")

    val sc = new SparkContext(conf)
    val dataPath = "/home/hduser/PRAC/cars.txt"

    val file = sc.textFile(dataPath)

    //case class cars(make:String, Model:String,MPG:String,Cylinders:Int,Engine:Int,Disp:Int,Horsepower:Int,Weight:Float,Accelerate:Float,Year:Int,Origin:String)

    val rdd = file.map(x=>x.split("\t"))

    val rdd2 = rdd.map(c=>cars(c(0).toString, c(1).toString,c(2).toString,c(3).toInt,c(4).toInt,c(5).toInt,c(6).toFloat,c(7).toFloat,c(8).toInt,c(9).toString))
*/
    class MyRegistrator extends KryoRegistrator {
      override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[Input])
      }
    }
  }

}
