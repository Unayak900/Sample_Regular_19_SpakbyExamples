package org.sparkbyexamples.application

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object DataSerialization {

    def main(args: Array[String]) {


      case class Input(val1:Int,Val2:Int,Val3:Int,Val4:Int)

      val start = System.currentTimeMillis()
      val conf = new SparkConf().setAppName("Test DataSerialization").setMaster("local")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", "MyRegistrator")
      conf.set("spark.kryo.registrationRequired","true")
      val sc = new SparkContext(conf)
      //val sqlContext = new SQLContext(sc)
      val dataPath = "/home/hduser/PRAC/Input.txt"
      val data = sc.textFile(dataPath)

      val rdd = data.map(x=>x.split("\t")).map(i=>Input(i(0).toInt,i(1).toInt,i(2).toInt,i(3).toInt))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      println(rdd.count())
      //println(System.currentTimeMillis() - start)
    }
  class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Input])
    }
  }



}


