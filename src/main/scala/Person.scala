package org.sparkbyexamples.application

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Person {

  def main(args: Array[String]): Unit = {

    case class Person(name: String, age: Int)

    val conf = new SparkConf()
      .setAppName("Person")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(
        Array(classOf[Person],classOf[Array[Person]],
          Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
      )

    val sparkContext = new SparkContext(conf)

    val personList = (1 to 999)
      .map(value => Person("p"+value, value)).toArray

    //creating RDD of Person
    val rddPerson = sparkContext.parallelize(personList,5)
    val evenAgePerson = rddPerson.filter(_.age % 2 == 0)

    evenAgePerson.persist(StorageLevel.MEMORY_ONLY_SER)

    evenAgePerson.take(5).foreach(x=>println(x.name,x.age))
  }
}
