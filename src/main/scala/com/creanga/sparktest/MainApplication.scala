package com.creanga.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object MainApplication extends SparkEnv with LazyLogging {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[4]").getOrCreate()
    val sc: SparkContext = SparkContext.getOrCreate()

    val urls = Array("https://www.evz.ro",
      "https://www.hotnews.ro",
      "https://www.gmedia.ro",
      "https://www.cotidianul.ro",
      "https://www.adevarul.ro")
    val rdd = spark.sparkContext.parallelize(urls)

    rdd.foreachPartition(partition => {
      //this code will be executed on the executors
      println(partition)
    })

  }


  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost = sc.getConf.get("spark.driver.host")
    allExecutors.filter(!_.split(":")(0).equals(driverHost)).toList
  }
}
