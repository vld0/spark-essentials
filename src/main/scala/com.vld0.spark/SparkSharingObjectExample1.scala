package com.vld0.spark

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import scala.util.Random

class Test { // add extends Serializable to make the example work
  println(s"Creating Test instance on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")
  def print(): Unit = println(s"${SparkEnv.get.executorId} is invoking method on Test for partition ${TaskContext.getPartitionId()}")
}

object SparkSharingObjectExample1 {
  def main(implicit args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val data = (1 to 10000).iterator.map(_ => Random.nextInt(10000)).toList.toDF("c0").repartition(4)

    val test = new Test()

    data.foreachPartition(_ => test.print())
  }
}