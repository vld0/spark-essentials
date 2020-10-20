package vld0

import java.util

import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkEnv, TaskContext}

import scala.util.Random

class Test extends Serializable { // add extends Serializable to make the example work
  println(s"Creating Test instance on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")
  def print(): Unit = println(s"${SparkEnv.get.executorId} is invoking method on Test for partition ${TaskContext.getPartitionId()}")
}

object SparkSharingObjectExample1 {
  def main(implicit args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Spark Job Anatomy")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val data = (1 to 10000).iterator.map(_ => Random.nextInt(10000)).toList.toDF("c0").repartition(4)

    val test = new Test()

    data.show()

    data.foreachPartition(new ForeachPartitionFunction[Row] {
      override def call(iterator: util.Iterator[Row]): Unit = test.print()
    });

    val data2 = Array(1, 2, 3, 4, 5)
    val rdd1 = sc.parallelize(data2, 3)
    rdd1.foreachPartition(partition => {
      println(s"Creating Test instance on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")
    })
    println(rdd1.getNumPartitions)
    println(rdd1.reduce(_+_))

    sc.getExecutorMemoryStatus


  }
}