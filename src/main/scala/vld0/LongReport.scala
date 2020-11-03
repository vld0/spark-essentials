package vld0

import com.creanga.sparktest.{LazyLogging, MainApplication}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import part5lowlevel.RDDs.spark

/**

 /mkdir /tmp/spark-event

 /spark/bin/spark-submit   --conf spark.speculation=true  --conf spark.eventLog.enabled=true  \
 --class vld0.LongReport        --master spark://5aba073cb0a0:7077        --deploy-mode client    \
 --verbose       --supervise       /opt/spark-apps/spark-essentials.jar


 /spark/sbin/start-history-server.sh


 */

object LongReport extends LazyLogging {

  def build(id: Int): String = {
    println(s"Processing ${id} on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")

    logger.info("Processing url {} on {} for partition {}", id+"", SparkEnv.get.executorId, TaskContext.getPartitionId()+"");
    if (id == 10) {
      println(s"Will wait for ${id} second(s)")
      logger.info("Will wait for {} second(s)", id)
      Thread.sleep(id * 1000)
    }
    Thread.sleep(2)
    s"Report ${id}"
  }

  def main(implicit args: Array[String]): Unit = {

    /*if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }*/

    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .config("spark.speculation", true)
      .appName("Spark Job Anatomy")
      .getOrCreate()

    val sc = spark.sparkContext

    println("############Executors:="+MainApplication.currentActiveExecutors(sc).foreach( x=> println(x)))
    println("############Executors.size:="+MainApplication.currentActiveExecutors(sc).size)

    import spark.implicits._

    val rdd = sc.parallelize(1 to 100).repartition(3)
    println("getNumPartitions="+  rdd.getNumPartitions)


    val df = rdd.toDF();
    val pDF = df.map(x => LongReport.build(x.getInt(0)));
    println("##########################count="+  pDF.count())
    pDF.show()




  }


}
