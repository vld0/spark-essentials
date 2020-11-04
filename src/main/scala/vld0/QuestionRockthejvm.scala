package vld0

import com.creanga.sparktest.{LazyLogging, MainApplication}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import part5lowlevel.RDDs.spark

/**

/mkdir /tmp/spark-event

 /spark/bin/spark-submit   --conf spark.speculation=true  --conf spark.eventLog.enabled=true  \
 --class vld0.QuestionRockthejvm        --master spark://d759584f22e1:7077        --deploy-mode client    \
 --verbose       --supervise       /opt/spark-apps/spark-essentials.jar


 /spark/sbin/start-history-server.sh


 */

object QuestionRockthejvm {

  def main(implicit args: Array[String]): Unit = {

    /*if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }*/

    val spark = SparkSession.builder()
      //.config("spark.master", "local")
      .appName("Spark Job Anatomy")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    sc.parallelize(1 to 100).repartition(4).foreach(
      p => println(s"Processing ${p} on ${SparkEnv.get.executorId} for partition ${TaskContext.getPartitionId()}")
    )

  }


}

