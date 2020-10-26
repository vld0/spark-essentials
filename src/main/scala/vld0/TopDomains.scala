package vld0

import com.creanga.sparktest.LazyLogging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, udf}


/**
  * How to run the Spark application on the Docker cluster
  *
  * 1. Start the cluster
  *   docker-compose up --scale spark-worker=3
  *
  * 2. Connect to the master node
  *   docker exec -it spark-cluster_spark-master_1 bash
  *
  * 3. Run the spark-submit command
  *   /spark/bin/spark-submit \
  *     --class vld0.TopDomains \
  *     --master spark://5aba073cb0a0:7077 \
  *     --deploy-mode client \
  *     --verbose \
  *     --supervise \
  *     spark-essentials.jar /opt/spark-data/top10Domains.csv /opt/spark-data/top10DomainsPageSize
  */

object TopDomains extends LazyLogging {

  def main(implicit args: Array[String]): Unit = {

    /*if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }*/

    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Spark Job Anatomy")
      .getOrCreate()

    val csvFile = if (args.length > 0 && args(0)!=null) args(0) else "src/main/resources/data/top10Domains.csv";

    val topDomainsDF = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFile)
      .repartition(3)


    println("getNumPartitions="+  topDomainsDF.rdd.getNumPartitions)

    //topDomainsDF.show()

    import spark.implicits._
    //topDomainsDF.select(col("Root Domain")).map( row => PageSize.call(row.getString(0)) ).show();

    val calculatePageSize = udf( (url: String) => PageSize.call(url) )
    val pageSizeDF = topDomainsDF.select(col("Root Domain"))
      .withColumn("Size", calculatePageSize( col("Root Domain")) );

    pageSizeDF.show()

    if (args.length > 1 && args(1) !=null) {
      pageSizeDF.write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .save(args(1))
    }


  }



}
