package vld0

import com.vld0.spark.PageSize
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TopDomains {

  def main(implicit args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Spark Job Anatomy")
      .getOrCreate()

    val topDomainsDF = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/top10Domains.csv")

    //topDomainsDF.show()

    val pageSize = new PageSize

    topDomainsDF.select(col("Root Domain"))
      .withColumn("Size", pageSize.call( col("Root Domain")) ).show();





  }



}
