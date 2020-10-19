package vld0

import java.util

import com.vld0.spark.PageSize
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{Row, SparkSession}

import scala.reflect.internal.util.TableDef.Column
import scala.util.Random

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
    val myJavaUdf = udf( pageSize.call _ )

    topDomainsDF.select(col("Root Domain"))
      .withColumn("Size", myJavaUdf(col("Root Domain")) ).show();





  }



}
