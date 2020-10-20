package vld0

import com.creanga.sparktest.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TopDomains extends LazyLogging {

  def main(implicit args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Spark Job Anatomy")
      .getOrCreate()

    val topDomainsDF = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/top10Domains.csv")

    //topDomainsDF.show()

    import spark.implicits._

    topDomainsDF.select(col("Root Domain")).map( row => PageSize.call(row.getString(0)) ).show();

    //TODO cum fac aici?
    //topDomainsDF.select(col("Root Domain"))
      //.withColumn("Size", pageSize.call( col("Root Domain")) ).show();





  }



}
