package com.vld0.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopDomains {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Simple Application")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> topDomainsDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/top10Domains.csv");
        topDomainsDF.show();

        /*MapFunction<Row, Long> websiteSize = row -> (long) row.get(0).toString().length();
            Encoder<Long> longEncoder = Encoders.LONG();

        topDomainsDF.select("Root Domain").map((MapFunction<Row, Long>) row -> (long) row.get(0).toString().length(),
                Encoders.LONG());*/

        PageSize pageSize = new com.vld0.spark.PageSize();

        //topDomainsDF.select("Root Domain").as(Encoders.STRING()).map(pageSize, Encoders.LONG()).show();

        topDomainsDF.select("Root Domain").as(Encoders.STRING())
                .withColumn("Size", pageSize.call( topDomainsDF.col("Root Domain") ) )
                .show();




        spark.stop();
    }

}
