package com.vld0.spark;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Simple Application")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> topDomainsDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/top500Domains.csv");
        //topDomainsDF.show();

        topDomainsDF.foreachPartition((ForeachPartitionFunction<Row>) iterator -> {
            System.out.println("TRalalalala");
        });


        spark.stop();
    }

}
