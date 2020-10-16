package com.vld0.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class SimpleApp {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Simple Application")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> topDomainsDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/top500Domains.csv");
        //topDomainsDF.show();

        MapFunction<Row, Long> websiteSize = row -> (long) row.get(0).toString().length();
            Encoder<Long> longEncoder = Encoders.LONG();

        topDomainsDF.select("Root Domain").map((MapFunction<Row, Long>) row -> (long) row.get(0).toString().length(),
                Encoders.LONG());

        topDomainsDF.select("Root Domain").as(Encoders.STRING())
                .map((MapFunction<String, Long>) s -> (long) s.length(), Encoders.LONG()).show();


        spark.stop();
    }

}
