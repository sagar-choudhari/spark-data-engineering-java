package com.sagar.spark.jobs;

import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountJob {

    public static void main(String[] args) {

        List<String> lines = Arrays.asList(
                "Apache Spark is fast",
                "Spark is written in Scala",
                "Java Spark API is also supported",
                "Spark"
        );

        SparkSession spark = SparkSessionFactory.get();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> rdd = sc.parallelize(lines);

        JavaPairRDD<String, Integer> wordCounts = rdd
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word.trim(), 1))
                .reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> result = wordCounts.collect();

        result.stream()
                .sorted((a, b) -> b._2() - a._2())
                .forEach(t -> System.out.printf("%-20s %d%n", t._1(), t._2()));

        SparkSessionFactory.stop();
    }

}
