package com.sagar.spark.jobs;

import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class WordCountDataFrameJob {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> lines = spark.createDataset(
                Arrays.asList(
                        "apache spark is fast",
                        "spark is written in scala",
                        "java spark api is also supported"
                ),
                org.apache.spark.sql.Encoders.STRING()
        ).toDF("line");

        Dataset<Row> words = lines
                .select(explode(split(col("line"), " ")).alias("word"));

        Dataset<Row> wordCounts = words
                .groupBy("word")
                .count()
                .orderBy(col("count").desc());

        wordCounts.show();

        SparkSessionFactory.stop();
    }
}

