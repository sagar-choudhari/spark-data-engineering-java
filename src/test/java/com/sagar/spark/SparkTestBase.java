package com.sagar.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;


public abstract class SparkTestBase {

    protected static SparkSession spark;

    @BeforeAll
    static void setupSpark() {
        spark = SparkSession.builder()
                .appName("SparkTestSuite")
                .master("local[2]")   // 2 cores — enough for tests, faster than local[*]
                .config("spark.sql.shuffle.partitions", "2")  // keep tests fast
                .config("spark.ui.enabled", "false")          // no Spark UI during tests
                .getOrCreate();
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }
}