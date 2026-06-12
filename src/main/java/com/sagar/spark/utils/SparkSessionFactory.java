package com.sagar.spark.utils;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    private static SparkSession instance;

    public static SparkSession get() {
        if (instance == null) {
            instance = SparkSession.builder()
                    .appName("SparkDELearning")
                    .master("local[*]")
                    .config("spark.sql.shuffle.partitions", "4")
                    .getOrCreate();
        }
        return instance;
    }

    public static void stop() {
        if (instance != null) {
            instance.stop();
            instance = null;
        }
    }

}
