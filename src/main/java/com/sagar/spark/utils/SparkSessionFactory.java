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
                    .config("spark.sql.extensions",
                            "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
