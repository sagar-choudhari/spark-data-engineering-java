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
                    // S3A configuration
                    .config("spark.hadoop.fs.s3a.impl",
                            "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                    .config("spark.hadoop.fs.s3a.endpoint",
                            "s3.amazonaws.com")
                    // performance tuning for S3
                    .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
                    .config("spark.hadoop.fs.s3a.fast.upload", "true")
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
