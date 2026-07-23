package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ParquetWriteJob {

    private static final String CSV_PATH_ORDERS = AppConfig.getFilePath("csv.path.orders");
    private static final String PARQUET_PATH_ORDERS = AppConfig.getFilePath("parquet.path.orders");
    private static final String PARQUET_PATH_PARTITIONED_ORDERS = AppConfig.getFilePath("parquet.path.partitioned.orders");

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        SparkSession sparkSession = SparkSessionFactory.get();

        Dataset<Row> csvData =
                sparkSession.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv(CSV_PATH_ORDERS);

        System.out.println("################# WRITING-TO-PARQUET-FILE ########################");

        csvData.write()
                .mode(SaveMode.Overwrite)
                .parquet(PARQUET_PATH_ORDERS);

        System.out.println("################# PARQUET-FILE-WRITTEN ########################");

        System.out.println("################# WRITE-AS-PARTITIONED-PARQUET ########################");
        csvData.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("status")
                .parquet(PARQUET_PATH_PARTITIONED_ORDERS);

        System.out.println("################# PARQUET-SCHEMA ########################");
        Dataset<Row> paraquetData = sparkSession.read().parquet(PARQUET_PATH_PARTITIONED_ORDERS);
        paraquetData.printSchema();

        System.out.println("################# READ-BACK-FROM-PARQUET ########################");
        paraquetData.filter(col("status").equalTo("COMPLETED"))
                .select("order_id", "customer_name", "product", "status")
                .show();

        SparkSessionFactory.stop();
    }

}
