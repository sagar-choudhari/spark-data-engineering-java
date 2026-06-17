package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CsvIngestJob {

    private static final String CSV_PATH_ORDERS = AppConfig.get("csv.path.orders");

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSessionFactory.get();

        Dataset<Row> csvData = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")  // While reading the file also Check other 'option' keys and value as well. e.g. delimiter, mode, dateFormat, timestamp,
                .csv(CSV_PATH_ORDERS);

        System.out.println("################# SCHEMA-INFERRED ########################");
        csvData.printSchema();

        System.out.println("################# SEE-RAW-DATA ########################");
        csvData.show();

        System.out.println("################# SELECT ########################");
        csvData.select("order_id", "customer_name", "order_date")
                .show();

        System.out.println("################# COMPLETED-ORDERS ########################");
        csvData.select("order_id", "customer_name", "order_date", "status").filter(col("status").equalTo("COMPLETED"))
                .show();

        System.out.println("################# ORDER-VALUE ########################");
        csvData.filter(col("status").equalTo("COMPLETED"))
                .withColumn("total_value", col("quantity").multiply(col("price")))
                .select("order_id", "customer_name", "product", "total_value")
                .show();

        System.out.println("################# REVENUE BY PRODUCT ########################");
        csvData.filter(col("status").equalTo("COMPLETED"))
                .withColumn("total_value", col("quantity").multiply(col("price")))
                .groupBy("product")
                .agg(
                        sum("total_value").alias("total_revenue"),
                        count("order_id").alias("order_count")
                )
                .orderBy(col("total_revenue").desc())
                .show();

        SparkSessionFactory.stop();
    }
}
