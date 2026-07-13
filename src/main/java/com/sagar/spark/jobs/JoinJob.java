package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.*;

public class JoinJob {
    private static final String CSV_PATH_CUSTOMERS= AppConfig.getFilePath("csv.path.customers");
    private static final String CSV_PATH_ORDERS_V2= AppConfig.getFilePath("csv.path.orders_v2");
    private static final String PARQUET_PATH_ENRICHED_ORDERS= AppConfig.getFilePath("parquet.path.enriched.orders");


    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_ORDERS_V2);

        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_CUSTOMERS);

        System.out.println("========================== INNER-JOIN ==========================");
        Dataset<Row> inner = customers.join(orders, orders.col("customer_id").equalTo(customers.col("customer_id")), "inner");

        inner.select(
                orders.col("order_id"),
                customers.col("customer_name"),
                customers.col("city"),
                customers.col("tier"),
                orders.col("product"),
                orders.col("status"),
                orders.col("price")
        ).show();

        System.out.println("========================== LEFT-JOIN ==========================");
        Dataset<Row> left = orders.join(customers, orders.col("customer_id").equalTo(customers.col("customer_id")),"left");

        left.select(
                orders.col("order_id"),
                customers.col("customer_name"),
                orders.col("product"),
                orders.col("status")
        ).show();

        System.out.println("========================== BROADCAST-JOIN ==========================");
        Dataset<Row> broadcastJoin = orders.join(
                broadcast(customers),
                orders.col("customer_id").equalTo(customers.col("customer_id")),
                "inner"
        );

        broadcastJoin.select(
                orders.col("order_id"),
                customers.col("customer_name"),
                customers.col("tier"),
                orders.col("product"),
                orders.col("status")
        ).show();

        System.out.println("========================== ENRICHED-ORDERS ==========================");
        Dataset<Row> enriched = orders.join(customers, orders.col("customer_id").equalTo(customers.col("customer_id")), "inner")
                        .withColumn("order_value", col("quantity").multiply(col("price")))
                        .select(
                                orders.col("order_id"),
                                customers.col("customer_name"),
                                customers.col("city"),
                                customers.col("tier"),
                                orders.col("product"),
                                orders.col("quantity"),
                                orders.col("price"),
                                col("order_value"),
                                orders.col("status"),
                                orders.col("order_date")
                                );
        enriched.show();

        enriched.write()
                .mode(SaveMode.Overwrite)
                .parquet(PARQUET_PATH_ENRICHED_ORDERS);

        System.out.println("========================== DATA-WRITTEN-TO-PARQUET ==========================");

        SparkSessionFactory.stop();

    }
}
