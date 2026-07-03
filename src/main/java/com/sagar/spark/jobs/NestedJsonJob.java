package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class NestedJsonJob {

    private static final String JSON_PATH_TRANSACTIONS = AppConfig.get("json.path.transactions");
    private static final String PARQUET_PATH_TRANSACTIONS = AppConfig.get("parquet.path.transactions");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> raw = spark.read()
                .option("multiline", "false")
                .json(JSON_PATH_TRANSACTIONS);

        System.out.println("========================== RAW-SCHEMA ==========================");
        raw.printSchema();

        System.out.println("========================== PRINT-RAW-DATA ==========================");
        raw.show(5, false);

        System.out.println("========================== ACCESS-STRUCT-FIELDS ==========================");
        Dataset<Row> structData = raw
                .select(
                        col("customer.id").alias("customer_id"),
                        col("customer.name").alias("customer_name"),
                        col("customer.tier").alias("tier"),
                        col("payment.method").alias("payment_method"),
                        col("payment.status").alias("payment_status"),
                        col("transaction_date"),
                        col("transaction_id")
                        );

        structData.show();

        System.out.println("========================== ACCESS-ARRAY-FIELDS-USING-EXPLODE ==========================");

        Dataset<Row> withExplodeData = raw
                .select(
                        col("customer.id").alias("customer_id"),
                        col("customer.name").alias("customer_name"),
                        col("customer.tier").alias("tier"),
                        col("payment.method").alias("payment_method"),
                        col("payment.status").alias("payment_status"),
                        col("transaction_date"),
                        col("transaction_id"),
                        explode(col("items")).alias("item")
                );

        withExplodeData.show();

        Dataset<Row> flatData = withExplodeData
                .select(
                        col("customer_id"),
                        col("customer_name"),
                        col("tier"),
                        col("payment_method"),
                        col("payment_status"),
                        col("transaction_date"),
                        col("transaction_id"),
                        col("item.price").alias("price"),
                        col("item.product").alias("product"),
                        col("item.quantity").alias("quantity"));

        flatData.show();

        System.out.println("========================== TRANSACTION-TOTALS ==========================");

        Dataset<Row> transactionTotals = flatData
                .filter(col("payment_status").equalTo("SUCCESS"))
                .groupBy("customer_id", "customer_name", "tier", "transaction_date")
                .agg(
                        sum(col("price").multiply(col("quantity"))).alias("total_value"),
                        count("product").alias("item_count"),
                        collect_list("product").alias("products_bought")
                );

        transactionTotals.show(false);

        System.out.println("========================== TRANSACTIONS-WRITING-PARQUET ==========================");

        flatData.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("transaction_date")
                .parquet(PARQUET_PATH_TRANSACTIONS);

        System.out.println("========================== TRANSACTIONS-LOADED-IN-PARQUET ==========================");

        SparkSessionFactory.stop();

    }
}

