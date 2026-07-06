package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.HashMap;

import static org.apache.spark.sql.functions.*;

public class DeduplicationJob {

    private static final String CSV_PATH_ORDERS_DUPES = AppConfig.get("csv.path.orders_dupes");
    private static final String PARQUET_PATH_ORDERS_DUPES = AppConfig.get("parquet.path.orders_dupes-removed");
    private static final String CSV_PATH_CUSTOMERS_CURRENT = AppConfig.get("csv.path.customers_current");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        HashMap<String, String> optionMap = new HashMap<>();
        optionMap.put("header", "true");
        optionMap.put("inferSchema", "true");

        Dataset<Row> orders = spark.read()
                .options(optionMap)
                .csv(CSV_PATH_ORDERS_DUPES);

        Dataset<Row> customers = spark.read()
                .options(optionMap)
                .csv(CSV_PATH_CUSTOMERS_CURRENT);

        System.out.println("################# RAW-ORDERS ########################");
        orders.show();

        System.out.println("################# DUPLICATE-DROPPED-ARBITRARILY-the first occurrence ########################");
        orders.dropDuplicates("order_id").show();

        System.out.println("################# DROP-OBJECTIVELY ########################");
        WindowSpec dupedWindow = Window
                .partitionBy(col("order_id"))
                .orderBy(col("updated_at").desc());

        Dataset<Row> deDuped = orders
                .withColumn("row_num", row_number().over(dupedWindow))
                .filter(col("row_num").equalTo(1))
                .drop("row_num");

        deDuped.show();

        System.out.println("################# WRITE-TO-PARQUET ########################");

        deDuped
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(PARQUET_PATH_ORDERS_DUPES);

        System.out.println("################# WRITEN-TO-PARQUET ########################");

        System.out.println("################# SLOWLY-CHANGING-DIMENSIONS ########################");
        System.out.println("################# SCD2/SCD-TYPE2-Data ########################");
        customers.show();

        Dataset<Row> currentCustomers = customers
                .filter(col("is_current").equalTo("true"));

        currentCustomers.show();

        System.out.println("################# POINT-IN-TIME-LOOKUP ########################");
        customers
                .filter(col("customer_id").equalTo(1))
                .filter(
                        col("effective_from").leq(to_date(lit("2024-01-10")))
                                .and(
                                        col("effective_to").isNull()
                                                .or(col("effective_to").geq(to_date(lit("2024-01-10"))))
                                )
                ).show();

        System.out.println("################# TEMPORAL JOIN — order with customer tier at time of order #################");
        deDuped.join(customers,
                        deDuped.col("customer_id").equalTo(customers.col("customer_id"))
                                .and(
                                        col("updated_at").geq(col("effective_from"))
                                                .and(
                                                        col("effective_to").isNull()
                                                                .or(col("updated_at").leq(col("effective_to")))
                                                )
                                ),
                        "left"
                )
                .select(
                        deDuped.col("order_id"),
                        deDuped.col("customer_id"),
                        deDuped.col("product"),
                        deDuped.col("status"),
                        deDuped.col("updated_at").alias("order_date"),
                        customers.col("tier").alias("tier_at_order_time")
                )
                .orderBy("order_id")
                .show();

        SparkSessionFactory.stop();
    }
}
