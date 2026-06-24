package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
public class WindowFunctionJob {
    private static final String PARQUET_PATH_ENRICHED_ORDERS = AppConfig.get("parquet.path.enriched.orders");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> orders = spark.read().parquet(PARQUET_PATH_ENRICHED_ORDERS);

        Dataset<Row> completed = orders.filter(col("status").equalTo("COMPLETED"));

        // Total revenue and order count
        System.out.println("========================== REVENUE-BY-TIER ==========================");
        completed
                .groupBy(col("tier"))
                .agg(
                        sum("order_value").alias("total_revenue_across_tier"),
                        count("order_id").alias("number_of_orders_per_tier"),
                        round(avg("order_value"), 2).alias("average_order_value")
                )
                .orderBy(col("total_revenue_across_tier").desc())
                .show();

        System.out.println("========================== CUSTOMER-RANK-WITHIN-TIER ==========================");
//        Compute total spend per customer.
        Dataset<Row> customerSpend = completed
                .groupBy("customer_name", "tier")
                .agg(
                        sum("order_value").alias("total_spend")
                );

//        Partition by tier, order by spend desc
        WindowSpec tierWindow = Window
                .partitionBy("tier")
                .orderBy(col("total_spend").desc());

//        Apply rank() over the window
        customerSpend
                .withColumn("rank_in_tier", rank().over(tierWindow))
                .orderBy("tier", "rank_in_tier")
                .show();

        System.out.println("========================== RUNNING-TOTAL-PER-CUSTOMER ==========================");
        WindowSpec customerDateWindow = Window
                .partitionBy("customer_name")
                .orderBy("order_date")
                        .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        completed
                .withColumn("running_total", sum("order_value").over(customerDateWindow))
                .select("customer_name", "order_date", "product", "order_value", "running_total")
                .orderBy("customer_name", "order_date")
                .show();

        System.out.println("========================== ORDER-VALUE vs PREVIOUS-ORDER (LAG) ==========================");
//        compare each order value to previous order by same customer
        WindowSpec customerOrderWindow = Window
                .partitionBy("customer_name")
                .orderBy("order_date");

        completed
                .withColumn("prev_order_value",
                        lag("order_value", 1).over(customerOrderWindow))
                .withColumn("value_change",
                        col("order_value").minus(col("prev_order_value")))
                .select("customer_name", "order_date",
                        "product", "order_value",
                        "prev_order_value", "value_change")
                .orderBy("customer_name", "order_date")
                .show();

        SparkSessionFactory.stop();

    }

}



















