package com.sagar.spark.jobs;


import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;


import static org.apache.spark.sql.functions.*;
public class ExerciseTwo {

    private static final String CSV_PATH_ORDERS = AppConfig.get("csv.path.orders_v2");
    private static final String CSV_PATH_CUSTOMERS = AppConfig.get("csv.path.customers");

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> orders = spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_ORDERS);

        Dataset<Row> customers = spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_CUSTOMERS);


//        problem 1
        System.out.println("========================== DENSE-RANK-OVER-TIER-FOR-CUSTOMER-SPENT ==========================");
        Dataset<Row> completedOrders = orders
                .filter(col("status").equalTo("COMPLETED"))
                .withColumn("amount", col("quantity").multiply(col("price")));

        WindowSpec tierWindow = Window
                .partitionBy("tier")
                .orderBy(col("total_spend").desc());

        Dataset<Row> customerSpent = completedOrders.join(customers, customers.col("customer_id").equalTo(completedOrders.col("customer_id")), "left")
                .groupBy(
                        customers.col("customer_id"),
                        customers.col("customer_name"),
                        customers.col("tier")
                ).agg(sum(completedOrders.col("amount")).alias("total_spend"));

        customerSpent
                .withColumn("dense_rank_over_amount", dense_rank().over(tierWindow)).show();

//        problem 2
        System.out.println("========================== CUSTOMERS-MAX-VALUE-ORDER ==========================");
        completedOrders
                .join(customers, customers.col("customer_id").equalTo(completedOrders.col("customer_id")), "left")
                .groupBy(
                        customers.col("customer_id"),
                        customers.col("customer_name")
                )
                .agg(
                        max("amount").alias("max_order_value")
                )
                .orderBy("max_order_value")
                .show();

//        Problem 3
        System.out.println("========================== CHANGE-VALUE-PERCENTAGE ==========================");

        WindowSpec customerWindow = Window
                .partitionBy(completedOrders.col("customer_id"))
                .orderBy("order_date");

        Dataset<Row> laggedAmount = completedOrders
                .join(customers, customers.col("customer_id").equalTo(completedOrders.col("customer_id")), "left")
                .withColumn("lag", lag("amount", 1).over(customerWindow))
                .select(
                        customers.col("customer_id"),
                        customers.col("tier"),
                        customers.col("city"),
                        completedOrders.col("order_id"),
                        completedOrders.col("amount"),
                        completedOrders.col("order_date"),
                        col("lag")
                );

        laggedAmount.show();

        Dataset<Row> changePercentage = laggedAmount
                .withColumn("lag_safe", coalesce(col("lag"), lit(0.0)))
                .withColumn("value_change_pct",
                        when(col("lag_safe").equalTo(0), lit(null))
                                .otherwise(
                                        round(col("amount").minus(col("lag_safe"))
                                                        .divide(col("lag_safe"))
                                                        .multiply(100),
                                                2)
                                )
                )
                .select(
                        "customer_id", "order_id", "amount",
                        "lag_safe", "value_change_pct"
                );

        changePercentage.show();

        System.out.println("========================== WINDOW-FRAME ==========================");

        WindowSpec sevenRowWindow = Window
                .partitionBy("customer_id")
                .orderBy("order_date")
                .rowsBetween(-3, 3);

        Dataset<Row> movingAvgOverWindow = completedOrders
                .withColumn("moving_avg_7",
                        round(avg("amount").over(sevenRowWindow),2)
                )
                .select("customer_id", "order_date", "amount", "moving_avg_7")
                .orderBy("customer_id", "order_date");

        movingAvgOverWindow.show();

    }
}






