package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowFunctionJobTest extends SparkTestBase {

    private Dataset<Row> completed;

    @BeforeEach
    void loadData() {
        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/dataFiles/csv/input/orders_v2.csv");

        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/dataFiles/csv/input/customers.csv");

        // replicate enrichment from JoinJob
        completed = orders
                .join(customers,
                        orders.col("customer_id").equalTo(customers.col("customer_id")),
                        "inner")
                .withColumn("order_value",
                        col("quantity").multiply(col("price")))
                .filter(col("status").equalTo("COMPLETED"));
    }

    @Test
    @DisplayName("Completed orders count is correct")
    void testCompletedCount() {
        assertEquals(8, completed.count(),
                "Should have exactly 8 COMPLETED orders in orders_v2.csv");
    }

    @Test
    @DisplayName("rank() resets per tier partition")
    void testRankResetsPerTier() {
        Dataset<Row> customerSpend = completed
                .groupBy("customer_name", "tier")
                .agg(sum("order_value").alias("total_spend"));

        WindowSpec tierWindow = Window
                .partitionBy("tier")
                .orderBy(col("total_spend").desc());

        Dataset<Row> ranked = customerSpend
                .withColumn("rank_in_tier", rank().over(tierWindow));

        // rank=1 must exist in every tier — not just overall
        long tiersWithRankOne = ranked
                .filter(col("rank_in_tier").equalTo(1))
                .select("tier")
                .distinct()
                .count();

        long totalTiers = ranked
                .select("tier")
                .distinct()
                .count();

        assertEquals(totalTiers, tiersWithRankOne,
                "Every tier must have exactly one rank=1 customer");
    }

    @Test
    @DisplayName("Running total is cumulative and non-decreasing per customer")
    void testRunningTotalIsNonDecreasing() {
        WindowSpec customerDateWindow = Window
                .partitionBy("customer_name")
                .orderBy("order_date")
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        Dataset<Row> withRunning = completed
                .withColumn("running_total",
                        sum("order_value").over(customerDateWindow));

        // for Ravi Kumar (3 orders) — running total must grow
        List<Row> raviRows = withRunning
                .filter(col("customer_name").equalTo("Ravi Kumar"))
                .orderBy("order_date")
                .select("running_total")
                .collectAsList();

        assertTrue(raviRows.size() >= 2,
                "Ravi Kumar should have at least 2 orders for running total test");

        for (int i = 1; i < raviRows.size(); i++) {
            long prev = ((Number) raviRows.get(i - 1).get(0)).longValue();
            long curr = ((Number) raviRows.get(i).get(0)).longValue();
            assertTrue(curr >= prev,
                    "Running total must be non-decreasing for Ravi Kumar");
        }
    }

    @Test
    @DisplayName("LAG is null for first order per customer")
    void testLagNullForFirstOrder() {
        WindowSpec customerOrderWindow = Window
                .partitionBy("customer_name")
                .orderBy("order_date");

        Dataset<Row> withLag = completed
                .withColumn("prev_order_value",
                        lag("order_value", 1).over(customerOrderWindow));

        // customers with only one order must have null lag
        Dataset<Row> singleOrderCustomers = withLag
                .groupBy("customer_name")
                .agg(count("order_id").alias("order_count"))
                .filter(col("order_count").equalTo(1));

        long nullLagCount = withLag
                .join(singleOrderCustomers, "customer_name")
                .filter(col("prev_order_value").isNull())
                .count();

        assertEquals(singleOrderCustomers.count(), nullLagCount,
                "Every single-order customer must have null LAG");
    }
}