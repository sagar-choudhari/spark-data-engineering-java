package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class SparkSQLJobTest extends SparkTestBase {

    @BeforeEach
    void registerViews() {
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/orders_v2.csv")
                .createOrReplaceTempView("orders");

        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/customers.csv")
                .createOrReplaceTempView("customers");
    }

    @Test
    @DisplayName("Basic SQL join returns only COMPLETED orders")
    void testBasicSQLJoin() {
        Dataset<Row> result = spark.sql("""
                SELECT o.order_id, c.customer_name, o.status
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.status = 'COMPLETED'
                """);

        long nonCompleted = result
                .filter(col("status").notEqual("COMPLETED"))
                .count();

        assertEquals(0, nonCompleted,
                "SQL join should return only COMPLETED orders");
    }

    @Test
    @DisplayName("Revenue aggregation by tier returns correct total")
    void testRevenueAggregation() {
        Dataset<Row> result = spark.sql("""
                SELECT
                    c.tier,
                    SUM(o.quantity * o.price) AS total_revenue
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.status = 'COMPLETED'
                GROUP BY c.tier
                ORDER BY total_revenue DESC
                """);

        // must have at least one tier in output
        assertTrue(result.count() > 0,
                "Revenue aggregation must return at least one tier");

        // total revenue across all tiers must be positive
        Row totals = result.agg(sum("total_revenue")).first();
        double total = ((Number) totals.get(0)).doubleValue();
        assertTrue(total > 0,
                "Total revenue across all tiers must be positive");
    }

    @Test
    @DisplayName("CTE returns only rank 1 customers per tier")
    void testCTERankFilter() {
        Dataset<Row> result = spark.sql("""
                WITH completed_orders AS (
                    SELECT
                        o.order_id,
                        o.customer_id,
                        o.quantity * o.price AS order_value
                    FROM orders o
                    WHERE o.status = 'COMPLETED'
                ),
                customer_spend AS (
                    SELECT
                        c.customer_name,
                        c.tier,
                        SUM(co.order_value) AS total_spend
                    FROM completed_orders co
                    JOIN customers c ON co.customer_id = c.customer_id
                    GROUP BY c.customer_name, c.tier
                ),
                ranked AS (
                    SELECT
                        customer_name,
                        tier,
                        total_spend,
                        DENSE_RANK() OVER (
                            PARTITION BY tier
                            ORDER BY total_spend DESC
                        ) AS rank_in_tier
                    FROM customer_spend
                )
                SELECT * FROM ranked WHERE rank_in_tier = 1
                """);

        // every row must have rank 1
        long nonRankOne = result
                .filter(col("rank_in_tier").notEqual(1))
                .count();

        assertEquals(0, nonRankOne,
                "CTE result must only contain rank_in_tier = 1 rows");

        // must have one result per tier minimum
        assertTrue(result.count() > 0,
                "CTE must return at least one top-ranked customer");
    }

    @Test
    @DisplayName("RANK, DENSE_RANK, ROW_NUMBER all present in window query")
    void testWindowFunctionColumns() {
        Dataset<Row> result = spark.sql("""
                SELECT
                    customer_name,
                    tier,
                    total_spend,
                    RANK()        OVER (PARTITION BY tier ORDER BY total_spend DESC) AS rnk,
                    DENSE_RANK()  OVER (PARTITION BY tier ORDER BY total_spend DESC) AS dense_rnk,
                    ROW_NUMBER()  OVER (PARTITION BY tier ORDER BY total_spend DESC) AS row_num
                FROM (
                    SELECT
                        c.customer_name,
                        c.tier,
                        SUM(o.quantity * o.price) AS total_spend
                    FROM orders o
                    JOIN customers c ON o.customer_id = c.customer_id
                    WHERE o.status = 'COMPLETED'
                    GROUP BY c.customer_name, c.tier
                ) t
                """);

        String[] columns = result.columns();
        assertAll(
                () -> assertTrue(containsColumn(columns, "rnk")),
                () -> assertTrue(containsColumn(columns, "dense_rnk")),
                () -> assertTrue(containsColumn(columns, "row_num"))
        );
    }

    private boolean containsColumn(String[] columns, String name) {
        for (String col : columns) {
            if (col.equals(name)) return true;
        }
        return false;
    }
}