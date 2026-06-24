package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;

class JoinJobTest extends SparkTestBase {

    private Dataset<Row> orders;
    private Dataset<Row> customers;

    @BeforeEach
    void loadData() {
        orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/dataFiles/csv/input/orders_v2.csv");

        customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/dataFiles/csv/input/customers.csv");
    }

    @Test
    @DisplayName("Inner join retains only matching rows")
    void testInnerJoinCount() {
        Dataset<Row> inner = orders.join(
                customers,
                orders.col("customer_id").equalTo(customers.col("customer_id")),
                "inner"
        );
        // all 12 orders have matching customers in our test data
        assertEquals(12, inner.count(),
                "Inner join should retain all 12 rows — all customers exist");
    }

    @Test
    @DisplayName("Left join retains all orders including unmatched")
    void testLeftJoinRetainsAllOrders() {
        Dataset<Row> left = orders.join(
                customers,
                orders.col("customer_id").equalTo(customers.col("customer_id")),
                "left"
        );
        // left join must never drop order rows
        assertEquals(orders.count(), left.count(),
                "Left join must retain all orders regardless of customer match");
    }

    @Test
    @DisplayName("Enriched dataset contains expected columns")
    void testEnrichedSchema() {
        Dataset<Row> enriched = orders
                .join(customers,
                        orders.col("customer_id").equalTo(customers.col("customer_id")),
                        "inner")
                .withColumn("order_value",
                        col("quantity").multiply(col("price")));

        String[] columns = enriched.columns();
        assertAll(
                () -> assertTrue(containsColumn(columns, "order_id")),
                () -> assertTrue(containsColumn(columns, "customer_name")),
                () -> assertTrue(containsColumn(columns, "tier")),
                () -> assertTrue(containsColumn(columns, "city")),
                () -> assertTrue(containsColumn(columns, "order_value"))
        );
    }

    @Test
    @DisplayName("Order value is correctly computed as quantity x price")
    void testOrderValueCalculation() {
        Dataset<Row> enriched = orders
                .join(customers,
                        orders.col("customer_id").equalTo(customers.col("customer_id")),
                        "inner")
                .withColumn("order_value",
                        col("quantity").multiply(col("price")));

        // order_id=1: quantity=1, price=75000 → order_value=75000
        Row row = enriched
                .filter(orders.col("order_id").equalTo(1))
                .select("order_value")
                .first();

        assertEquals(75000.0, row.getInt(0),
                "order_id=1 order_value should be 75000");
    }

    @Test
    @DisplayName("No nulls in customer_name after inner join")
    void testNoNullCustomerNameAfterInnerJoin() {
        Dataset<Row> inner = orders.join(
                customers,
                orders.col("customer_id").equalTo(customers.col("customer_id")),
                "inner"
        );
        long nullNames = inner
                .filter(col("customer_name").isNull())
                .count();
        assertEquals(0, nullNames,
                "Inner join should never produce null customer_name");
    }

    private boolean containsColumn(String[] columns, String name) {
        for (String col : columns) {
            if (col.equals(name)) return true;
        }
        return false;
    }
}