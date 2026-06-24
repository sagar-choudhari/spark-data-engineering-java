package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class CsvIngestJobTest extends SparkTestBase {

    private Dataset<Row> orders;

    @BeforeEach
    void loadData() {
        orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/dataFiles/csv/input/orders.csv");
    }

    @Test
    @DisplayName("CSV loads correct number of rows")
    void testRowCount() {
        assertEquals(10, orders.count(),
                "orders.csv should have exactly 10 rows");
    }

    @Test
    @DisplayName("Schema contains all expected columns")
    void testSchema() {
        String[] columns = orders.columns();
        assertAll(
                () -> assertTrue(containsColumn(columns, "order_id")),
                () -> assertTrue(containsColumn(columns, "customer_name")),
                () -> assertTrue(containsColumn(columns, "product")),
                () -> assertTrue(containsColumn(columns, "quantity")),
                () -> assertTrue(containsColumn(columns, "price")),
                () -> assertTrue(containsColumn(columns, "status")),
                () -> assertTrue(containsColumn(columns, "order_date"))
        );
    }

    @Test
    @DisplayName("Filter returns only COMPLETED orders")
    void testCompletedFilter() {
        Dataset<Row> completed = orders
                .filter(col("status").equalTo("COMPLETED"));

        // verify count
        assertEquals(6, completed.count(),
                "Should have exactly 6 COMPLETED orders");

        // verify no other statuses leaked through
        long nonCompleted = completed
                .filter(col("status").notEqual("COMPLETED"))
                .count();
        assertEquals(0, nonCompleted,
                "No non-COMPLETED rows should exist after filter");
    }

    @Test
    @DisplayName("Total revenue calculation is correct")
    void testRevenueCalculation() {
        Row result = orders
                .filter(col("status").equalTo("COMPLETED"))
                .withColumn("total_value", col("quantity").multiply(col("price")))
                .agg(sum("total_value").cast("double").alias("grand_total"))
                .first();

        double grandTotal = result.isNullAt(0) ? 0.0 : result.getDouble(0);

        // manually verified from orders.csv:
        assertEquals(400000.0, grandTotal, 0.01,
                "Grand total revenue should be 400000");
    }

    @Test
    @DisplayName("No null values in critical columns")
    void testNoNullsInCriticalColumns() {
        long nullOrderIds = orders
                .filter(col("order_id").isNull())
                .count();
        long nullStatus = orders
                .filter(col("status").isNull())
                .count();

        assertEquals(0, nullOrderIds, "order_id should never be null");
        assertEquals(0, nullStatus, "status should never be null");
    }

    private boolean containsColumn(String[] columns, String name) {
        for (String col : columns) {
            if (col.equals(name)) return true;
        }
        return false;
    }
}