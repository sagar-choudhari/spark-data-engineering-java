package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class DeduplicationJobTest extends SparkTestBase {

    private Dataset<Row> orders;
    private Dataset<Row> customers;
    private Dataset<Row> deduped;

    @BeforeEach
    void loadData() {
        orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/orders_dupes.csv");

        customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/customers_current.csv");

        // replicate smart dedup logic
        WindowSpec dedupeWindow = Window
                .partitionBy("order_id")
                .orderBy(col("updated_at").desc());

        deduped = orders
                .withColumn("row_num", row_number().over(dedupeWindow))
                .filter(col("row_num").equalTo(1))
                .drop("row_num");
    }

    @Test
    @DisplayName("Raw orders file contains duplicates")
    void testRawContainsDuplicates() {
        long rawCount = orders.count();
        long distinctCount = orders.dropDuplicates("order_id").count();
        assertTrue(rawCount > distinctCount,
                "Raw file should contain duplicate order_ids");
    }

    @Test
    @DisplayName("Smart dedup produces one row per order_id")
    void testSmartDedupOneRowPerOrderId() {
        long totalRows = deduped.count();
        long distinctOrderIds = deduped
                .select("order_id")
                .distinct()
                .count();
        assertEquals(totalRows, distinctOrderIds,
                "After smart dedup every order_id must appear exactly once");
    }

    @Test
    @DisplayName("Smart dedup keeps latest status for order_id=1")
    void testOrder1KeepsLatestStatus() {
        Row row = deduped
                .filter(col("order_id").equalTo(1))
                .select("status")
                .first();
        assertEquals("COMPLETED", row.getString(0),
                "order_id=1 latest status should be COMPLETED not PENDING");
    }

    @Test
    @DisplayName("Smart dedup keeps latest status for order_id=3")
    void testOrder3KeepsLatestStatus() {
        Row row = deduped
                .filter(col("order_id").equalTo(3))
                .select("status")
                .first();
        assertEquals("COMPLETED", row.getString(0),
                "order_id=3 latest status should be COMPLETED not CANCELLED or PENDING");
    }

    @Test
    @DisplayName("Naive dedup row count matches smart dedup row count")
    void testNaiveDedupCountMatchesSmart() {
        // both should produce same number of rows
        // even if naive picks wrong versions
        long naiveCount = orders.dropDuplicates("order_id").count();
        long smartCount = deduped.count();
        assertEquals(naiveCount, smartCount,
                "Both dedup strategies should produce same row count — 5 distinct orders");
    }

    @Test
    @DisplayName("SCD Type 2 table contains history rows for customer_id=1")
    void testSCDType2HistoryExists() {
        long historyRows = customers
                .filter(col("customer_id").equalTo(1))
                .count();
        assertEquals(2, historyRows,
                "customer_id=1 should have 2 rows in SCD Type 2 — SILVER and GOLD");
    }

    @Test
    @DisplayName("Current snapshot returns exactly one row per customer")
    void testCurrentSnapshotOneRowPerCustomer() {
        Dataset<Row> current = customers
                .filter(col("is_current").equalTo(true));

        long totalRows = current.count();
        long distinctIds = current
                .select("customer_id")
                .distinct()
                .count();

        assertEquals(totalRows, distinctIds,
                "Current snapshot must have exactly one row per customer_id");
    }

    @Test
    @DisplayName("Point-in-time lookup returns SILVER for customer_id=1 on 2024-01-10")
    void testPointInTimeLookup() {
        Row row = customers
                .filter(col("customer_id").equalTo(1))
                .filter(
                        col("effective_from").leq(to_date(lit("2024-01-10")))
                                .and(
                                        col("effective_to").isNull()
                                                .or(col("effective_to").geq(to_date(lit("2024-01-10"))))
                                )
                )
                .select("tier")
                .first();

        assertEquals("SILVER", row.getString(0),
                "customer_id=1 tier on 2024-01-10 should be SILVER");
    }

    @Test
    @DisplayName("Point-in-time lookup returns GOLD for customer_id=1 on 2024-01-20")
    void testPointInTimeLookupAfterUpgrade() {
        Row row = customers
                .filter(col("customer_id").equalTo(1))
                .filter(
                        col("effective_from").leq(to_date(lit("2024-01-20")))
                                .and(
                                        col("effective_to").isNull()
                                                .or(col("effective_to").geq(to_date(lit("2024-01-20"))))
                                )
                )
                .select("tier")
                .first();

        assertEquals("GOLD", row.getString(0),
                "customer_id=1 tier on 2024-01-20 should be GOLD");
    }

    @Test
    @DisplayName("Temporal join matches order to correct customer tier at order time")
    void testTemporalJoin() {
        // order_id=1 placed at 2024-01-15 11:30
        // customer_id=1 upgraded to GOLD on 2024-01-15
        // effective_from=2024-01-15 so GOLD applies
        Row row = deduped.join(customers,
                        deduped.col("customer_id").equalTo(customers.col("customer_id"))
                                .and(
                                        to_date(col("updated_at"), "dd-MM-yyyy HH:mm")
                                                .geq(col("effective_from"))
                                                .and(
                                                        col("effective_to").isNull()
                                                                .or(to_date(col("updated_at"), "dd-MM-yyyy HH:mm")
                                                                        .leq(col("effective_to")))
                                                )
                                ),
                        "left"
                )
                .filter(deduped.col("order_id").equalTo(1))
                .select(customers.col("tier"))
                .first();

        assertEquals("GOLD", row.getString(0),
                "order_id=1 placed on 2024-01-15 should match GOLD tier — effective from that date");
    }
}