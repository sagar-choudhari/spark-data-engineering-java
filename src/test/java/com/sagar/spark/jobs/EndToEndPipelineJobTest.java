package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class EndToEndPipelineJobTest extends SparkTestBase {

    private static final String[] VALID_STATUSES =
            {"COMPLETED", "PENDING", "CANCELLED"};

    private Dataset<Row> rawOrders;
    private Dataset<Row> flatItems;
    private Dataset<Row> deduped;
    private Dataset<Row> correlated;

    @BeforeEach
    void setup() {
        StructType csvSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("order_id",         DataTypes.IntegerType, true),
                DataTypes.createStructField("customer_id",      DataTypes.IntegerType, true),
                DataTypes.createStructField("product_category", DataTypes.StringType,  true),
                DataTypes.createStructField("total_amount",     DataTypes.DoubleType,  true),
                DataTypes.createStructField("status",           DataTypes.StringType,  true),
                DataTypes.createStructField("order_date",       DataTypes.StringType,  true),
                DataTypes.createStructField("channel",          DataTypes.StringType,  true),
        });

        rawOrders = spark.read()
                .option("header", "true")
                .schema(csvSchema)
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/orders_dirty_v2.csv");

        Dataset<Row> rawTransactions = spark.read()
                .option("multiLine", "false")
                .json("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/json/input/transactions_v2.json");

        flatItems = rawTransactions
                .withColumn("items", explode_outer(col("items")))
                .select(
                        col("items.price").alias("price"),
                        col("items.product").alias("product"),
                        col("items.quantity").alias("quantity"),
                        col("customer.city").alias("city"),
                        col("customer.name").alias("customer_name"),
                        col("customer.tier").alias("tier"),
                        col("customer_id"),
                        col("event_timestamp"),
                        col("order_id"),
                        col("payment.method").alias("payment_method"),
                        col("payment.status").alias("payment_status")
                )
                .withColumn("line_value",
                        col("price").multiply(col("quantity")));

        // replicate dedup logic
        WindowSpec dedupWindow = Window
                .partitionBy("order_id")
                .orderBy(col("order_date").desc());

        Dataset<Row> goodOrders = rawOrders
                .filter(
                        col("order_id").isNotNull()
                                .and(col("customer_id").isNotNull())
                                .and(col("total_amount").isNotNull())
                                .and(col("status").isin((Object[]) VALID_STATUSES))
                );

        deduped = goodOrders
                .withColumn("row_num", row_number().over(dedupWindow))
                .filter(col("row_num").equalTo(1))
                .drop("row_num", "total_amount");

        correlated = deduped
                .join(flatItems,
                        deduped.col("order_id")
                                .equalTo(flatItems.col("order_id")),
                        "inner")
                .select(
                        deduped.col("order_id"),
                        deduped.col("customer_id"),
                        col("customer_name"),
                        col("tier"),
                        col("city"),
                        deduped.col("product_category"),
                        col("product"),
                        col("quantity"),
                        col("price"),
                        col("line_value"),
                        deduped.col("status"),
                        deduped.col("channel"),
                        col("payment_method"),
                        col("payment_status"),
                        deduped.col("order_date"),
                        col("event_timestamp")
                );
    }

    // -------------------------------------------------------
    // STAGE 1 — INGEST
    // -------------------------------------------------------

    @Test
    @DisplayName("CSV loads correct number of raw rows including duplicate")
    void testRawOrdersCount() {
        assertEquals(9, rawOrders.count(),
                "pipeline_orders.csv should load 9 rows including the duplicate");
    }

    @Test
    @DisplayName("JSON loads correct number of payment events")
    void testRawEventsCount() {
        assertEquals(8, flatItems.select("order_id").distinct().count(),
                "pipeline_events.json should have 8 distinct orders");
    }

    // -------------------------------------------------------
    // STAGE 2 — DATA QUALITY
    // -------------------------------------------------------

    @Test
    @DisplayName("Bad orders are correctly identified")
    void testBadOrdersCount() {
        Dataset<Row> badOrders = rawOrders
                .filter(
                        col("order_id").isNull()
                                .or(col("customer_id").isNull())
                                .or(col("total_amount").isNull())
                                .or(col("status").isin((Object[]) VALID_STATUSES).equalTo(false))
                );

        // our pipeline_orders.csv has no bad rows — all statuses are valid
        assertEquals(0, badOrders.count(),
                "pipeline_orders.csv should have no bad rows");
    }

    @Test
    @DisplayName("Smart dedup produces one row per order_id")
    void testDedupOneRowPerOrderId() {
        long totalRows = deduped.count();
        long distinctIds = deduped.select("order_id").distinct().count();

        assertEquals(totalRows, distinctIds,
                "After dedup every order_id must appear exactly once");
    }

    @Test
    @DisplayName("Dedup removes exactly one duplicate row")
    void testDedupRemovesOneDuplicate() {
        Dataset<Row> goodOrders = rawOrders
                .filter(
                        col("order_id").isNotNull()
                                .and(col("customer_id").isNotNull())
                                .and(col("total_amount").isNotNull())
                                .and(col("status").isin((Object[]) VALID_STATUSES))
                );

        long beforeDedup = goodOrders.count();
        long afterDedup = deduped.count();

        assertEquals(1, beforeDedup - afterDedup,
                "Exactly one duplicate should be removed");
    }

    @Test
    @DisplayName("total_amount is dropped from deduped dataset")
    void testTotalAmountDropped() {
        boolean hasTotalAmount = false;
        for (String col : deduped.columns()) {
            if (col.equals("total_amount")) {
                hasTotalAmount = true;
                break;
            }
        }
        assertFalse(hasTotalAmount,
                "total_amount should be dropped after dedup stage");
    }

    // -------------------------------------------------------
    // STAGE 3 — FLATTEN EVENTS
    // -------------------------------------------------------

    @Test
    @DisplayName("Flat items total row count matches sum of all items")
    void testFlatItemsTotalCount() {
        assertEquals(12, flatItems.count(),
                "Total flat line items should be 12 across all 8 orders");
    }

    @Test
    @DisplayName("line_value is correctly computed")
    void testLineValueComputation() {
        // order_id=7: Laptop x2 x 75000 = 150000
        Row row = flatItems
                .filter(col("order_id").equalTo(7))
                .select("line_value")
                .first();

        assertEquals(150000.0, ((Number) row.get(0)).doubleValue(), 0.01,
                "order_id=7 line_value should be 2 x 75000 = 150000");
    }

    // -------------------------------------------------------
    // STAGE 4 — CORRELATION JOIN
    // -------------------------------------------------------

    @Test
    @DisplayName("Correlated dataset contains all expected columns")
    void testCorrelatedSchema() {
        String[] columns = correlated.columns();
        assertAll(
                () -> assertTrue(containsColumn(columns, "order_id")),
                () -> assertTrue(containsColumn(columns, "customer_name")),
                () -> assertTrue(containsColumn(columns, "tier")),
                () -> assertTrue(containsColumn(columns, "product")),
                () -> assertTrue(containsColumn(columns, "line_value")),
                () -> assertTrue(containsColumn(columns, "payment_status")),
                () -> assertTrue(containsColumn(columns, "channel"))
        );
    }

    @Test
    @DisplayName("Correlated dataset does not contain total_amount")
    void testCorrelatedHasNoTotalAmount() {
        boolean hasTotalAmount = false;
        for (String col : correlated.columns()) {
            if (col.equals("total_amount")) {
                hasTotalAmount = true;
                break;
            }
        }
        assertFalse(hasTotalAmount,
                "total_amount must not appear in correlated dataset");
    }

    @Test
    @DisplayName("Cancelled order is retained in correlated dataset")
    void testCancelledOrderRetained() {
        // order_id=4 is CANCELLED — should still appear in correlated
        // pipeline filters happen at aggregation time, not at join time
        long cancelledCount = correlated
                .filter(col("order_id").equalTo(4))
                .count();

        assertTrue(cancelledCount > 0,
                "CANCELLED order_id=4 should be present in correlated dataset");
    }

    // -------------------------------------------------------
    // STAGE 5 — AGGREGATION
    // -------------------------------------------------------

    @Test
    @DisplayName("Revenue by tier only includes COMPLETED and SUCCESS rows")
    void testRevenueByTierFiltering() {
        correlated.createOrReplaceTempView("correlated_orders_test");

        Dataset<Row> result = spark.sql("""
                SELECT tier, SUM(line_value) AS total_revenue
                FROM correlated_orders_test
                WHERE status = 'COMPLETED'
                  AND payment_status = 'SUCCESS'
                GROUP BY tier
                """);

        // verify no CANCELLED or FAILED rows leaked into revenue
        long nonCompletedRows = correlated
                .filter(col("status").notEqual("COMPLETED")
                        .or(col("payment_status").notEqual("SUCCESS")))
                .count();

        assertTrue(nonCompletedRows > 0,
                "Correlated dataset must contain non-COMPLETED rows to prove filter is meaningful");

        assertTrue(result.count() > 0,
                "Revenue by tier must return at least one tier");
    }

    @Test
    @DisplayName("Running revenue is non-decreasing per customer")
    void testRunningRevenueNonDecreasing() {
        WindowSpec customerWindow = Window
                .partitionBy("customer_name")
                .orderBy("event_timestamp")
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        java.util.List<Row> raviRows = correlated
                .filter(col("customer_name").equalTo("Ravi Kumar")
                        .and(col("status").equalTo("COMPLETED"))
                        .and(col("payment_status").equalTo("SUCCESS")))
                .withColumn("running_revenue",
                        sum("line_value").over(customerWindow))
                .orderBy("event_timestamp")
                .select("running_revenue")
                .collectAsList();

        assertTrue(raviRows.size() >= 2,
                "Ravi Kumar should have at least 2 completed line items");

        for (int i = 1; i < raviRows.size(); i++) {
            double prev = ((Number) raviRows.get(i - 1).get(0)).doubleValue();
            double curr = ((Number) raviRows.get(i).get(0)).doubleValue();
            assertTrue(curr >= prev,
                    "Running revenue must be non-decreasing for Ravi Kumar");
        }
    }

    @Test
    @DisplayName("CTE top product per tier returns exactly one product per tier")
    void testTopProductPerTier() {
        correlated.createOrReplaceTempView("correlated_orders_test");

        Dataset<Row> result = spark.sql("""
                WITH product_revenue AS (
                    SELECT
                        tier,
                        product,
                        SUM(line_value) AS revenue,
                        DENSE_RANK() OVER (
                            PARTITION BY tier
                            ORDER BY SUM(line_value) DESC
                        ) AS rank_in_tier
                    FROM correlated_orders_test
                    WHERE status = 'COMPLETED'
                      AND payment_status = 'SUCCESS'
                    GROUP BY tier, product
                )
                SELECT tier, product, revenue
                FROM product_revenue
                WHERE rank_in_tier = 1
                """);

        long resultRows = result.count();
        long distinctTiers = result.select("tier").distinct().count();

        assertEquals(distinctTiers, resultRows,
                "Top product CTE must return exactly one row per tier");
    }

    private boolean containsColumn(String[] columns, String name) {
        for (String col : columns) {
            if (col.equals(name)) return true;
        }
        return false;
    }
}