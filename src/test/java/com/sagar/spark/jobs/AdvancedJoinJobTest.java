package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class AdvancedJoinJobTest extends SparkTestBase {

    private Dataset<Row> orders;
    private Dataset<Row> customers;
    private Dataset<Row> products;

    @BeforeEach
    void setup() {
        spark.conf().set("spark.sql.shuffle.partitions", "4");
        spark.conf().set("spark.sql.adaptive.enabled", "true");

        orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/orders_skewed.csv");

        customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/customers.csv");

        products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/products.csv");
    }

    // -------------------------------------------------------
    // BROADCAST HASH JOIN
    // -------------------------------------------------------

    @Test
    @DisplayName("Broadcast join produces same row count as default join")
    void testBroadcastJoinRowCount() {
        long defaultCount = orders.join(
                customers,
                orders.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        ).count();

        long broadcastCount = orders.join(
                broadcast(customers),
                orders.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        ).count();

        assertEquals(defaultCount, broadcastCount,
                "Broadcast join must produce same row count as default join");
    }

    @Test
    @DisplayName("Broadcast join physical plan contains BroadcastHashJoin")
    void testBroadcastJoinPlan() {
        Dataset<Row> joined = orders.join(
                broadcast(customers),
                orders.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        );

        String plan = joined.queryExecution()
                .executedPlan()
                .toString();

        assertTrue(plan.contains("BroadcastHashJoin"),
                "Physical plan must contain BroadcastHashJoin when broadcast hint is used");
    }

    @Test
    @DisplayName("Broadcast join contains all expected columns")
    void testBroadcastJoinSchema() {
        Dataset<Row> joined = orders.join(
                broadcast(customers),
                orders.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        );

        String[] columns = joined.columns();
        assertAll(
                () -> assertTrue(containsColumn(columns, "order_id")),
                () -> assertTrue(containsColumn(columns, "customer_name")),
                () -> assertTrue(containsColumn(columns, "tier")),
                () -> assertTrue(containsColumn(columns, "product"))
        );
    }

    // -------------------------------------------------------
    // SKEW DETECTION
    // -------------------------------------------------------

    @Test
    @DisplayName("Skew is detectable — customer_id=1 has most orders")
    void testSkewDetection() {
        Row topCustomer = orders
                .groupBy("customer_id")
                .agg(count("order_id").alias("order_count"))
                .orderBy(col("order_count").desc())
                .first();

        assertEquals(1, topCustomer.getInt(0),
                "customer_id=1 should be the hot key with most orders");

        long hotKeyCount = topCustomer.getLong(1);
        assertTrue(hotKeyCount > 5,
                "Hot key customer_id=1 should have significantly more orders than others");
    }

    @Test
    @DisplayName("Skew ratio — hot key has significantly more rows than average")
    void testSkewRatio() {
        Dataset<Row> distribution = orders
                .groupBy("customer_id")
                .agg(count("order_id").alias("order_count"));

        Row maxRow = distribution
                .orderBy(col("order_count").desc())
                .first();

        double avgCount = ((Number) distribution
                .agg(avg("order_count"))
                .first().get(0)).doubleValue();

        double hotKeyCount = maxRow.getLong(1);
        double ratio = hotKeyCount / avgCount;

        assertTrue(ratio > 3.0,
                "Hot key should have at least 3x more rows than average — confirms skew");
    }

    // -------------------------------------------------------
    // MANUAL SALTING
    // -------------------------------------------------------

    @Test
    @DisplayName("Salted join produces same row count as regular join")
    void testSaltedJoinRowCount() {
        int saltBuckets = 3;

        Dataset<Row> saltedOrders = orders
                .withColumn("salt",
                        (rand().multiply(saltBuckets)).cast("int"))
                .withColumn("salted_customer_id",
                        concat(col("customer_id").cast("string"),
                                lit("_"),
                                col("salt").cast("string")));

        Dataset<Row> saltedCustomers = customers
                .withColumn("salt",
                        explode(array(lit(0), lit(1), lit(2))))
                .withColumn("salted_customer_id",
                        concat(col("customer_id").cast("string"),
                                lit("_"),
                                col("salt").cast("string")));

        Dataset<Row> saltedJoin = saltedOrders.join(
                saltedCustomers,
                saltedOrders.col("salted_customer_id")
                        .equalTo(saltedCustomers.col("salted_customer_id")),
                "inner"
        );

        long regularCount = orders.join(
                customers,
                orders.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        ).count();

        assertEquals(regularCount, saltedJoin.count(),
                "Salted join must produce same row count as regular join");
    }

    @Test
    @DisplayName("Salted orders have salt column values within bucket range")
    void testSaltColumnRange() {
        int saltBuckets = 3;

        Dataset<Row> saltedOrders = orders
                .withColumn("salt",
                        (rand().multiply(saltBuckets)).cast("int"));

        long invalidSalts = saltedOrders
                .filter(col("salt").lt(0).or(col("salt").geq(saltBuckets)))
                .count();

        assertEquals(0, invalidSalts,
                "All salt values must be within [0, saltBuckets) range");
    }

    @Test
    @DisplayName("Replicated customers have saltBuckets rows per customer")
    void testCustomerReplication() {
        int saltBuckets = 3;

        Dataset<Row> saltedCustomers = customers
                .withColumn("salt",
                        explode(array(lit(0), lit(1), lit(2))));

        long originalCount = customers.count();
        long replicatedCount = saltedCustomers.count();

        assertEquals(originalCount * saltBuckets, replicatedCount,
                "Each customer must be replicated exactly saltBuckets times");
    }

    // -------------------------------------------------------
    // BUCKETING
    // -------------------------------------------------------

    @Test
    @DisplayName("Bucketed table join produces correct row count")
    void testBucketedJoinRowCount() {
        orders.write()
                .mode(org.apache.spark.sql.SaveMode.Overwrite)
                .bucketBy(4, "customer_id")
                .sortBy("customer_id")
                .saveAsTable("test_bucketed_orders");

        customers.write()
                .mode(org.apache.spark.sql.SaveMode.Overwrite)
                .bucketBy(4, "customer_id")
                .sortBy("customer_id")
                .saveAsTable("test_bucketed_customers");

        long bucketedCount = spark.table("test_bucketed_orders")
                .join(spark.table("test_bucketed_customers"),
                        "customer_id")
                .count();

        long regularCount = orders.join(
                customers,
                orders.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        ).count();

        assertEquals(regularCount, bucketedCount,
                "Bucketed join must produce same row count as regular join");

        // cleanup
        spark.sql("DROP TABLE IF EXISTS test_bucketed_orders");
        spark.sql("DROP TABLE IF EXISTS test_bucketed_customers");
    }

    // -------------------------------------------------------
    // SQL JOIN HINTS
    // -------------------------------------------------------

    @Test
    @DisplayName("BROADCAST SQL hint produces BroadcastHashJoin in plan")
    void testSQLBroadcastHint() {
        orders.createOrReplaceTempView("orders_hint_test");
        customers.createOrReplaceTempView("customers_hint_test");

        Dataset<Row> result = spark.sql("""
                SELECT /*+ BROADCAST(c) */
                    o.order_id,
                    c.customer_name,
                    c.tier
                FROM orders_hint_test o
                JOIN customers_hint_test c
                ON o.customer_id = c.customer_id
                """);

        String plan = result.queryExecution()
                .executedPlan()
                .toString();

        assertTrue(plan.contains("BroadcastHashJoin"),
                "SQL BROADCAST hint must produce BroadcastHashJoin in physical plan");
    }

    @Test
    @DisplayName("SQL join hint and DataFrame broadcast produce same results")
    void testSQLHintAndDataFrameHintConsistency() {
        orders.createOrReplaceTempView("orders_consistency_test");
        customers.createOrReplaceTempView("customers_consistency_test");

        long sqlHintCount = spark.sql("""
                SELECT /*+ BROADCAST(c) */
                    o.order_id
                FROM orders_consistency_test o
                JOIN customers_consistency_test c
                ON o.customer_id = c.customer_id
                """).count();

        long dfHintCount = orders.join(
                broadcast(customers),
                orders.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        ).count();

        assertEquals(sqlHintCount, dfHintCount,
                "SQL hint and DataFrame broadcast hint must produce same row count");
    }

    private boolean containsColumn(String[] columns, String name) {
        for (String col : columns) {
            if (col.equals(name)) return true;
        }
        return false;
    }
}