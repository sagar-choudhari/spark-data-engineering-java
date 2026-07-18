package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.*;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class DeltaLakeJobTest extends SparkTestBase {

    private static final String DELTA_PATH = "data/output/delta/orders_test";

    private Dataset<Row> v1;
    private Dataset<Row> v2;

    @BeforeEach
    void setup() {
        v1 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/delta_orders_v1.csv");

        v2 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/delta_orders_v2.csv");

        // fresh Delta table before each test
        v1.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .save(DELTA_PATH);
    }

    @AfterEach
    void cleanup() {
        // drop delta table after each test
        try {
            org.apache.commons.io.FileUtils.deleteDirectory(
                    new java.io.File(DELTA_PATH));
        } catch (Exception e) {
            // best effort cleanup
        }
    }

    // -------------------------------------------------------
    // INITIAL WRITE
    // -------------------------------------------------------

    @Test
    @DisplayName("Initial Delta write loads correct row count")
    void testInitialWrite() {
        Dataset<Row> result = spark.read()
                .format("delta")
                .load(DELTA_PATH);

        assertEquals(5, result.count(),
                "Initial Delta write should contain 5 orders");
    }

    @Test
    @DisplayName("Initial write all orders have PENDING status")
    void testInitialWriteAllPending() {
        long nonPending = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .filter(col("status").notEqual("PENDING"))
                .count();

        assertEquals(0, nonPending,
                "All orders in v1 should have PENDING status");
    }

    // -------------------------------------------------------
    // APPEND
    // -------------------------------------------------------

    @Test
    @DisplayName("Append adds new records without affecting existing ones")
    void testAppend() {
        // append only new orders (order_id 6 and 7)
        v2.filter(col("order_id").gt(5))
                .write()
                .format("delta")
                .mode(SaveMode.Append)
                .save(DELTA_PATH);

        Dataset<Row> result = spark.read()
                .format("delta")
                .load(DELTA_PATH);

        assertEquals(7, result.count(),
                "After append should have 7 orders");
    }

    @Test
    @DisplayName("Append preserves existing records unchanged")
    void testAppendPreservesExisting() {
        v2.filter(col("order_id").gt(5))
                .write()
                .format("delta")
                .mode(SaveMode.Append)
                .save(DELTA_PATH);

        // original order_id=1 must still be PENDING — not updated by append
        Row order1 = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .filter(col("order_id").equalTo(1))
                .select("status")
                .first();

        assertEquals("PENDING", order1.getString(0),
                "Append must not modify existing records — order_id=1 must remain PENDING");
    }

    // -------------------------------------------------------
    // MERGE (UPSERT)
    // -------------------------------------------------------

    @Test
    @DisplayName("Merge updates existing records correctly")
    void testMergeUpdatesExisting() {
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);

        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        // order_id=1 should now be COMPLETED
        Row order1 = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .filter(col("order_id").equalTo(1))
                .select("status")
                .first();

        assertEquals("COMPLETED", order1.getString(0),
                "order_id=1 should be COMPLETED after merge");
    }

    @Test
    @DisplayName("Merge inserts new records not in target")
    void testMergeInsertsNew() {
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);

        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        // order_id=6 and 7 are new — must exist after merge
        long newOrders = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .filter(col("order_id").geq(6))
                .count();

        assertEquals(2, newOrders,
                "Merge should insert order_id=6 and 7 as new records");
    }

    @Test
    @DisplayName("Merge produces correct total row count")
    void testMergeTotalCount() {
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);

        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        // 5 original + 2 new (order_id 6,7) = 7
        assertEquals(7, spark.read()
                        .format("delta")
                        .load(DELTA_PATH)
                        .count(),
                "After merge should have 7 total orders");
    }

    @Test
    @DisplayName("Merge preserves unmatched target records")
    void testMergePreservesUnmatchedTarget() {
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);

        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        // order_id=4 and 5 not in v2 — must remain PENDING unchanged
        long unchangedCount = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .filter(col("order_id").isin(4, 5)
                        .and(col("status").equalTo("PENDING")))
                .count();

        assertEquals(2, unchangedCount,
                "order_id=4 and 5 not in source — must remain PENDING");
    }

    // -------------------------------------------------------
    // DELETE
    // -------------------------------------------------------

    @Test
    @DisplayName("Delete removes CANCELLED orders correctly")
    void testDelete() {
        // first merge to get CANCELLED order
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);
        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        // delete CANCELLED
        DeltaTable.forPath(spark, DELTA_PATH)
                .delete(col("status").equalTo("CANCELLED"));

        long cancelledCount = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .filter(col("status").equalTo("CANCELLED"))
                .count();

        assertEquals(0, cancelledCount,
                "No CANCELLED orders should remain after delete");
    }

    // -------------------------------------------------------
    // TIME TRAVEL
    // -------------------------------------------------------

    @Test
    @DisplayName("Time travel version 0 returns initial 5 PENDING orders")
    void testTimeTravelVersion0() {
        // perform a merge to create version 1
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);
        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        // time travel back to version 0
        Dataset<Row> version0 = spark.read()
                .format("delta")
                .option("versionAsOf", "0")
                .load(DELTA_PATH);

        assertEquals(5, version0.count(),
                "Version 0 should have exactly 5 orders");

        long pendingCount = version0
                .filter(col("status").equalTo("PENDING"))
                .count();

        assertEquals(5, pendingCount,
                "All orders in version 0 should be PENDING");
    }

    @Test
    @DisplayName("Time travel version 1 reflects merge changes")
    void testTimeTravelVersion1() {
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);
        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        Dataset<Row> version1 = spark.read()
                .format("delta")
                .option("versionAsOf", "1")
                .load(DELTA_PATH);

        // version 1 should have 7 orders after merge
        assertEquals(7, version1.count(),
                "Version 1 should have 7 orders after merge");

        // order_id=1 should be COMPLETED in version 1
        Row order1 = version1
                .filter(col("order_id").equalTo(1))
                .select("status")
                .first();

        assertEquals("COMPLETED", order1.getString(0),
                "order_id=1 should be COMPLETED in version 1");
    }

    // -------------------------------------------------------
    // TRANSACTION HISTORY
    // -------------------------------------------------------

    @Test
    @DisplayName("Transaction history records all operations")
    void testTransactionHistory() {
        // perform merge and delete
        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);
        target.as("target")
                .merge(v2.as("source"),
                        "target.order_id = source.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        DeltaTable.forPath(spark, DELTA_PATH)
                .delete(col("status").equalTo("CANCELLED"));

        Dataset<Row> history = DeltaTable
                .forPath(spark, DELTA_PATH)
                .history();

        // should have 3 versions: WRITE, MERGE, DELETE
        assertTrue(history.count() >= 3,
                "History should contain at least 3 operations");

        // verify operation names present
        long mergeOps = history
                .filter(col("operation").equalTo("MERGE"))
                .count();

        assertEquals(1, mergeOps,
                "History should contain exactly one MERGE operation");
    }

    // -------------------------------------------------------
    // SCHEMA EVOLUTION
    // -------------------------------------------------------

    @Test
    @DisplayName("Schema evolution adds new column successfully")
    void testSchemaEvolution() {
        Dataset<Row> withNewColumn = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .withColumn("discount_pct", lit(0.0));

        withNewColumn.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .option("overwriteSchema", "true")
                .save(DELTA_PATH);

        Dataset<Row> evolved = spark.read()
                .format("delta")
                .load(DELTA_PATH);

        boolean hasDiscountColumn = false;
        for (String col : evolved.columns()) {
            if (col.equals("discount_pct")) {
                hasDiscountColumn = true;
                break;
            }
        }

        assertTrue(hasDiscountColumn,
                "Evolved schema must contain discount_pct column");
    }

    @Test
    @DisplayName("Schema evolution preserves existing row count")
    void testSchemaEvolutionPreservesRows() {
        long beforeCount = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .count();

        Dataset<Row> withNewColumn = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .withColumn("discount_pct", lit(0.0));

        withNewColumn.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .option("overwriteSchema", "true")
                .save(DELTA_PATH);

        long afterCount = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .count();

        assertEquals(beforeCount, afterCount,
                "Schema evolution must not change row count");
    }
}