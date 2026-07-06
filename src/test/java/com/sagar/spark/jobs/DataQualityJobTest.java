package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class DataQualityJobTest extends SparkTestBase {

    private static final String[] VALID_STATUS = {"COMPLETED", "PENDING", "CANCELLED"};
    private Dataset<Row> raw;

    @BeforeEach
    void loadDirtyData() {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("order_id",    DataTypes.IntegerType, true),
                DataTypes.createStructField("customer_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("product",     DataTypes.StringType,  true),
                DataTypes.createStructField("quantity",    DataTypes.IntegerType, true),
                DataTypes.createStructField("price",       DataTypes.DoubleType,  true),
                DataTypes.createStructField("status",      DataTypes.StringType,  true),
                DataTypes.createStructField("order_date",  DataTypes.StringType,  true),
        });

        raw = spark.read()
                .option("header", "true")
                .option("mode", "PERMISSIVE")
                .schema(schema)
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/orders_dirty.csv");
    }

    @Test
    @DisplayName("Raw dirty file loads all 12 rows including bad ones")
    void testRawRowCount() {
        assertEquals(12, raw.count(),
                "Raw file should load all 12 rows, including dirty ones");
    }

    @Test
    @DisplayName("Null audit correctly counts nulls per column")
    void testNullAudit() {
        long nullCustomerIds = raw.filter(col("customer_id").isNull()).count();
        long nullProducts = raw.filter(col("product").isNull()).count();
        long nullPrices = raw.filter(col("price").isNull()).count();

        assertEquals(1, nullCustomerIds, "Should have exactly 1 null customer_id");
        assertEquals(1, nullProducts, "Should have exactly 1 null product");
        assertEquals(1, nullPrices, "Should have exactly 1 null price");
    }

    @Test
    @DisplayName("Duplicate order_id is detected correctly")
    void testDuplicateDetection() {
        Dataset<Row> duplicates = raw
                .groupBy("order_id")
                .agg(count("order_id").alias("occurrence_count"))
                .filter(col("occurrence_count").gt(1));

        assertEquals(1, duplicates.count(),
                "Should find exactly 1 order_id with duplicates");

        Row dup = duplicates.first();
        assertEquals(1, dup.getInt(0),
                "The duplicated order_id should be 1");
        assertEquals(2L, dup.getLong(1),
                "order_id=1 should appear exactly twice");
    }

    @Test
    @DisplayName("Orphaned customer_id is correctly identified via left anti join")
    void testOrphanedForeignKey() {
        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/customers.csv");

        Dataset<Row> orphaned = raw
                .join(customers,
                        raw.col("customer_id").equalTo(customers.col("customer_id")),
                        "left_anti");

        // customer_id=999 doesn't exist in customers.csv
        long orphanedWith999 = orphaned
                .filter(col("customer_id").equalTo(999))
                .count();

        assertEquals(1, orphanedWith999,
                "order with customer_id=999 should be flagged as orphaned");
    }

    @Test
    @DisplayName("Good and bad record split adds up to total minus null customer_id rows excluded from join")
    void testGoodBadSplitCompleteness() {
        Dataset<Row> badRecords = raw
                .filter(
                        col("customer_id").isNull()
                                .or(col("product").isNull())
                                .or(col("price").isNull())
                                .or(col("quantity").lt(0))
                                .or(col("status").isin((Object[]) VALID_STATUS).equalTo(false))
                );

        Dataset<Row> goodRecords = raw
                .filter(
                        col("customer_id").isNotNull()
                                .and(col("product").isNotNull())
                                .and(col("price").isNotNull())
                                .and(col("quantity").isNotNull().and(col("quantity").geq(0)))
                                .and(col("status").isin((Object[]) VALID_STATUS))
                );

        // every row must be classified as either good or bad — none left out
        assertEquals(raw.count(), badRecords.count() + goodRecords.count(),
                "Every row must be classified as either good or bad");
    }

    @Test
    @DisplayName("Deduplication removes exactly one duplicate row from good records")
    void testDeduplicationRemovesOneRow() {
        Dataset<Row> goodRecords = raw
                .filter(
                        col("customer_id").isNotNull()
                                .and(col("product").isNotNull())
                                .and(col("price").isNotNull())
                                .and(col("quantity").isNotNull().and(col("quantity").geq(0)))
                                .and(col("status").isin((Object[]) VALID_STATUS))
                );

        Dataset<Row> deduped = goodRecords.dropDuplicates("order_id");

        assertEquals(1, goodRecords.count() - deduped.count(),
                "Deduplication should remove exactly 1 duplicate row");
    }

    @Test
    @DisplayName("Final clean record count matches expected business rule")
    void testFinalCleanRecordCount() {
        Dataset<Row> goodRecords = raw
                .filter(
                        col("customer_id").isNotNull()
                                .and(col("product").isNotNull())
                                .and(col("price").isNotNull())
                                .and(col("quantity").isNotNull().and(col("quantity").geq(0)))
                                .and(col("status").isin((Object[]) VALID_STATUS))
                );

        Dataset<Row> deduped = goodRecords.dropDuplicates("order_id");

        assertEquals(6, deduped.count(),
                "Final clean dataset should have exactly 6 rows after quality filtering and dedup");
    }
}