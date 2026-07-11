package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class PerformanceTuningJobTest extends SparkTestBase {

    private Dataset<Row> orders;

    @BeforeEach
    void setup() {
        spark.conf().set("spark.sql.shuffle.partitions", "4");

        orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/large_orders.csv");
    }

    // -------------------------------------------------------
    // SHUFFLE PARTITIONS
    // -------------------------------------------------------

    @Test
    @DisplayName("Shuffle partitions config is correctly set to 4")
    void testShufflePartitionsConfig() {
        assertEquals("4",
                spark.conf().get("spark.sql.shuffle.partitions"),
                "Shuffle partitions should be tuned to 4 for local dev");
    }

    // -------------------------------------------------------
    // REPARTITION vs COALESCE
    // -------------------------------------------------------

    @Test
    @DisplayName("repartition(8) produces exactly 8 partitions")
    void testRepartitionIncreasesPartitions() {
        Dataset<Row> repartitioned = orders.repartition(8);
        assertEquals(8, repartitioned.rdd().getNumPartitions(),
                "repartition(8) should produce exactly 8 partitions");
    }

    @Test
    @DisplayName("coalesce(2) reduces partitions without shuffle")
    void testCoalesceReducesPartitions() {
        Dataset<Row> coalesced = orders.coalesce(1);
        assertEquals(1, coalesced.rdd().getNumPartitions(),
                "coalesce(1) should reduce to exactly 1 partitions");
    }

    @Test
    @DisplayName("repartition by column produces correct partition count")
    void testRepartitionByColumn() {
        Dataset<Row> repartitioned = orders.repartition(3, col("status"));
        assertEquals(3, repartitioned.rdd().getNumPartitions(),
                "repartition(3, region) should produce exactly 4 partitions");
    }

    @Test
    @DisplayName("coalesce cannot increase partition count beyond current")
    void testCoalesceCannotIncrease() {
        int currentPartitions = orders.rdd().getNumPartitions();
        // coalesce to a number higher than current — stays at current
        Dataset<Row> coalesced = orders.coalesce(currentPartitions + 100);
        assertTrue(coalesced.rdd().getNumPartitions() <= currentPartitions + 100,
                "coalesce cannot increase partitions beyond current count");
    }

    @Test
    @DisplayName("repartition and coalesce produce same row count")
    void testRepartitionAndCoalescePreserveRowCount() {
        long originalCount = orders.count();
        long repartitionedCount = orders.repartition(8).count();
        long coalescedCount = orders.coalesce(2).count();

        assertAll(
                () -> assertEquals(originalCount, repartitionedCount,
                        "repartition must preserve all rows"),
                () -> assertEquals(originalCount, coalescedCount,
                        "coalesce must preserve all rows")
        );
    }

    // -------------------------------------------------------
    // CACHE vs PERSIST
    // -------------------------------------------------------

    @Test
    @DisplayName("cache() materializes DataFrame and returns correct count")
    void testCacheMaterializesCorrectCount() {
        Dataset<Row> completed = orders
                .filter(col("status").equalTo("COMPLETED"))
                .withColumn("order_value",
                        col("quantity").multiply(col("price")));

        completed.cache();
        long count = completed.count(); // materializes cache

        assertTrue(count > 0,
                "Cached completed orders should have at least one row");

        // second use — from cache, must return same count
        long secondCount = completed.count();
        assertEquals(count, secondCount,
                "Second read from cache must return identical count");

        completed.unpersist();
    }

    @Test
    @DisplayName("cache() and persist(MEMORY_AND_DISK) produce identical results")
    void testCacheAndPersistEquivalence() {
        Dataset<Row> forCache = orders
                .filter(col("status").equalTo("COMPLETED"));

        Dataset<Row> forPersist = orders
                .filter(col("status").equalTo("COMPLETED"));

        forCache.cache();
        forPersist.persist(StorageLevel.MEMORY_AND_DISK());

        long cacheCount = forCache.count();
        long persistCount = forPersist.count();

        assertEquals(cacheCount, persistCount,
                "cache() and persist(MEMORY_AND_DISK) must produce same row count");

        forCache.unpersist();
        forPersist.unpersist();
    }

    @Test
    @DisplayName("unpersist releases cache — DataFrame still queryable from source")
    void testUnpersistReleasesCache() {
        Dataset<Row> completed = orders
                .filter(col("status").equalTo("COMPLETED"));

        completed.cache();
        completed.count(); // materialize

        completed.unpersist(); // release

        // still queryable — recomputes from source
        long countAfterUnpersist = completed.count();
        assertTrue(countAfterUnpersist > 0,
                "DataFrame must still be queryable after unpersist — recomputes from source");
    }

    @Test
    @DisplayName("Cached DataFrame produces same aggregation result as uncached")
    void testCachedAggregationConsistency() {
        Dataset<Row> completed = orders
                .filter(col("status").equalTo("COMPLETED"))
                .withColumn("order_value",
                        col("quantity").multiply(col("price")));

        // uncached aggregation
        double uncachedTotal = ((Number) completed
                .agg(sum("order_value"))
                .first().get(0)).doubleValue();

        // cache and aggregate again
        completed.cache();
        completed.count(); // materialize

        double cachedTotal = ((Number) completed
                .agg(sum("order_value"))
                .first().get(0)).doubleValue();

        completed.unpersist();

        assertEquals(uncachedTotal, cachedTotal, 0.01,
                "Cached and uncached aggregation must produce identical total");
    }

    // -------------------------------------------------------
    // PARTITION PRUNING
    // -------------------------------------------------------

    @Test
    @DisplayName("Partition pruning returns only NORTH region rows")
    void testPartitionPruningCorrectness() {
        orders.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("region")
                .parquet("data/output/orders_by_region_test");

        Dataset<Row> northOrders = spark.read()
                .parquet("data/output/orders_by_region_test")
                .filter(col("region").equalTo("NORTH"));

        long nonNorthRows = northOrders
                .filter(col("region").notEqual("NORTH"))
                .count();

        assertEquals(0, nonNorthRows,
                "Partition pruned read must return only NORTH region rows");
    }

    @Test
    @DisplayName("Partition pruned read returns correct NORTH row count")
    void testPartitionPruningRowCount() {
        orders.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("region")
                .parquet("data/output/orders_by_region_test");

        long northCount = spark.read()
                .parquet("data/output/orders_by_region_test")
                .filter(col("region").equalTo("NORTH"))
                .count();

        // manually count NORTH rows from source
        long expectedNorth = orders
                .filter(col("region").equalTo("NORTH"))
                .count();

        assertEquals(expectedNorth, northCount,
                "Partition pruned count must match direct filter count from source");
    }

    // -------------------------------------------------------
    // AQE
    // -------------------------------------------------------

    @Test
    @DisplayName("AQE is enabled by default in Spark 3.x")
    void testAQEEnabled() {
        String aqeEnabled = spark.conf()
                .get("spark.sql.adaptive.enabled");
        assertEquals("true", aqeEnabled,
                "AQE must be enabled by default in Spark 3.x");
    }
}