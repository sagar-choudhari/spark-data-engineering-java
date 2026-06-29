package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TempViewJobTest extends SparkTestBase {

    @BeforeEach
    void registerViews() {
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/dataFiles/csv/input/orders_v2.csv")
                .createOrReplaceTempView("orders_session");

        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/dataFiles/csv/input/orders_v2.csv")
                .createOrReplaceGlobalTempView("orders_global");
    }

    @Test
    @DisplayName("Session temp view is queryable by name")
    void testSessionTempView() {
        Dataset<Row> result = spark.sql(
                "SELECT COUNT(*) AS row_count FROM orders_session");
        long count = result.first().getLong(0);
        assertEquals(12, count,
                "Session temp view should expose all 12 rows");
    }

    @Test
    @DisplayName("Global temp view is queryable via global_temp prefix")
    void testGlobalTempView() {
        Dataset<Row> result = spark.sql(
                "SELECT COUNT(*) AS row_count FROM global_temp.orders_global");
        long count = result.first().getLong(0);
        assertEquals(12, count,
                "Global temp view should expose all 12 rows via global_temp prefix");
    }

    @Test
    @DisplayName("Dropped session view is no longer queryable")
    void testDropSessionView() {
        spark.catalog().dropTempView("orders_session");

        // querying a dropped view must throw an exception
        assertThrows(Exception.class, () ->
                        spark.sql("SELECT * FROM orders_session").show(),
                "Querying a dropped temp view must throw an exception"
        );
    }

    @Test
    @DisplayName("Session view and global view return same row count")
    void testSessionAndGlobalViewConsistency() {
        long sessionCount = spark.sql(
                        "SELECT COUNT(*) FROM orders_session")
                .first().getLong(0);

        long globalCount = spark.sql(
                        "SELECT COUNT(*) FROM global_temp.orders_global")
                .first().getLong(0);

        assertEquals(sessionCount, globalCount,
                "Session and global views over same data must have equal row counts");
    }
}