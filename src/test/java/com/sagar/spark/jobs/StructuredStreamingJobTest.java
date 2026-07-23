package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;

import java.io.File;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class StructuredStreamingJobTest extends SparkTestBase {

    private static final String TEST_INPUT      = "data/streaming/test/input";
    private static final String TEST_OUTPUT     = "data/streaming/test/output";
    private static final String TEST_CHECKPOINT = "data/streaming/test/checkpoint";

    private StructType schema;

    @BeforeEach
    void setup() throws Exception {
        schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("transaction_id", DataTypes.LongType,   true),
                DataTypes.createStructField("customer_id",   DataTypes.LongType,   true),
                DataTypes.createStructField("amount",        DataTypes.DoubleType,  true),
                DataTypes.createStructField("status",        DataTypes.StringType,  true),
                DataTypes.createStructField("event_time",    DataTypes.StringType,  true),
        });

        // create test input directory
        new File(TEST_INPUT).mkdirs();
        new File(TEST_OUTPUT).mkdirs();
        new File(TEST_CHECKPOINT).mkdirs();

        // write test data file
        String testData =
                "{\"transaction_id\":1,\"customer_id\":1,\"amount\":75000.0," +
                        "\"status\":\"SUCCESS\",\"event_time\":\"2024-01-15 10:00:00\"}\n" +
                        "{\"transaction_id\":2,\"customer_id\":2,\"amount\":25000.0," +
                        "\"status\":\"SUCCESS\",\"event_time\":\"2024-01-15 10:01:00\"}\n" +
                        "{\"transaction_id\":3,\"customer_id\":1,\"amount\":15000.0," +
                        "\"status\":\"FAILED\",\"event_time\":\"2024-01-15 10:02:00\"}\n" +
                        "{\"transaction_id\":4,\"customer_id\":3,\"amount\":120000.0," +
                        "\"status\":\"SUCCESS\",\"event_time\":\"2024-01-15 10:03:00\"}\n" +
                        "{\"transaction_id\":5,\"customer_id\":2,\"amount\":8000.0," +
                        "\"status\":\"SUCCESS\",\"event_time\":\"2024-01-15 10:04:00\"}";

        java.nio.file.Files.write(
                java.nio.file.Paths.get(TEST_INPUT + "/test_001.json"),
                testData.getBytes()
        );
    }

    @AfterEach
    void cleanup() throws Exception {
        // stop any active streams
        spark.streams().active();
        for (StreamingQuery q : spark.streams().active()) {
            q.stop();
        }

        // cleanup test directories
        try {
            org.apache.commons.io.FileUtils
                    .deleteDirectory(new File("data/streaming/test"));
        } catch (Exception e) {
            // best effort
        }
    }

    // -------------------------------------------------------
    // SCHEMA AND READ
    // -------------------------------------------------------

    @Test
    @DisplayName("Streaming DataFrame is created with correct schema")
    void testStreamingSchema() {
        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .option("maxFilesPerTrigger", "1")
                .json(TEST_INPUT);

        assertTrue(stream.isStreaming(),
                "Dataset created with readStream must be a streaming Dataset");

        String[] columns = stream.columns();
        assertAll(
                () -> assertTrue(containsColumn(columns, "transaction_id")),
                () -> assertTrue(containsColumn(columns, "customer_id")),
                () -> assertTrue(containsColumn(columns, "amount")),
                () -> assertTrue(containsColumn(columns, "status")),
                () -> assertTrue(containsColumn(columns, "event_time"))
        );
    }

    @Test
    @DisplayName("Streaming Dataset isStreaming returns true")
    void testIsStreaming() {
        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .json(TEST_INPUT);

        assertTrue(stream.isStreaming(),
                "readStream() must produce a streaming Dataset");
    }

    @Test
    @DisplayName("Batch Dataset isStreaming returns false")
    void testBatchIsNotStreaming() {
        Dataset<Row> batch = spark.read()
                .schema(schema)
                .json(TEST_INPUT);

        assertFalse(batch.isStreaming(),
                "read() must produce a non-streaming Dataset");
    }

    // -------------------------------------------------------
    // STATELESS TRANSFORMATIONS
    // Verified by running stream and reading output
    // -------------------------------------------------------

    @Test
    @DisplayName("SUCCESS filter writes only successful transactions")
    void testSuccessFilter() throws Exception {
        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .option("maxFilesPerTrigger", "1")
                .json(TEST_INPUT);

        StreamingQuery query = stream
                .filter(col("status").equalTo("SUCCESS"))
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", TEST_OUTPUT + "/success")
                .option("checkpointLocation", TEST_CHECKPOINT + "/success")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination(10000);
        query.stop();

        // read output and verify
        Dataset<Row> result = spark.read()
                .schema(schema)
                .json(TEST_OUTPUT + "/success");

        long failedCount = result
                .filter(col("status").equalTo("FAILED"))
                .count();

        assertEquals(0, failedCount,
                "No FAILED transactions should appear in SUCCESS filtered output");

        assertTrue(result.count() > 0,
                "At least one SUCCESS transaction should be written");
    }

    @Test
    @DisplayName("Fraud detection flags transactions above threshold")
    void testFraudDetection() throws Exception {
        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .option("maxFilesPerTrigger", "1")
                .json(TEST_INPUT);

        StreamingQuery query = stream
                .filter(col("amount").geq(100000)
                        .and(col("status").equalTo("SUCCESS")))
                .withColumn("alert_type", lit("HIGH_VALUE_TRANSACTION"))
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", TEST_OUTPUT + "/fraud")
                .option("checkpointLocation", TEST_CHECKPOINT + "/fraud")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination(10000);
        query.stop();

        Dataset<Row> alerts = spark.read()
                .json(TEST_OUTPUT + "/fraud");

        // transaction_id=4 has amount=120000 — should be flagged
        long flaggedCount = alerts
                .filter(col("transaction_id").equalTo(4))
                .count();

        assertEquals(1, flaggedCount,
                "transaction_id=4 with amount=120000 should be flagged as fraud alert");
    }

    @Test
    @DisplayName("is_large_transaction flag correctly identifies high value transactions")
    void testLargeTransactionFlag() throws Exception {
        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .option("maxFilesPerTrigger", "1")
                .json(TEST_INPUT);

        StreamingQuery query = stream
                .filter(col("status").equalTo("SUCCESS"))
                .withColumn("is_large_transaction",
                        col("amount").geq(50000))
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", TEST_OUTPUT + "/flagged")
                .option("checkpointLocation", TEST_CHECKPOINT + "/flagged")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination(10000);
        query.stop();

        Dataset<Row> result = spark.read()
                .json(TEST_OUTPUT + "/flagged");

        // transaction_id=1 amount=75000 → is_large_transaction=true
        Row largeTx = result
                .filter(col("transaction_id").equalTo(1))
                .select("is_large_transaction")
                .first();

        assertTrue(largeTx.getBoolean(0),
                "transaction_id=1 with amount=75000 should be flagged as large");
    }

    @Test
    @DisplayName("Timestamp conversion produces non-null event_timestamp")
    void testTimestampConversion() throws Exception {
        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .option("maxFilesPerTrigger", "1")
                .json(TEST_INPUT);

        StreamingQuery query = stream
                .withColumn("event_timestamp",
                        to_timestamp(col("event_time"),
                                "yyyy-MM-dd HH:mm:ss"))
                .filter(col("event_timestamp").isNotNull())
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", TEST_OUTPUT + "/timestamps")
                .option("checkpointLocation", TEST_CHECKPOINT + "/timestamps")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination(10000);
        query.stop();

        Dataset<Row> result = spark.read()
                .json(TEST_OUTPUT + "/timestamps");

        long nullTimestamps = result
                .filter(col("event_timestamp").isNull())
                .count();

        assertEquals(0, nullTimestamps,
                "All valid event_time strings must convert to non-null timestamps");
    }

    // -------------------------------------------------------
    // STREAMING QUERY MANAGEMENT
    // -------------------------------------------------------

    @Test
    @DisplayName("StreamingQuery is active after start")
    void testQueryIsActiveAfterStart() throws Exception {
        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .json(TEST_INPUT);

        StreamingQuery query = stream
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", TEST_OUTPUT + "/active")
                .option("checkpointLocation", TEST_CHECKPOINT + "/active")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        assertTrue(query.isActive(),
                "StreamingQuery must be active immediately after start");

        query.stop();

        assertFalse(query.isActive(),
                "StreamingQuery must be inactive after stop");
    }

    @Test
    @DisplayName("Multiple streaming queries can run concurrently")
    void testMultipleQueriesConcurrently() throws Exception {
        Dataset<Row> stream1 = spark.readStream()
                .schema(schema)
                .json(TEST_INPUT);

        Dataset<Row> stream2 = spark.readStream()
                .schema(schema)
                .json(TEST_INPUT);

        StreamingQuery q1 = stream1
                .filter(col("status").equalTo("SUCCESS"))
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", TEST_OUTPUT + "/q1")
                .option("checkpointLocation", TEST_CHECKPOINT + "/q1")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        StreamingQuery q2 = stream2
                .filter(col("amount").geq(50000))
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", TEST_OUTPUT + "/q2")
                .option("checkpointLocation", TEST_CHECKPOINT + "/q2")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        assertTrue(q1.isActive(), "Query 1 must be active");
        assertTrue(q2.isActive(), "Query 2 must be active");
        assertEquals(2, spark.streams().active().length,
                "Exactly 2 streaming queries must be active");

        q1.stop();
        q2.stop();
    }

    private boolean containsColumn(String[] columns, String name) {
        for (String col : columns) {
            if (col.equals(name)) return true;
        }
        return false;
    }
}