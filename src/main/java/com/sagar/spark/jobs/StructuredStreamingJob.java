package com.sagar.spark.jobs;

import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class StructuredStreamingJob {

    private static final String INPUT_PATH  = "data/streaming/input";
    private static final String OUTPUT_PATH = "data/streaming/output";
    private static final String CHECKPOINT  = "data/streaming/checkpoint";

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSessionFactory.get();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("transaction_id", DataTypes.LongType,   true),
                DataTypes.createStructField("customer_id",   DataTypes.LongType,   true),
                DataTypes.createStructField("amount",        DataTypes.DoubleType,  true),
                DataTypes.createStructField("status",        DataTypes.StringType,  true),
                DataTypes.createStructField("event_time",    DataTypes.StringType,  true),
        });

        Dataset<Row> stream = spark.readStream()
                .schema(schema)
                .option("maxFilesPerTrigger", "1")
                .json(INPUT_PATH);

        Dataset<Row> withTimestamp = stream
                .withColumn("event_timestamp",
                        to_timestamp(col("event_time"),
                                "yyyy-MM-dd HH:mm:ss")
                );

        Dataset<Row> successTransactions = withTimestamp
                .filter(col("status").equalTo("SUCCESS"))
                .withColumn("is_large_transaction",
                        col("amount").geq(50000)
                );

        System.out.println("============================ STARTING STREAM 1: APPEND MODE ========================================");

        StreamingQuery appendQuery = successTransactions
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", OUTPUT_PATH + "/success_transactions")
                .option("checkpointLocation", CHECKPOINT + "/append")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        System.out.println("=========================== STARTING STREAM 2: WINDOWED AGGREGATION =================================");

        Dataset<Row> windowedCounts = withTimestamp
                .withWatermark("event_timestamp", "2 minutes")
                .groupBy(
                        window(col("event_timestamp"), "5 minutes")
                )
                .agg(
                        count("transaction_id").alias("transaction_count"),
                        sum("amount").alias("total_amount"),
                        max("amount").alias("max_amount")
                );

        StreamingQuery windowQuery = windowedCounts
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", CHECKPOINT + "/window")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        System.out.println("=========================== STARTING STREAM 3: FRAUD DETECTION =====================================");

        Dataset<Row> fraudAlerts = withTimestamp
                .filter(col("amount").geq(100000)
                        .and(col("status").equalTo("SUCCESS")))
                .withColumn("alert_type", lit("HIGH_VALUE_TRANSACTION"))
                .withColumn("alert_time", current_timestamp())
                .select(
                        "transaction_id",
                        "customer_id",
                        "amount",
                        "event_timestamp",
                        "alert_type",
                        "alert_time"
                );

        StreamingQuery fraudQuery = fraudAlerts
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", OUTPUT_PATH + "/fraud_alerts")
                .option("checkpointLocation", CHECKPOINT + "/fraud")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        System.out.println("===================================== STREAMING QUERIES RUNNING =====================================");
        System.out.println("Active queries: " + spark.streams().active().length + "     <=====================================");

        Thread.sleep(30000);

        System.out.println("\n===================================== STREAM PROGRESS =====================================");
        System.out.println("Append query progress: " + appendQuery.lastProgress());

        System.out.println("Window query progress: " + windowQuery.lastProgress());

        System.out.println("Fraud query progress: " + fraudQuery.lastProgress());

        // Stop all queries gracefully
        appendQuery.stop();
        windowQuery.stop();
        fraudQuery.stop();

        System.out.println("All streaming queries stopped......................................");

        SparkSessionFactory.stop();

    }
}