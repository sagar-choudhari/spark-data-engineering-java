package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class DeltaLakeJob {
    private static final String CSV_PATH_DELTA_ORDERS_V1 = AppConfig.getFilePath("csv.path.delta_orders_v1");
    private static final String CSV_PATH_DELTA_ORDERS_V2 = AppConfig.getFilePath("csv.path.delta_orders_v2");
    private static final String DELTA_PATH = AppConfig.getFilePath("delta.path.delta_orders");

    public static void main (String[] args) throws InterruptedException {

        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> ordersV1 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_DELTA_ORDERS_V1);

        Dataset<Row> ordersV2 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_DELTA_ORDERS_V2);

        System.out.println("========================== INITIAL-DELTA-WRITE ==========================");

        ordersV1.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .save(DELTA_PATH);

        System.out.println("Initial write complete to Delta lake........................."+"\nReading Back............................................");

        Dataset<Row> initial = spark.read()
                .format("delta")
                .load(DELTA_PATH);

        initial.show();

        System.out.println("========================== APPEND-NEW-RECORDS ==========================");
        ordersV2.filter(col("order_id").gt(5))
                .write()
                .format("delta")
                .mode(SaveMode.Append)
                .save(DELTA_PATH);

        System.out.println("After Append............................................");

        spark.read().format("delta").load(DELTA_PATH).show();

        System.out.println("========================== MERGE (UPSERT) ==========================");

        ordersV1.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .save(DELTA_PATH);

        DeltaTable target = DeltaTable.forPath(spark, DELTA_PATH);

        target.alias("target")
                .merge(
                        ordersV2.alias("source"), "target.order_id = source.order_id"
                )
                .whenMatched()
                .updateAll()
                .whenNotMatched()
                .insertAll().execute();

        System.out.println(" Result..................................................");
        spark.read().format("delta").load(DELTA_PATH).orderBy("order_id").show();

        System.out.println("========================== DELETE CANCELLED ORDERS ==========================");
        DeltaTable deleteTable = DeltaTable.forPath(spark, DELTA_PATH);

        deleteTable.delete(col("status").equalTo("CANCELLED"));
        System.out.println(" Result..................................................");
        spark.read().format("delta").load(DELTA_PATH).show();

        System.out.println("========================== TIME TRAVEL ==========================");
        System.out.println(" 0th: Result..................................................");
        spark.read().format("delta").option("versionAsOf", 0).load(DELTA_PATH).show();

        System.out.println(" Nth: Result..................................................");
        spark.read().format("delta").option("versionAsOf", 1).load(DELTA_PATH).show();

        System.out.println(" Current: Result..................................................");
        spark.read().format("delta").load(DELTA_PATH).show();

        System.out.println("========================== SCHEMA EVOLUTION ==========================");
        Dataset<Row> withCol = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .withColumn("discount_pct", lit(0.0));

        withCol.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .option("overwriteSchema", "true")
                .save(DELTA_PATH);

        spark.read()
                .format("delta")
                .load(DELTA_PATH).printSchema();

        System.out.println("========================== OPTIMIZE (compact small files) ==========================");

        DeltaTable.forPath(spark, DELTA_PATH).optimize().executeCompaction();

        System.out.println("Optimize complete.........................");

        System.out.println("========================== VACUUM (remove old files) ==========================");
        spark.conf().set("spark.databricks.delta.retentionDurationCheck.enabled",
                "false");
        DeltaTable.forPath(spark, DELTA_PATH).vacuum(0);    // Do not use Zero, as it immediately removes the data.
        System.out.println("Vacuum complete...........................");

        System.out.println("========================== TRANSACTION-LOG-HISTORY(commitInfo) ==========================");

        DeltaTable deltaTable = DeltaTable.forPath(spark, DELTA_PATH);

        Dataset<Row> history = deltaTable.history();

        history
                .select(
                        "version",
                        "timestamp",
                        "operation",
                        "operationParameters",
                        "operationMetrics"
                ).
                orderBy("version").show(true);

        SparkSessionFactory.stop();

    }
}

