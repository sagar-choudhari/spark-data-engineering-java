package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DataQualityJob {

    private static final List<String> VALID_STATUS = new ArrayList<>(List.of(new String[]{"COMPLETED", "PENDING", "CANCELLED"}));
    private static final String CSV_PATH_ORDERS_DIRTY = AppConfig.get("csv.path.orders_dirty");
    private static final String CSV_PATH_CUSTOMERS = AppConfig.get("csv.path.customers");
    private static final String PARQUET_PATH_CLEAN_ORDERS = AppConfig.get("parquet.path.clean.orders");
    private static final String PARQUET_PATH_QUARANTINE_ORDERS = AppConfig.get("parquet.path.quarantine.orders");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_CUSTOMERS);

        StructField orderId = DataTypes.createStructField("order_id", DataTypes.IntegerType, true);
        StructField customerId = DataTypes.createStructField("customer_id", DataTypes.IntegerType, true);
        StructField product = DataTypes.createStructField("product", DataTypes.StringType, true);
        StructField quantity = DataTypes.createStructField("quantity", DataTypes.IntegerType, true);
        StructField price = DataTypes.createStructField("price", DataTypes.DoubleType, true);
        StructField status = DataTypes.createStructField("status", DataTypes.StringType, true);
        StructField order_date = DataTypes.createStructField("order_date", DataTypes.StringType, true);

        StructField[] fields = {orderId, customerId, product, quantity, price, status, order_date};

        StructType ordersSchema = DataTypes.createStructType(fields);

        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .option("mode", "PERMISSIVE")
                .schema(ordersSchema)
                .csv(CSV_PATH_ORDERS_DIRTY);

        System.out.println("========================== RAW-DATA ==========================");

        raw.show();

        System.out.println("========================== NULL-AUDIT ==========================");

        Column[] nullColumns= Arrays.stream(raw.columns())
                .map(c->sum(col(c).isNull().cast("int")).alias(c+"_nulls"))
                .toArray(Column[]::new);

        Dataset<Row> nullsPerCol = raw.select(nullColumns);

        nullsPerCol.show();

        System.out.println("========================== TAG-ROW-WITH-QUALITY-ISSUE ==========================");

        WindowSpec orderIdWindow = Window.partitionBy("order_id");

        Dataset<Row> anomalyTagged = raw
                .withColumn("is_duplicate", count("order_id").over(orderIdWindow).gt(1))
                .withColumn("is_null_customer_id", col("customer_id").isNull())
                .withColumn("is_null_product", col("product").isNull())
                .withColumn("is_negative_quantity", col("quantity").cast("int").lt(0))
                .withColumn("is_null_price", col("price").isNull())
                .withColumn("is_invalid_status", col("status").isNull().or(col("status").isin(VALID_STATUS.toArray())).equalTo(false))
                .withColumn("is_null_order_date", col("order_date").isNull());

        anomalyTagged.show();

        System.out.println("========================== DUPLICATE-ORDER-IDs ==========================");

        Dataset<Row> duplicates = raw
                .groupBy(col("order_id"))
                .agg(
                        count(col("order_id")).alias("occurrence_count")
                )
                .filter(col("occurrence_count").gt(1));

        duplicates.show();

        System.out.println("========================== ORPHANED-CUSTOMER-IDs ==========================");

        Dataset<Row> orphaned = raw
                .join(
                        customers,
                        customers.col("customer_id").equalTo(raw.col("customer_id")),
                        "left_anti"
                );

        orphaned.select("order_id", "customer_id", "product", "status").show();

        System.out.println("========================== BAD-RECORDS ==========================");

        Dataset<Row> badRecords = raw
                .filter(
                        col("customer_id").isNull()
                                .or(col("product").isNull())
                                .or(col("price").isNull())
                                .or(col("quantity").isNull())
                                .or(col("status").isNull().or(col("status").isin(VALID_STATUS.toArray()).equalTo(false)))
                );

        badRecords.show();

        System.out.println("========================== GOOD-RECORDS ==========================");

        Dataset<Row> goodRecords = raw
                .filter(
                        col("customer_id").isNotNull()
                                .and(col("product").isNotNull())
                                .and(col("price").isNotNull())
                                .and(col("quantity").isNotNull().and(col("quantity").geq(0)))
                                .and(col("status").isin(VALID_STATUS.toArray()))
                );

        goodRecords.show();

        System.out.println("========================== GOOD-RECORDS-WITH-DUPLICATE-REMOVED ==========================");

        Dataset<Row> cleaned = goodRecords.dropDuplicates("order_id");

        cleaned.show();

        System.out.println("========================== OVERVIEW ==========================");

        System.out.printf("Total: %d | Good: %d | Bad: %d | Duplicates removed: %d%n",
                raw.count(),
                goodRecords.count(),
                badRecords.count(),
                goodRecords.count()-cleaned.count()
                );

        System.out.println("========================== WRITE-CLEANED-ORDERS-TO-PARQUET ==========================");

        cleaned.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("status")
                .parquet(PARQUET_PATH_CLEAN_ORDERS);

        System.out.println("========================== WRITE-BAD-ORDERS-TO-PARQUET ==========================");

        badRecords
                .withColumn("quarantine_date", current_date())
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("quarantine_date")
                .parquet(PARQUET_PATH_QUARANTINE_ORDERS);

        System.out.println("========================== CLEAN-RECORDS-WRITTEN ==========================");
        System.out.println("========================== BAD-RECORDS-WRITTEN ==========================");

        SparkSessionFactory.stop();

    }
}
