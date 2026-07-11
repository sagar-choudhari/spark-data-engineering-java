package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.*;

public class PerformanceTuningJob {
    private static final String CSV_PATH_LARGE_ORDERS = AppConfig.get("csv.path.large_orders");
    private static final String PARQUET_PATH_LARGE_ORDERS = AppConfig.get("parquet.path.large_orders");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        System.out.println("################# SPARK'S-SHUFFLE-PARTITIONS-CONFIGURATION ########################");

        System.out.println("Current shuffle partitions(4 is set in SparkSessionFactory default is 200): " + spark.conf().get("spark.sql.shuffle.partitions"));

        spark.conf().set("spark.sql.shuffle.partitions", 10);

        System.out.println("Newly set shuffle partitions: " + spark.conf().get("spark.sql.shuffle.partitions"));

        System.out.println("################# SPARK'S-SHUFFLE-PARTITIONS-COUNT-AFTER-READ ########################");
        Dataset<Row> rawOrders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_LARGE_ORDERS);

        System.out.println("Partitions after read: " + rawOrders.rdd().getNumPartitions());

        System.out.println("\n################# REPARTITION-AND-COALESCE ########################");

        Dataset<Row> repartitioned = rawOrders.repartition(10);

        System.out.println("After Repartition applied: " +repartitioned.rdd().getNumPartitions());

        Dataset<Row> repartitionOnCol = rawOrders.repartition(9, col("status"));

        System.out.println("After Repartition applied over a Column: " +repartitionOnCol.rdd().getNumPartitions());

        Dataset<Row> coalesced = rawOrders.coalesce(6);

        System.out.println("After Coalesce applied: " +coalesced.rdd().getNumPartitions());

        System.out.println("\n################# CACHE-AND-PERSIST ########################");

        Dataset<Row> completedOrders = rawOrders
                .filter(col("status").equalTo("COMPLETED"))
                .withColumn("order_value", col("quantity").multiply(col("price")));

        completedOrders.cache();

        // Force materialization — cache is lazy too
        // First action triggers computation and caches result
        long completedOrderCount = completedOrders.count();

        System.out.println("After this count(an action trigger) the completedOrders Dataset will be cached. The count: "+completedOrderCount);

        System.out.println("\n------------------------ Cached Or Persisted Dataset in use ------------------------");

        completedOrders.agg(
                sum("order_value").alias("total_revenue")
                )
                .show();

        completedOrders
                .groupBy("product")
                .agg(
                        count("order_id").alias("order_count"),
                        sum("order_value").alias("product_revenue")
                )
                .orderBy(col("product_revenue").desc())
                .show();

        completedOrders.unpersist();

        System.out.println("Cache released.");

        Dataset<Row> expensiveDataset = rawOrders
                .join(
                        broadcast(rawOrders.select("order_id", "product")),
                        "order_id"
                ).filter(col("status").equalTo("COMPLETED"));

        expensiveDataset.persist(StorageLevel.MEMORY_ONLY());
        expensiveDataset.count();

        System.out.println("Persisted with MEMORY_AND_DISK");
        expensiveDataset.unpersist();

        System.out.println("\n################# AQE ########################");
        System.out.println("Adaptive Enabled: "+ spark.conf().get("spark.sql.adaptive.enabled"));

        System.out.println("\n################# PARTITION-PRUNING-VERIFICATION ########################");
        rawOrders.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("status")
                .parquet(PARQUET_PATH_LARGE_ORDERS);

        Dataset<Row> pruned = spark.read()
                .parquet(PARQUET_PATH_LARGE_ORDERS)
                .filter(col("status").equalTo("COMPLETED"));

        System.out.println("Physical plan — look for PartitionFilters:");
        pruned.explain();
        System.out.println("completed orders row count: " + pruned.count());

        SparkSessionFactory.stop();

    }
}
