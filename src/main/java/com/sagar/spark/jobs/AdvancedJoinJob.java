package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class AdvancedJoinJob {

    private static final String CSV_PATH_CUSTOMERS= AppConfig.getFilePath("csv.path.customers");
    private static final String CSV_PATH_ORDERS_SKEWED= AppConfig.getFilePath("csv.path.orders_skewed");
    private static final String CSV_PATH_PRODUCTS= AppConfig.getFilePath("csv.path.products");
    private static final String PARQUET_PATH_CUSTOMERS= AppConfig.getFilePath("parquet.path.customers");
    private static final String PARQUET_PATH_TRANSACTIONS= AppConfig.getFilePath("parquet.path.transactions_large");

    public static void main(String[] args) throws InterruptedException {

        SparkSession spark = SparkSessionFactory.get();

        spark.conf().set("spark.sql.shuffle.partitions", "4");

        Dataset<Row> ordersSkewed = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_ORDERS_SKEWED);

        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_PRODUCTS);

        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_CUSTOMERS);

        Dataset<Row> customersDim = spark.read()
                .parquet(PARQUET_PATH_CUSTOMERS);

        Dataset<Row> transactionsFact = spark.read()
                .parquet(PARQUET_PATH_TRANSACTIONS);

        System.out.println("################# DEFAULT-JOIN(Sort-Merge-Join) ########################");
        Dataset<Row> defaultJoin = customersDim.join(
                transactionsFact,
                "customer_id",
                "left"
        );

        defaultJoin.explain();
        defaultJoin.show();

        System.out.println("################# BROADCAST-JOIN(With hint. Default for less than 10 MB Dataset, no hint needed.(AQE should not be false)) ########################");
        Dataset<Row> broadcastJoin = transactionsFact.join(
                broadcast(customersDim),
                "customer_id",
                "inner"
        );

        broadcastJoin.explain();
        broadcastJoin.show();

        System.out.println("################# AQE-AUTO-CONVERSION #################");
        spark.conf().set("spark.sql.adaptive.enabled", "true");
        spark.conf().set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10mb");

        Dataset<Row> aqeJoin = ordersSkewed.join(
                customers,
                ordersSkewed.col("customer_id")
                        .equalTo(customers.col("customer_id")),
                "inner"
        );

        aqeJoin.count();
        aqeJoin.explain(true);

        System.out.println("################# SKEW-DETECTION #################");

        transactionsFact.groupBy("customer_id")
                        .agg(
                                count("txn_id").alias("total_transactions")
                        )
                                .orderBy(col("total_transactions").desc())
                .show();

        spark.conf().set("spark.sql.adaptive.skewJoin.enabled", "true");
        spark.conf().set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2");
        spark.conf().set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1b");

        System.out.println("################# SKEW JOIN WITH AQE #################");

        Dataset<Row> skewJoin = transactionsFact.join(
                customersDim,
                transactionsFact.col("customer_id")
                        .equalTo(customersDim.col("customer_id")),
                "inner"
        );

        skewJoin.count();

        skewJoin.explain(true);

        System.out.println("################# MANUAL SALTING #################");

        int saltBuckets = 3;

        Dataset<Row> saltedOrders = ordersSkewed
                .withColumn("salt", rand().multiply(saltBuckets).cast("int"))
                .withColumn("salted_customer_id",
                        concat(
                            col("customer_id").cast("string"),
                            lit("_"),
                            col("salt").cast("string")
                        )
                );

        Dataset<Row> saltedCustomers = customers
                .withColumn("salt", explode(array(lit(0), lit(1), lit(2))))
                .withColumn("salted_customer_id",
                        concat(
                                col("customer_id").cast("string"),
                                lit("_"),
                                col("salt").cast("string")
                        )
                );

        Dataset<Row> saltedJoin = saltedOrders.join(
                saltedCustomers,
                saltedOrders.col("salted_customer_id")
                        .equalTo(saltedCustomers.col("salted_customer_id")),
                "inner"
        ).select(
                saltedOrders.col("order_id"),
                saltedOrders.col("customer_id"),
                saltedCustomers.col("customer_name"),
                saltedCustomers.col("tier"),
                saltedOrders.col("product"),
                saltedOrders.col("status")
        );

        System.out.println("Salted join row count: " + saltedJoin.count());
        saltedJoin.show();

        System.out.println("################# BUCKETING #################");

        spark.sql("DROP TABLE IF EXISTS bucketed_transactions");
        spark.sql("DROP TABLE IF EXISTS bucketed_customers");

        ordersSkewed.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(4, "customer_id")
                .sortBy("customer_id")
                .saveAsTable("bucketed_orders");

        customers.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(4, "customer_id")
                .sortBy("customer_id")
                .saveAsTable("bucketed_customers");

        Dataset<Row> bucketedJoin = spark.table("bucketed_orders")
                .join(spark.table("bucketed_customers"), "customer_id");

        System.out.println("################# BUCKETED JOIN PLAN — no Exchange expected #################");
        bucketedJoin.explain();
        bucketedJoin.show(5);

        System.out.println("################# JOIN HINTS IN SQL #################");

        ordersSkewed.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");
        products.createOrReplaceTempView("products");

        spark.sql("""
                SELECT /*+ BROADCAST(c) */
                    o.order_id,
                    c.customer_name,
                    c.tier,
                    o.product,
                    o.status
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.status = 'COMPLETED'
                """).explain();

        spark.sql("""
                SELECT /*+ MERGE(o, c) */
                    o.order_id,
                    c.customer_name,
                    o.product
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                """).explain();

        SparkSessionFactory.stop();

    }
}
