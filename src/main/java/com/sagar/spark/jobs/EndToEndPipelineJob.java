package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
public class EndToEndPipelineJob {

    private static final String CSV_PATH_ORDERS_DIRTY = AppConfig.getFilePath("csv.path.orders_dirty_v2");
    private static final String JSON_PATH_TRANSACTIONS = AppConfig.getFilePath("json.path.transactions_v2");
    private static final String[] PAYMENT_STATUS = {"COMPLETED", "PENDING", "CANCELLED"};

    private static final String PARQUET_PATH_QUARANTINE_ORDERS = AppConfig.getFilePath("parquet.path.quarantine.orders");
    private static final String PARQUET_PATH_CORRELATED_ORDERS = AppConfig.getFilePath("parquet.path.quarantine.correlated_orders");
    private static final String PARQUET_PATH_REVENUE_REPORT = AppConfig.getFilePath("parquet.path.quarantine.revenue_report");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        StructField orderId = DataTypes.createStructField("order_id", DataTypes.IntegerType, true);
        StructField customerId = DataTypes.createStructField("customer_id", DataTypes.IntegerType, true);
        StructField productCategory = DataTypes.createStructField("product_category", DataTypes.StringType, true);
        StructField totalAmount = DataTypes.createStructField("total_amount", DataTypes.DoubleType, true);
        StructField status = DataTypes.createStructField("status", DataTypes.StringType, true);
        StructField orderDate = DataTypes.createStructField("order_date", DataTypes.StringType, true);
        StructField channel = DataTypes.createStructField("channel", DataTypes.StringType, true);

        StructField[] fields = new StructField[]{orderId, customerId, productCategory, totalAmount, status, orderDate, channel};

        StructType csvSchema = DataTypes.createStructType(fields);

        Dataset<Row> rawOrders = spark
                .read()
                .option("header", "true")
                .schema(csvSchema)
                .csv(CSV_PATH_ORDERS_DIRTY);

//        rawOrders.show();

        Dataset<Row> rawTransactions = spark
                .read()
                .option("multiline", "false")
                .json(JSON_PATH_TRANSACTIONS);

        Dataset<Row> flatItems = rawTransactions
                .withColumn("items", explode_outer(col("items")))         // Or explode(col("items")) - this drops empty or null
                .select(
                        col("items.price").alias("price"),
                        col("items.product").alias("product"),
                        col("items.quantity").alias("quantity"),
                        col("customer.city").alias("city"),
                        col("customer.name").alias("customer_name"),
                        col("customer.tier").alias("tier"),
                        col("customer_id"),
                        col("event_timestamp"),
                        col("order_id"),
                        col("payment.amount").alias("amount"),
                        col("payment.method").alias("payment_method"),
                        col("payment.status").alias("payment_status")
                ).withColumn("line_value" ,col("price").multiply(col("quantity")));

//        flatItems.show();

        System.out.printf("Ingested: %d order rows(with dupes), %d payment events%n", rawOrders.count(), rawTransactions.count());

        Dataset<Row> badOrders = rawOrders
                .filter(
                        col("order_id").isNull()
                                .or(col("customer_id").isNull())
                                .or(col("total_amount").isNull())
                                .or(col("status").isin((Object[]) PAYMENT_STATUS).equalTo(false))
                );

        Dataset<Row> goodOrders = rawOrders
                .filter(
                        col("order_id").isNotNull()
                                .and(col("customer_id").isNotNull())
                                .and(col("total_amount").isNotNull())
                                .and(col("status").isin((Object[]) PAYMENT_STATUS))
                );

        WindowSpec dedupWindow = Window
                .partitionBy("order_id")
                .orderBy(to_date(col("order_date"), "dd-MM-yyyy").desc());

        Dataset<Row> deduplicatedOrders = goodOrders
                .withColumn("row_number", row_number().over(dedupWindow))
                .filter(col("row_number").equalTo(1))
                .drop("row_number");

        long goodOrdersCount = goodOrders.count();

        long badOrdersCount = badOrders.count();

        long droppedRows =rawOrders.count()-deduplicatedOrders.count();

        System.out.printf("good Orders: %d, bad Orders: %d, dropped Orders: %d", goodOrdersCount, badOrdersCount, droppedRows);

        if(badOrdersCount > 0){
            badOrders
                    .withColumn("quarantine_date", current_date())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("quarantine_date")
                    .parquet(PARQUET_PATH_QUARANTINE_ORDERS);
        }

        System.out.printf("Flat line items: %d rows%n", flatItems.count());

        Dataset<Row> correlated = deduplicatedOrders
                .join(flatItems,
                        deduplicatedOrders.col("order_id").equalTo(flatItems.col("order_id")),
                        "inner"
                )
                .select(
                        deduplicatedOrders.col("order_id"),
                        deduplicatedOrders.col("customer_id"),
                        col("customer_name"),
                        col("tier"),
                        col("city"),
                        deduplicatedOrders.col("product_category"),
                        col("product"),
                        col("quantity"),
                        col("price"),
                        col("line_value"),
                        deduplicatedOrders.col("status"),
                        deduplicatedOrders.col("channel"),
                        col("payment_method"),
                        col("payment_status"),
                        deduplicatedOrders.col("order_date"),
                        col("event_timestamp")
                );

//        correlated.show();

        correlated
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("order_date")
                .parquet(PARQUET_PATH_CORRELATED_ORDERS);

        System.out.println("=================================== Aggregations ===================================");

        correlated.createOrReplaceTempView("correlated_orders");

        System.out.println("=================================== REVENUE BY TIER ===================================");

        spark.sql("""
                SELECT
                    tier,
                    COUNT(DISTINCT order_id)            AS order_count,
                    SUM(line_value)                     AS total_revenue,
                    ROUND(AVG(line_value), 2)           AS avg_line_value,
                    RANK() OVER (ORDER BY SUM(line_value) DESC) AS revenue_rank
                FROM correlated_orders
                WHERE status = 'COMPLETED'
                AND payment_status = 'SUCCESS'
                GROUP BY tier
                ORDER BY total_revenue DESC
                """).show();

        System.out.println("=================================== TOP PRODUCT PER TIER ===================================");
        spark.sql("""
                WITH product_revenue as(
                    SELECT tier, product, sum(line_value) as revenue,
                    DENSE_RANK() OVER (
                        PARTITION BY tier
                        ORDER BY SUM(line_value) DESC
                        ) AS rank_in_tier
                    FROM correlated_orders
                    WHERE status = 'COMPLETED' AND payment_status= 'SUCCESS'
                    GROUP BY tier, product
                )
                SELECT tier, product, revenue
                FROM product_revenue
                WHERE rank_in_tier = 1
                ORDER BY revenue DESC
                """).show();

        System.out.println("=================================== REVENUE BY CHANNEL ===================================");

        spark.sql("""
                SELECT
                    channel,
                    COUNT(DISTINCT order_id)    AS order_count,
                    SUM(line_value)             AS total_revenue,
                    ROUND(AVG(line_value), 2)   AS avg_line_value
                FROM correlated_orders
                WHERE status = 'COMPLETED'
                AND payment_status = 'SUCCESS'
                GROUP BY channel
                ORDER BY total_revenue DESC
                """).show();

        System.out.println("=================================== RUNNING REVENUE PER CUSTOMER ===================================");
        WindowSpec customerWindow = Window
                .partitionBy("customer_name")
                .orderBy("event_timestamp")
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        correlated
                .filter(col("status").equalTo("COMPLETED").and(col("payment_status").equalTo("SUCCESS")))
                .withColumn("running_revenue",
                        sum("line_value").over(customerWindow))
                .select("customer_name", "tier", "product",
                        "line_value", "running_revenue", "event_timestamp")
                .orderBy("customer_name", "event_timestamp")
                .show();


        System.out.println("=================================== OUTPUT ===================================");

        spark.sql("""
                SELECT
                    tier,
                    COUNT(DISTINCT order_id)  AS order_count,
                    SUM(line_value)           AS total_revenue
                FROM correlated_orders
                WHERE status = 'COMPLETED'
                  AND payment_status = 'SUCCESS'
                GROUP BY tier
                ORDER BY total_revenue DESC
                """)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(PARQUET_PATH_REVENUE_REPORT);

        SparkSessionFactory.stop();

    }
}

