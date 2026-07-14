package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
public class ParquetReadJob {
    private static final String PARQUET_PATH_ORDERS = AppConfig.getFilePath("parquet.path.orders");
    private static final String PARQUET_PATH_PARTITIONED_ORDERS = AppConfig.getFilePath("parquet.path.partitioned.orders");

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSessionFactory.get();

        Dataset<Row> orders = sparkSession.read().parquet(PARQUET_PATH_ORDERS);

        System.out.println("################# PRINT-SCHEMA-OF-PARQUET-FILE ########################");
        orders.printSchema();

        System.out.println("################# TOTAL-ROW-COUNT ########################");
        long countOfRows = orders.count();
        System.out.println("Number Of Rows in the Dataset are: " + countOfRows);

        System.out.println("################# READ-PARTITIONED-PARQUET ########################");
        Dataset<Row> partitionedOrders = sparkSession.read().parquet(PARQUET_PATH_PARTITIONED_ORDERS);

        System.out.println("################# PRINT-SCHEMA-OF-PARTITIONED-PARQUET ########################");
        partitionedOrders.printSchema();

        System.out.println("################# PARTITION-PRUNING-read only partitioned data ########################");
        partitionedOrders.filter(col("status").equalTo("COMPLETED"))
                .show();

        System.out.println("################# QUERY-USING-SQL ########################");
        partitionedOrders.createOrReplaceTempView("orders");
        sparkSession.sql("""
                SELECT
                    product,
                    COUNT(order_id) AS order_count,
                    SUM(quantity*price) AS total_revenue,
                    ROUND(AVG(quantity*price), 2) AS avg_order_value
                FROM orders
                WHERE status = 'COMPLETED'
                GROUP BY product
                ORDER BY total_revenue DESC
                """).show();

        System.out.println("################# NULL-CHECK-PER-COLUMN ########################");
        orders.select(
                orders.columns() != null ? Arrays.stream(orders.columns())
                        .map(c->sum(col(c).isNull().cast("int")).alias(c+"_nulls"))
                        .toArray(Column[]::new)
                        : new Column[]{}
        ).show();

        System.out.println("################# SUMMARY-STATS ########################");
        orders.select("quantity", "price")
                .summary()
                .show();

        SparkSessionFactory.stop();
    }
}
