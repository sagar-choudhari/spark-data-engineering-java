package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class GlueCatalogJob {

    private static final String GLUE_DATABASE =
            AppConfig.getFilePath("glue.database");
    private static final String GLUE_TABLE_ORDERS =
            AppConfig.getFilePath("glue.table.orders");
    private static final String GLUE_TABLE_REVENUE =
            AppConfig.getFilePath("glue.table.revenue");
    private static final String S3_OUTPUT_PARQUET =
            AppConfig.getFilePath("s3.output.path.parquet");
    private static final String S3_OUTPUT_REPORT =
            AppConfig.getFilePath("s3.output.path.report");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        System.out.println("================================= CREATE GLUE DATABASE ======================");
        spark.sql("CREATE DATABASE IF NOT EXISTS "+ GLUE_DATABASE);

        System.out.println("Database created: " + GLUE_DATABASE);

        System.out.println("================================= CREATE GLUE DATABASE ======================");
        spark.sql("""
                CREATE TABLE IF NOT EXISTS %s.%s
                USING parquet
                LOCATION '%s'
                """.formatted(
                        GLUE_DATABASE, GLUE_TABLE_ORDERS, S3_OUTPUT_PARQUET
                )
        );

        System.out.println("Table registered: " +
                GLUE_DATABASE + "." + GLUE_TABLE_ORDERS);

        System.out.println("================================= PARTITION DISCOVERY ======================");
        spark.sql("MSCK REPAIR TABLE "+ GLUE_DATABASE + "." + GLUE_TABLE_ORDERS);

        System.out.println("Partition discovery complete...");


        System.out.println("================================= QUERY GLUE TABLE ======================");
        spark.sql("""
                SELECT
                    product,
                    order_date,
                    COUNT(order_id)        AS order_count,
                    SUM(order_value)       AS total_revenue
                FROM %s.%s
                GROUP BY product, order_date
                ORDER BY total_revenue DESC
                """.formatted(GLUE_DATABASE, GLUE_TABLE_ORDERS))
                .show();

        System.out.println("================================= REGISTER REVENUE REPORT TABLE ======================");
        spark.sql("""
                CREATE TABLE IF NOT EXISTS %s.%s
                USING parquet
                LOCATION '%s'
                """.formatted(GLUE_DATABASE, GLUE_TABLE_REVENUE, S3_OUTPUT_REPORT)
        );

        System.out.println("TABLES IN GLUE DATABASE ................");
        spark.sql("SHOW TABLES IN " + GLUE_DATABASE).show();

        System.out.println("================================= DESCRIBE TABLE ======================");
        spark.sql("DESCRIBE TABLE " + GLUE_DATABASE +"."+GLUE_TABLE_ORDERS).show();

        SparkSessionFactory.stop();

    }
}
