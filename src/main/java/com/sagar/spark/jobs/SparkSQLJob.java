package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLJob {

    private static final String CSV_PATH_ORDERS = AppConfig.get("csv.path.orders_v2");
    private static final String CSV_PATH_CUSTOMERS = AppConfig.get("csv.path.customers");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_ORDERS);

        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_CUSTOMERS);

//        Temp view register to use directly in the SQL-query referencing.
        orders.createOrReplaceTempView("orders");
        customers.createOrReplaceTempView("customers");

        System.out.println("========================== BASIC-SQL ==========================");
        Dataset<Row> completed = spark.sql("""
                SELECT
                    o.order_id,
                    c.customer_name,
                    c.tier,
                    o.product,
                    o.quantity * o.price AS order_value,
                    o.status
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.status = 'COMPLETED'
                ORDER BY order_value DESC
                """);

        completed.show();

        System.out.println("========================== REVENUE-BY-TIER-AND-PRODUCT ==========================");

        Dataset<Row> revenue = spark.sql("""
                SELECT
                    c.tier,
                    o.product,
                    COUNT(o.order_id)             AS order_count,
                    SUM(o.quantity * o.price)     AS total_revenue,
                    ROUND(AVG(o.quantity * o.price), 2) AS avg_order_value
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.status = 'COMPLETED'
                GROUP BY c.tier, o.product
                ORDER BY c.tier, total_revenue DESC
                """);

        revenue.show();

        System.out.println("========================== WINDOW-FUNCTION-IN-SQL ==========================");

        Dataset<Row> ranks_1 =spark.sql("""
                SELECT
                       customer_name,
                       tier,
                       total_spend,
                       RANK()       OVER (PARTITION BY tier ORDER BY total_spend DESC) AS rnk,
                       DENSE_RANK() OVER (PARTITION BY tier ORDER BY total_spend DESC) AS dense_rnk,
                       ROW_NUMBER() OVER (PARTITION BY tier ORDER BY total_spend DESC) AS row_num
                FROM (
                        SELECT
                            c.customer_name,
                            c.tier,
                            SUM(o.quantity * o.price) AS total_spend
                        FROM orders o
                        JOIN customers c ON o.customer_id = c.customer_id
                        WHERE o.status = 'COMPLETED'
                        GROUP BY c.customer_name, c.tier
                ) customer_totals
                ORDER BY tier, rnk
                """);

        ranks_1.show();

        System.out.println("========================== CTE-VERSION ==========================");

        Dataset<Row> ranks_2 = spark.sql("""
                WITH completed_orders AS (
                    SELECT
                        o.order_id,
                        o.customer_id,
                        o.product,
                        o.quantity * o.price AS order_value,
                        o.order_date
                    FROM orders o
                    WHERE o.status = 'COMPLETED'
                ),
                customer_spend AS (
                    SELECT
                        c.customer_name,
                        c.tier,
                        c.city,
                        SUM(co.order_value)        AS total_spend,
                        COUNT(co.order_id)         AS order_count,
                        ROUND(AVG(co.order_value), 2) AS avg_spend
                    FROM completed_orders co
                    JOIN customers c ON co.customer_id = c.customer_id
                    GROUP BY c.customer_name, c.tier, c.city
                ),
                ranked_customers AS (
                    SELECT
                        customer_name,
                        tier,
                        city,
                        total_spend,
                        order_count,
                        avg_spend,
                        DENSE_RANK() OVER (
                            PARTITION BY tier
                            ORDER BY total_spend DESC
                        ) AS rank_in_tier
                    FROM customer_spend
                )
                SELECT *
                FROM ranked_customers
                WHERE rank_in_tier = 1
                ORDER BY tier
                """);

        ranks_2.show();

        SparkSessionFactory.stop();

    }
}
