package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class UDFJob {

    public static final String CSV_PATH_ORDERS_V3 = AppConfig.get("csv.path.orders_v3");

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.get();

        spark.sparkContext().setLogLevel("ERROR");

        UDF2<Double, String, Double> applyDiscount =
                (orderValue, promoCode) -> {
                    if (orderValue == null) return 0.0;
                    if (promoCode == null) return orderValue;

                    return switch (promoCode.toUpperCase()) {
                        case "SAVE10" -> orderValue * 0.90;
                        case "FLAT500" -> Math.max(0.0, orderValue - 500.0);
                        case "WELCOME200" -> Math.max(0.0, orderValue - 200.0);
                        default -> orderValue;
                    };
                };

        spark.udf().register(
                "apply_discount",
                applyDiscount,
                DataTypes.DoubleType
        );

        UDF2<Double, String, String> orderCategory =
                (orderValue, status) -> {
                    if (orderValue == null || status == null ) return "UNKNOWN";
                    if (!status.equals("COMPLETED")) return "NON_COMPLETED";
                    if (orderValue >= 100000.0) return "PREMIUM";
                    if (orderValue >= 50000.0)  return "HIGH";
                    if (orderValue >= 10000.0)  return "MEDIUM";
                    return "LOW";
                };

        spark.udf().register(
                "order_category",
                orderCategory,
                DataTypes.StringType
        );

        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_ORDERS_V3);

        System.out.println("========================== DISCOUNTED-ORDER-VALUES ==========================");

        Dataset<Row> withDiscount = orders
                .withColumn("order_value", col("price").cast("double").multiply(col("quantity")))
                .withColumn("discounted_value",
                        callUDF("apply_discount",
                                col("order_value"),
                                col("promo_code")))
                .withColumn("discount_amount",
                        col("order_value").minus(col("discounted_value")))
                .withColumn("category",
                        callUDF("order_category",
                                col("discounted_value"),
                                col("status")));

        withDiscount.select(
                "order_id", "product", "promo_code",
                "order_value", "discounted_value",
                "discount_amount", "category"
        ).show();

        System.out.println("========================== UDFs-IN-SPARK-SQL ==========================");

        withDiscount.createOrReplaceTempView("orders_with_discount");

        spark.sql("""
                SELECT
                    product,
                    promo_code,
                    COUNT(order_id)                         AS order_count,
                    ROUND(SUM(order_value), 2)              AS gross_revenue,
                    ROUND(SUM(discounted_value), 2)         AS net_revenue,
                    ROUND(SUM(discount_amount), 2)          AS total_discount_given
                FROM orders_with_discount
                WHERE status = 'COMPLETED'
                GROUP BY product, promo_code
                ORDER BY gross_revenue DESC
                """).show();


        System.out.println("========================== SAME-CATEGORY-LOGIC-WITHOUT-UDF (preferred for simple cases eg. up to max 10 when otherwise statement else look for UDFs) ==========================");
        orders
                .withColumn("order_value",
                        col("quantity").multiply(col("price")))
                .withColumn("category_builtin",
                        when(col("status").notEqual("COMPLETED"),
                                lit("NON_COMPLETED"))
                                .when(col("order_value").geq(100000), lit("PREMIUM"))
                                .when(col("order_value").geq(50000),  lit("HIGH"))
                                .when(col("order_value").geq(10000),  lit("MEDIUM"))
                                .otherwise(lit("LOW")))
                .select("order_id", "order_value", "status", "category_builtin")
                .show();

        SparkSessionFactory.stop();


    }
}
