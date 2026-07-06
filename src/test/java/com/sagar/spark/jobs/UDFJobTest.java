package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class UDFJobTest extends SparkTestBase {

    private Dataset<Row> orders;

    @BeforeEach
    void setup() {
        // Register UDFs before each test — UDFs are session-scoped
        // but re-registering is idempotent and safer in shared test sessions
        UDF2<Double, String, Double> applyDiscount =
                (orderValue, promoCode) -> {
                    if (orderValue == null) return 0.0;
                    if (promoCode == null) return orderValue;
                    switch (promoCode.toUpperCase()) {
                        case "SAVE10":     return orderValue * 0.90;
                        case "FLAT500":    return Math.max(0, orderValue - 500);
                        case "WELCOME200": return Math.max(0, orderValue - 200);
                        default:           return orderValue;
                    }
                };

        UDF2<Double, String, String> orderCategory =
                (orderValue, status) -> {
                    if (orderValue == null || status == null) return "UNKNOWN";
                    if (!status.equals("COMPLETED")) return "NON_COMPLETED";
                    if (orderValue >= 100000) return "PREMIUM";
                    if (orderValue >= 50000)  return "HIGH";
                    if (orderValue >= 10000)  return "MEDIUM";
                    return "LOW";
                };

        spark.udf().register("apply_discount", applyDiscount, DataTypes.DoubleType);
        spark.udf().register("order_category", orderCategory, DataTypes.StringType);

        orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/csv/input/orders_v3.csv")
                .withColumn("order_value",
                        col("quantity").multiply(
                                col("price").cast(DataTypes.DoubleType)));
    }

    @Test
    @DisplayName("SAVE10 applies 10% discount correctly")
    void testSave10Discount() {
        // order_id=1: quantity=1, price=75000 → order_value=75000
        // SAVE10 → 75000 * 0.90 = 67500
        Row row = orders
                .filter(col("order_id").equalTo(1))
                .withColumn("discounted_value",
                        callUDF("apply_discount",
                                col("order_value"), col("promo_code")))
                .select("discounted_value")
                .first();

        assertEquals(67500.0, row.getDouble(0), 0.01,
                "SAVE10 should apply 10% discount to 75000 → 67500");
    }

    @Test
    @DisplayName("FLAT500 deducts 500 flat correctly")
    void testFlat500Discount() {
        // order_id=3: quantity=1, price=35000 → order_value=35000
        // FLAT500 → 35000 - 500 = 34500
        Row row = orders
                .filter(col("order_id").equalTo(3))
                .withColumn("discounted_value",
                        callUDF("apply_discount",
                                col("order_value"), col("promo_code")))
                .select("discounted_value")
                .first();

        assertEquals(34500.0, row.getDouble(0), 0.01,
                "FLAT500 should deduct 500 from 35000 → 34500");
    }

    @Test
    @DisplayName("Null promo code returns original order value unchanged")
    void testNullPromoCode() {
        // order_id=2: no promo code → discounted_value should equal order_value
        Row row = orders
                .filter(col("order_id").equalTo(2))
                .withColumn("discounted_value",
                        callUDF("apply_discount",
                                col("order_value"), col("promo_code")))
                .select("order_value", "discounted_value")
                .first();

        assertEquals(row.getDouble(0), row.getDouble(1), 0.01,
                "Null promo code should return original order value unchanged");
    }

    @Test
    @DisplayName("order_category returns NON_COMPLETED for non-completed orders")
    void testCategoryNonCompleted() {
        // order_id=3: status=PENDING → NON_COMPLETED regardless of value
        Row row = orders
                .filter(col("order_id").equalTo(3))
                .withColumn("category",
                        callUDF("order_category",
                                col("order_value"), col("status")))
                .select("category")
                .first();

        assertEquals("NON_COMPLETED", row.getString(0),
                "PENDING status should produce NON_COMPLETED category");
    }

    @Test
    @DisplayName("order_category returns HIGH for completed order between 50000 and 99999")
    void testCategoryHigh() {
        // order_id=1: order_value=75000, status=COMPLETED → HIGH
        Row row = orders
                .filter(col("order_id").equalTo(1))
                .withColumn("category",
                        callUDF("order_category",
                                col("order_value"), col("status")))
                .select("category")
                .first();

        assertEquals("HIGH", row.getString(0),
                "Completed order with value 75000 should be HIGH category");
    }

    @Test
    @DisplayName("UDF and when/otherwise produce identical category results")
    void testUDFAndBuiltinProduceSameResults() {
        // Both approaches must produce identical output
        // This test enforces that UDF is consistent with the built-in equivalent
        Dataset<Row> udfResult = orders
                .withColumn("category_udf",
                        callUDF("order_category",
                                col("order_value"), col("status")));

        Dataset<Row> builtinResult = udfResult
                .withColumn("category_builtin",
                        when(col("status").notEqual("COMPLETED"),
                                lit("NON_COMPLETED"))
                                .when(col("order_value").geq(100000), lit("PREMIUM"))
                                .when(col("order_value").geq(50000),  lit("HIGH"))
                                .when(col("order_value").geq(10000),  lit("MEDIUM"))
                                .otherwise(lit("LOW")));

        // count rows where UDF and builtin disagree
        long mismatches = builtinResult
                .filter(col("category_udf")
                        .notEqual(col("category_builtin")))
                .count();

        assertEquals(0, mismatches,
                "UDF and when/otherwise must produce identical category for every row");
    }

    @Test
    @DisplayName("UDFs are accessible via Spark SQL after registration")
    void testUDFAvailableInSparkSQL() {
        orders.withColumn("discounted_value",
                        callUDF("apply_discount",
                                col("order_value"), col("promo_code")))
                .createOrReplaceTempView("orders_udf_test");

        Dataset<Row> result = spark.sql("""
                SELECT
                    order_id,
                    apply_discount(order_value, promo_code) AS sql_discounted
                FROM orders_udf_test
                WHERE order_id = 1
                """);

        Row row = result.first();
        assertEquals(67500.0, row.getDouble(1), 0.01,
                "UDF invoked via Spark SQL should produce same result as DataFrame API");
    }
}