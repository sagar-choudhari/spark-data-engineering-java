package com.sagar.spark.jobs;

import com.sagar.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class NestedJsonJobTest extends SparkTestBase {

    private Dataset<Row> raw;
    private Dataset<Row> flat;

    @BeforeEach
    void loadData() {
        raw = spark.read()
                .option("multiLine", "false")
                .json("C:/Users/sgrch/Desktop/central-learning/data-engineering/data-file/json/input/transactions.json");

        flat = raw
                .select(
                        col("transaction_id"),
                        col("customer.name").alias("customer_name"),
                        col("customer.tier").alias("tier"),
                        col("payment.status").alias("payment_status"),
                        col("transaction_date"),
                        explode(col("items")).alias("item")
                )
                .select(
                        col("transaction_id"),
                        col("customer_name"),
                        col("tier"),
                        col("payment_status"),
                        col("transaction_date"),
                        col("item.product").alias("product"),
                        col("item.quantity").alias("quantity"),
                        col("item.price").alias("price"),
                        col("item.quantity")
                                .multiply(col("item.price"))
                                .alias("line_value")
                );
    }

    @Test
    @DisplayName("Raw JSON loads correct number of transactions")
    void testRawTransactionCount() {
        assertEquals(5, raw.count(),
                "Should load exactly 5 transactions from JSON");
    }

    @Test
    @DisplayName("Schema contains nested struct columns")
    void testNestedSchemaExists() {

        String schemaString = raw.schema().treeString();
        assertTrue(schemaString.contains("customer"),
                "Schema must contain customer struct");
        assertTrue(schemaString.contains("items"),
                "Schema must contain items array");
        assertTrue(schemaString.contains("payment"),
                "Schema must contain payment struct");
    }

    @Test
    @DisplayName("Struct field access returns correct customer name")
    void testStructFieldAccess() {

        Row row = raw
                .filter(col("transaction_id").equalTo(1))
                .select(col("customer.name").alias("customer_name"))
                .first();

        assertEquals("Ravi Kumar", row.getString(0),
                "customer.name for transaction_id=1 should be Ravi Kumar");
    }

    @Test
    @DisplayName("Explode expands transaction 1 into correct number of rows")
    void testExplodeExpandsRows() {

        long rowCount = flat
                .filter(col("transaction_id").equalTo(1))
                .count();

        assertEquals(2, rowCount,
                "transaction_id=1 has 2 items — should explode into 2 rows");
    }

    @Test
    @DisplayName("Explode expands transaction 3 into correct number of rows")
    void testExplodeExpandsThreeItems() {

        long rowCount = flat
                .filter(col("transaction_id").equalTo(3))
                .count();

        assertEquals(3, rowCount,
                "transaction_id=3 has 3 items — should explode into 3 rows");
    }

    @Test
    @DisplayName("Total row count after explode matches sum of all items")
    void testTotalRowCountAfterExplode() {

        assertEquals(9, flat.count(),
                "Total rows after explode should be 9 — sum of all items across all transactions");
    }

    @Test
    @DisplayName("line_value is correctly computed as quantity x price")
    void testLineValueCalculation() {

        Row row = flat
                .filter(col("transaction_id").equalTo(1)
                        .and(col("product").equalTo("Laptop")))
                .select("line_value")
                .first();

        assertEquals(75000.0, row.getDouble(0), 0.01,
                "Laptop line_value should be 1 x 75000 = 75000");
    }

    @Test
    @DisplayName("Transaction 1 total value matches manual calculation")
    void testTransaction1TotalValue() {

        Row row = flat
                .filter(col("transaction_id").equalTo(1))
                .agg(sum("line_value").alias("total_value"))
                .first();

        assertEquals(78000.0, ((Number) row.get(0)).doubleValue(), 0.01,
                "Transaction 1 total value should be 78000");
    }

    @Test
    @DisplayName("Transaction 3 total value matches manual calculation")
    void testTransaction3TotalValue() {

        Row row = flat
                .filter(col("transaction_id").equalTo(3))
                .agg(sum("line_value").alias("total_value"))
                .first();

        assertEquals(36700.0, ((Number) row.get(0)).doubleValue(), 0.01,
                "Transaction 3 total value should be 36700");
    }

    @Test
    @DisplayName("FAILED payment transactions are excluded from success filter")
    void testFailedPaymentExclusion() {

        long failedCount = flat
                .filter(col("payment_status").equalTo("SUCCESS"))
                .filter(col("transaction_id").equalTo(4))
                .count();

        assertEquals(0, failedCount,
                "transaction_id=4 with FAILED payment should be excluded from SUCCESS filter");
    }

    @Test
    @DisplayName("collect_list produces array with correct products for transaction 1")
    void testCollectList() {
        Row row = flat
                .filter(col("transaction_id").equalTo(1))
                .agg(collect_list("product").alias("products"))
                .first();

        List<String> products = row.getList(0);
        assertEquals(2, products.size(),
                "Transaction 1 should have 2 products in collect_list");
        assertTrue(products.contains("Laptop"),
                "Products list should contain Laptop");
        assertTrue(products.contains("Mouse"),
                "Products list should contain Mouse");
    }

    @Test
    @DisplayName("Normalised dim_customers has no duplicate customer_ids")
    void testNormalisedCustomersNoDuplicates() {
        Dataset<Row> customers = raw
                .select(col("customer.id").alias("customer_id"))
                .dropDuplicates("customer_id");

        long totalCustomers = raw
                .select(col("customer.id").alias("customer_id"))
                .count();

        long distinctCustomers = customers.count();

        assertEquals(totalCustomers, distinctCustomers,
                "All customer_ids in test data are distinct — no duplicates expected");
    }
}