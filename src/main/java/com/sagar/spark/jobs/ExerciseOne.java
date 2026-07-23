package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class ExerciseOne {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Solve the Example Problems").master("local[*]").getOrCreate();

//        Problem 2: Add Schema Explicitly
        StructField fieldOne = DataTypes.createStructField("order_id ", DataTypes.IntegerType, true);

        StructField[] structFields = new StructField[]{
                fieldOne,
                DataTypes.createStructField("customer_name", DataTypes.StringType, true),
                DataTypes.createStructField("product", DataTypes.StringType, true),
                DataTypes.createStructField("quantity", DataTypes.IntegerType, true),
                DataTypes.createStructField("price", DataTypes.DoubleType, true),
                DataTypes.createStructField("status", DataTypes.StringType, true),
                DataTypes.createStructField("order_date", DataTypes.StringType, true)};

        StructType orders_schema = DataTypes.createStructType(structFields);

        Dataset<Row> ordersCsvData = spark.read().option("header", "true").option("inferSchema", "false").schema(orders_schema).csv(AppConfig.getFilePath("csv.path.orders"));

        System.out.println("################# SCHEMA-DEFINED-EXPLICITLY ########################");
        ordersCsvData.printSchema();


//        Problem 3: Write The filter Job. Write the DataFrame code (no SQL) to answer: "For each customer, what is their total spend across all COMPLETED orders, sorted highest to lowest?"
        System.out.println("################# FILTER-JOB ########################");

//        Always structure your code as:
//        Filter Early -> Group & Aggregate -> Sort Results -> Trigger Action.
//        This pattern minimizes data volume early, prevents pipeline syntax bugs, and ensures the best execution performance.
        ordersCsvData.filter(col("status").equalTo("COMPLETED"))
                .groupBy("customer_name")
                .agg(
                        sum(col("price").multiply(col("quantity"))).alias("total_expense"),
                        count("customer_name").alias("number_of_orders")
                )
                .select("customer_name",  "total_expense", "number_of_orders")
                .orderBy(col("total_expense").desc())
                .show(10);

    }
}
