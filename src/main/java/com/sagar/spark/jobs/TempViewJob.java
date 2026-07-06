package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TempViewJob {

    private static final String CSV_PATH_ORDERS = AppConfig.get("csv.path.orders_v2");

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSessionFactory.get();

        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH_ORDERS);

//        Session-scoped temp view
        orders.createOrReplaceTempView("orders_session");

//        Global temp view
        orders.createOrReplaceGlobalTempView("orders_global");

        System.out.println("========================== SESSION-TEMP-VIEW ==========================");
        spark.sql("SELECT COUNT(*) AS row_count FROM orders_session").show();

        // Global temp view must be prefixed with global_temp.
        System.out.println("========================== global_temp-VIEW ==========================");
        spark.sql("SELECT COUNT(*) AS row_count FROM global_temp.orders_global").show();

        System.out.println("========================== CATALOG-ONLY-SESSION-TABLE/VIEWS ==========================");
        spark.catalog().listTables().show();
        System.out.println("========================== CATALOG-ALL-TABLE/VIEWS ==========================");
        spark.catalog().listTables("global_temp").show();

        System.out.println("========================== DROP-VIEWS ==========================");
        spark.catalog().dropTempView("orders_session");
        spark.catalog().dropGlobalTempView("orders_global");

        spark.catalog().listTables("global_temp").show();
        System.out.println("========================== Session temp view dropped. ==========================");

    }
}
