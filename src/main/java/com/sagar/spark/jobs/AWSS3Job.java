package com.sagar.spark.jobs;

import com.sagar.spark.utils.AppConfig;
import com.sagar.spark.utils.SparkSessionFactory;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class AWSS3Job {

    private static final String CSV_ORDERS_V2 = AppConfig.getFilePath("csv.path.orders_v2");
    private static final String S3_BUCKET = AppConfig.getFilePath("s3.bucket");
    private static final String S3_INPUT = AppConfig.getFilePath("s3.input.path");
    private static final String S3_OUTPUT_PARQUET = AppConfig.getFilePath("s3.output.path.parquet");
    private static final String S3_OUTPUT_DELTA = AppConfig.getFilePath("s3.output.path.delta");
    private static final String S3_OUTPUT_REPORT = AppConfig.getFilePath("s3.output.path.report");

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSessionFactory.get();

        System.out.println("================================= UPLOADING TO S3 ====================");
        uploadToS3(CSV_ORDERS_V2, S3_BUCKET, "raw/orders/orders_v2.csv");

        System.out.println("Upload complete.........................");

        System.out.println("================================= READ FROM S3 =======================");

        StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("order_id",    DataTypes.IntegerType, true),
                        DataTypes.createStructField("customer_id", DataTypes.IntegerType, true),
                        DataTypes.createStructField("product",     DataTypes.StringType,  true),
                        DataTypes.createStructField("quantity",    DataTypes.IntegerType, true),
                        DataTypes.createStructField("price",       DataTypes.DoubleType,  true),
                        DataTypes.createStructField("status",      DataTypes.StringType,  true),
                        DataTypes.createStructField("order_date",  DataTypes.StringType,  true)
        }
        );

        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(S3_INPUT + "orders_v2.csv");

        orders.printSchema();
        System.out.println("Row count from S3: " + orders.count());

        Dataset<Row> enriched = orders.filter(
                col("status").equalTo("COMPLETED")
        ).withColumn("order_value", col("price").multiply(col("quantity")));

        System.out.println("================================= WRITE PARQUET TO S3 =======================");
        enriched.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("order_date")
                .parquet(S3_OUTPUT_PARQUET);

        System.out.println("Parquet written to: " + S3_OUTPUT_PARQUET);

        System.out.println("================================= WRITE DELTA TO S3 ======================");
        if (DeltaTable.isDeltaTable(spark, S3_OUTPUT_DELTA)) {
            DeltaTable.forPath(spark, S3_OUTPUT_DELTA)
                    .alias("target")
                    .merge(enriched.alias("source"),
                            "target.order_id = source.order_id")
                    .whenMatched().updateAll()
                    .whenNotMatched().insertAll()
                    .execute();
        } else {
            enriched.write()
                    .format("delta")
                    .mode(SaveMode.Overwrite)
                    .partitionBy("order_date")
                    .save(S3_OUTPUT_DELTA);
        }

        System.out.println("Delta written to: " + S3_OUTPUT_DELTA);

        System.out.println("================================= REVENUE REPORT TO S3 ======================");
        Dataset<Row> report = enriched.groupBy("product", "order_date")
                .agg(
                        sum(col("order_value")).alias("total_revenue"),
                        count("order_id").alias("order_count"),
                        round(avg("order_value"), 2).alias("avg_order_value")
                )
                .orderBy(col("total_revenue").desc());

        report.show();

        report.write()
                .mode(SaveMode.Overwrite)
                .parquet(S3_OUTPUT_REPORT);

        System.out.println("Report written to: " + S3_OUTPUT_REPORT);

        System.out.println("================================= VERIFY S3 WRITE ======================");
        long writtenCount = spark.read()
                .format("parquet")
                .load(S3_OUTPUT_DELTA)
                .count();

        System.out.println("Rows written to S3  : " + writtenCount);
        System.out.println("Rows in source      : " + enriched.count());

        if (writtenCount != enriched.count()){
            throw new RuntimeException("Write verification failed — " +"expected " + enriched.count() +" got " + writtenCount);
        }

        SparkSessionFactory.stop();

    }

    // -------------------------------------------------------
    // Helper — upload local file to S3 using AWS CLI
    // In production — files arrive via Kinesis or direct upload
    // This helper is for local dev only
    // -------------------------------------------------------

    private static void uploadToS3(String localPath, String bucket, String s3Key) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "aws", "s3", "cp",
                localPath,
                "s3://" + bucket + "/" + s3Key
        );
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException(
                    "AWS S3 upload failed with exit code: " + exitCode);
        }
    }

}
