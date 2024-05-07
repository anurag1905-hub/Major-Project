/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.spark.basics;

import static org.apache.parquet.example.Paper.schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author workspace
 */
public class ReadFromKafka {
    public static void main(String args[]){
        
        SparkSession spark = SparkSession
                             .builder()
                             .appName("ReadingDataFromKafka")
                             .config("spark.master", "local")
                             .getOrCreate();
        
        Dataset<Row> df = spark
        .read()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "dataStorageTopic")
        .option("startingOffsets", "earliest")
        .load();
         
        Dataset<Row> jsonDF = df.selectExpr("cast(value as string) as value");
        
        StructType schema = new StructType()
                                 .add("id", "string")
                                 .add("age", "string")
                                 .add("gender","string")
                                 .add("height","string")
                                 .add("weight","string")
                                 .add("ap_hi","string")
                                 .add("ap_lo","string")
                                 .add("cholestrol","string")
                                 .add("gluc","string")
                                 .add("smoke","string")
                                 .add("alco","string")
                                 .add("active","string")
                                 .add("cardio","string");
        
        Dataset<Row> valueDF = jsonDF.select(from_json(col("value").cast("string"), schema).alias("value"));
        
        Dataset<Row>explodedDF = valueDF.selectExpr(
                "value.id",
                "value.age",
                "value.gender",
                "value.height",
                "value.weight",
                "value.ap_hi",
                "value.ap_lo",
                "value.cholestrol",
                "value.gluc",
                "value.smoke",
                "value.alco",
                "value.active",
                "value.cardio"
        );
        
        Dataset<Row>exploded_df = explodedDF.withColumn("id", col("id").cast("integer"))
                .withColumn("age", col("age").cast("integer"))
                .withColumn("gender", col("gender").cast("integer"))
                .withColumn("height", col("height").cast("integer"))
                .withColumn("weight", col("weight").cast("integer"))
                .withColumn("ap_hi", col("ap_hi").cast("integer"))
                .withColumn("ap_lo", col("ap_lo").cast("integer"))
                .withColumn("cholestrol", col("cholestrol").cast("integer"))
                .withColumn("gluc", col("gluc").cast("integer"))
                .withColumn("smoke", col("smoke").cast("integer"))
                .withColumn("alco", col("alco").cast("integer"))
                .withColumn("active", col("active").cast("integer"))
                .withColumn("cardio", col("cardio").cast("integer"));
        
        exploded_df.show(false);
        


    }
}
