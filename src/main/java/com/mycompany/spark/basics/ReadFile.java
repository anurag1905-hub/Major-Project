/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.spark.basics;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import scala.*;

/**
 *
 * @author workspace
 */
public class ReadFile {

    public static void main(String args[]) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                             .builder()
                             .appName("ReadingFile")
                             .config("spark.master", "local") 
                .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled","false")
                             .getOrCreate();
         
         StructType userSchema = new StructType()
                                 .add("id", "integer")
                                 .add("age", "integer")
                                 .add("gender","integer")
                                 .add("height","integer")
                                 .add("weight","float")
                                 .add("ap_hi","integer")
                                 .add("ap_lo","integer")
                                 .add("cholestrol","integer")
                                 .add("gluc","integer")
                                 .add("smoke","integer")
                                 .add("alco","integer")
                                 .add("active","integer")
                                 .add("cardio","integer")
                                 .add("timestamp","Timestamp");
                                             
        Dataset<Row> csvDF = spark
                            .readStream()
                            .option("sep", ",") // make separator , or ;
                            .schema(userSchema)      // Specify schema of the csv files
                            .csv("/home/workspace/Desktop/Timestamp-Data");
        
//        Dataset<Row> csv_df_with_timestamp = csvDF.withColumn("timestamp", current_timestamp());

        Dataset<Row> countDF = csvDF.withColumn("counter", lit(1));
        
        Dataset<Row> windowedDF = countDF
                .withWatermark("timestamp", "30 seconds")
                .groupBy(window(col("timestamp"), "1 minutes"))
                .agg(sum("counter").as("sum_count"));
        
        Dataset<Row> averageEventsPerWindow = windowedDF
                .select(avg("sum_count").as("average_events_per_window"));
        
        StreamingQuery query = averageEventsPerWindow
                .writeStream()
                .option("truncate", false)
                .outputMode("complete")
                .format("console")
                .start();

        // Await termination
        query.awaitTermination();
        
    }
}
