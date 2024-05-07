/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.spark.basics;

/**
 *
 * @author workspace
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

public class Predictions {
    public static void main(String args[]){

        SparkSession spark = SparkSession
                             .builder()
                             .appName("RealTimePredictions")
                             .config("spark.master", "local")
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
                                 .add("cardio","integer");

         
        Dataset<Row> csvDF = spark
                            .readStream()
                            .option("sep", ";")
                            .schema(userSchema)      // Specify schema of the csv files
                            .csv("/home/workspace/Downloads/archive (11)/");
        
        Dataset<Row> filteredDataFrame = csvDF.filter("cholestrol>0");

        Dataset<Row> aggDF = csvDF.groupBy("cholestrol").count();
        
        filteredDataFrame.printSchema();
        
        try {
            StreamingQuery query = filteredDataFrame
                    .writeStream()
                    .format("console")
                    .start();
            query.awaitTermination();
        } catch (TimeoutException ex) {
            System.out.println("There was an error");
        } catch (StreamingQueryException ex) {
            Logger.getLogger(Predictions.class.getName()).log(Level.SEVERE, null, ex);
        }
          
    }
}
