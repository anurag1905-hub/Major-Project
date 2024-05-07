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
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import scala.*;

/**
 *
 * @author workspace
 */
public class SendDataToKafka {
    public static void main(String args[]){
        
        SparkSession spark = SparkSession
                             .builder()
                             .appName("SendingDataToKafka")
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
         
//         StructType Schema = new StructType()
//                                 .add("City", "String")
//                                 .add("Date", "String")
//                                 .add("PM2_dot_5","double")
//                                 .add("O3","double")
//                                 .add("AQI_Bucket","String")
//                                 .add("AQI","double")
//                                 .add("Benzene","double")
//                                 .add("CO","double")
//                                 .add("NH3","double")
//                                 .add("NO","double")
//                                 .add("NO2","double")
//                                 .add("NOx","double")
//                                 .add("PM10","double")
//                                 .add("SO2","double")
//                                 .add("Toluene","double")
//                                 .add("Xylene","double");
                                

         
        Dataset<Row> csvDF = spark
                            .read()
                            .option("sep", ",") // make separator , or ;
                            .schema(userSchema)      // Specify schema of the csv files
                            .csv("/home/workspace/Desktop/Helper/final-data.csv");
        
//        csvDF.show();
        
        csvDF.toJSON()
        .write()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "topic")
        .save();
       
    }
}
