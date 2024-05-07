/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.spark.basics;

import io.netty.handler.codec.http.HttpResponse;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import net.sourceforge.jFuzzyLogic.FIS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;        



/**
 *
 * @author workspace
 */
public class KafkaSiddhi {
    static long eventsProcessed = 0;
    public static void main(String args[]) throws InterruptedException{
        SiddhiManager siddhiManager = new SiddhiManager();
        
        eventsProcessed=0;
         //Define Siddhi application
        String siddhiApp = "@App:name('KafkaSiddhiApp') \n" +
                "@source(type='kafka', topic.list='topic', " +
                "threading.option='single.thread', group.id='group', " +
                "bootstrap.servers='localhost:9092', " +
                "@map(type='json')) " +
                "define stream InputStream (id int, age int,gender int,height int,weight float,ap_hi int,ap_lo int,cholestrol int,gluc int,smoke int,alco int,active int,cardio int); \n" +
                "@info(name = 'query') " +
                "from InputStream[age>0] " +
                "select * " +
                "insert into OutputStream;";
        
        long startTime = System.nanoTime();
        
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
           
            public void receive(Event[] events) {
                EventPrinter.print(events);
                System.out.println(events[0]);
                Integer age = (Integer) events[0].getData(1);
                Integer gender = (Integer) events[0].getData(2);
                Integer ap_hi = (Integer) events[0].getData(5);
                Integer ap_lo = (Integer) events[0].getData(6);
                Integer cholestrol = (Integer) events[0].getData(7);
                Integer glucose = (Integer) events[0].getData(8);
                Integer smoker = (Integer) events[0].getData(9);
                eventsProcessed+=1;      
                
                                                                                       
                String fileName = "/home/workspace/Desktop/jFuzzyLogic_v3.0/fcl/health.fcl";
                FIS fis = FIS.load(fileName,true);
                // Error while loading?
                if( fis == null ) { 
                    System.err.println("Can't load file: '" 
                                           + fileName + "'");
                }
                            

                fis.setVariable("age", age);
                fis.setVariable("systolic_bp", ap_hi);
                fis.setVariable("diastolic_bp", ap_lo);
                fis.setVariable("gender", gender);
                fis.setVariable("cholestrol", cholestrol);
                fis.setVariable("smoking", smoker);

                // Evaluate
                fis.evaluate();

                double risk =  fis.getVariable("risk").getLatestDefuzzifiedValue();
                Integer riskValue = (int) risk;
                if(riskValue<20){
                    System.out.println("Very low risk");
                }
                else if(riskValue>=15 && riskValue<=45){
                    System.out.println("Low risk");
                }
                else if(riskValue>=35 && riskValue<=65){
                    System.out.println("Medium risk");
                }
                else if(riskValue>=55 && riskValue<=85){
                    System.out.println("High risk");
                }
                else if(riskValue>75){
                    System.out.println("Very high risk");
                }
                
                try {
                    PostRequestSender sender = new PostRequestSender();
                    sender.sendPost(ap_hi,ap_lo,cholestrol,glucose,riskValue);
                } catch (Exception ex) {
                    Logger.getLogger(KafkaSiddhi.class.getName()).log(Level.SEVERE, null, ex);
                }
                    }
                });
        
//        // Define callback to receive query results
//        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
//            @Override
//            public void receive(Event[] events) {
//                for (Event event : events) {
//                    System.out.println(event.toString());
//                }
//            }
//        });

        // Start processing
        siddhiAppRuntime.start();
        Thread.sleep(25000);
        
        long endTime = System.nanoTime();
        endTime-=20;
        double executionTime = (endTime - startTime)/1000000000.0;
        System.out.println("Time taken to execute query = "+executionTime+" seconds");
        System.out.println("Number of events processed = "+eventsProcessed);
        
        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();
        
    }
}
