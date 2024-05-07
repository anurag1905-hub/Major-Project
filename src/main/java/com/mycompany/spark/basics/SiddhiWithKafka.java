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


import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;


public class SiddhiWithKafka {
    public static void main(String args[]) throws InterruptedException{
       

        //Siddhi Application
        String siddhiApp = "@App:name('TestApp')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream[volume < 150] " +
                "select symbol, price " +
                "insert into OutputStream;";
        
        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Generate runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        //Adding callback to retrieve output events from stream
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
           
            public void receive(Event[] events) {
                EventPrinter.print(events);
                System.out.println(events[0]);
                // To convert and print event as a map
                // EventPrinter.print(toMap(events));
                
            }
        });

        //Get InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");

        //Start processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"IBM", 700f, 100L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"WSO2", 45.6f, 50L});

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();
    }
}
