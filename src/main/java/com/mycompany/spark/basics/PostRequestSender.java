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
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



public class PostRequestSender {
    public static CloseableHttpClient httpClient = HttpClients.createDefault();
    public static HttpPost post = new HttpPost("http://localhost:9000/storeData");
    
    PostRequestSender(){
        
    }
    
    public void close() throws IOException {
        httpClient.close();
    }
    
    public void sendPost(Integer ap_hi,Integer ap_lo,Integer Cholestrol,Integer Glucose,Integer riskValue) throws Exception {

        // add request parameter, form parameters
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("ap_hi", ap_hi.toString()));
        urlParameters.add(new BasicNameValuePair("ap_lo", ap_lo.toString()));
        urlParameters.add(new BasicNameValuePair("cholestrol", Cholestrol.toString()));
        urlParameters.add(new BasicNameValuePair("glucose", Glucose.toString()));
        urlParameters.add(new BasicNameValuePair("riskValue", riskValue.toString()));

        post.setEntity(new UrlEncodedFormEntity(urlParameters));

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(post)) {

            System.out.println(EntityUtils.toString(response.getEntity()));
        }

    }
}
