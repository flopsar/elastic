package org.flopsar.addons.elastic;

import com.google.common.io.Resources;
import org.apache.commons.codec.Charsets;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;


public class ElasticSearch {

    private static final DateFormat format = new SimpleDateFormat("YYYY.MM.dd");
    private final CloseableHttpClient httpclient;
    public static final String TYPE_CALL = "call";
    private final RestClient restClient;

    private final String index;
    private String currentDay = "";
    private String MAPPING;
    private final String elasticURL;


    private ElasticSearch(String host,int portHTTP,String index,String username,String password) {
        this.elasticURL = String.format("http://%s:%d/",host,portHTTP);
        this.index = index;

        CredentialsProvider credsProvider = username != null ? new BasicCredentialsProvider() : null;
        if (username != null) {
            credsProvider.setCredentials(new AuthScope(host, portHTTP), new UsernamePasswordCredentials(username, password));
        }

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, portHTTP, "http"));
        this.restClient = credsProvider == null ? builder.build()
                : builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credsProvider)).build();
        this.httpclient = credsProvider == null ? HttpClients.createDefault()
                : HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
    }


    private void createIndexIfAbsent(String mapping) throws IOException {
        String url = String.format("%s%s%s",elasticURL,index,currentDay);
        System.out.println("Checking index existence "+url);
        HttpHead head = new HttpHead(url);
        HttpResponse response = httpclient.execute(head);
        int rcode = response.getStatusLine().getStatusCode();
        if (rcode == 404){
            System.out.println("New index creating: "+currentDay);
            int code = send(url,mapping);
            if (200 != code){
                System.err.println("Response Index Create Code : " + code);
            }
        } else if (rcode != 200){
            System.err.println(response.toString());
        }
    }

    private int send(String url,String json) throws IOException {
        HttpPut post = new HttpPut(url);
        StringEntity jsonEntity = new StringEntity(json, ContentType.create("text/plain", "UTF-8"));
        post.setEntity(jsonEntity);
        HttpResponse response = httpclient.execute(post);
        return handleResponse(response);
    }


    private int handleResponse(HttpResponse response) throws IOException {
        int code = response.getStatusLine().getStatusCode();
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

        StringBuilder result = new StringBuilder();
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        if(code != 200 && code != 201)
            System.out.println(result.toString());
        return code;
    }


    public String ensureIndex(long tstamp){
        try {
            String day = format.format(new Date(tstamp));
            if (!day.equals(currentDay)) {
                currentDay = day;
                createIndexIfAbsent(MAPPING);
            }
            return String.format("%s%s",index,currentDay);
        } catch(IOException ex){
            return null;
        }
    }


    public boolean sendRest(String json,String endpoint){
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);
        try {
            Response response = restClient.performRequest("POST", endpoint,params, entity);
            int rcode = response.getStatusLine().getStatusCode();
            if (200 != rcode && 201 != rcode){
                String responseBody = EntityUtils.toString(response.getEntity());
                System.err.println(responseBody);
                return false;
            }
        } catch (IOException e) {
            System.err.println(json);
            e.printStackTrace();
            return false;
        }
        return true;
    }


    public static ElasticSearch init(String host,int portHTTP,String index,String username,String password) throws IOException {
        ElasticSearch es = new ElasticSearch(host,portHTTP,index,username,password);
        URL url = Resources.getResource("mapping.json");
        es.MAPPING = Resources.toString(url, Charsets.UTF_8);
        return es;
    }





}
