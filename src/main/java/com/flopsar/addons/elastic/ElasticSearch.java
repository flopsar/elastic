package com.flopsar.addons.elastic;

import com.google.common.io.Resources;
import org.apache.commons.codec.Charsets;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;



public class ElasticSearch {

    private static final DateFormat format = new SimpleDateFormat("YYYY.MM.dd");
    private final CloseableHttpClient httpclient = HttpClients.createDefault();
    public static final String TYPE_KV = "kv";
    public static final String TYPE_CALL = "call";
    private final TransportClient client;

    private final String index;
    private String currentDay = "";
    private String MAPPING;
    private final String elasticURL;


    private ElasticSearch(String host,int portHTTP,int portTCP,String index) throws UnknownHostException {
        this.elasticURL = String.format("http://%s:%d/",host,portHTTP);
        this.index = index;
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), portTCP));
    }


    public void createIndexIfAbsent(String mapping) throws IOException {
        String url = String.format("%s/%s%s",elasticURL,index,currentDay);
        System.out.println("Checking index existence "+url);
        HttpHead head = new HttpHead(url);
        HttpResponse response = httpclient.execute(head);
        boolean notExists = response.getStatusLine().getStatusCode() == 404;
        if(notExists){
            System.out.println("New index creating: "+currentDay);
            int code = send(url,mapping);
            System.out.println("Response Index Create Code : " + code);
        }
    }

    private int send(String url,String json) throws IOException {
        HttpPost post = new HttpPost(url);
        StringEntity jsonEntity = new StringEntity(json, ContentType.create("text/plain", "UTF-8"));
        post.setEntity(jsonEntity);
        HttpResponse response = httpclient.execute(post);
        return handleResponse(response);
    }


    private int handleResponse(HttpResponse response) throws IOException {
        int code = response.getStatusLine().getStatusCode();
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

        StringBuilder result = new StringBuilder();
        String line = "";
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


    public void sendBulk(BulkRequestBuilder bulk){
        try {
            //System.out.println("Sending bulk data...");
            BulkResponse bulkResponse = bulk.get();
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse r : bulkResponse) {
                    System.err.println(r.getFailureMessage());
                }
            }
        } catch(Throwable tx){
            tx.printStackTrace();
        }
    }


    public static ElasticSearch init(String host,int portHTTP,int portTCP,String index) throws IOException {
        ElasticSearch es = new ElasticSearch(host,portHTTP,portTCP,index);
        URL url = Resources.getResource("mapping.json");
        es.MAPPING = Resources.toString(url, Charsets.UTF_8);
        return es;
    }


    public BulkRequestBuilder prepareBulk(){
        return client.prepareBulk();
    }
    public TransportClient getClient(){
        return client;
    }



    public void submit(){
        BulkRequestBuilder bulk = client.prepareBulk();

    }


}
