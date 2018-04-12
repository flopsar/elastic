package org.flopsar.addons.elastic;


import com.flopsar.fdbc.api.FDBCFactory;
import com.flopsar.fdbc.api.fdb.ConnectionFDB;

import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Main {

    private static final String ES_HOST= "elastic.host";
    private static final String ES_USER= "elastic.username";
    private static final String ES_PASSWORD= "elastic.password";
    private static final String ES_PORT_HTTP= "elastic.port.http";
    private static final String ES_INDEX = "elastic.index.prefix";
    private static final String FLOPSAR_ADDRESS = "flopsar.address";
    private static final String FLOPSAR_AGENT_PATTERN = "flopsar.agent.pattern";
    private static final String FLOPSAR_DURATION_THRESHOLD = "flopsar.duration.threshold";

    private String flopsarHost;
    private int flopsarPort;
    private String elasticIndexPrefix;
    private String elasticHost;
    private int elasticHttpPort;
    private Pattern flopsarAgentPattern;
    private int threshold;
    private static String currentIndex;
    private String elasticUsername;
    private String elasticPassword;
    private static final Properties defaults = new Properties();

    static {
        defaults.setProperty(ES_HOST,"localhost");
        defaults.setProperty(ES_PORT_HTTP,"9200");
        defaults.setProperty(FLOPSAR_DURATION_THRESHOLD,"5");
        defaults.setProperty(ES_INDEX,"flopsar-");
    }


    private void loadSettings(String settingsFile) throws Exception {
        Properties props = new Properties(defaults);
        props.load(new FileInputStream(settingsFile));

        String flopsarAddress = props.getProperty(FLOPSAR_ADDRESS);
        if (flopsarAddress == null){
            System.err.println("Flopsar database address not specified.");
            return;
        }
        String[] address = flopsarAddress.split(":");
        if (address.length != 2){
            System.err.println("Invalid Flopsar database address");
            return;
        }
        flopsarHost = address[0];
        flopsarPort = Integer.parseInt(address[1]);
        elasticIndexPrefix =  props.getProperty(ES_INDEX);
        elasticHost = props.getProperty(ES_HOST);
        elasticUsername = props.getProperty(ES_USER);
        elasticPassword = props.getProperty(ES_PASSWORD);
        elasticHttpPort = Integer.parseInt(props.getProperty(ES_PORT_HTTP));
        threshold = Integer.parseInt(props.getProperty(FLOPSAR_DURATION_THRESHOLD));
        String agentPattern = props.getProperty(FLOPSAR_AGENT_PATTERN);
        if (agentPattern == null || agentPattern.isEmpty()){
            System.err.println("Invalid Flopsar database address.");
            return;
        }
        flopsarAgentPattern = Pattern.compile(agentPattern);
    }



    private void run(ElasticSearch es,String host,int port) throws Exception {
        Flopsar fdbc = new Flopsar();
        System.out.println("Connecting to "+host);
        ConnectionFDB conn = FDBCFactory.createConnection(host,port,(dbHost, dbPort) -> {
            /* Here, handle database connection closed event. */
        },0);
        if(conn == null)
            return;

        long to = System.currentTimeMillis();
        long from = to - TimeUnit.MILLISECONDS.convert(5,TimeUnit.MINUTES);
        while (true){
            System.out.println("Next cycle started. FROM: "+new Date(from)+" TO: "+new Date(to));
            currentIndex = es.ensureIndex(to);
            if (currentIndex == null){
                System.err.println("Unable to create a new index.");
                break;
            }

            long startTime = System.currentTimeMillis();
            fdbc.loadCalls(conn,es,from,to,flopsarAgentPattern,threshold);
            long endTime = System.currentTimeMillis();

            long secs = TimeUnit.SECONDS.convert(endTime - startTime,TimeUnit.MILLISECONDS);
            long sleep = TimeUnit.MILLISECONDS.convert(30,TimeUnit.SECONDS);
            System.out.println(String.format("Cycle ended within %d seconds", secs));
            if (secs == 0){
                Thread.sleep(sleep);
                endTime += sleep;
            }
            from = to + 1;
            to = endTime;
        }
    }



    public static void main(String[] args) throws Exception {
        if(args.length < 1){
            System.err.println("Missing argument: settings file.");
            return;
        }

        Main m = new Main();
        m.loadSettings(args[0]);
        ElasticSearch elastic = ElasticSearch.init(m.elasticHost,m.elasticHttpPort,m.elasticIndexPrefix,m.elasticUsername,m.elasticPassword);
        m.run(elastic,m.flopsarHost,m.flopsarPort);
    }


    public static String getCurrentIndex(){
        return currentIndex;
    }

}
