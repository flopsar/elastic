package com.flopsar.addons.elastic;

import com.flopsar.api.AgentId;
import com.flopsar.fdbc.api.agent.Agent;
import com.flopsar.fdbc.api.fdb.ConnectionFDB;
import com.flopsar.fdbc.api.fdb.ResponseAsyncCallback;
import com.flopsar.fdbc.api.fdb.RootCall;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Flopsar {

    private final ConcurrentHashMap<Long,FlopsarAgent> flopsarAgentsMap = new ConcurrentHashMap<>();






    private ConcurrentLinkedQueue<RootCall> retrieveRootCalls(ConnectionFDB conn, int threshold, Map<Long,long[]> agentsMap) throws Exception {
        ConcurrentLinkedQueue<RootCall> rootCallsQueue = new ConcurrentLinkedQueue<>();

        conn.findRootCallsByPattern(threshold, Integer.MAX_VALUE, agentsMap, null, null, null, (response, responseStatus) -> {
            if (response != null && (ResponseAsyncCallback.RESPONSE_ERROR != responseStatus)) {
                System.out.println("Some data received. "+response.size());
                rootCallsQueue.addAll(response);
            }
            if (0 != responseStatus) {
                synchronized (rootCallsQueue) {
                    rootCallsQueue.notify();
                }
            }
        });
        synchronized (rootCallsQueue) {
            while (true) {
                try {
                    rootCallsQueue.wait();
                    break;
                } catch (InterruptedException e) { }
            }
        }

        return rootCallsQueue;
    }




    private void agentsToQuery(Map<Long,long[]> agentsMap,long from,long to,List<Agent> agents,Pattern flopsarAgentPattern){
        agentsMap.clear();
        if (agents.isEmpty())
            return;

        for (Agent a : agents) {
            Matcher m = flopsarAgentPattern.matcher(a.getId().getName());
            if (!m.find() || a.getId().getType() != AgentId.AgentType.JVM_AGENT)
                continue;

            FlopsarAgent fa = flopsarAgentsMap.get(a.getId().getId());
            if (fa == null) {
                fa = new FlopsarAgent(a);
                flopsarAgentsMap.put(fa.getId().getId(),fa);
                fa.setFrom(from);
                fa.setTo(to);
            }
            if (fa.getTo() <= to)
                agentsMap.put(fa.getId().getId(), new long[]{fa.getFrom(), fa.getTo()});
        }
    }



    void loadCalls(ConnectionFDB conn, ElasticSearch elastic, long from, long to,Pattern flopsarAgentPattern,int threshold) throws Exception {
        List<Agent> agents = conn.readAgents();
        if (agents.isEmpty())
            return;

        System.out.println("Agents available "+agents.size());
        Map<Long,long[]> agentsMap = new HashMap<>();
        do {
            agentsToQuery(agentsMap,from,to,agents,flopsarAgentPattern);
            if (agentsMap.isEmpty())
                return;

            ConcurrentLinkedQueue<RootCall> rootCallsQueue = retrieveRootCalls(conn,threshold,agentsMap);
            if (rootCallsQueue.isEmpty())
                return;

            for (RootCall rc : rootCallsQueue) {
                FlopsarAgent fa = flopsarAgentsMap.get(rc.getAgentId());
                if (fa == null) {
                    /* No such agent. */
                    continue;
                }
                fa.completeSymbols(rc);
            }
            rootCallsQueue.clear();

            for (FlopsarAgent fa: flopsarAgentsMap.values()) {
                fa.completeMissingSymbols(conn,elastic);
            }
        } while (!agentsMap.isEmpty());



//                BulkRequestBuilder bulk = es.prepareBulk();
//                int collected = 0;
//                for (Stack s : stacks) {
//                    //System.out.println("Processing stack ... "+s.getOrigin().getClassName());
//                    // List<Invocation> calls = conn.queryStackInvocations(s);
//
//                    Invocation i = s.getOrigin();
////                    for(Invocation i : calls){
//                    String index = es.ensureIndex(i.getTimeStamp());
//                    if (index == null) {
//                        System.err.println("Unable to create a new index!");
//                        break;
//                    }
//                    if (i.getClassName().equals("WebRequest")) {
//                        String json = JsonConverter.convertWEB(a.getIdentifier(), i.getClassName(), i.getDuration(), i.getTimeStamp(), i.getParameters());
//                        if (json == null)
//                            continue;
//                        bulk.add(es.getClient().prepareIndex(index, ElasticSearch.TYPE_WEB).setSource(json));
//                        collected++;
//                    } else {
//                        String json = JsonConverter.convertCALL(a.getIdentifier(), i.getClassName(), i.getMethodName(),
//                                i.getMethodSignature(), i.getDuration(), i.getTimeStamp(), i.getParameters(), i.isExceptionThrown());
//                        if (json == null)
//                            continue;
//                        bulk.add(es.getClient().prepareIndex(index, ElasticSearch.TYPE_CALL).setSource(json));
//                        collected++;
//                    }
//                    //}
//                    if (collected > 0)
//                        es.sendBulk(bulk);
//                }
//            }
        }
    }
