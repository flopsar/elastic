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
                rootCallsQueue.addAll(response);
            } else if (ResponseAsyncCallback.RESPONSE_GEOF == responseStatus || ResponseAsyncCallback.RESPONSE_ERROR == responseStatus) {
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


    private void retrieveSymbols(ConnectionFDB conn,List<Long> classesToRetrieve,List<Long> methodsToRetrieve) {
        if (!classesToRetrieve.isEmpty()) {
//            conn.findSymbolsById(SymbolType.SYMBOL_CLASS,classesToRetrieve,new ResponseAsyncCallback<List<Symbol>>(){
//                @Override
//                public void onResponseReceived(List<Symbol> symbols, int i) {
//
//                }
//            });
        }
    }







    void loadCalls(ConnectionFDB conn, ElasticSearch elastic, long from, long to,Pattern flopsarAgentPattern,int threshold) throws Exception {
        List<Agent> agents = conn.readAgents();
        if (agents.isEmpty())
            return;

        Map<Long,long[]> agentsMap = new HashMap<>();

        for (Agent a : agents) {
            Matcher m = flopsarAgentPattern.matcher(a.getId().getName());
            if (!m.find() || a.getId().getType() != AgentId.AgentType.JVM_AGENT)
                continue;

            FlopsarAgent fa = flopsarAgentsMap.get(a.getId().getId());
            if (fa == null) {
                fa = new FlopsarAgent(a);
                flopsarAgentsMap.put(fa.getId(),fa);
                fa.setFrom(from);
                fa.setTo(to);
            }
            agentsMap.put(fa.getId(), new long[]{fa.getFrom(), fa.getTo()});
        }

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



//
//
//            //System.out.println("Processing agent ... "+a.getIdentifier());
//            long t1 = from;
//            while (t1 < to) {
//                List<RootCall> stacks = conn.
//                        conn.queryStacks(a.getIdentifier(), t1, to, 0, Long.MAX_VALUE, 8000, null, false, false);
//
//                Collections.sort(stacks, (o1, o2) -> (int) (o1.getOrigin().getTimeStamp() - o2.getOrigin().getTimeStamp()));
//
//                long newT1 = stacks.get(stacks.size() - 1).getOrigin().getTimeStamp() + 1;
//                if (newT1 > t1) {
//                    t1 = newT1;
//                }
//
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
