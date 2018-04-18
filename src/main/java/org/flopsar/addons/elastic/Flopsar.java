package org.flopsar.addons.elastic;

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






    private ConcurrentLinkedQueue<RootCall> retrieveRootCalls(ConnectionFDB conn, int threshold, final Map<Long,long[]> agentsMap) throws Exception {
        ConcurrentLinkedQueue<RootCall> rootCallsQueue = new ConcurrentLinkedQueue<>();
        long timeout = 20000;
        conn.findRootCallsByPattern(threshold, Integer.MAX_VALUE, agentsMap, null, null, null, (response, responseStatus) -> {
            if (response != null && (ResponseAsyncCallback.RESPONSE_ERROR != responseStatus)) {
                System.out.println("# RootCalls received: "+response.size());
                rootCallsQueue.addAll(response);
            }
            System.out.println("# RootCalls received status "+responseStatus);
            if (0 != responseStatus) {
                synchronized (rootCallsQueue) {
                    rootCallsQueue.notify();
                }
            }
        });
        synchronized (rootCallsQueue) {
            while (true) {
                try {
                    rootCallsQueue.wait(timeout);
                    System.out.println("RootCalls data ready.");
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
            if (fa.getTo() <= to) {
                fa.setTo(to);
                agentsMap.put(fa.getId().getId(), new long[]{fa.getFrom(), fa.getTo()});
                System.out.println("Agent "+fa.getId().getName()+" selected.");
            }
            System.out.println("Agent "+fa.getId().getName()+" time range from: "+fa.getFrom()+" to: "+fa.getTo());
        }
    }



    void loadCalls(ConnectionFDB conn, ElasticSearch elastic, long from, long to,Pattern flopsarAgentPattern,int threshold) throws Exception {

        List<Agent> agents = conn.readAgents();
        if (agents.isEmpty())
            return;

        Map<Long,long[]> agentsMap = new HashMap<>();
        int totalSent = 0;
        do {
            agentsToQuery(agentsMap,from,to,agents,flopsarAgentPattern);
            if (agentsMap.isEmpty())
                break;

            ConcurrentLinkedQueue<RootCall> rootCallsQueue = retrieveRootCalls(conn,threshold,agentsMap);
            if (rootCallsQueue.isEmpty())
                break;

            for (RootCall rc : rootCallsQueue) {
                FlopsarAgent fa = flopsarAgentsMap.get(rc.getAgentId());
                if (fa == null) {
                    /* No such agent. */
                    continue;
                }
                fa.completeSymbols(rc);
            }
            rootCallsQueue.clear();

            for (Long aid : agentsMap.keySet()) {
                FlopsarAgent fa = flopsarAgentsMap.get(aid);
                totalSent += fa.completeMissingSymbols(conn,elastic);
            }
        } while (!agentsMap.isEmpty());

        System.out.println("Total calls sent: "+totalSent);
    }

    }
