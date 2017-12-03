package com.flopsar.addons.elastic;

import com.flopsar.fdbc.api.agent.Agent;
import com.flopsar.fdbc.api.fdb.RootCall;
import com.flopsar.fdbc.api.fdb.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class FlopsarAgent {

    private final Agent agent;
    private long from;
    private long to;
    private final ConcurrentHashMap<Long,Symbol> symbolsClass = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long,Symbol> symbolsMethod = new ConcurrentHashMap<>();
    private final List<Long> missingsClasses = new ArrayList<>();
    private final List<Long> missingMethods = new ArrayList<>();
    private final List<RootCall> rootCalls = new ArrayList<>();
    private final List<RootCall> incompleteRootCalls = new ArrayList<>();

    public FlopsarAgent(Agent a){
        agent = a;
    }




    public void completeSymbols(RootCall rc){
        boolean rootCallCompleted = false;

        if (rc.getTimestamp() > from)
            from = rc.getTimestamp();

        Symbol classSymbol = symbolsClass.get(rc.getClassSymbol().getId());
        if (classSymbol == null) {
            missingsClasses.add(rc.getClassSymbol().getId());
        } else {
            rc.setClassSymbol(classSymbol);
            rootCallCompleted = true;
        }
        Symbol methodSymbol = symbolsMethod.get(rc.getMethodSymbol().getId());
        if (methodSymbol == null) {
            missingMethods.add(rc.getMethodSymbol().getId());
            rootCallCompleted = false;
        } else {
            rc.setMethodSymbol(methodSymbol);
        }
        if (rootCallCompleted)
            rootCalls.add(rc);
        else
            incompleteRootCalls.add(rc);
    }


    public void submitToElastic(ElasticSearch elastic){
        if (!rootCalls.isEmpty()){
            elastic.submit();
        }

    }


    public void completeMissingSymbols(){
        if (!missingMethods.isEmpty()){

        }
        if (!missingsClasses.isEmpty()){

        }
    }



    public long getId(){
        return agent.getId().getId();
    }

    public Agent getAgent() {
        return agent;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }


    public ConcurrentHashMap<Long, Symbol> getSymbolsClass() {
        return symbolsClass;
    }

    public ConcurrentHashMap<Long, Symbol> getSymbolsMethod() {
        return symbolsMethod;
    }
}
