package com.flopsar.addons.elastic;

import com.flopsar.fdbc.api.agent.Agent;
import com.flopsar.fdbc.api.fdb.*;
import com.flopsar.fdbc.exception.FDBCException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FlopsarAgent {

    private final Agent agent;
    private long from;
    private long to;
    private final ConcurrentHashMap<Long,Symbol> symbolsClass = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long,Symbol> symbolsMethod = new ConcurrentHashMap<>();
    private final List<Long> missingClasses = new ArrayList<>();
    private final List<Long> missingMethods = new ArrayList<>();
    private final List<RootCall> rootCalls = new ArrayList<>();
    private final List<RootCall> incompleteRootCalls = new ArrayList<>();
    private final AtomicInteger symbolsMonitor = new AtomicInteger(0);

    public FlopsarAgent(Agent a){
        agent = a;
    }




    public void completeSymbols(RootCall rc){
        boolean rootCallCompleted = false;

        if (rc.getTimestamp() > from)
            from = rc.getTimestamp();

        Symbol classSymbol = symbolsClass.get(rc.getClassSymbol().getId());
        if (classSymbol == null) {
            missingClasses.add(rc.getClassSymbol().getId());
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


    private void submitToElastic(ElasticSearch elastic){
        if (!rootCalls.isEmpty()){
            for (RootCall rc : rootCalls){
                System.out.println("ROOT_CALL "+rc.getClassSymbol().getName()+" "+rc.getMethodSymbol().getName());
            }
        }
        rootCalls.clear();
    }


    private void handleSymbolsData(List<Symbol> symbols,int responseStatus,ConcurrentHashMap<Long,Symbol> symbolsMap){
        if (symbols != null && ResponseAsyncCallback.RESPONSE_ERROR != responseStatus){
            for (Symbol s : symbols){
                symbolsMap.putIfAbsent(s.getId(),s);
            }
        }
        if (0 != responseStatus){
            synchronized (symbolsMonitor){
                symbolsMonitor.getAndDecrement();
                symbolsMonitor.notify();
            }
        }
    }



    public void completeMissingSymbols(ConnectionFDB conn,ElasticSearch elastic) throws FDBCException {
        if (!missingClasses.isEmpty()){
            symbolsMonitor.getAndIncrement();
        }
        if (!missingMethods.isEmpty()){
            symbolsMonitor.getAndIncrement();
        }

        if (!missingClasses.isEmpty()){
            conn.findSymbolsById(SymbolType.SYMBOL_CLASS,getId(),missingClasses, (symbols, responseStatus) -> {
                handleSymbolsData(symbols,responseStatus,symbolsClass);
            });
        }
        if (!missingMethods.isEmpty()){
            conn.findSymbolsById(SymbolType.SYMBOL_METHOD,getId(),missingMethods, (symbols, responseStatus) -> {
                handleSymbolsData(symbols,responseStatus,symbolsMethod);
            });
        }
        synchronized (symbolsMonitor){
            while (symbolsMonitor.get() > 0){
                try {
                    symbolsMonitor.wait();
                } catch (InterruptedException e) { }
            }
        }
        for (RootCall rc : incompleteRootCalls){
            completeSymbols(rc);
        }

        submitToElastic(elastic);

        missingMethods.clear();
        missingClasses.clear();
        incompleteRootCalls.clear();
    }



    public long getId(){
        return agent.getId().getId();
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

}
