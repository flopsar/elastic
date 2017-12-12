package com.flopsar.addons.elastic;

import org.json.simple.JSONObject;

import java.util.Map;

public class JsonConverter {

    private static final String AGENT = "agent";
    private static final String TSTAMP = "tstamp";
    private static final String CLASS = "class";
    private static final String PARAMS = "parameters";
    private static final String PARAMS_OTHER = "other";

    private static final String CALL_METHOD = "methodname";
    private static final String CALL_SIGNATURE = "signature";
    private static final String CALL_EXCEPTION = "exception";
    private static final String CALL_DURATION = "duration";



    private static JSONObject prepare(String agent, long tstamp, String clazz) {
        JSONObject obj = new JSONObject();
        obj.put(AGENT, agent);
        obj.put(TSTAMP, tstamp);
        obj.put(CLASS, clazz);
        return obj;
    }



    public static String convertCALL(String agent, String clazz, String method, String signature,
                                     long duration, long tstamp, Map<String, String> parameters, boolean exception) {
        JSONObject obj = prepare(agent, tstamp, clazz);
        obj.put(CALL_METHOD, method);
        obj.put(CALL_SIGNATURE, signature);
        obj.put(CALL_DURATION, duration);

        if (parameters != null && !parameters.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String k : parameters.keySet()) {
                sb.append(k);
                sb.append("|");
                sb.append(parameters.get(k));
            }
            JSONObject p = new JSONObject();
            p.put(PARAMS_OTHER,sb.toString());
            obj.put(PARAMS, p);
        }

        return obj.toJSONString();
    }




}