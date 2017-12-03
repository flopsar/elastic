package com.flopsar.addons.elastic;

import com.flopsar.fdbc.api.fdb.Invocation;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonConverter {

    private static final String AGENT = "agent";
    private static final String TSTAMP = "tstamp";
    private static final String CLASS = "class";
    private static final String PARAMS = "parameters";

    private static final String KV_NAME = "name";
    private static final String KV_VALUE = "value";

    private static final String CALL_METHOD = "methodname";
    private static final String CALL_SIGNATURE = "signature";
    private static final String CALL_EXCEPTION = "exception";
    private static final String CALL_DURATION = "duration";


    private static final String WEB_LOCATION = "location";
    private static final String WEB_LATITUDE = "latitude";
    private static final String WEB_LONGITUDE = "longitude";
    private static final String WEB_CLASS = "Web";
    private static final String WEB_PARAM_URL = "url";
    private static final String WEB_PARAM_UA = "useragent";
    private static final String WEB_PARAM_DEVICE = "device";
    private static final String WEB_PARAM_VENDOR = "vendor";
    private static final String WEB_PARAM_CUSTOMER = "customerid";
    private static final String WEB_PARAM_REFERER = "referer";
    private static final String WEB_PARAM_DESTINATION = "destination";
    private static final String WEB_PARAM_SCR_RESOL = "resolution";
    private static final String WEB_PARAM_OTHER = "other";

    private static final String WEB_PARAM_RH = "remotehost";
    private static final String WEB_RUM = "rum";
    private static final String WEB_RUM_SCT = "srv_connect_time";
    private static final String WEB_RUM_RAT = "response_avail_time";
    private static final String WEB_RUM_DDT = "doc_download_time";
    private static final String WEB_RUM_DPT = "doc_proc_time";
    private static final String WEB_RUM_DRT = "doc_render_time";

    private static final String WEB_PKEY_GEOIP = "GEOIP";
    private static final String WEB_PKEY_SCT = "ServerConnectionTime";
    private static final String WEB_PKEY_RAT = "ResponseAvailTime";
    private static final String WEB_PKEY_DDT = "DocumentDownloadTime";
    private static final String WEB_PKEY_DPT = "DocumentProcessingTime";
    private static final String WEB_PKEY_DRT = "DocumentRenderingTime";

    private static final Pattern pattern = Pattern.compile("gip=(.*)");
    private static final JSONParser parser = new JSONParser();

    private static final Set<String> excludedURLs = new HashSet<>();

    static {
        excludedURLs.add("http://fs16demo.flopsar.com:8780/konakart/script/");
        excludedURLs.add("http://fs16demo.flopsar.com:8780/konakart/images/");
        excludedURLs.add("http://fs16demo.flopsar.com:8780/konakart/styles/");
    }


    private static JSONObject prepare(String agent, long tstamp, String clazz) {
        JSONObject obj = new JSONObject();
        obj.put(AGENT, agent);
        obj.put(TSTAMP, tstamp);
        obj.put(CLASS, clazz);
        return obj;
    }


    public static String convertKV(String agent, String clazz, String name, long value, long tstamp) {
        JSONObject obj = prepare(agent, tstamp, clazz);
        obj.put(KV_NAME, name);
        obj.put(KV_VALUE, value);
        return obj.toJSONString();
    }


    public static String convertCALL(String agent, String clazz, String method, String signature,
                                     long duration, long tstamp, Map<String, String> parameters, boolean exception) {
        JSONObject obj = prepare(agent, tstamp, clazz);
        obj.put(CALL_METHOD, method);
        obj.put(CALL_SIGNATURE, signature);
        obj.put(CALL_DURATION, duration);

        if (parameters != null && !parameters.isEmpty()) {
            String exc = parameters.remove(Invocation.EXCEPTION_KEY);
            if (exc != null)
                obj.put(CALL_EXCEPTION, exc);

            String url = parameters.remove("URL");
            StringBuffer sb = new StringBuffer();
            for (String k : parameters.keySet()) {
                sb.append(k);
                sb.append("\\|");
                sb.append(parameters.get(k));
            }
            if (url != null) {
                for (String e : excludedURLs)
                    if (url.startsWith(e))
                        return null;
            }

            JSONObject p = new JSONObject();
            p.put(WEB_PARAM_URL, url);
            p.put(WEB_PARAM_OTHER, sb.toString());
            obj.put(PARAMS, p);
        }

        return obj.toJSONString();
    }


    public static String convertWEB(String agent, String clazz, long duration, long tstamp, Map<String, String> parameters) {
        JSONObject obj = prepare(agent, tstamp, clazz);
        obj.put(CALL_DURATION, duration);

        if (parameters != null && !parameters.isEmpty()) {
            String exc = parameters.remove(Invocation.EXCEPTION_KEY);
            String location = parameters.remove(WEB_PKEY_GEOIP);
            if (exc != null)
                obj.put(CALL_EXCEPTION, exc);

            if (location != null) {
                try {
                    parser.reset();
                    Matcher m = pattern.matcher(location);
                    if (m.find()) {
                        location = m.group(1);
                    }
                    System.out.println("TO PARSE " + location);
                    JSONObject web = (JSONObject) parser.parse(location);
                    obj.put(WEB_LOCATION, String.format("%s, %s", web.get(WEB_LATITUDE), web.get(WEB_LONGITUDE)));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            String url = parameters.remove("u");
            String remoteHost = parameters.remove("RemoteHost");
            String userAgent = parameters.remove("UserAgent");
            String device = parameters.remove("ua.plt");
            String vendor = parameters.remove("ua.vnd");
            String referer = parameters.remove("u");
            String destination = parameters.remove("r");
            String deviceResol = parameters.remove("scr.xy");
            String customerid = parameters.remove("CUSTOMER_UUID");

            String sct = parameters.remove(WEB_PKEY_SCT);
            String ddt = parameters.remove(WEB_PKEY_DDT);
            String dpt = parameters.remove(WEB_PKEY_DPT);
            String drt = parameters.remove(WEB_PKEY_DRT);
            String rat = parameters.remove(WEB_PKEY_RAT);

            StringBuffer sb = new StringBuffer();
            for (String k : parameters.keySet()) {
                sb.append(k);
                sb.append("\\|");
                sb.append(parameters.get(k));
            }

            JSONObject p = new JSONObject();
            p.put(WEB_PARAM_URL, url);
            p.put(WEB_PARAM_RH, remoteHost);
            p.put(WEB_PARAM_UA, userAgent);
            p.put(WEB_PARAM_OTHER, sb.toString());
            p.put(WEB_PARAM_DEVICE, device);
            p.put(WEB_PARAM_VENDOR, vendor);
            p.put(WEB_PARAM_REFERER, referer);
            p.put(WEB_PARAM_DESTINATION, destination);
            p.put(WEB_PARAM_CUSTOMER, customerid);
            p.put(WEB_PARAM_SCR_RESOL, deviceResol);
            obj.put(PARAMS, p);

            JSONObject rum = new JSONObject();
            if (sct != null) {
                try {
                    rum.put(WEB_RUM_SCT, Long.parseLong(sct));
                } catch (NumberFormatException e) {
                }
            }
            if (rat != null) {
                try {
                    rum.put(WEB_RUM_RAT, Long.parseLong(rat));
                } catch (NumberFormatException e) {
                }
            }
            if (ddt != null) {
                try {
                    rum.put(WEB_RUM_DDT, Long.parseLong(ddt));
                } catch (NumberFormatException e) {
                }
            }
            if (dpt != null) {
                try {
                    rum.put(WEB_RUM_DPT, Long.parseLong(dpt));
                } catch (NumberFormatException e) {
                }
            }
            if (drt != null) {
                try {
                    rum.put(WEB_RUM_DRT, Long.parseLong(drt));
                } catch (NumberFormatException e) {
                }
            }
            obj.put(WEB_RUM, rum);
        }


        return obj.toJSONString();
    }


}