import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    private String host;

    /**
     * Only {@link MyInterceptor.Builder} can build me
     */
    private MyInterceptor() {
        InetAddress addr;
        try {
            addr = InetAddress.getLocalHost();
            host = addr.getCanonicalHostName();
        } catch (UnknownHostException e) {
            host = "can't get hostname";
        }
    }

    public void initialize() {

    }

    /**
     * main part of interceptor. handle each event.
     *
     * @param event flume event
     * @return Event
     */
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        headers.put("host", host);
        event.setHeaders(headers);
        String body = new String(event.getBody(), Charsets.UTF_8);

        JSONObject jsonin = null;
        try {
            jsonin = JSON.parseObject(body);
        } catch (Exception e) {
            //e.printStackTrace();
        }
        if (jsonin != null) {
            String jsonout = transform(jsonin);
            if (jsonout != null) {
                event.setBody(jsonout.getBytes());
                return event;
            } else return null;
        } else {
            return null;
        }
    }

    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new MyInterceptor();
        }

        public void configure(Context context) {
        }
    }


    private String transform(JSONObject json) {

        if (json.containsKey("a")) {
            String ak = json.getString("a");
            if (ak.length() != 10) {
                System.out.println("illegal a");
                return null;
            }
        } else {
            System.out.println("does not contain a");
            return null;
        }

        //time correction
        long current = System.currentTimeMillis();
        long zero = current / (1000 * 3600 * 24) * 24 * 3600 * 1000 - 8 * 3600 * 1000;
        long zero2 = zero + 24 * 3600 * 1000;


        String st;
        if (json.containsKey("st")) {
            st = json.getString("st");
        } else{
            System.out.println("does not contain time");
            return null;
        }
        Long longst = Long.parseLong(st);
        if (longst > zero2 || longst < zero) {
            System.out.println("time incorrect");
            return null;
        }

        //handling et
         if(!json.containsKey("et")) {
            json.put("et", json.getString("st"));
        }
        json.put("ETL", "ETL");
        return json.toString();
    }

}
