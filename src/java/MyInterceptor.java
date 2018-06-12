import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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


    private String transform(JSONObject jsonin) {
        Set<String> fields = new HashSet<>();
        fields.add("ak");
        fields.add("at");
        fields.add("st");

        JSONObject jsonout = new JSONObject();
        Boolean flag = true;
        for (String field : fields) {
            if (!jsonin.containsKey(field)) {
                flag = false;
                break;
            }
        }






        if (flag) {
            Set<String> keySet = jsonin.keySet();
            keySet.remove("uu");
            keySet.remove("st");
            for (String field : keySet) {
                jsonout.put(field, jsonin.getString(field));
            }
            jsonout.put("ETL", "ETL");
            String result = jsonout.toString();
            System.out.println(result);

            return result;
        } else {
            System.out.println("does not contain one of " + fields.toString());
            return null;
        }


    }

}
