package cn.haoxin.flume.source;


import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/3/5 16:39
 */
public class JsonInterceptor implements Interceptor {

    private String[] schema; //数据模型
    private String symbol;  //数据分割符

    public JsonInterceptor(String schema,String symbol) {
        this.schema = schema.split("[,]");
        this.symbol = symbol;
    }


    @Override
    public void initialize() {

    }

    /**
     * 接收event并处理返回
     */
    @Override
    public Event intercept(Event event) {
        LinkedHashMap<String, String> linkedHashMap = new LinkedHashMap<>();
        //获取event中body内容，加上schema
        String line = new String(event.getBody());
        String[] fields = line.split(symbol);
        for (int i = 0; i < schema.length; i++) {
            String key = schema[i];
            String value = fields[i];
            linkedHashMap.put(key, value);
        }
        //转换成json
        String json = JSONObject.toJSONString(linkedHashMap);
        //再次把body加入 event
        event.setBody(json.getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event e:events) {
            intercept(e);
        }
        return events;
    }

    @Override
    public void close() {

    }

    /**
     * Interceptor.Builder的生命周期方法
     * 构造器 -> configure -> build
     */
    public static class Builder implements Interceptor.Builder{
        private String fields;
        private String symbol;

        @Override
        public Interceptor build() {
            return new JsonInterceptor(fields,symbol);
        }

        @Override
        public void configure(Context context) {
            fields = context.getString("fields");
            symbol = context.getString("symbol");

        }
    }
}
