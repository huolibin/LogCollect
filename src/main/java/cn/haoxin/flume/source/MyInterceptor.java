package cn.haoxin.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * 拦截器的设计，把小写改成大写
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/3/6 10:53
 */
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 拦截source发送到通道channel中的消息
     *
     * @param event 接收过滤的event
     * @return event  根据业务处理后的event
     */
    @Override
    public Event intercept(Event event) {
        //获取事件中的字节数据
        byte[] arr = event.getBody();
        // 将数据转换成大写
        byte[] arr1 = new String(arr).toUpperCase().getBytes();
        //再放入event
        event.setBody(arr1);
        return event;
    }

    /**
     * 接收被过滤事件集合
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event:events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder {

        //获取配置文件的属性
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
