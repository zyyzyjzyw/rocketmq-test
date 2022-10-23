package com.tedu.java.filter;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author： zyy
 * @date： 2022/10/23 16:54
 * @description： TODO
 * @version: 1.0
 * @描述：
 **/
public class FilterBySQLProducer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        producer.start();
        for(int i=0;i<10;i++){
            try {
                byte[] body = ("Hi" + i).getBytes();
                Message message = new Message("MyTopic", "myTag", body);
                //事先埋入通户属性age
                message.putUserProperty("age",i+"");
                SendResult send = producer.send(message);
                System.out.println(send);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
