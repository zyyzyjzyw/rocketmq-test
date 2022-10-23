package com.tedu.java.filter;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author： zyy
 * @date： 2022/10/23 16:46
 * @description： TODO
 * @version: 1.0
 * @描述：生产者使用tag过滤
 **/
public class FilterByTagProducer {
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        producer.start();
        String [] tags ={"myTagA","myTagB","myTagC"};
        for(int i=0;i<10;i++){
            byte[] body = ("Hi" + i).getBytes();
            String tag = tags[i % tags.length];
            Message msg = new Message("myTopic", tag, body);
            SendResult send = producer.send(msg);
            System.out.println(send);
        }
        producer.shutdown();
    }
}
