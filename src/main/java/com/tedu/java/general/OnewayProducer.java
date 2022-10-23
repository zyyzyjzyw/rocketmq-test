package com.tedu.java.general;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author： zyy
 * @date： 2022/10/23 10:41
 * @description： TODO
 * @version: 1.0
 * @描述：单向消息发送
 **/
public class OnewayProducer {
    public static void main(String[] args) throws RemotingException, MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        producer.start();
        for(int i=0;i<100;i++){
            byte[] body = ("Hi," + i).getBytes();
            Message message = new Message("signle", "someTag", body);
            //单向发送
            producer.sendOneway(message);
        }
        producer.shutdown();
        System.out.println("produce shutdown");
    }
}
