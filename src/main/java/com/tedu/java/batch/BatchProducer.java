package com.tedu.java.batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author： zyy
 * @date： 2022/10/23 16:04
 * @description： TODO
 * @version: 1.0
 * @描述：批量生产
 **/
public class BatchProducer {
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        //指定发送的消息最大大小，默认4M，如果要修改该属性，还需要修改broker加载的配置文件 maxMessageSize(4*1024*1024)
        producer.setMaxMessageSize(4*1024*1024);
        producer.start();
        //定义的发送集合
        List<Message> messages = new ArrayList<>();
        for (int i=0;i<100;i++){
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("BatchTopic", "batchTag", body);
            messages.add(message);
        }
        //定义消息列表分割器，将消息列表分割为多个不超过4M大小的小列表
        MessageListSplitter splitter = new MessageListSplitter(messages);
        while (splitter.hasNext()){
            try{
              List<Message> listItem = splitter.next();
              producer.send(listItem);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
