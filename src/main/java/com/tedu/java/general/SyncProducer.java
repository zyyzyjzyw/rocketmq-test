package com.tedu.java.general;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author： zyy
 * @date： 2022/10/22 11:52
 * @description： TODO
 * @version: 1.0
 * @描述：定义同步消息发送生产者
 **/
public class SyncProducer {
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //创建一个producer,参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定nameServer地址
        producer.setNamesrvAddr("rocketmqOS1:9876");
        //设置当发送失败时，重试的次数，默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置发送超时时限为5s,默认为3s
        producer.setSendMsgTimeout(5000);
        //开启生产者
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        //生产并发送100条消息
        for(int i=0;i<100;i++){
            byte[] body =("Hi,"+i).getBytes();
            Message message = new Message("someTopic","someTag",body);
            //为消息指定key
            message.setKeys("key-"+i);
            //发送消息
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        //关闭producer
        producer.shutdown();
    }
}
