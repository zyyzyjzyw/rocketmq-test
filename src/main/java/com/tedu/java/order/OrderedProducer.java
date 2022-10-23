package com.tedu.java.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * @author： zyy
 * @date： 2022/10/23 11:40
 * @description： TODO
 * @version: 1.0
 * @描述：顺序消息
 **/
public class OrderedProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        producer.start();
        for (int i=0;i<100;i++){
            //为了演示简单，使用整数作为orderId
            Integer orderId = 1;
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("TopicA", "TagA", body);
            //将orderId作为消息key
            message.setKeys(orderId.toString());
            //send()的第三个参数值会传递给选择器给select()的第三个参数
            //该send方法为同步发送
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                //具体的选择算法在该方法中定义
               @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                   //以下是使用消息key作为选择的选择算法
                   String keys = message.getKeys();
                   Integer id = Integer.valueOf(keys);
                   //以下是选择arg作为key的选择算法
                   //Integer id = (Integer) arg;
                   int index = id%mqs.size();
                   return mqs.get(index);
                }
            },orderId);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}
