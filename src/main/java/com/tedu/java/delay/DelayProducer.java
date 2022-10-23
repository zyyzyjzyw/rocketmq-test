package com.tedu.java.delay;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author： zyy
 * @date： 2022/10/23 12:22
 * @description： TODO
 * @version: 1.0
 * @描述：延迟消费失败者
 **/
public class DelayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        producer.start();
        for(int i=0;i<100;i++){
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("TopicB","someTag",body);
            //指定消息延迟登记为3，即10s
            message.setDelayTimeLevel(3);
            SendResult sendResult = producer.send(message);
            //输出消息被发送的事件
            System.out.println(new SimpleDateFormat("mm:ss").format(new Date()));
            System.out.println(" ,"+sendResult);
        }
        producer.shutdown();
    }
}
