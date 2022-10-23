package com.tedu.java.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author： zyy
 * @date： 2022/10/23 10:47
 * @description： TODO
 * @version: 1.0
 * @描述：消费者
 **/
public class SomeConsumer {
    public static void main(String[] args) throws MQClientException {
        //定义一个pull消费者
        //DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("cg");
        //定义一个push消费者
        DefaultMQPushConsumer consumer =new DefaultMQPushConsumer("cg");
        //指定nameServer
        consumer.setNamesrvAddr("rocketmqOS1:9876");
        //指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //指定消费topic与tag
        consumer.subscribe("TTopic","*");
        //采用广播模式进行消费，默认为集群模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            //一旦broker中有其订阅的消息就会触发该方法的执行
            //其返回值为当前consumer消费的状态
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                //逐条消费信息
                for(MessageExt msg : msgs){
                    System.out.println(msg);
                }
                //返回消费状态:消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }

        });
        //开启消费者消费
        consumer.start();
        System.out.println("Consumer started");
    }
}
