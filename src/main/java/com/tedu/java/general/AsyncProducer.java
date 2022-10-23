package com.tedu.java.general;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author： zyy
 * @date： 2022/10/22 12:21
 * @description： TODO
 * @version: 1.0
 * @描述：定义异步消息发送生产者
 **/
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        //指定异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendAsyncFailed(0);
        //指定新创建的Topic的Queue的数量为2，默认为4
        producer.setDefaultTopicQueueNums(2);
        producer.start();
        for (int i=0;i<100;i++){
            byte [] body = ("Hi,"+i).getBytes();
            try{
                Message msg = new Message("myTopicA","myTag",body);
                //异步发送，指定回调
                producer.send(msg, new SendCallback() {
                    //当produce接收到MQ发送来的ACK就会触发该回调
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                    }
                });
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        //sleep一会 由于采用的是异步发送，所以若这里不使用sleep，则消息还没有发送就会将producer给关闭。就会报错
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
    }
}
