package com.tedu.java.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.*;

/**
 * @author： zyy
 * @date： 2022/10/23 15:21
 * @description： TODO
 * @version: 1.0
 * @描述：事务消息生产者
 **/
public class TransActionProducer {
    public static void main(String[] args) throws MQClientException {
        TransactionMQProducer producer = new TransactionMQProducer("tpg");
        producer.setNamesrvAddr("rocketmqOS1:9876");
        /**
         * Creates a new {@code ThreadPoolExecutor} with the given initial
         * parameters and default rejected execution handler.
         *
         * @param corePoolSize 核心线程数量
         * @param maximumPoolSize 线程池中最多的线程数量
         * @param keepAliveTime 这是一个时间。当线程池中线程数量大于核心线程数量时，多余的线程存活的事件。
         * @param unit 时间单位
         * @param workQueue 临时存放任务的队列，七参数就是队列的长度。
         * @param threadFactory 线程工厂
         * @throws IllegalArgumentException if one of the following holds:<br>
         *         {@code corePoolSize < 0}<br>
         *         {@code keepAliveTime < 0}<br>
         *         {@code maximumPoolSize <= 0}<br>
         *         {@code maximumPoolSize < corePoolSize}
         * @throws NullPointerException if {@code workQueue}
         *         or {@code threadFactory} is null
         */
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-chenk-thread");
                return thread;
            }
        });
        //为生产者指定一个线程池
        producer.setExecutorService(executorService);
        //为生产者添加事务监听器
        producer.setTransactionListener(new ICBCTransactionListener());
        producer.start();
        String[] tags={"TAGA","TAGB","TAGC"};
        for(int i=0;i<3;i++){
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("TTopic", tags[i], body);
            //发送事务消息
            //第二个参数用于指定在执行本地事务时要使用的业务参数
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.out.println("发送结果为："+sendResult.getSendStatus());
        }
    }
}
