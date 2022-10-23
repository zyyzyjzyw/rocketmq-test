package com.tedu.java.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author： zyy
 * @date： 2022/10/23 15:39
 * @description： TODO
 * @version: 1.0
 * @描述：
 **/
public class ICBCTransactionListener implements TransactionListener {
    /**
     * 回调操作，消息预提交成功就会触发该方法的执行，用于完成本地事务。
     * @param msg
     * @param arg
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("预提交消息成功:"+msg);
        // TAGA表示成功；TAGB表示失败；TAGC表示未知
        if(StringUtils.equals("TAGA",msg.getTags())){
            return LocalTransactionState.COMMIT_MESSAGE;
        }else  if(StringUtils.equals("TAGB",msg.getTags())){
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }else  if(StringUtils.equals("TAGC",msg.getTags())){
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.UNKNOW;
    }

    /**
     * 消息回查方法
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println("执行消息回查"+msg.getTags());
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
