package com.yuan.rocketmq.demo.config.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;

/**
 * @author David Hong
 * @version 1.0
 * @description
 */
@Slf4j
public abstract class AbstractConsumer {

    /**
     * NameServer 地址
     */
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    private static int count = 0;

    private synchronized static int upCount() {
        return ++count;
    }

    /**
     * 开启消费者监听服务
     *
     * @param topic
     * @param tag
     * @return void
     * @author David Hong
     */
    public void listener(String topic, String tag) throws MQClientException {
        // TODO consumeGroupName不能相同
        String consumeGroupName = "consumeGroupName-" + upCount();
        log.info("TAG=AbstractConsumer, consumeGroupName={}", consumeGroupName);
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(consumeGroupName);
        defaultMQPushConsumer.setNamesrvAddr(namesrvAddr);
        defaultMQPushConsumer.subscribe(topic, tag);
        // 开启内部类实现监听
        defaultMQPushConsumer.registerMessageListener((List<MessageExt> msgs, ConsumeConcurrentlyContext context) -> {
            return AbstractConsumer.this.dealBody(msgs);
        });
        defaultMQPushConsumer.start();
    }

    /**
     * 处理消息
     *
     * @param msgs
     * @return org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus
     * @author David Hong
     */
    public abstract ConsumeConcurrentlyStatus dealBody(List<MessageExt> msgs);

}
