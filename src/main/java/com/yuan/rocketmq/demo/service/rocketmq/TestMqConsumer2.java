package com.yuan.rocketmq.demo.service.rocketmq;

import com.yuan.rocketmq.demo.config.rocketmq.AbstractConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author David Hong
 * @version 1.0
 * @description
 */
@Slf4j
@Component
public class TestMqConsumer2 extends AbstractConsumer implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        log.info("TAG=TestMqConsumer2, 开启监听");
        try {
            super.listener("TopicTest", "Tag1");
        } catch (MQClientException e) {
            log.error("TAG=TestMqConsumer2, 消费者监听器启动失败, e={}", e);
        }
    }

    @Override
    public ConsumeConcurrentlyStatus dealBody(List<MessageExt> msgs) {
        log.info("TAG=TestMqConsumer2, 进入");
        for(MessageExt msg : msgs) {
            try {
                String msgStr = new String(msg.getBody(), "utf-8");
                log.info("TAG=TestMqConsumer2, msg={}", msgStr);
            } catch (UnsupportedEncodingException e) {
                log.error("TAG=TestMqConsumer2, body转字符串解析失败");
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

}
