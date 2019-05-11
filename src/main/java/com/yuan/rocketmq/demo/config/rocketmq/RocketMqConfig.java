package com.yuan.rocketmq.demo.config.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author David Hong
 * @version 1.0
 * @description
 */
@Slf4j
@Configuration
public class RocketMqConfig {

    /**
     * NameServer 地址
     */
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    /**
     * 生产者的组名
     */
    @Value("${apache.rocketmq.producer.producerGroup}")
    private String producerGroup;

    /**
     * 注入DefaultMQProducer
     *
     * @return org.apache.rocketmq.client.producer.DefaultMQProducer
     * @author David Hong
     */
    @Bean
    public DefaultMQProducer defaultMQProducer() {
        log.info("TAG=defaultMQProducer, 初始化defaultMQProducer");
        //生产者的组名
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(namesrvAddr);
        producer.setSendMsgTimeout(6000);

        /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        try {
            producer.start();
        } catch (MQClientException e) {
            log.info("TAG=logException, e={}", e.getErrorMessage());
        }
        log.info("TAG=defaultMQProducer, defaultMQProducer成功初始化");
        return producer;
    }

}
