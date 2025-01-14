package com.myimooc.kafka.example.consumer;

import com.alibaba.fastjson.JSONObject;
import com.myimooc.kafka.example.common.MessageEntity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * <br>
 * 标题: 消费者<br>
 * 描述: 消费者<br>
 * 时间: 2018/09/09<br>
 *
 * @author zc
 */
@Component
public class SimpleConsumer {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @KafkaListener(topics = "${kafka.topic.default}")
    public void listen(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println();
        System.out.println(Thread.currentThread().getName() + "            111111111111111111111111111111111111111111");
        System.out.println();
//        listen0(record, topic);
    }

    private void listen0(ConsumerRecord<?, ?> record, String topic) {
        //判断是否NULL
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            //获取消息
            Object message = kafkaMessage.get();

            MessageEntity messageEntity = JSONObject.parseObject(message.toString(), MessageEntity.class);

            logger.info("接收消息Topic：{}", topic);
            logger.info("接收消息Record：{}", record);
            logger.info("接收消息Message：{}", messageEntity);
        }
    }

    @KafkaListener(topics = "myimooc-kafka-topic2")
    public void listen2(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println();
        System.out.println(Thread.currentThread().getName() + "            00000000000000000000000000");
        System.out.println();
//        listen0(record, topic);
    }

}
