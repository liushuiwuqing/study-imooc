package com.myimooc.rabbitmq.ha.producer;

import com.myimooc.rabbitmq.entity.Order;
import com.myimooc.rabbitmq.ha.constant.Constants;
import com.myimooc.rabbitmq.ha.dao.mapper.BrokerMessageLogMapper;
import com.myimooc.rabbitmq.ha.dao.po.BrokerMessageLogPO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

/**
 * <br>
 * 标题: 订单消息发送者<br>
 * 描述: 订单消息发送者<br>
 * 时间: 2018/09/06<br>
 *
 * @author zc
 */
@Component
public class OrderSender {

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private BrokerMessageLogMapper brokerMessageLogMapper;
    private Logger logger = LoggerFactory.getLogger(getClass());
    /**
     * 回调方法：confirm确认
     * 只能表示 message 已经到达服务器，并不能保证消息一定会被投递到目标 queue 里。所以还需要用到 returnCallback 。
     */
    private final RabbitTemplate.ConfirmCallback confirmCallback = new RabbitTemplate.ConfirmCallback() {
        @Override
        public void confirm(CorrelationData correlationData, boolean ack, String cause) {
            System.out.println("correlationData：" + correlationData + ",ack:" + ack);
            String messageId = correlationData.getId();
            if (ack) {
                // 如果confirm返回成功，则进行更新
                BrokerMessageLogPO messageLogPO = new BrokerMessageLogPO();
                messageLogPO.setMessageId(messageId);
                messageLogPO.setStatus(Constants.OrderSendStatus.SEND_SUCCESS);
                brokerMessageLogMapper.changeBrokerMessageLogStatus(messageLogPO);
            } else {
                // 失败则进行具体的后续操作：重试或者补偿等
                System.out.println("异常处理...");
            }
        }
    };

    /**
     * 这样如果未能投递到目标 queue 里将调用 returnCallback ，可以记录下详细到投递数据，定期的巡检或者自动纠错都需要这些数据。
     */
    private final RabbitTemplate.ReturnCallback returnCallback = new RabbitTemplate.ReturnCallback() {
        @Override
        public void returnedMessage(Message message, int replyCode, String replyText,
                                    String exchange, String routingKey) {
            logger.info(MessageFormat.format("消息发送ReturnCallback:{0},{1},{2},{3},{4},{5}", message, replyCode, replyText, exchange, routingKey));
        }
    };

    /**
     * 发送订单
     *
     * @param order 订单
     */
    public void send(Order order) {
        // 设置回调方法
        this.rabbitTemplate.setConfirmCallback(confirmCallback);
        //以下代码不生效???
        // this.rabbitTemplate.setReturnCallback(returnCallback);
        // 消息ID,我的实践证明,是可以发送相同的correlationDataId数据到消息队列中
        CorrelationData correlationData = new CorrelationData(order.getMessageId());
        // 发送消息
        this.rabbitTemplate.convertAndSend("order-exchange", "order.a", order, correlationData);
    }
}
