package com.evalarm.charger.common.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 알림 발송 작업 큐.
 *
 * <p>Kafka는 "자리가 났다"는 사실(이벤트)을, RabbitMQ는 "사용자 X에게 보내라"는 할 일(작업)을 다룬다.
 * 발송에 최종 실패한 메시지는 DLX를 거쳐 DLQ로 이동한다.
 */
@Configuration
public class RabbitConfig {

    private static final String MESSAGE_PACKAGE = "com.evalarm.charger.common.messaging";

    private final AppProperties properties;

    public RabbitConfig(AppProperties properties) {
        this.properties = properties;
    }

    @Bean
    public DirectExchange notificationExchange() {
        return new DirectExchange(properties.rabbit().exchange());
    }

    @Bean
    public Queue notificationQueue() {
        return QueueBuilder.durable(properties.rabbit().queue())
                .deadLetterExchange(properties.rabbit().deadLetterExchange())
                .deadLetterRoutingKey(properties.rabbit().routingKey())
                .build();
    }

    @Bean
    public Binding notificationBinding() {
        return BindingBuilder.bind(notificationQueue()).to(notificationExchange()).with(properties.rabbit().routingKey());
    }

    @Bean
    public DirectExchange notificationDeadLetterExchange() {
        return new DirectExchange(properties.rabbit().deadLetterExchange());
    }

    @Bean
    public Queue notificationDeadLetterQueue() {
        return QueueBuilder.durable(properties.rabbit().deadLetterQueue()).build();
    }

    @Bean
    public Binding notificationDeadLetterBinding() {
        return BindingBuilder.bind(notificationDeadLetterQueue())
                .to(notificationDeadLetterExchange())
                .with(properties.rabbit().routingKey());
    }

    /** 메시지의 타입 정보(__TypeId__)는 이 프로젝트의 메시지 패키지만 믿는다 */
    @Bean
    public MessageConverter rabbitMessageConverter() {
        return new JacksonJsonMessageConverter(MESSAGE_PACKAGE);
    }
}
