package com.evalarm.charger.common.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 토픽 정의. 브로커의 자동 생성은 꺼 두고 애플리케이션이 토픽을 명시적으로 만든다.
 *
 * <p>두 토픽 모두 메시지 키는 충전소 ID(stationId)다.
 * 같은 충전소의 이벤트는 같은 파티션으로 가서 순서가 보장된다.
 */
@Configuration
public class KafkaTopicConfig {

    private final AppProperties properties;

    public KafkaTopicConfig(AppProperties properties) {
        this.properties = properties;
    }

    /** 수집한 모든 충전기 상태 스냅샷 */
    @Bean
    public NewTopic chargerStatusTopic() {
        return toNewTopic(properties.kafka().topics().chargerStatus());
    }

    /** 상태가 '충전대기'로 바뀐 순간만 발행되는 도메인 이벤트 */
    @Bean
    public NewTopic chargerAvailableTopic() {
        return toNewTopic(properties.kafka().topics().chargerAvailable());
    }

    private static NewTopic toNewTopic(AppProperties.Kafka.Topic topic) {
        return TopicBuilder.name(topic.name())
                .partitions(topic.partitions())
                .replicas(topic.replicas())
                .build();
    }
}
