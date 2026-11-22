package com.evalarm.charger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.evalarm.charger.common.config.AppProperties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * 1일차 검증: 애플리케이션이 모든 인프라에 연결되고,
 * Flyway 스키마와 JPA 엔티티가 일치하며(ddl-auto=validate), 토픽과 큐가 생성되는지 확인한다.
 */
@SpringBootTest(properties = {
    "app.collector.enabled=false", "app.public-api.zcode=11", "app.dev-tools.enabled=false", "app.detector.enabled=false",
})
@Import(TestcontainersConfig.class)
class InfrastructureIntegrationTest {

    @Autowired
    AppProperties properties;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    AmqpAdmin amqpAdmin;

    @Test
    @DisplayName("Kafka 토픽이 생성된다")
    void kafkaTopicsAreCreated() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertTrue(topics.contains(properties.kafka().topics().chargerStatus().name()));
            assertTrue(topics.contains(properties.kafka().topics().chargerAvailable().name()));
        }
    }

    @Test
    @DisplayName("RabbitMQ 알림 큐와 DLQ가 생성된다")
    void rabbitQueueAndDlqAreCreated() {
        assertNotNull(amqpAdmin.getQueueInfo(properties.rabbit().queue()));
        assertNotNull(amqpAdmin.getQueueInfo(properties.rabbit().deadLetterQueue()));
    }
}
