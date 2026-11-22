package com.evalarm.charger.notification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.NotificationCommand;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/** 발송이 계속 실패하는 경우와 보내지 말아야 하는 경우. 발송 채널은 항상 실패하도록 바꿔 둔다 */
@SpringBootTest(properties = {
    "app.collector.enabled=false", "app.public-api.zcode=11", "app.dev-tools.enabled=false",
    "app.detector.enabled=false", "app.notification.enabled=true",
})
@Import(TestcontainersConfig.class)
class NotificationFailureTest {

    @MockitoSpyBean
    NotificationPushChannel pushChannel;

    @Autowired
    KafkaTemplate<String, ChargerAvailableEvent> kafkaTemplate;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    AppProperties properties;

    @Test
    @DisplayName("발송이 계속 실패하면 3번 시도 후 FAILED로 기록하고 DLQ로 보낸다")
    void marksFailedAndSendsToDlqAfterThreeAttempts() throws Exception {
        doThrow(new RedisConnectionFailureException("Redis 장애(테스트)")).when(pushChannel).publish(any());

        String stationId = "F" + UUID.randomUUID().toString().substring(0, 8);
        jdbcTemplate.update(
                """
                INSERT INTO station (station_id, name, address, latitude, longitude, zcode, zscode, active, created_at, updated_at)
                VALUES (?, '실패 테스트 충전소', '서울특별시', 37.5, 127.0, '11', '11680', TRUE, now(), now())""",
                stationId);
        jdbcTemplate.update("INSERT INTO subscriber (client_key, created_at) VALUES (?, now())", UUID.randomUUID().toString());
        jdbcTemplate.update(
                """
                INSERT INTO subscription (subscriber_id, station_id, active, cooldown_minutes, created_at)
                VALUES ((SELECT max(id) FROM subscriber), ?, TRUE, 10, now())""",
                stationId);

        Instant occurredAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        ChargerAvailableEvent event = new ChargerAvailableEvent(
                ChargerAvailableEvent.eventIdOf(stationId, "01", occurredAt),
                stationId,
                "01",
                ChargerStatus.CHARGING,
                occurredAt);
        kafkaTemplate.send(properties.kafka().topics().chargerAvailable().name(), stationId, event).get();

        long deadline = System.currentTimeMillis() + 60_000;
        String status = null;
        while (System.currentTimeMillis() < deadline) {
            List<String> statuses = jdbcTemplate.queryForList(
                    "SELECT status FROM notification_log WHERE event_id = ?", String.class, event.eventId());
            status = statuses.isEmpty() ? null : statuses.getFirst();
            if ("FAILED".equals(status)) {
                break;
            }
            Thread.sleep(200);
        }
        assertEquals("FAILED", status);
        assertEquals(
                3,
                jdbcTemplate.queryForObject(
                        "SELECT attempt_count FROM notification_log WHERE event_id = ?", int.class, event.eventId()));

        NotificationCommand dead =
                (NotificationCommand) rabbitTemplate.receiveAndConvert(properties.rabbit().deadLetterQueue(), 10_000);
        assertNotNull(dead);
        assertEquals(event.eventId(), dead.eventId());
    }

    @Test
    @DisplayName("매칭 뒤 발송 전에 구독을 해지했으면 보내지 않고 CANCELLED로 남긴다")
    void cancelsWhenUnsubscribedBeforeSending() throws Exception {
        // 보내려 하면 실패(FAILED)하므로, CANCELLED가 되면 발송을 시도하지 않은 것이다
        doThrow(new RedisConnectionFailureException("Redis 장애(테스트)")).when(pushChannel).publish(any());

        String stationId = "C" + UUID.randomUUID().toString().substring(0, 8);
        String clientKey = UUID.randomUUID().toString();
        jdbcTemplate.update(
                """
                INSERT INTO station (station_id, name, address, latitude, longitude, zcode, zscode, active, created_at, updated_at)
                VALUES (?, '해지 테스트 충전소', '서울특별시', 37.5, 127.0, '11', '11680', TRUE, now(), now())""",
                stationId);
        jdbcTemplate.update("INSERT INTO subscriber (client_key, created_at) VALUES (?, now())", clientKey);
        long subscriptionId = Objects.requireNonNull(jdbcTemplate.queryForObject(
                """
                INSERT INTO subscription (subscriber_id, station_id, active, cooldown_minutes, created_at)
                VALUES ((SELECT id FROM subscriber WHERE client_key = ?), ?, FALSE, 10, now()) RETURNING id""",
                long.class, clientKey, stationId));
        Instant occurredAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        String eventId = ChargerAvailableEvent.eventIdOf(stationId, "01", occurredAt);
        long notificationId = Objects.requireNonNull(jdbcTemplate.queryForObject(
                """
                INSERT INTO notification_log (subscription_id, event_id, station_id, charger_id, status, attempt_count, occurred_at, created_at)
                VALUES (?, ?, ?, '01', 'QUEUED', 0, ?, now()) RETURNING id""",
                long.class, subscriptionId, eventId, stationId, Timestamp.from(occurredAt)));

        rabbitTemplate.convertAndSend(
                properties.rabbit().exchange(),
                properties.rabbit().routingKey(),
                new NotificationCommand(
                        notificationId, eventId, subscriptionId, clientKey, stationId, "해지 테스트 충전소", "01", occurredAt));

        long deadline = System.currentTimeMillis() + 30_000;
        String status = null;
        while (System.currentTimeMillis() < deadline) {
            status = jdbcTemplate.queryForObject(
                    "SELECT status FROM notification_log WHERE id = ?", String.class, notificationId);
            if (!"QUEUED".equals(status)) {
                break;
            }
            Thread.sleep(100);
        }
        assertEquals("CANCELLED", status);
        assertEquals(
                0,
                jdbcTemplate.queryForObject(
                        "SELECT attempt_count FROM notification_log WHERE id = ?", int.class, notificationId));
    }
}
