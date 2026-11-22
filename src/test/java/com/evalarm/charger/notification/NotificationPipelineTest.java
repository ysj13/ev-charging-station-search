package com.evalarm.charger.notification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.common.text.Whitespace;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * 빈자리 이벤트(Kafka) → 구독 매칭 → 발송 큐(RabbitMQ) → 발송 → Redis Pub/Sub → 구독자의 실시간 연결(SSE)까지
 * 실제 인프라로 이어서 검증한다.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "app.collector.enabled=false", "app.public-api.zcode=11", "app.dev-tools.enabled=false",
            "app.detector.enabled=false", "app.notification.enabled=true",
            "app.notification.include-manual-events=false",
        })
@Import(TestcontainersConfig.class)
class NotificationPipelineTest {

    @LocalServerPort
    int port;

    @Autowired
    KafkaTemplate<String, ChargerAvailableEvent> kafkaTemplate;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    AppProperties properties;

    /** 묶음 적재 횟수를 확인하려고 실제 저장소를 감싼다 (동작은 그대로) */
    @MockitoSpyBean
    NotificationRepository notificationRepository;

    private final HttpClient http = HttpClient.newHttpClient();
    private String stationId;
    private final String clientKey = UUID.randomUUID().toString();

    private volatile boolean connected = false;

    @BeforeEach
    void seed() throws Exception {
        stationId = "N" + UUID.randomUUID().toString().substring(0, 8);
        jdbcTemplate.update(
                """
                INSERT INTO station (station_id, name, address, latitude, longitude, zcode, zscode, active, created_at, updated_at)
                VALUES (?, '알림 테스트 충전소', '서울특별시 강남구', 37.5, 127.0, '11', '11680', TRUE, now(), now())""",
                stationId);
        HttpResponse<String> created = post("/api/subscriptions", "{\"stationId\":\"" + stationId + "\"}");
        assertEquals(201, created.statusCode());
    }

    @Test
    @DisplayName("구독한 충전소에 빈자리가 나면 구독자의 실시간 연결로 알림이 가고 알림함에 남는다")
    void deliversNotificationToStreamAndInbox() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        HttpResponse<InputStream> stream = openStream();
        try {
            assertEquals(200, stream.statusCode());
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream.body(), StandardCharsets.UTF_8));
            Future<@Nullable String> firstNotification = executor.submit(() -> readUntilEvent(reader, "notification"));
            awaitConnected();

            ChargerAvailableEvent event = vacancy("01", Instant.now().truncatedTo(ChronoUnit.MILLIS));
            send(event);

            String data = firstNotification.get(30, TimeUnit.SECONDS);
            assertTrue(data != null && data.contains("\"eventId\":\"" + event.eventId() + "\""), "알림을 받지 못했습니다: " + data);
            assertTrue(data.contains("\"stationName\":\"알림 테스트 충전소\""));
            assertTrue(!data.contains(clientKey), "다른 사람에게 보이면 안 되는 clientKey가 화면으로 나갔습니다");

            awaitStatus(event.eventId(), "SENT");
            Map<String, Object> row = jdbcTemplate.queryForMap(
                    "SELECT attempt_count, sent_at, occurred_at FROM notification_log WHERE event_id = ?", event.eventId());
            assertEquals(1, row.get("attempt_count"));

            HttpResponse<String> inbox = get("/api/notifications");
            assertEquals(200, inbox.statusCode());
            assertTrue(inbox.body().contains(event.eventId()));
        } finally {
            stream.body().close();
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("같은 이벤트가 다시 와도, 쿨다운 안에 다른 충전기가 비어도 알림은 한 번만 만든다")
    void createsNotificationOnceForRedeliveryAndWithinCooldown() throws Exception {
        Instant occurredAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        ChargerAvailableEvent first = vacancy("01", occurredAt);
        send(first);
        awaitStatus(first.eventId(), "SENT");

        send(first); // 재전달 (at-least-once)
        send(vacancy("02", occurredAt.plusSeconds(60))); // 10분 쿨다운 안
        // 쿨다운이 지난 빈자리는 다시 알린다 (기준은 이벤트 발생 시각)
        ChargerAvailableEvent afterCooldown = vacancy("03", occurredAt.plus(Duration.ofMinutes(10)));
        send(afterCooldown);
        awaitStatus(afterCooldown.eventId(), "SENT");

        List<String> events = jdbcTemplate.queryForList(
                "SELECT event_id FROM notification_log WHERE station_id = ? ORDER BY id", String.class, stationId);
        assertEquals(List.of(first.eventId(), afterCooldown.eventId()), events);
    }

    @Test
    @DisplayName("시연용(MANUAL) 이벤트와 오래된 이벤트는 알리지 않는다")
    void doesNotNotifyManualOrStaleEvents() throws Exception {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        send(vacancy("01", now, StatusSource.MANUAL));
        send(vacancy("02", now.minus(properties.detector().maxEventAge()).minusSeconds(60)));
        // 순서대로 처리되므로 마지막 이벤트가 처리됐으면 앞의 두 이벤트도 처리가 끝났다
        ChargerAvailableEvent marker = vacancy("03", now);
        send(marker);
        awaitStatus(marker.eventId(), "SENT");

        List<String> events = jdbcTemplate.queryForList(
                "SELECT event_id FROM notification_log WHERE station_id = ?", String.class, stationId);
        assertEquals(List.of(marker.eventId()), events);
    }

    @Test
    @DisplayName("해지한 구독에는 알리지 않는다")
    void doesNotNotifyCancelledSubscription() throws Exception {
        HttpResponse<String> deleted = http.send(
                request("/api/subscriptions/" + stationId).DELETE().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(204, deleted.statusCode());
        // 같은 충전소를 구독 중인 다른 사람은 알림을 받는다
        String otherKey = UUID.randomUUID().toString();
        assertEquals(201, post("/api/subscriptions", "{\"stationId\":\"" + stationId + "\"}", otherKey).statusCode());

        ChargerAvailableEvent event = vacancy("01", Instant.now().truncatedTo(ChronoUnit.MILLIS));
        send(event);
        awaitStatus(event.eventId(), "SENT");

        List<String> recipients = jdbcTemplate.queryForList(
                """
                SELECT sub.client_key FROM notification_log l
                  JOIN subscription s ON s.id = l.subscription_id
                  JOIN subscriber sub ON sub.id = s.subscriber_id
                 WHERE l.event_id = ?""",
                String.class, event.eventId());
        assertEquals(List.of(otherKey), recipients);
    }

    @Test
    @DisplayName("구독자가 많아도 묶음으로 나눠 모두 보낸다")
    void sendsToManySubscribersInChunks() throws Exception {
        // 적재 묶음(500건)보다 많은 구독자
        int extra = 1_100;
        String prefix = "bulk-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        jdbcTemplate.update(
                """
                INSERT INTO subscriber (client_key, created_at)
                SELECT ? || lpad(g::text, 12, '0'), now() FROM generate_series(1, ?) g""",
                prefix, extra);
        jdbcTemplate.update(
                """
                INSERT INTO subscription (subscriber_id, station_id, active, cooldown_minutes, created_at)
                SELECT id, ?, TRUE, 10, now() FROM subscriber WHERE client_key LIKE ?""",
                stationId, prefix + "%");

        ChargerAvailableEvent event = vacancy("01", Instant.now().truncatedTo(ChronoUnit.MILLIS));
        send(event);

        long deadline = System.currentTimeMillis() + 120_000;
        int sent = 0;
        while (System.currentTimeMillis() < deadline) {
            sent = Objects.requireNonNull(jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM notification_log WHERE event_id = ? AND status = 'SENT'", int.class, event.eventId()));
            if (sent == extra + 1) {
                break;
            }
            Thread.sleep(500);
        }
        assertEquals(extra + 1, sent); // 대량 구독자 + 기본 구독자 1명
        // 1,101건을 500건씩: 500 + 500 + 101
        verify(notificationRepository, atLeast(3)).findPendingCommands(eq(event.eventId()), eq(500));
    }

    private ChargerAvailableEvent vacancy(String chargerId, Instant occurredAt) {
        return vacancy(chargerId, occurredAt, StatusSource.CHANGE);
    }

    private ChargerAvailableEvent vacancy(String chargerId, Instant occurredAt, StatusSource source) {
        return new ChargerAvailableEvent(
                ChargerAvailableEvent.eventIdOf(stationId, chargerId, occurredAt),
                stationId,
                chargerId,
                ChargerStatus.CHARGING,
                occurredAt,
                source);
    }

    private void send(ChargerAvailableEvent event) throws Exception {
        kafkaTemplate.send(properties.kafka().topics().chargerAvailable().name(), event.stationId(), event).get();
    }

    private void awaitStatus(String eventId, String status) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            List<String> current = jdbcTemplate.queryForList(
                    "SELECT status FROM notification_log WHERE event_id = ?", String.class, eventId);
            if (!current.isEmpty() && current.stream().allMatch(status::equals)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(eventId + " 알림이 " + status + " 상태가 되지 않았습니다");
    }

    private HttpRequest.Builder request(String path) {
        return request(path, clientKey);
    }

    private HttpRequest.Builder request(String path, String key) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("X-Client-Key", key)
                .timeout(Duration.ofSeconds(30));
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return http.send(request(path).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body) throws IOException, InterruptedException {
        return post(path, body, clientKey);
    }

    private HttpResponse<String> post(String path, String body, String key) throws IOException, InterruptedException {
        return http.send(
                request(path, key).header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<InputStream> openStream() throws IOException, InterruptedException {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/vacancies/stream?clientKey=" + clientKey))
                        .header("Accept", "text/event-stream")
                        .timeout(Duration.ofSeconds(60))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofInputStream());
    }

    private void awaitConnected() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && !connected) {
            Thread.sleep(50);
        }
        assertTrue(connected, "connected 이벤트를 받지 못했습니다");
    }

    /** SSE 본문에서 이름이 name인 첫 이벤트의 data를 돌려준다 */
    private @Nullable String readUntilEvent(BufferedReader reader, String name) throws IOException {
        String current = "message";
        StringBuilder data = new StringBuilder();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            if (line.startsWith("event:")) {
                current = Whitespace.trim(line.substring("event:".length()));
            } else if (line.startsWith("data:")) {
                data.append(line.substring("data:".length()));
            } else if (line.isEmpty()) {
                if (current.equals("connected")) {
                    connected = true;
                }
                if (current.equals(name)) {
                    return data.toString();
                }
                current = "message";
                data.setLength(0);
            }
        }
    }
}
