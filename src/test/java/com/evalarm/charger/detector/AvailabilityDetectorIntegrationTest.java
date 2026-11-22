package com.evalarm.charger.detector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.collector.ChargerRow;
import com.evalarm.charger.collector.StationUpsertRepository;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.station.domain.ChargerKey;
import com.evalarm.charger.station.domain.ChargerStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/** charger.status → (빈자리 감지) → charger.available, DB, Redis 까지 실제 인프라로 검증한다 */
@SpringBootTest(properties = {
    "app.collector.enabled=false", "app.public-api.zcode=11", "app.dev-tools.enabled=false", "app.detector.enabled=true",
})
@Import(TestcontainersConfig.class)
class AvailabilityDetectorIntegrationTest {

    /** 전체 동기화가 기록했다고 가정하는 updated_at */
    private static final Instant SYNC_MARK = Instant.parse("2026-11-21T00:00:00Z");

    @Autowired
    KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ChargerStateStore stateStore;

    @Autowired
    AppProperties properties;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    StationUpsertRepository upsertRepository;

    /** 테스트마다 1회만 실패를 주입하기 위한 스파이. 테스트가 끝나면 원래 동작으로 돌아간다 */
    @MockitoSpyBean
    ChargerStatusUpdateRepository statusRepository;

    // 빈자리 이벤트는 최근(15분 이내) 것만 발행하므로 현재 시각 기준으로 만든다
    private final Instant t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(300);
    private String stationId;

    @BeforeEach
    void setUp() {
        // 테스트마다 다른 충전소 ID를 써서 Kafka·Redis에 남은 이전 테스트 데이터와 섞이지 않게 한다
        stationId = "IT" + UUID.randomUUID().toString().substring(0, 8);
        jdbcTemplate.update(
                """
                INSERT INTO station (station_id, name, address, latitude, longitude, zcode, created_at, updated_at)
                VALUES (?, '테스트충전소', '서울', 37.5, 127.0, '11', now(), now())""",
                stationId);
        jdbcTemplate.update(
                """
                INSERT INTO charger (station_id, charger_id, status, status_updated_at, created_at, updated_at)
                VALUES (?, '01', 'CHARGING', ?, now(), ?)""",
                stationId, offset(t0.minusSeconds(60)), offset(SYNC_MARK));
    }

    @Test
    @DisplayName("충전중이던 충전기가 충전대기가 되면 빈자리 이벤트를 발행하고 DB와 Redis에 반영한다")
    void publishesVacancyAndUpdatesDbAndRedis() throws Exception {
        send(ChargerStatus.CHARGING, t0, StatusSource.SNAPSHOT);
        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60));

        List<String> events = consumeAvailable(1);

        assertEquals(1, events.size());
        assertTrue(events.getFirst().contains("\"previousStatus\":\"CHARGING\""));
        assertTrue(events.getFirst().contains(
                "\"eventId\":\"" + stationId + ":01:" + t0.plusSeconds(60).toEpochMilli() + "\""));
        awaitDbStatus("AVAILABLE");
        assertEquals(new ChargerState(ChargerStatus.AVAILABLE, t0.plusSeconds(60)), redisState());
    }

    @Test
    @DisplayName("상태를 반영해도 전체 동기화용 updated_at은 바꾸지 않는다")
    void doesNotTouchSyncUpdatedAt() throws Exception {
        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60));

        awaitDbStatus("AVAILABLE");
        assertEquals(
                SYNC_MARK,
                Objects.requireNonNull(jdbcTemplate.queryForObject(
                        "SELECT updated_at FROM charger WHERE station_id = ? AND charger_id = '01'",
                        OffsetDateTime.class, stationId)).toInstant());
    }

    @Test
    @DisplayName("Redis에 직전 상태가 없으면 DB 상태를 기준으로 빈자리를 감지한다")
    void fallsBackToDbStateWhenRedisIsEmpty() throws Exception {
        // Redis를 잃은 상황: Redis에는 아무것도 없고 DB에는 충전중
        assertNull(redisState());
        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60));

        assertEquals(1, consumeAvailable(1).size());
        awaitDbStatus("AVAILABLE");
    }

    @Test
    @DisplayName("오래전에 일어난 빈자리는 상태만 반영하고 이벤트는 발행하지 않는다")
    void staleVacancyUpdatesStateWithoutEvent() throws Exception {
        Instant longAgo = t0.minus(Duration.ofHours(2));
        jdbcTemplate.update(
                "UPDATE charger SET status_updated_at = ? WHERE station_id = ?", offset(longAgo.minusSeconds(60)), stationId);
        send(ChargerStatus.AVAILABLE, longAgo);

        awaitDbStatus("AVAILABLE");
        assertTrue(consumeAvailable(1, 5_000).isEmpty());
    }

    @Test
    @DisplayName("DB 갱신이 실패하면 다시 처리하되 이미 발행한 이벤트는 다시 발행하지 않는다")
    void retriesAfterDbFailureWithoutRepublishing() throws Exception {
        double suppressedBefore = meterRegistry.counter("detector.vacancy.duplicates.suppressed").count();
        // 이 테스트의 충전기가 들어 있는 배치에서만 한 번 실패시킨다 (이전 테스트의 남은 배치가 예외를 소진하지 않도록)
        ChargerKey key = new ChargerKey(stationId, "01");
        doThrow(new DataAccessResourceFailureException("DB 장애 주입"))
                .doCallRealMethod()
                .when(statusRepository).updateIfNewer(mapContainingKey(key));

        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60));

        awaitDbStatus("AVAILABLE"); // 재시도로 결국 반영됨
        assertEquals(1, consumeAvailable(2, 5_000).size());
        assertEquals(new ChargerState(ChargerStatus.AVAILABLE, t0.plusSeconds(60)), redisState());
        // 실제로 재처리 경로를 탔고, 그때 같은 이벤트를 다시 발행하지 않았음을 확인한다
        assertEquals(suppressedBefore + 1, meterRegistry.counter("detector.vacancy.duplicates.suppressed").count());
    }

    @Test
    @DisplayName("전체 동기화는 이미 있는 충전기의 상태를 직접 바꾸지 않아 감지기가 전이를 놓치지 않는다")
    void fullSyncDoesNotOverwriteStatusSoDetectorSeesTransition() throws Exception {
        // 04시 동기화가 감지기보다 먼저 같은 충전기를 충전대기로 받은 상황
        upsertRepository.upsertChargers(
                List.of(new ChargerRow(stationId, "01", "06", 50, ChargerStatus.AVAILABLE, t0.plusSeconds(60))),
                Instant.now());
        assertEquals("CHARGING", currentDbStatus()); // 상태는 감지기에 맡긴다

        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60), StatusSource.SNAPSHOT);

        assertEquals(1, consumeAvailable(1).size());
        awaitDbStatus("AVAILABLE");
    }

    @Test
    @DisplayName("DB에서 채운 기준 상태는 Redis에 심어 둔다")
    void seedsRedisWithBaselineLoadedFromDb() throws Exception {
        // 상태가 그대로인 SNAPSHOT: 무시되지만, DB에서 읽은 기준 상태가 Redis에 저장되어야 한다
        send(ChargerStatus.CHARGING, t0.minusSeconds(60), StatusSource.SNAPSHOT);

        long deadline = System.currentTimeMillis() + 15_000;
        while (redisState() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        assertEquals(new ChargerState(ChargerStatus.CHARGING, t0.minusSeconds(60)), redisState());
    }

    @Test
    @DisplayName("중복이나 오래된 메시지로는 이벤트도 상태 변경도 일어나지 않는다")
    void duplicateOrStaleMessagesCauseNoEventOrChange() throws Exception {
        send(ChargerStatus.CHARGING, t0, StatusSource.SNAPSHOT);
        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60));
        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60), StatusSource.SNAPSHOT); // 중복
        send(ChargerStatus.CHARGING, t0.plusSeconds(30), StatusSource.SNAPSHOT); // 늦게 온 오래된 상태

        List<String> events = consumeAvailable(2, 8_000);

        assertEquals(1, events.size());
        awaitDbStatus("AVAILABLE");
    }

    @Test
    @DisplayName("Redis 저장은 더 최신 값일 때만 덮어쓴다")
    void redisSaveOverwritesOnlyNewerValues() {
        ChargerKey key = new ChargerKey(stationId, "09");
        stateStore.saveIfNewer(Map.of(key, new ChargerState(ChargerStatus.AVAILABLE, t0.plusSeconds(60))));
        stateStore.saveIfNewer(Map.of(key, new ChargerState(ChargerStatus.CHARGING, t0.plusSeconds(30))));
        stateStore.saveIfNewer(Map.of(key, new ChargerState(ChargerStatus.CHARGING, t0.plusSeconds(60)))); // 같은 시각

        assertEquals(new ChargerState(ChargerStatus.AVAILABLE, t0.plusSeconds(60)), stateStore.load(List.of(key)).get(key));
    }

    @Test
    @DisplayName("역직렬화할 수 없는 메시지는 건너뛰고 다음 메시지를 처리한다")
    void skipsUndeserializableMessageAndProcessesNext() throws Exception {
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(kafkaAdmin.getConfigurationProperties(), new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(properties.kafka().topics().chargerStatus().name(), stationId, "{not-json")).get();
        }
        send(ChargerStatus.AVAILABLE, t0.plusSeconds(60));

        assertEquals(1, consumeAvailable(1).size());
    }

    @Test
    @DisplayName("필수 값이 빠진 메시지는 건너뛰고 같은 파티션의 다음 메시지를 처리한다")
    void skipsMessageMissingRequiredFieldWithoutBlockingPartition() throws Exception {
        jdbcTemplate.update(
                """
                INSERT INTO charger (station_id, charger_id, status, status_updated_at, created_at, updated_at)
                VALUES (?, '02', 'CHARGING', ?, now(), ?)""",
                stationId, offset(t0.minusSeconds(60)), offset(SYNC_MARK));
        double invalidBefore = meterRegistry.counter("detector.messages.invalid").count();

        // JSON 형식과 타입 정보는 맞지만 필수 값(status)이 없는 메시지. 충전소 ID가 키라 다음 메시지와 같은 파티션에 들어간다
        String missingStatus = """
                {"stationId":"%s","chargerId":"01","statusUpdatedAt":"%s","collectedAt":"%s","source":"CHANGE"}"""
                .formatted(stationId, t0.plusSeconds(30), Instant.now());
        // 같은 방식(원시 JSON + 타입 헤더)으로 만든 정상 메시지. 이것이 처리되면 앞의 메시지는 status가 없어서만 걸러진 것이다
        String valid = """
                {"stationId":"%s","chargerId":"02","status":"AVAILABLE","statusUpdatedAt":"%s","collectedAt":"%s","source":"CHANGE"}"""
                .formatted(stationId, t0.plusSeconds(60), Instant.now());
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(kafkaAdmin.getConfigurationProperties(), new StringSerializer(), new StringSerializer())) {
            producer.send(rawStatusMessage(missingStatus)).get();
            producer.send(rawStatusMessage(valid)).get();
        }

        List<String> events = consumeAvailable(1);

        assertEquals(1, events.size());
        assertTrue(events.getFirst().contains("\"chargerId\":\"02\""));
        awaitDbStatus("02", "AVAILABLE");
        // 뒤의 메시지가 처리됐다면 앞의 깨진 메시지는 이미 건너뛴 것이다
        assertTrue(meterRegistry.counter("detector.messages.invalid").count() >= invalidBefore + 1);
        assertEquals("CHARGING", currentDbStatus()); // 깨진 메시지는 충전기 01에 반영되지 않는다
    }

    /** KafkaTemplate을 거치지 않은 상태 메시지. JacksonJsonSerializer처럼 타입 헤더를 붙인다 */
    private ProducerRecord<String, String> rawStatusMessage(String json) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(properties.kafka().topics().chargerStatus().name(), stationId, json);
        record.headers().add("__TypeId__", ChargerStatusMessage.class.getName().getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private void send(ChargerStatus status, Instant updatedAt) throws Exception {
        send(status, updatedAt, StatusSource.CHANGE);
    }

    private void send(ChargerStatus status, Instant updatedAt, StatusSource source) throws Exception {
        send("01", status, updatedAt, source);
    }

    private void send(String chargerId, ChargerStatus status, Instant updatedAt, StatusSource source) throws Exception {
        ChargerStatusMessage message = new ChargerStatusMessage(stationId, chargerId, status, updatedAt, Instant.now(), source);
        kafkaTemplate.send(properties.kafka().topics().chargerStatus().name(), stationId, message).get();
    }

    private @Nullable ChargerState redisState() {
        return stateStore.load(List.of(new ChargerKey(stationId, "01"))).get(new ChargerKey(stationId, "01"));
    }

    private List<String> consumeAvailable(int expected) {
        return consumeAvailable(expected, 20_000);
    }

    private List<String> consumeAvailable(int expected, long waitMillis) {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "detector-test-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(properties.kafka().topics().chargerAvailable().name()));
            List<String> result = new ArrayList<>();
            long deadline = System.currentTimeMillis() + waitMillis;
            while (System.currentTimeMillis() < deadline && result.size() < expected) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (stationId.equals(record.key())) {
                        result.add(record.value());
                    }
                }
            }
            return result;
        }
    }

    private void awaitDbStatus(String expected) throws InterruptedException {
        awaitDbStatus("01", expected);
    }

    private void awaitDbStatus(String chargerId, String expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        String actual = null;
        while (System.currentTimeMillis() < deadline) {
            actual = jdbcTemplate.queryForObject(
                    "SELECT status FROM charger WHERE station_id = ? AND charger_id = ?", String.class, stationId, chargerId);
            if (expected.equals(actual)) {
                return;
            }
            Thread.sleep(200);
        }
        assertEquals(expected, actual);
    }

    private static OffsetDateTime offset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Map<ChargerKey, ChargerState> mapContainingKey(ChargerKey key) {
        return argThat(map -> map != null && map.containsKey(key));
    }

    private @Nullable String currentDbStatus() {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM charger WHERE station_id = ? AND charger_id = '01'", String.class, stationId);
    }
}
