package com.evalarm.charger.devtools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 화면의 "테스트 빈자리 만들기" 버튼이 쓰는 개발 도구.
 * 상태 주입 → 빈자리 감지 → 매칭 → 발송까지 실제 경로를 타고, 시연 알림은 쿨다운 없이 매번 가며
 * 실제 알림의 쿨다운 기준(마지막 알림 시각)을 바꾸지 않는지 검증한다.
 */
@SpringBootTest(properties = {
    "app.collector.enabled=false", "app.public-api.zcode=11",
    "app.dev-tools.enabled=true", "app.detector.enabled=true",
    "app.notification.enabled=true", "app.notification.include-manual-events=true",
})
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
class TestVacancyNotificationTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaTemplate<String, ChargerAvailableEvent> kafkaTemplate;

    @Autowired
    AppProperties properties;

    private String stationId;
    private long subscriptionId;

    @BeforeEach
    void seed() {
        stationId = "T" + UUID.randomUUID().toString().substring(0, 8);
        jdbcTemplate.update(
                """
                INSERT INTO station (station_id, name, address, latitude, longitude, zcode, zscode, active, created_at, updated_at)
                VALUES (?, '테스트 버튼 충전소', '서울특별시', 37.5, 127.0, '11', '11680', TRUE, now(), now())""",
                stationId);
        for (String chargerId : List.of("01", "02")) {
            jdbcTemplate.update(
                    """
                    INSERT INTO charger (station_id, charger_id, charger_type, output_kw, status, status_updated_at, created_at, updated_at)
                    VALUES (?, ?, '02', 7, 'AVAILABLE', now() - interval '1 hour', now(), now())""",
                    stationId, chargerId);
        }
        String clientKey = UUID.randomUUID().toString();
        jdbcTemplate.update("INSERT INTO subscriber (client_key, created_at) VALUES (?, now())", clientKey);
        subscriptionId = Objects.requireNonNull(jdbcTemplate.queryForObject(
                """
                INSERT INTO subscription (subscriber_id, station_id, active, cooldown_minutes, created_at)
                VALUES ((SELECT id FROM subscriber WHERE client_key = ?), ?, TRUE, 10, now()) RETURNING id""",
                Long.class, clientKey, stationId));
    }

    @Test
    @DisplayName("테스트 빈자리는 실제 경로로 알림까지 가고, 10분 안에 다시 눌러도 또 간다")
    void testVacancyGoesThroughRealPathAndRepeatsWithinCooldown() throws Exception {
        testVacancy()
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.chargerId").value("01"));
        awaitSentCount(1);
        // 감지기가 DB 상태까지 바꿨다 (빈자리 이벤트를 직접 넣은 것이 아님)
        assertEquals(
                "AVAILABLE",
                jdbcTemplate.queryForObject(
                        "SELECT status FROM charger WHERE station_id = ? AND charger_id = '01'", String.class, stationId));

        testVacancy().andExpect(status().isAccepted());
        awaitSentCount(2);

        // 시연 알림은 실제 알림의 쿨다운 기준을 건드리지 않는다
        assertEquals(null, lastNotifiedAt());
    }

    @Test
    @DisplayName("시연 알림 뒤에도 실제 빈자리는 바로 알리고, 그때부터 쿨다운이 적용된다")
    void realVacancyAfterDemoIsNotifiedAndStartsCooldown() throws Exception {
        testVacancy();
        awaitSentCount(1);

        Instant occurredAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        sendReal("02", occurredAt);
        awaitSentCount(2);
        assertNotNull(lastNotifiedAt());

        // 10분 안의 다른 실제 빈자리는 알리지 않는다. 뒤이은 시연 알림이 처리됐으면 앞의 이벤트도 처리가 끝났다
        ChargerAvailableEvent withinCooldown = sendReal("01", occurredAt.plusSeconds(60));
        testVacancy();
        awaitSentCount(3);
        assertEquals(
                0,
                jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM notification_log WHERE event_id = ?", int.class, withinCooldown.eventId()));
    }

    @Test
    @DisplayName("X-Requested-With 헤더가 없으면 거절한다 (다른 사이트의 몰래 호출 방지)")
    void rejectsWithoutRequestedWithHeader() throws Exception {
        mockMvc.perform(post("/dev/stations/" + stationId + "/test-vacancy")).andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("충전기를 고를 수 있고, 없는 충전소는 404")
    void canChooseChargerAndUnknownStationIs404() throws Exception {
        mockMvc.perform(post("/dev/stations/" + stationId + "/test-vacancy")
                        .param("chargerId", "02")
                        .header("X-Requested-With", "fetch"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.chargerId").value("02"));
        mockMvc.perform(post("/dev/stations/NOPE/test-vacancy").header("X-Requested-With", "fetch"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("개발 도구가 켜져 있으면 화면 설정에 알린다")
    void clientConfigReportsDevToolsEnabled() throws Exception {
        mockMvc.perform(get("/api/client-config")).andExpect(jsonPath("$.devToolsEnabled").value(true));
    }

    private ResultActions testVacancy() throws Exception {
        return mockMvc.perform(post("/dev/stations/" + stationId + "/test-vacancy").header("X-Requested-With", "fetch"));
    }

    private ChargerAvailableEvent sendReal(String chargerId, Instant occurredAt) throws Exception {
        ChargerAvailableEvent event = new ChargerAvailableEvent(
                ChargerAvailableEvent.eventIdOf(stationId, chargerId, occurredAt),
                stationId,
                chargerId,
                ChargerStatus.CHARGING,
                occurredAt,
                StatusSource.CHANGE);
        kafkaTemplate.send(properties.kafka().topics().chargerAvailable().name(), stationId, event).get();
        return event;
    }

    private @Nullable Object lastNotifiedAt() {
        return jdbcTemplate.queryForObject(
                "SELECT last_notified_at FROM subscription WHERE id = ?", Object.class, subscriptionId);
    }

    private void awaitSentCount(int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        int count = 0;
        while (System.currentTimeMillis() < deadline) {
            count = Objects.requireNonNull(jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM notification_log WHERE subscription_id = ? AND status = 'SENT'",
                    int.class, subscriptionId));
            if (count >= expected) {
                break;
            }
            Thread.sleep(100);
        }
        assertEquals(expected, count);
    }
}
