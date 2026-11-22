package com.evalarm.charger.scenario;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.collector.ChargerStatusCollector;
import com.evalarm.charger.collector.StationSyncResult;
import com.evalarm.charger.collector.StationSyncService;
import com.evalarm.charger.collector.client.ChargerInfoItem;
import com.evalarm.charger.collector.client.ChargerStatusItem;
import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiPage;
import com.evalarm.charger.common.config.AppProperties;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 전 구간 시나리오: 공공 API(가짜) → 전체 동기화·상태 수집 → Kafka → 빈자리 감지 → DB → 검색 API.
 * 공공 API 응답만 가짜로 두고 Kafka, Redis, PostgreSQL, 감지기, 검색 API는 실제로 동작한다.
 */
@SpringBootTest(properties = {
    "app.collector.enabled=false",
    "app.public-api.zcode=11", // 테스트 데이터의 시도 코드. .env의 COLLECT_ZCODE와 무관하게 고정 // 스케줄 대신 테스트가 직접 수집을 실행한다
    "app.detector.enabled=true",
    "app.dev-tools.enabled=true",
})
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
class VacancyScenarioTest {

    private static final Pattern CHARGER_ID = Pattern.compile("\"chargerId\":\"(\\w+)\"");

    @MockitoBean
    EvChargerApiClient apiClient;

    @Autowired
    StationSyncService stationSyncService;

    @Autowired
    ChargerStatusCollector statusCollector;

    @Autowired
    MockMvc mockMvc;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    AppProperties properties;

    private String stationId;
    private String stationName;

    @BeforeEach
    void setUp() throws Exception {
        // 최신 위치부터 읽는 빈자리 버퍼가 파티션을 배정받기 전에 이벤트가 나가면 놓치므로 기다린다
        for (MessageListenerContainer container : listenerRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 3);
        }
        jdbcTemplate.execute("TRUNCATE notification_log, subscription, subscriber, charger, station CASCADE");

        stationId = "SC" + UUID.randomUUID().toString().substring(0, 6).toUpperCase(Locale.ROOT);
        stationName = "시나리오충전소" + stationId;
        // 충전기 2대가 모두 충전중인 충전소
        given(apiClient.fetchChargerInfo(1, 9999)).willReturn(
                new PublicApiPage<>(
                        2,
                        1,
                        List.of(info("01", "3", minutesAgo(5)), info("02", "3", minutesAgo(5)))));
        StationSyncResult synced = stationSyncService.syncAll();
        if (!(synced.isComplete() && synced.chargers() == 2)) {
            throw new IllegalStateException("Check failed.");
        }
    }

    @Test
    @DisplayName("충전이 끝나면 빈자리가 감지되고 검색 결과에 반영되며 같은 수집이 반복돼도 한 번만 감지된다")
    void vacancyIsDetectedReflectedInSearchAndDetectedOnlyOnce() throws Exception {
        searchByName()
                .andExpect(jsonPath("$.items[0].availableCount").value(0))
                .andExpect(jsonPath("$.items[0].chargingCount").value(2));

        // 2분 뒤 상태 수집: 01번 충전 종료
        givenStatusChanges(new ChargerStatusItem(null, stationId, "01", "2", kst(Instant.now()), null));
        statusCollector.collectOnce();

        awaitVacancies(1);
        searchByName()
                .andExpect(jsonPath("$.items[0].availableCount").value(1))
                .andExpect(jsonPath("$.items[0].chargingCount").value(1));
        mockMvc.perform(get("/api/stations")
                        .param("q", stationName)
                        .param("availableOnly", "true"))
                .andExpect(jsonPath("$.totalCount").value(1));

        // 조회 기간이 겹쳐 같은 변경이 다시 수집돼도 빈자리는 한 번만
        statusCollector.collectOnce();
        // 같은 충전소(같은 파티션)의 다음 변경: 02번 충전 종료. 이 이벤트가 보이면 앞의 중복 메시지도 이미 처리된 것이다
        givenStatusChanges(
                new ChargerStatusItem(null, stationId, "01", "2", kst(Instant.now().minusSeconds(1)), null),
                new ChargerStatusItem(null, stationId, "02", "2", kst(Instant.now().plusSeconds(1)), null));
        statusCollector.collectOnce();

        // 메모리 버퍼(eventId로 중복 제거)가 아니라 토픽 원본에서 센다. 중복 발행이 있으면 여기서 드러난다
        List<String> published = consumeAvailableChargerIds(2);
        assertEquals(List.of("01", "02"), published);
    }

    @Test
    @DisplayName("상태 변경 주입기로 연달아 넣어도 같은 경로로 빈자리가 감지되고 출처가 표시된다")
    void injectedStatusChangesAreDetectedWithSource() throws Exception {
        // 같은 초 안에 충전중 → 충전대기를 연달아 주입해도 두 번째가 무시되지 않아야 한다
        for (String status : List.of("CHARGING", "AVAILABLE")) {
            mockMvc.perform(post("/dev/chargers/" + stationId + "/02/status")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"status\":\"" + status + "\"}"))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.status").value(status));
        }

        awaitVacancies(1);
        String byStation = "$[?(@.stationId == '" + stationId + "')]";
        mockMvc.perform(get("/dev/vacancies"))
                .andExpect(jsonPath(byStation + ".chargerId").value(contains("02")))
                .andExpect(jsonPath(byStation + ".previousStatus").value(contains("CHARGING")))
                .andExpect(jsonPath(byStation + ".source").value(contains("MANUAL")));
        mockMvc.perform(get("/api/stations/" + stationId))
                .andExpect(jsonPath("$.availableCount").value(1));
    }

    @Test
    @DisplayName("없는 충전기나 잘못된 상태는 주입하지 않는다")
    void rejectsUnknownChargerOrInvalidStatus() throws Exception {
        mockMvc.perform(post("/dev/chargers/" + stationId + "/99/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"AVAILABLE\"}"))
                .andExpect(status().isNotFound());

        mockMvc.perform(post("/dev/chargers/" + stationId + "/01/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"EMPTY\"}"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/dev/chargers/" + stationId + "/01/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());
    }

    /**
     * charger.available 토픽에서 이 충전소의 이벤트를 처음부터 읽어 충전기 ID를 순서대로 돌려준다.
     * expected건이 모이면 조금 더 읽어 그 뒤에 중복이 따라오는지까지 확인한다.
     */
    private List<String> consumeAvailableChargerIds(int expected) {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "scenario-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(properties.kafka().topics().chargerAvailable().name()));
            List<String> ids = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 20_000;
            int extraPolls = 0;
            while (System.currentTimeMillis() < deadline && extraPolls < 4) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (stationId.equals(record.key())) {
                        Matcher matcher = CHARGER_ID.matcher(record.value());
                        if (!matcher.find()) {
                            throw new IllegalStateException("chargerId가 없는 이벤트: " + record.value());
                        }
                        ids.add(matcher.group(1));
                    }
                }
                if (ids.size() >= expected) {
                    extraPolls++;
                }
            }
            return ids;
        }
    }

    private void givenStatusChanges(ChargerStatusItem... items) throws Exception {
        given(apiClient.fetchChargerStatus(1, 9999, 5)).willReturn(new PublicApiPage<>(items.length, 1, List.of(items)));
    }

    private ResultActions searchByName() throws Exception {
        return mockMvc.perform(get("/api/stations").param("q", stationName));
    }

    private int vacancyCount() throws Exception {
        String body = mockMvc.perform(get("/dev/vacancies")).andReturn().getResponse().getContentAsString();
        Matcher matcher = Pattern.compile("\"stationId\":\"" + stationId + "\"").matcher(body);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private void awaitVacancies(int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && vacancyCount() < expected) {
            Thread.sleep(300);
        }
        mockMvc.perform(get("/dev/vacancies"))
                .andExpect(jsonPath("$[?(@.stationId == '" + stationId + "')]", hasSize(expected)));
    }

    private ChargerInfoItem info(String chargerId, String stat, Instant updatedAt) {
        return new ChargerInfoItem(
                stationName, // statNm
                stationId, // statId
                chargerId, // chgerId
                "04", // chgerType
                "서울특별시 중구 세종대로 110", // addr
                null, // addrDetail
                null, // location
                "37.5665", // lat
                "126.9780", // lng
                null, // useTime
                null, // busiId
                "테스트운영사", // busiNm
                null, // busiCall
                stat, // stat
                kst(updatedAt), // statUpdDt
                "100", // output
                "11", // zcode
                "11140", // zscode
                null, // parkingFree
                null, // limitYn
                null, // limitDetail
                "N"); // delYn
    }

    private static Instant minutesAgo(long minutes) {
        return Instant.now().minusSeconds(minutes * 60);
    }

    /** 공공데이터의 상태갱신일시 형식 (한국 시간 yyyyMMddHHmmss) */
    private static String kst(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("Asia/Seoul")).format(instant);
    }
}
