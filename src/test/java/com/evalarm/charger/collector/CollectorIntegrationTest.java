package com.evalarm.charger.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.reset;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.collector.client.ChargerInfoItem;
import com.evalarm.charger.collector.client.ChargerStatusItem;
import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiException;
import com.evalarm.charger.collector.client.PublicApiPage;
import com.evalarm.charger.common.config.AppProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 페이지 처리를 검증하기 위해 페이지 크기를 2로 줄인다.
 * 각 안전장치를 따로 검증하기 위해 삭제 비율 제한은 끈다(1.0). 비율 제한은 {@link CleanupRatioGuardIntegrationTest}에서 검증한다.
 */
@SpringBootTest(properties = {
    "app.collector.enabled=false",
    "app.public-api.zcode=11", // 테스트 데이터의 시도 코드. .env의 COLLECT_ZCODE와 무관하게 고정
    "app.detector.enabled=false",
    "app.dev-tools.enabled=false",
    "app.collector.page-size=2",
    "app.collector.max-stale-ratio=1.0",
})
@Import(TestcontainersConfig.class)
class CollectorIntegrationTest {

    private static final String DEFAULT_STAT = "2";
    private static final String DEFAULT_STAT_UPD_DT = "20261121235042";
    private static final String DEFAULT_DEL_YN = "N";
    private static final String DEFAULT_LAT = "37.5";

    @MockitoBean
    EvChargerApiClient apiClient;

    @Autowired
    StationSyncService stationSyncService;

    @Autowired
    ChargerStatusCollector statusCollector;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    AppProperties properties;

    @BeforeEach
    void cleanDatabase() {
        jdbcTemplate.execute("TRUNCATE notification_log, subscription, subscriber, charger, station CASCADE");
    }

    @Test
    @DisplayName("여러 페이지에 걸친 충전소를 한 번만 세고 재실행해도 중복 없이 갱신한다")
    void countsStationsAcrossPagesOnceAndIsIdempotent() throws Exception {
        givenInfoPages(
                5,
                List.of(info("ST001", "01"), info("ST001", "02")),
                List.of(info("ST001", "03"), info("ST002", "01")),
                List.of(deletedInfo("ST003", "01"))); // 삭제된 충전기

        StationSyncResult first = stationSyncService.syncAll();
        StationSyncResult second = stationSyncService.syncAll();

        assertEquals(2, first.stations());
        assertEquals(4, first.chargers());
        assertEquals(3, first.pages());
        assertTrue(first.isComplete());
        assertEquals(List.of(first.stations(), first.chargers()), List.of(second.stations(), second.chargers()));
        assertEquals(2, count("SELECT count(*) FROM station"));
        assertEquals(4, count("SELECT count(*) FROM charger"));
    }

    @Test
    @DisplayName("전체 동기화는 더 오래된 상태로 최신 상태를 되돌리지 않는다")
    void fullSyncDoesNotRevertNewerStatus() throws Exception {
        givenInfoPages(1, List.of(infoWithStatus("ST001", "01", "3", "20261121120000")));
        stationSyncService.syncAll();

        // 더 오래된 시각의 '충전대기' → 무시
        givenInfoPages(1, List.of(infoWithStatus("ST001", "01", "2", "20261121110000")));
        stationSyncService.syncAll();
        assertEquals("CHARGING", statusOf("ST001", "01"));

        // 갱신 시각이 없는 값 → 무시
        givenInfoPages(1, List.of(infoWithStatus("ST001", "01", "2", "")));
        stationSyncService.syncAll();
        assertEquals("CHARGING", statusOf("ST001", "01"));

        // 더 최신 시각 → 반영
        givenInfoPages(1, List.of(infoWithStatus("ST001", "01", "2", "20261121130000")));
        stationSyncService.syncAll();
        assertEquals("AVAILABLE", statusOf("ST001", "01"));
    }

    @Test
    @DisplayName("응답에서 사라지거나 삭제된 충전기는 지우고 충전기가 없는 충전소는 비활성화한다")
    void removesMissingOrDeletedChargersAndDeactivatesEmptyStations() throws Exception {
        seedThreeChargers();

        givenInfoPages(
                2,
                List.of(info("ST001", "01"), deletedInfo("ST001", "02"))); // ST001-02 삭제, ST002 사라짐
        StationSyncResult result = stationSyncService.syncAll();

        assertEquals(1, result.removedInSource());
        assertNotNull(result.cleanup());
        assertEquals(2, result.cleanup().deletedChargers());
        assertEquals(1, result.cleanup().deactivatedStations());
        assertEquals(1, count("SELECT count(*) FROM charger"));
        assertEquals(true, activeOf("ST001"));
        assertEquals(false, activeOf("ST002"));
    }

    @Test
    @DisplayName("일부 페이지가 실패하면 나머지는 반영하고 정리는 건너뛴다")
    void appliesRemainingPagesAndSkipsCleanupWhenSomePagesFail() throws Exception {
        seedThreeChargers();

        given(apiClient.fetchChargerInfo(1, 2)).willReturn(new PublicApiPage<>(4, 1, List.of(info("ST001", "01"))));
        given(apiClient.fetchChargerInfo(2, 2)).willThrow(new PublicApiException("getChargerInfo", null, "timeout"));
        StationSyncResult result = stationSyncService.syncAll();

        assertEquals(List.of(2), result.failedPages());
        assertNull(result.cleanup());
        assertNothingRemoved();
    }

    @Test
    @DisplayName("정상 응답이라도 비어 있으면 아무것도 지우지 않는다")
    void removesNothingOnEmptyResponse() throws Exception {
        seedThreeChargers();

        givenInfoPages(0, List.of());
        StationSyncResult result = stationSyncService.syncAll();

        assertNull(result.cleanup());
        assertNotNull(result.cleanupSkippedReason());
        assertNothingRemoved();
    }

    @Test
    @DisplayName("받은 건수가 totalCount보다 적으면 아무것도 지우지 않는다")
    void removesNothingWhenReceivedLessThanTotalCount() throws Exception {
        seedThreeChargers();

        // totalCount는 3인데 페이지가 잘려 1건만 옴
        givenInfoPages(3, List.of(info("ST001", "01")), List.of());
        StationSyncResult result = stationSyncService.syncAll();

        assertNull(result.cleanup());
        assertNothingRemoved();
    }

    @Test
    @DisplayName("응답에 같은 충전기가 중복되면 고유 건수가 모자라므로 아무것도 지우지 않는다")
    void removesNothingWhenResponseHasDuplicates() throws Exception {
        seedThreeChargers();

        // 페이지 경계 밀림으로 ST001-01이 두 번 나오고 ST001-02가 누락된 상황
        givenInfoPages(3, List.of(info("ST001", "01"), info("ST001", "01")), List.of(info("ST002", "01")));
        StationSyncResult result = stationSyncService.syncAll();

        assertEquals(2, result.uniqueChargers());
        assertTrue(Objects.requireNonNull(result.cleanupSkippedReason()).contains("고유"));
        assertNothingRemoved();
    }

    @Test
    @DisplayName("동기화 도중 totalCount가 바뀌면 아무것도 지우지 않는다")
    void removesNothingWhenTotalCountChangesDuringSync() throws Exception {
        seedThreeChargers();

        given(apiClient.fetchChargerInfo(1, 2))
                .willReturn(new PublicApiPage<>(3, 1, List.of(info("ST001", "01"), info("ST001", "02"))));
        given(apiClient.fetchChargerInfo(2, 2)).willReturn(new PublicApiPage<>(4, 2, List.of(info("ST009", "01"))));
        StationSyncResult result = stationSyncService.syncAll();

        assertTrue(Objects.requireNonNull(result.cleanupSkippedReason()).contains("totalCount가 바뀐"));
        assertEquals(3, count("SELECT count(*) FROM charger WHERE station_id IN ('ST001', 'ST002')"));
    }

    @Test
    @DisplayName("응답에 있지만 값이 이상해 걸러진 충전기는 지우지 않는다")
    void keepsChargersFilteredForInvalidValues() throws Exception {
        seedThreeChargers();

        givenInfoPages(
                3,
                List.of(info("ST001", "01"), info("ST001", "02")),
                List.of(infoWithLat("ST002", "01", "0"))); // 좌표 이상 → 저장하지 않지만 응답에는 있음
        StationSyncResult result = stationSyncService.syncAll();

        assertEquals(1, result.skipped());
        assertNothingRemoved();
    }

    @Test
    @DisplayName("전체 동기화는 모든 충전기 상태를 SNAPSHOT으로 발행한다")
    void fullSyncPublishesAllStatusesAsSnapshot() throws Exception {
        givenInfoPages(
                2,
                List.of(infoWithStatus("SNAP01", "01", "2", DEFAULT_STAT_UPD_DT),
                        infoWithStatus("SNAP01", "02", "3", DEFAULT_STAT_UPD_DT)));

        stationSyncService.syncAll();

        List<String> records = consume(properties.kafka().topics().chargerStatus().name(), "SNAP01", 2);
        assertEquals(2, records.size());
        assertTrue(records.stream().allMatch(it -> it.contains("\"source\":\"SNAPSHOT\"")));
    }

    @Test
    @DisplayName("충전기 상태를 충전소 ID를 키로 Kafka에 발행한다")
    void publishesStatusesKeyedByStationId() throws Exception {
        given(apiClient.fetchChargerStatus(1, 2, 5)).willReturn(
                new PublicApiPage<>(
                        2,
                        1,
                        List.of(
                                new ChargerStatusItem(null, "KAFKA01", "01", "2", "20261121235042", null),
                                new ChargerStatusItem(null, "KAFKA01", "02", "3", "20261121235053", null))));

        StatusCollectResult result = statusCollector.collectOnce();
        assertEquals(2, result.published());

        List<String> records = consume(properties.kafka().topics().chargerStatus().name(), "KAFKA01", 2);
        assertEquals(2, records.size());
        assertTrue(records.stream().anyMatch(it -> it.contains("\"status\":\"AVAILABLE\"")));
        assertTrue(records.stream().anyMatch(it -> it.contains("\"status\":\"CHARGING\"")));
        assertTrue(records.stream().allMatch(it -> it.contains("\"source\":\"CHANGE\"")));
    }

    /** ST001(01, 02), ST002(01) 세 충전기를 저장해 둔다 */
    private void seedThreeChargers() throws Exception {
        givenInfoPages(
                3,
                List.of(info("ST001", "01"), info("ST001", "02")),
                List.of(info("ST002", "01")));
        StationSyncResult seeded = stationSyncService.syncAll();
        if (!(seeded.isComplete() && seeded.chargers() == 3)) {
            throw new IllegalStateException("Check failed.");
        }
        reset(apiClient);
    }

    private void assertNothingRemoved() {
        assertEquals(3, count("SELECT count(*) FROM charger"));
        assertEquals(2, count("SELECT count(*) FROM station WHERE active"));
    }

    @SafeVarargs
    private void givenInfoPages(int totalCount, List<ChargerInfoItem>... pages) throws Exception {
        for (int index = 0; index < pages.length; index++) {
            given(apiClient.fetchChargerInfo(index + 1, 2)).willReturn(new PublicApiPage<>(totalCount, index + 1, pages[index]));
        }
    }

    private List<String> consume(String topic, String key, int expected) {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "collector-test-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            List<String> result = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline && result.size() < expected) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        result.add(record.value());
                    }
                }
            }
            return result;
        }
    }

    private int count(String sql) {
        return Objects.requireNonNull(jdbcTemplate.queryForObject(sql, int.class));
    }

    private @Nullable String statusOf(String stationId, String chargerId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM charger WHERE station_id = ? AND charger_id = ?",
                String.class, stationId, chargerId);
    }

    private boolean activeOf(String stationId) {
        return Objects.requireNonNull(
                jdbcTemplate.queryForObject("SELECT active FROM station WHERE station_id = ?", boolean.class, stationId));
    }

    private static ChargerInfoItem info(String statId, String chgerId) {
        return info(statId, chgerId, DEFAULT_STAT, DEFAULT_STAT_UPD_DT, DEFAULT_DEL_YN, DEFAULT_LAT);
    }

    private static ChargerInfoItem infoWithStatus(String statId, String chgerId, String stat, String statUpdDt) {
        return info(statId, chgerId, stat, statUpdDt, DEFAULT_DEL_YN, DEFAULT_LAT);
    }

    private static ChargerInfoItem deletedInfo(String statId, String chgerId) {
        return info(statId, chgerId, DEFAULT_STAT, DEFAULT_STAT_UPD_DT, "Y", DEFAULT_LAT);
    }

    private static ChargerInfoItem infoWithLat(String statId, String chgerId, String lat) {
        return info(statId, chgerId, DEFAULT_STAT, DEFAULT_STAT_UPD_DT, DEFAULT_DEL_YN, lat);
    }

    private static ChargerInfoItem info(
            String statId, String chgerId, String stat, String statUpdDt, String delYn, String lat) {
        return new ChargerInfoItem(
                "테스트충전소" + statId, // statNm
                statId, // statId
                chgerId, // chgerId
                "06", // chgerType
                "서울특별시 테스트구 테스트로 1", // addr
                null, // addrDetail
                null, // location
                lat, // lat
                "127.0", // lng
                null, // useTime
                "ME", // busiId
                "기후에너지환경부", // busiNm
                null, // busiCall
                stat, // stat
                statUpdDt, // statUpdDt
                "50", // output
                "11", // zcode
                "11620", // zscode
                "Y", // parkingFree
                "N", // limitYn
                null, // limitDetail
                delYn); // delYn
    }
}
