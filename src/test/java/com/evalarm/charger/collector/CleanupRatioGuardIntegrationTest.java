package com.evalarm.charger.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.reset;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.collector.client.ChargerInfoItem;
import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiPage;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** 응답이 완전해 보여도 지울 충전기가 허용 비율을 넘으면 정리하지 않는지 검증한다 (허용 비율 50%) */
@SpringBootTest(properties = {
    "app.collector.enabled=false", "app.public-api.zcode=11", "app.dev-tools.enabled=false", "app.detector.enabled=false",
    "app.collector.max-stale-ratio=0.5",
})
@Import(TestcontainersConfig.class)
class CleanupRatioGuardIntegrationTest {

    @MockitoBean
    EvChargerApiClient apiClient;

    @Autowired
    StationSyncService stationSyncService;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    MeterRegistry meterRegistry;

    @BeforeEach
    void cleanDatabase() {
        jdbcTemplate.execute("TRUNCATE notification_log, subscription, subscriber, charger, station CASCADE");
    }

    @Test
    @DisplayName("지울 충전기가 허용 비율을 넘으면 정리를 건너뛰고 지표로 알린다")
    void skipsCleanupAboveAllowedRatioAndReportsMetric() throws Exception {
        givePage(info("ST001", "01"), info("ST001", "02"), info("ST002", "01"));
        stationSyncService.syncAll();
        reset(apiClient);

        // 3대 중 2대가 사라짐(67%) → 허용치 50%(1대) 초과. 응답 자체는 완전함(받은 건수 = totalCount)
        givePage(info("ST001", "01"));
        StationSyncResult result = stationSyncService.syncAll();

        assertTrue(result.isComplete());
        assertNull(result.cleanup());
        assertTrue(Objects.requireNonNull(result.cleanupSkippedReason()).contains("허용치"));
        assertEquals(3, jdbcTemplate.queryForObject("SELECT count(*) FROM charger", int.class));
        assertEquals(2.0, meterRegistry.get("collector.station.cleanup.pending").gauge().value());
    }

    @Test
    @DisplayName("허용 비율 이내면 정리하고 대기 지표를 0으로 되돌린다")
    void cleansUpWithinAllowedRatioAndResetsMetric() throws Exception {
        givePage(info("ST001", "01"), info("ST001", "02"), info("ST002", "01"));
        stationSyncService.syncAll();
        reset(apiClient);

        givePage(info("ST001", "01"), info("ST001", "02")); // 1대(33%) 사라짐
        StationSyncResult result = stationSyncService.syncAll();

        assertNotNull(result.cleanup());
        assertEquals(1, result.cleanup().deletedChargers());
        assertEquals(0.0, meterRegistry.get("collector.station.cleanup.pending").gauge().value());
    }

    private void givePage(ChargerInfoItem... items) throws Exception {
        given(apiClient.fetchChargerInfo(1, 9999)).willReturn(new PublicApiPage<>(items.length, 1, List.of(items)));
    }

    private static ChargerInfoItem info(String statId, String chgerId) {
        return new ChargerInfoItem(
                "테스트충전소" + statId, // statNm
                statId, // statId
                chgerId, // chgerId
                null, // chgerType
                "서울특별시 테스트구 테스트로 1", // addr
                null, // addrDetail
                null, // location
                "37.5", // lat
                "127.0", // lng
                null, // useTime
                null, // busiId
                null, // busiNm
                null, // busiCall
                "2", // stat
                "20261121235042", // statUpdDt
                null, // output
                "11", // zcode
                null, // zscode
                null, // parkingFree
                null, // limitYn
                null, // limitDetail
                "N"); // delYn
    }
}
