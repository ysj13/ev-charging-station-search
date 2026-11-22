package com.evalarm.charger.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import com.evalarm.charger.collector.client.ChargerStatusItem;
import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiException;
import com.evalarm.charger.collector.client.PublicApiPage;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class ChargerStatusCollectorTest {

    private final EvChargerApiClient apiClient = mock(EvChargerApiClient.class);

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate = mock(KafkaTemplate.class);

    private final MutableClock clock = new MutableClock(Instant.parse("2026-11-21T12:00:00Z"));
    private final ChargerStatusCollector collector =
            new ChargerStatusCollector(apiClient, kafkaTemplate, properties(), clock);

    @BeforeEach
    void setUp() throws Exception {
        stubApiSuccess();
        given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.<SendResult<String, ChargerStatusMessage>>completedFuture(null));
    }

    @Test
    @DisplayName("첫 수집과 정상 주기에서는 기본 조회 기간을 쓴다")
    void usesDefaultPeriodOnFirstAndRegularCollection() throws Exception {
        assertEquals(5, collector.collectOnce().periodMinutes());
        clock.advance(Duration.ofMinutes(2));
        assertEquals(5, collector.collectOnce().periodMinutes());
    }

    @Test
    @DisplayName("수집이 연속 실패하면 복구 시 마지막 성공 시점부터 다시 조회한다")
    void resumesFromLastSuccessAfterConsecutiveFailures() throws Exception {
        collector.collectOnce(); // 12:00 성공

        stubApiFailure();
        clock.advance(Duration.ofMinutes(2));
        assertThrows(PublicApiException.class, collector::collectOnce); // 12:02 실패
        clock.advance(Duration.ofMinutes(2));
        assertThrows(PublicApiException.class, collector::collectOnce); // 12:04 실패

        stubApiSuccess();
        clock.advance(Duration.ofMinutes(2));
        StatusCollectResult result = collector.collectOnce(); // 12:06 복구: 6분 + 여유 2분

        assertEquals(8, result.periodMinutes());
        assertFalse(result.needsCatchUp());
        verify(apiClient).fetchChargerStatus(1, 9999, 8);
    }

    @Test
    @DisplayName("조회 범위를 넘게 밀리면 상한으로 조회하고 전체 동기화로 따라잡도록 알린다")
    void capsPeriodAndRequestsCatchUpWhenTooFarBehind() throws Exception {
        collector.collectOnce();

        stubApiFailure();
        clock.advance(Duration.ofMinutes(10));
        assertThrows(PublicApiException.class, collector::collectOnce);

        stubApiSuccess();
        clock.advance(Duration.ofMinutes(10));
        StatusCollectResult result = collector.collectOnce();

        assertEquals(10, result.periodMinutes());
        assertTrue(result.needsCatchUp());
        verify(apiClient).fetchChargerStatus(1, 9999, 10);

        // 따라잡은 뒤에는 정상 주기로 돌아온다
        clock.advance(Duration.ofMinutes(2));
        assertFalse(collector.collectOnce().needsCatchUp());
    }

    private void stubApiSuccess() throws Exception {
        reset(apiClient);
        given(apiClient.fetchChargerStatus(anyInt(), anyInt(), anyInt()))
                .willReturn(new PublicApiPage<>(1, 1, List.of(new ChargerStatusItem(null, "ST001", "01", "2", null, null))));
    }

    private void stubApiFailure() throws Exception {
        reset(apiClient);
        given(apiClient.fetchChargerStatus(anyInt(), anyInt(), anyInt()))
                .willThrow(new PublicApiException("getChargerStatus", null, "timeout"));
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static AppProperties properties() {
        return new AppProperties(
                new AppProperties.PublicApi(
                        "http://localhost",
                        "key",
                        "11",
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        Duration.ZERO),
                new AppProperties.Collector(
                        false,
                        "-",
                        Duration.ofMinutes(2),
                        5,
                        10,
                        0.05,
                        9999),
                new AppProperties.Kafka(
                        new AppProperties.Kafka.Topics(
                                new AppProperties.Kafka.Topic("charger.status", 1, (short) 1),
                                new AppProperties.Kafka.Topic("charger.available", 1, (short) 1))),
                new AppProperties.Rabbit("x", "r", "q", "dlx", "dlq"),
                AppProperties.Detector.defaults(),
                AppProperties.Web.defaults(),
                AppProperties.Notification.defaults());
    }
}
