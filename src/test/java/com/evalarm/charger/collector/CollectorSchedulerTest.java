package com.evalarm.charger.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.TaskExecutor;

class CollectorSchedulerTest {

    private final EvChargerApiClient apiClient = mock(EvChargerApiClient.class);
    private final StationSyncService stationSyncService = mock(StationSyncService.class);
    private final ChargerStatusCollector statusCollector = mock(ChargerStatusCollector.class);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-11-21T12:00:00Z"));

    // 비동기 실행을 같은 스레드에서 바로 실행해 결과를 결정적으로 검증한다
    private final TaskExecutor syncExecutor = Runnable::run;
    private final CollectorScheduler scheduler =
            new CollectorScheduler(apiClient, stationSyncService, statusCollector, syncExecutor, clock);

    @BeforeEach
    void setUp() throws Exception {
        given(apiClient.hasServiceKey()).willReturn(true);
        given(statusCollector.collectOnce()).willReturn(new StatusCollectResult(5, 0, 0, false));
    }

    @Test
    @DisplayName("상태 수집이 조회 범위를 넘게 밀렸으면 전체 동기화로 따라잡는다")
    void catchesUpWithFullSyncWhenCollectionFellBehind() throws Exception {
        given(statusCollector.collectOnce()).willReturn(new StatusCollectResult(10, 0, 0, true));
        given(stationSyncService.syncAll()).willReturn(syncResult(true));

        scheduler.collectStatus();

        verify(stationSyncService).syncAll();
    }

    @Test
    @DisplayName("동기화가 실패하면 지수 백오프로 재시도하고 성공하면 해제한다")
    void retriesWithExponentialBackoffAndClearsOnSuccess() throws Exception {
        given(stationSyncService.syncAll()).willThrow(new PublicApiException("getChargerInfo", "05", "504"));
        scheduler.runStationSync("테스트"); // 1회 실패 → 10분 뒤
        assertEquals(clock.instant().plus(Duration.ofMinutes(10)), scheduler.getStationSyncRetryAt());

        clock.advance(Duration.ofMinutes(5));
        scheduler.collectStatus();
        verify(stationSyncService, times(1)).syncAll(); // 아직 재시도 시각 전

        clock.advance(Duration.ofMinutes(5));
        scheduler.collectStatus(); // 2회 실패 → 20분 뒤
        verify(stationSyncService, times(2)).syncAll();
        assertEquals(clock.instant().plus(Duration.ofMinutes(20)), scheduler.getStationSyncRetryAt());

        reset(stationSyncService);
        given(stationSyncService.syncAll()).willReturn(syncResult(true));
        clock.advance(Duration.ofMinutes(20));
        scheduler.collectStatus();
        verify(stationSyncService).syncAll();
        assertNull(scheduler.getStationSyncRetryAt());
    }

    @Test
    @DisplayName("일부 페이지만 실패해도 재시도를 예약한다")
    void schedulesRetryWhenSomePagesFail() throws Exception {
        given(stationSyncService.syncAll()).willReturn(syncResult(false));

        scheduler.runStationSync("테스트");

        assertNotNull(scheduler.getStationSyncRetryAt());
    }

    @Test
    @DisplayName("인증키 오류처럼 재시도해도 나아지지 않는 실패는 재시도하지 않는다")
    void doesNotRetryNonRetryableFailures() throws Exception {
        given(stationSyncService.syncAll())
                .willThrow(new PublicApiException("getChargerInfo", "30", "SERVICE_KEY_IS_NOT_REGISTERED_ERROR", null, false));

        scheduler.runStationSync("테스트");
        clock.advance(Duration.ofHours(1));
        scheduler.collectStatus();

        assertNull(scheduler.getStationSyncRetryAt());
        verify(stationSyncService, times(1)).syncAll();
    }

    @Test
    @DisplayName("인증키가 없으면 수집하지 않는다")
    void skipsCollectionWithoutServiceKey() throws Exception {
        given(apiClient.hasServiceKey()).willReturn(false);

        scheduler.collectStatus();

        verify(statusCollector, never()).collectOnce();
    }

    @Test
    @DisplayName("재시도 간격은 두 배씩 늘어나고 4시간을 넘지 않는다")
    void retryDelayDoublesUpToFourHours() {
        assertEquals(
                List.of(10L, 20L, 40L, 80L, 160L, 240L, 240L),
                IntStream.rangeClosed(1, 7).mapToObj(n -> CollectorScheduler.retryDelay(n).toMinutes()).toList());
    }

    private static StationSyncResult syncResult(boolean complete) {
        return new StationSyncResult(
                1, // totalCount
                1, // received
                1, // uniqueChargers
                1, // stations
                1, // chargers
                0, // removedInSource
                0, // skipped
                1, // pages
                complete ? List.of() : List.of(1), // failedPages
                null, // cleanup
                null); // cleanupSkippedReason
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
}
