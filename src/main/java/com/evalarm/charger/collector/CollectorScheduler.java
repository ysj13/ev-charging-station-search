package com.evalarm.charger.collector;

import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 수집 스케줄.
 * <ul>
 *   <li>기동 직후 충전소 전체 동기화 (별도 스레드에서 실행해 기동과 readiness를 막지 않음)</li>
 *   <li>매일 04시(한국 시간) 전체 동기화</li>
 *   <li>짧은 간격으로 상태 수집 (스케줄러 스레드 2개로 전체 동기화와 동시에 실행 가능)</li>
 *   <li>상태 수집이 조회 범위를 넘게 밀렸으면 전체 동기화로 따라잡기</li>
 *   <li>전체 동기화가 실패하면 지수 백오프(10분 → 20분 → … 최대 4시간)로 재시도</li>
 * </ul>
 *
 * <p>공공 API 장애가 스케줄 스레드를 멈추지 않도록 예외는 기록만 하고 다음 주기에 다시 시도한다.
 */
@Component
@ConditionalOnProperty(prefix = "app.collector", name = "enabled", havingValue = "true")
public class CollectorScheduler {

    private static final Logger log = LoggerFactory.getLogger(CollectorScheduler.class);

    private static final Duration SYNC_RETRY_BASE = Duration.ofMinutes(10);
    private static final Duration SYNC_RETRY_MAX = Duration.ofHours(4);

    private final EvChargerApiClient apiClient;
    private final StationSyncService stationSyncService;
    private final ChargerStatusCollector statusCollector;
    private final TaskExecutor taskExecutor;
    private final Clock clock;
    private final AtomicBoolean stationSyncRunning = new AtomicBoolean(false);

    /** 전체 동기화가 실패했을 때 다시 시도할 시각. 성공하거나 재시도할 수 없는 오류면 null */
    private volatile @Nullable Instant stationSyncRetryAt;

    private volatile int consecutiveSyncFailures = 0;

    public CollectorScheduler(
            EvChargerApiClient apiClient,
            StationSyncService stationSyncService,
            ChargerStatusCollector statusCollector,
            @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor,
            Clock clock) {
        this.apiClient = apiClient;
        this.stationSyncService = stationSyncService;
        this.statusCollector = statusCollector;
        this.taskExecutor = taskExecutor;
        this.clock = clock;
    }

    /**
     * 기동할 때마다 동기화한다. 이전 동기화가 중간에 실패했어도 재기동으로 복구되고,
     * 다운타임 동안 놓친 상태 변경도 SNAPSHOT으로 따라잡는다. (서울 기준 API 8회, 약 20초)
     */
    @EventListener(ApplicationReadyEvent.class)
    public void syncOnStartup() {
        if (!apiClient.hasServiceKey()) {
            log.warn("인증키가 없어 수집을 건너뜁니다. .env의 DATA_GO_KR_SERVICE_KEY를 설정하세요.");
            return;
        }
        taskExecutor.execute(() -> runStationSync("기동 시"));
    }

    @Scheduled(cron = "${app.collector.station-sync-cron}", zone = "Asia/Seoul")
    public void scheduledStationSync() {
        if (apiClient.hasServiceKey()) {
            runStationSync("정기");
        }
    }

    @Scheduled(
            fixedDelayString = "${app.collector.status-interval}",
            initialDelayString = "${app.collector.status-interval}")
    public void collectStatus() {
        if (!apiClient.hasServiceKey()) {
            return;
        }
        try {
            StatusCollectResult result = statusCollector.collectOnce();
            // 상태 조회 범위를 넘을 만큼 수집이 밀렸으면 전체 동기화(SNAPSHOT 발행)로 놓친 변경을 따라잡는다
            if (result.needsCatchUp()) {
                taskExecutor.execute(() -> runStationSync("수집 지연 따라잡기"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("종료 중이라 상태 수집을 중단합니다.");
            return;
        } catch (Exception e) {
            log.error("충전기 상태 수집 실패: {}", e.getMessage(), e);
        }

        // 실패했던 전체 동기화는 정기 시각(04시)까지 기다리지 않고 다시 시도한다
        Instant retryAt = stationSyncRetryAt;
        if (retryAt != null && !clock.instant().isBefore(retryAt)) {
            taskExecutor.execute(() -> runStationSync("실패 재시도"));
        }
    }

    void runStationSync(String trigger) {
        // 기동 시·정기·재시도·따라잡기 동기화가 겹치지 않도록 한 번에 하나만 실행한다
        if (!stationSyncRunning.compareAndSet(false, true)) {
            log.info("{} 충전소 동기화를 건너뜁니다: 이미 실행 중", trigger);
            return;
        }
        try {
            StationSyncResult result = stationSyncService.syncAll();
            if (result.isComplete()) {
                markSyncSucceeded();
                log.info("{} 충전소 전체 동기화 완료: {}", trigger, result);
            } else {
                markSyncFailed(true);
                log.error("{} 충전소 동기화 일부 실패, {}에 다시 시도: {}", trigger, stationSyncRetryAt, result);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("{} 충전소 동기화를 종료 신호로 중단합니다.", trigger);
        } catch (Exception e) {
            boolean retryable = e instanceof PublicApiException publicApiException ? publicApiException.isRetryable() : true;
            markSyncFailed(retryable);
            if (retryable) {
                log.error("{} 충전소 전체 동기화 실패, {}에 다시 시도: {}", trigger, stationSyncRetryAt, e.getMessage(), e);
            } else {
                log.error("{} 충전소 전체 동기화 실패(재시도해도 해결되지 않는 오류, 설정 확인 필요): {}", trigger, e.getMessage(), e);
            }
        } finally {
            stationSyncRunning.set(false);
        }
    }

    /** 전체 동기화가 실패했을 때 다시 시도할 시각 (테스트에서 확인) */
    @Nullable Instant getStationSyncRetryAt() {
        return stationSyncRetryAt;
    }

    private void markSyncSucceeded() {
        consecutiveSyncFailures = 0;
        stationSyncRetryAt = null;
    }

    private void markSyncFailed(boolean retryable) {
        if (!retryable) {
            // 인증키 오류 등은 기다려도 나아지지 않는다. 정기 동기화나 재기동 때 다시 시도한다
            stationSyncRetryAt = null;
            return;
        }
        consecutiveSyncFailures++;
        stationSyncRetryAt = clock.instant().plus(retryDelay(consecutiveSyncFailures));
    }

    /** 10분, 20분, 40분, 80분, 160분, 이후 4시간 */
    static Duration retryDelay(int failures) {
        int exponent = Math.min(Math.max(failures - 1, 0), 10);
        Duration delay = SYNC_RETRY_BASE.multipliedBy(1L << exponent);
        return delay.compareTo(SYNC_RETRY_MAX) > 0 ? SYNC_RETRY_MAX : delay;
    }
}
