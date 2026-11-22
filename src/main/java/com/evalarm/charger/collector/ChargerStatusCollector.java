package com.evalarm.charger.collector;

import static com.evalarm.charger.collector.client.PublicApiValues.blankToNull;
import static com.evalarm.charger.collector.client.PublicApiValues.parseDateTime;

import com.evalarm.charger.collector.client.ChargerStatusItem;
import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiPage;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * getChargerStatus로 최근 상태가 바뀐 충전기를 받아 Kafka {@code charger.status} 토픽에 발행한다.
 * <ul>
 *   <li>조회 기간(period)을 수집 간격보다 길게 잡으므로 같은 상태가 여러 번 발행될 수 있다.
 *   이 중복은 다음 단계(빈자리 감지)에서 직전 상태와 비교해 걸러낸다.</li>
 *   <li>마지막 성공 시각을 기억해, 장애로 수집이 밀리면 그 시점부터 다시 조회한다.
 *   API는 최근 약 10분까지만 조회할 수 있어, 그보다 길게 밀렸으면 needsCatchUp을 알려
 *   전체 동기화(모든 충전기 상태를 SNAPSHOT으로 발행)로 따라잡게 한다.</li>
 *   <li>재기동하면 마지막 성공 시각이 사라지지만, 기동 시 전체 동기화가 같은 방식으로 따라잡는다.</li>
 * </ul>
 */
@Service
public class ChargerStatusCollector {

    private static final Logger log = LoggerFactory.getLogger(ChargerStatusCollector.class);

    private static final long SEND_TIMEOUT_SECONDS = 30L;
    private static final int MARGIN_MINUTES = 2;

    private final EvChargerApiClient apiClient;
    private final KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate;
    private final AppProperties properties;
    private final Clock clock;

    private volatile @Nullable Instant lastSuccessAt;

    public ChargerStatusCollector(
            EvChargerApiClient apiClient,
            KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate,
            AppProperties properties,
            Clock clock) {
        this.apiClient = apiClient;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    public StatusCollectResult collectOnce() throws InterruptedException, ExecutionException, TimeoutException {
        Instant collectedAt = clock.instant();
        int neededMinutes = minutesSinceLastSuccess(collectedAt);
        int periodMinutes = coerceIn(
                neededMinutes,
                properties.collector().statusPeriodMinutes(),
                properties.collector().statusMaxPeriodMinutes());
        boolean needsCatchUp = neededMinutes > properties.collector().statusMaxPeriodMinutes();

        int pageSize = properties.collector().pageSize();
        String topic = properties.kafka().topics().chargerStatus().name();
        int pageNo = 1;
        int received = 0;
        List<CompletableFuture<?>> sends = new ArrayList<>();

        while (true) {
            PublicApiPage<ChargerStatusItem> page = apiClient.fetchChargerStatus(pageNo, pageSize, periodMinutes);
            received += page.items().size();
            List<ChargerStatusMessage> messages = new ArrayList<>();
            for (ChargerStatusItem item : page.items()) {
                ChargerStatusMessage message = toMessage(item, collectedAt);
                if (message != null) {
                    messages.add(message);
                }
            }
            for (ChargerStatusMessage message : messages) {
                // 키 = 충전소 ID: 같은 충전소의 상태는 같은 파티션에 순서대로 쌓인다
                sends.add(kafkaTemplate.send(topic, message.stationId(), message));
            }
            if (page.items().size() < pageSize || (long) pageNo * pageSize >= page.totalCount()) {
                break;
            }
            pageNo++;
        }

        // 브로커 응답까지 기다린다. 발행이 실패하면 예외가 나고 lastSuccessAt이 그대로라 다음 주기에 같은 구간을 다시 조회한다
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        lastSuccessAt = collectedAt;

        if (needsCatchUp) {
            log.warn("마지막 성공 수집 후 {}분이 지나 상태 조회 범위를 넘었습니다. 전체 동기화로 따라잡아야 합니다.", neededMinutes);
        }
        log.info("충전기 상태 수집: 조회 {}분, 수신 {}건, 발행 {}건", periodMinutes, received, sends.size());
        return new StatusCollectResult(periodMinutes, received, sends.size(), needsCatchUp);
    }

    /** 마지막 성공 이후 지난 시간 + 여유분(분). 첫 수집이면 기본 조회 기간 */
    private int minutesSinceLastSuccess(Instant now) {
        Instant last = lastSuccessAt;
        if (last == null) {
            return properties.collector().statusPeriodMinutes();
        }
        long elapsedSeconds = Math.max(Duration.between(last, now).toSeconds(), 0L);
        return (int) ((elapsedSeconds + 59) / 60) + MARGIN_MINUTES;
    }

    private static @Nullable ChargerStatusMessage toMessage(ChargerStatusItem item, Instant collectedAt) {
        String stationId = blankToNull(item.statId());
        if (stationId == null) {
            return null;
        }
        String chargerId = blankToNull(item.chgerId());
        if (chargerId == null) {
            return null;
        }
        return new ChargerStatusMessage(
                stationId,
                chargerId,
                ChargerStatus.fromCode(item.stat()),
                parseDateTime(item.statUpdDt()),
                collectedAt,
                StatusSource.CHANGE);
    }

    /** value를 [minimum, maximum] 범위로 맞춘다. 범위가 비어 있으면(최소 &gt; 최대) 설정 오류로 본다 */
    private static int coerceIn(int value, int minimum, int maximum) {
        if (minimum > maximum) {
            throw new IllegalArgumentException(
                    "Cannot coerce value to an empty range: maximum " + maximum + " is less than minimum " + minimum + ".");
        }
        return Math.min(Math.max(value, minimum), maximum);
    }
}
