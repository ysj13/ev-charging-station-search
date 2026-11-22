package com.evalarm.charger.common.messaging;

import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Kafka {@code charger.status} 토픽 메시지. 수집한 충전기 상태.
 * 키: stationId
 *
 * <p>같은 충전기의 상태가 CHANGE와 SNAPSHOT으로 중복되거나 순서가 뒤바뀌어 올 수 있다.
 * 소비자는 statusUpdatedAt이 직전 값보다 최신일 때만 반영해야 한다.
 *
 * @param statusUpdatedAt 공공데이터의 상태갱신일시(statUpdDt)
 * @param collectedAt 수집 시각
 * @param source 출처. 없으면 CHANGE
 */
public record ChargerStatusMessage(
        String stationId,
        String chargerId,
        ChargerStatus status,
        @Nullable Instant statusUpdatedAt,
        Instant collectedAt,
        StatusSource source) {

    public ChargerStatusMessage {
        // 필수 값이 빠진 메시지는 역직렬화 단계에서 실패시킨다. ErrorHandlingDeserializer가 실패를 기록해 두면
        // 배치 리스너는 null 값으로 받아 건너뛰고, 단건 리스너는 컨테이너가 재시도 없이 오류 로그를 남기고 건너뛴다.
        // 그래서 깨진 메시지 하나가 처리 중 예외로 파티션을 계속 막지 않는다.
        // source는 없거나 null이면 CHANGE로 본다
        Objects.requireNonNull(stationId, "stationId");
        Objects.requireNonNull(chargerId, "chargerId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(collectedAt, "collectedAt");
        if (source == null) {
            source = StatusSource.CHANGE;
        }
    }

    public ChargerStatusMessage(
            String stationId, String chargerId, ChargerStatus status, @Nullable Instant statusUpdatedAt, Instant collectedAt) {
        this(stationId, chargerId, status, statusUpdatedAt, collectedAt, StatusSource.CHANGE);
    }
}
