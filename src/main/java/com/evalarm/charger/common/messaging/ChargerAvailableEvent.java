package com.evalarm.charger.common.messaging;

import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import java.util.Objects;

/**
 * Kafka {@code charger.available} 토픽 메시지. 충전기가 '충전대기'로 바뀐 순간의 도메인 이벤트.
 * 키: stationId
 *
 * @param eventId 멱등 처리용 이벤트 ID. "{stationId}:{chargerId}:{statusUpdatedAt epochMilli}"
 * @param source 이 빈자리를 만든 상태 메시지의 출처. MANUAL이면 실제 데이터가 아니므로 알림 등에서 거를 수 있다. 없으면 CHANGE
 */
public record ChargerAvailableEvent(
        String eventId,
        String stationId,
        String chargerId,
        ChargerStatus previousStatus,
        Instant occurredAt,
        StatusSource source) {

    public ChargerAvailableEvent {
        // 필수 값이 빠진 메시지는 역직렬화 단계에서 실패시킨다. ErrorHandlingDeserializer가 실패를 기록해 두면
        // 배치 리스너는 null 값으로 받아 건너뛰고, 단건 리스너는 컨테이너가 재시도 없이 오류 로그를 남기고 건너뛴다.
        // 그래서 깨진 메시지 하나가 처리 중 예외로 파티션을 계속 막지 않는다.
        // source는 없거나 null이면 CHANGE로 본다
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(stationId, "stationId");
        Objects.requireNonNull(chargerId, "chargerId");
        Objects.requireNonNull(previousStatus, "previousStatus");
        Objects.requireNonNull(occurredAt, "occurredAt");
        if (source == null) {
            source = StatusSource.CHANGE;
        }
    }

    public ChargerAvailableEvent(
            String eventId, String stationId, String chargerId, ChargerStatus previousStatus, Instant occurredAt) {
        this(eventId, stationId, chargerId, previousStatus, occurredAt, StatusSource.CHANGE);
    }

    public static String eventIdOf(String stationId, String chargerId, Instant statusUpdatedAt) {
        return stationId + ":" + chargerId + ":" + statusUpdatedAt.toEpochMilli();
    }
}
