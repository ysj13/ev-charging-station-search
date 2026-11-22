package com.evalarm.charger.common.messaging;

import java.time.Instant;
import java.util.Objects;

/**
 * RabbitMQ 알림 발송 작업. 이벤트 1건이 구독자 수만큼의 작업으로 나뉜다.
 * notificationId(notification_log.id)로 발송 상태를 기록하고, 이미 보낸 작업이 다시 오면 건너뛴다.
 */
public record NotificationCommand(
        long notificationId,
        String eventId,
        long subscriptionId,
        String subscriberClientKey,
        String stationId,
        String stationName,
        String chargerId,
        Instant occurredAt) {

    public NotificationCommand {
        // 필수 값이 빠진 작업은 역직렬화 단계에서 실패시킨다 (재시도 뒤 DLQ)
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(subscriberClientKey, "subscriberClientKey");
        Objects.requireNonNull(stationId, "stationId");
        Objects.requireNonNull(stationName, "stationName");
        Objects.requireNonNull(chargerId, "chargerId");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }
}
