package com.evalarm.charger.notification;

import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** 알림함에 보여 줄 알림 한 건 */
public record NotificationView(
        long notificationId,
        String eventId,
        String stationId,
        String stationName,
        String chargerId,
        @Nullable Instant occurredAt,
        @Nullable Instant sentAt) {

    public NotificationView {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(stationId, "stationId");
        Objects.requireNonNull(stationName, "stationName");
        Objects.requireNonNull(chargerId, "chargerId");
    }
}
