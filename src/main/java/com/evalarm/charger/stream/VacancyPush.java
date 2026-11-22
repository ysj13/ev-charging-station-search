package com.evalarm.charger.stream;

import com.evalarm.charger.common.messaging.StatusSource;
import java.time.Instant;

/** 브라우저로 보내는 빈자리 알림 */
public record VacancyPush(
        String eventId,
        String stationId,
        String chargerId,
        Instant occurredAt,
        /** MANUAL이면 개발 도구로 넣은 상태 */
        StatusSource source) {
}
