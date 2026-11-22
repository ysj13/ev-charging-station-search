package com.evalarm.charger.devtools;

import java.time.Instant;

public record TestVacancyResponse(
        String stationId,
        String chargerId,
        /** 만든 빈자리의 상태갱신일시 */
        Instant occurredAt) {
}
