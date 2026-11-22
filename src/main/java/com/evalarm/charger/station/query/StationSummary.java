package com.evalarm.charger.station.query;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

public record StationSummary(
        String stationId,
        String name,
        String address,
        double latitude,
        double longitude,
        /** 반경 검색일 때만 */
        @Nullable Integer distanceMeters,
        @Nullable String operatorName,
        @Nullable String useTime,
        @Nullable Boolean parkingFree,
        int chargerCount,
        int availableCount,
        int chargingCount,
        @Nullable Integer maxOutputKw,
        /** 이 충전소 충전기 중 가장 최근 상태 갱신 시각 */
        @Nullable Instant statusUpdatedAt) {
}
