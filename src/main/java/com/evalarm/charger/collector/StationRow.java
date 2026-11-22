package com.evalarm.charger.collector;

import org.jspecify.annotations.Nullable;

public record StationRow(
        String stationId,
        String name,
        String address,
        @Nullable String locationDetail,
        double latitude,
        double longitude,
        @Nullable String useTime,
        @Nullable String operatorId,
        @Nullable String operatorName,
        @Nullable String operatorCall,
        String zcode,
        @Nullable String zscode,
        @Nullable Boolean parkingFree,
        @Nullable Boolean limited,
        @Nullable String limitDetail) {
}
