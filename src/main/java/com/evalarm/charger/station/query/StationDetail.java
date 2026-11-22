package com.evalarm.charger.station.query;

import java.util.List;
import org.jspecify.annotations.Nullable;

public record StationDetail(
        String stationId,
        String name,
        String address,
        @Nullable String locationDetail,
        double latitude,
        double longitude,
        @Nullable String operatorName,
        @Nullable String operatorCall,
        @Nullable String useTime,
        @Nullable Boolean parkingFree,
        @Nullable Boolean limited,
        @Nullable String limitDetail,
        int availableCount,
        List<ChargerDetail> chargers) {
}
