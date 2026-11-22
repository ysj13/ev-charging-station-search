package com.evalarm.charger.collector;

import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

public record ChargerRow(
        String stationId,
        String chargerId,
        @Nullable String chargerType,
        @Nullable Integer outputKw,
        ChargerStatus status,
        @Nullable Instant statusUpdatedAt) {
}
