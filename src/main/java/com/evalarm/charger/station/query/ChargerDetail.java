package com.evalarm.charger.station.query;

import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

public record ChargerDetail(
        String chargerId,
        @Nullable String chargerType,
        @Nullable Integer outputKw,
        ChargerStatus status,
        String statusDescription,
        @Nullable Instant statusUpdatedAt) {
}
