package com.evalarm.charger.devtools;

import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;

public record InjectStatusResponse(String stationId, String chargerId, ChargerStatus status, Instant statusUpdatedAt) {
}
