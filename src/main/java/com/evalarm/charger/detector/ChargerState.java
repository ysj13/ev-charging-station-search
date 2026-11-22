package com.evalarm.charger.detector;

import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;

/** 충전기의 마지막으로 반영한 상태 */
public record ChargerState(ChargerStatus status, Instant statusUpdatedAt) {
}
