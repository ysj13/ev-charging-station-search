package com.evalarm.charger.devtools;

import com.evalarm.charger.station.domain.ChargerStatus;
import jakarta.validation.constraints.NotNull;
import org.jspecify.annotations.Nullable;

public record InjectStatusRequest(
        @NotNull(message = "status는 필수입니다")
        @Nullable ChargerStatus status) {
}
