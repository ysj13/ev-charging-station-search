package com.evalarm.charger.subscription.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.jspecify.annotations.Nullable;

public record SubscribeRequest(
        @NotBlank(message = "충전소 ID가 필요합니다")
        @Size(max = 20, message = "충전소 ID는 20자 이하입니다")
        @Nullable String stationId) {
}
