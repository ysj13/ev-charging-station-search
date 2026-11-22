package com.evalarm.charger.collector.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.jspecify.annotations.Nullable;

/** getChargerStatus 응답 항목. period(분) 안에 상태가 갱신된 충전기만 내려온다. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ChargerStatusItem(
        @Nullable String busiId,
        @Nullable String statId,
        @Nullable String chgerId,
        @Nullable String stat,
        @Nullable String statUpdDt,
        @Nullable String nowTsdt) {
}
