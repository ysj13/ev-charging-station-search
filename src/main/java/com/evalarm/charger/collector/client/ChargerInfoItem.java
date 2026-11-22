package com.evalarm.charger.collector.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.jspecify.annotations.Nullable;

/** getChargerInfo 응답 항목. 충전기 1대당 1건이며 충전소 정보가 함께 들어 있다. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ChargerInfoItem(
        @Nullable String statNm,
        @Nullable String statId,
        @Nullable String chgerId,
        @Nullable String chgerType,
        @Nullable String addr,
        @Nullable String addrDetail,
        @Nullable String location,
        @Nullable String lat,
        @Nullable String lng,
        @Nullable String useTime,
        @Nullable String busiId,
        @Nullable String busiNm,
        @Nullable String busiCall,
        @Nullable String stat,
        @Nullable String statUpdDt,
        @Nullable String output,
        @Nullable String zcode,
        @Nullable String zscode,
        @Nullable String parkingFree,
        @Nullable String limitYn,
        @Nullable String limitDetail,
        @Nullable String delYn) {
}
