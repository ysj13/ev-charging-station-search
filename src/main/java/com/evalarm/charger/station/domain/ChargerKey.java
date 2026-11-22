package com.evalarm.charger.station.domain;

/** 충전기 식별자. 공공데이터의 충전소 ID(statId) + 충전기 ID(chgerId) */
public record ChargerKey(String stationId, String chargerId) {
}
