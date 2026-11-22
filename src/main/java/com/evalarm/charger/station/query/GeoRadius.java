package com.evalarm.charger.station.query;

/** 좌표 반경 조건 */
public record GeoRadius(double latitude, double longitude, int radiusMeters) {
}
