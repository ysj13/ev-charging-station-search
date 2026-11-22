package com.evalarm.charger.station.api;

public class StationNotFoundException extends RuntimeException {

    public StationNotFoundException(String stationId) {
        super("충전소를 찾을 수 없습니다: " + stationId);
    }
}
