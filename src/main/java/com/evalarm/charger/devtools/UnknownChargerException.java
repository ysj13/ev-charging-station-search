package com.evalarm.charger.devtools;

public class UnknownChargerException extends RuntimeException {

    public UnknownChargerException(String stationId, String chargerId) {
        super("충전기를 찾을 수 없습니다: " + stationId + " / " + chargerId);
    }
}
