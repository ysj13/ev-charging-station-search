package com.evalarm.charger.collector;

public record StatusCollectResult(
        int periodMinutes,
        int received,
        int published,
        /** 마지막 성공 후 조회 가능한 범위(약 10분)를 넘겨, 전체 동기화로 따라잡아야 하는지 */
        boolean needsCatchUp) {
}
