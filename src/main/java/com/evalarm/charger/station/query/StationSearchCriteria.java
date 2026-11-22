package com.evalarm.charger.station.query;

import org.jspecify.annotations.Nullable;

public record StationSearchCriteria(
        /** 이름·주소 부분 일치 */
        @Nullable String keyword,
        /** 시군구 코드 (예: 11680 = 강남구) */
        @Nullable String zscode,
        @Nullable GeoRadius geo,
        /** 사용 가능한 충전기가 하나 이상 있는 충전소만 */
        boolean availableOnly,
        /** 이 출력(kW) 이상인 충전기만 집계한다. 해당하는 충전기가 없는 충전소는 제외 */
        @Nullable Integer minOutputKw,
        int page,
        int size) {
}
