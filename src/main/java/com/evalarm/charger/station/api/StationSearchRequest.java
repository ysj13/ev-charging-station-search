package com.evalarm.charger.station.api;

import com.evalarm.charger.common.text.Whitespace;
import com.evalarm.charger.station.query.GeoRadius;
import com.evalarm.charger.station.query.StationSearchCriteria;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.jspecify.annotations.Nullable;

/**
 * GET /api/stations 쿼리 파라미터.
 *
 * <p>쿼리 파라미터는 정규 생성자로 바인딩한다. availableOnly, page, size는 파라미터가 없으면(null) 기본값을 쓴다.
 */
public record StationSearchRequest(
        /** 충전소 이름·주소 부분 일치 */
        @Size(max = 50, message = "검색어는 50자 이하여야 합니다")
        @Nullable String q,

        /** 시군구 코드 5자리 (예: 11680 = 강남구) */
        @Pattern(regexp = "\\d{5}", message = "시군구 코드는 숫자 5자리여야 합니다")
        @Nullable String zscode,

        @DecimalMin(value = "33.0", message = "위도는 대한민국 범위(33~39)여야 합니다")
        @DecimalMax(value = "39.0", message = "위도는 대한민국 범위(33~39)여야 합니다")
        @Nullable Double lat,

        @DecimalMin(value = "124.0", message = "경도는 대한민국 범위(124~132)여야 합니다")
        @DecimalMax(value = "132.0", message = "경도는 대한민국 범위(124~132)여야 합니다")
        @Nullable Double lng,

        /** 반경(m). 좌표를 주면 기본 1km */
        @Min(value = 100, message = "반경은 100m 이상이어야 합니다")
        @Max(value = 10_000, message = "반경은 10km 이하여야 합니다")
        @Nullable Integer radius,

        /** 사용 가능한 충전기가 있는 충전소만. 없으면 false */
        Boolean availableOnly,

        /** 이 출력(kW) 이상인 충전기만 (예: 50 → 급속 위주) */
        @Min(value = 1, message = "최소 출력은 1kW 이상이어야 합니다")
        @Max(value = 400, message = "최소 출력은 400kW 이하여야 합니다")
        @Nullable Integer minOutputKw,

        /** 없으면 0 */
        @Min(value = 0, message = "페이지는 0 이상이어야 합니다")
        @Max(value = 1_000, message = "페이지는 1000 이하여야 합니다")
        Integer page,

        /** 없으면 20 */
        @Min(value = 1, message = "페이지 크기는 1 이상이어야 합니다")
        @Max(value = 100, message = "페이지 크기는 100 이하여야 합니다")
        Integer size) {

    public static final int DEFAULT_RADIUS_METERS = 1_000;
    public static final int MIN_KEYWORD_LENGTH = 2;
    public static final int MAX_OFFSET = 10_000;

    public StationSearchRequest {
        if (availableOnly == null) {
            availableOnly = false;
        }
        if (page == null) {
            page = 0;
        }
        if (size == null) {
            size = 20;
        }
    }

    /** 단일 필드로 표현할 수 없는 조건을 검사하고 조회 조건으로 바꾼다 */
    public StationSearchCriteria toCriteria() {
        if ((lat == null) != (lng == null)) {
            throw new InvalidSearchRequestException("위도(lat)와 경도(lng)는 함께 지정해야 합니다");
        }
        if (radius != null && lat == null) {
            throw new InvalidSearchRequestException("반경(radius)은 위도·경도와 함께 지정해야 합니다");
        }
        // 길이는 앞뒤 공백을 뺀 뒤에 검사한다 (" a"처럼 공백으로 최소 길이를 우회하는 요청 방지)
        String keyword = q != null ? Whitespace.trim(q) : null;
        if (keyword != null && keyword.isEmpty()) {
            keyword = null;
        }
        if (keyword != null && keyword.length() < MIN_KEYWORD_LENGTH) {
            throw new InvalidSearchRequestException("검색어는 공백을 제외하고 " + MIN_KEYWORD_LENGTH + "자 이상이어야 합니다");
        }
        // 이 페이지의 마지막 결과 위치까지 1만 건 안에 들어와야 한다
        if (((long) page + 1) * size > MAX_OFFSET) {
            // 깊은 페이지는 앞의 결과를 모두 정렬·계산해야 해 비싸다. 조건을 좁혀 검색하도록 안내한다
            throw new InvalidSearchRequestException("결과의 앞 " + MAX_OFFSET + "건까지만 조회할 수 있습니다. 검색 조건을 좁혀 주세요");
        }
        if (keyword == null && zscode == null && lat == null) {
            // 서울만 1.3만 곳이라 조건 없이 전체를 훑는 요청은 받지 않는다
            throw new InvalidSearchRequestException("검색어(q), 시군구 코드(zscode), 좌표(lat, lng) 중 하나는 지정해야 합니다");
        }
        GeoRadius geo = lat != null && lng != null
                ? new GeoRadius(lat, lng, radius != null ? radius : DEFAULT_RADIUS_METERS)
                : null;
        return new StationSearchCriteria(keyword, zscode, geo, availableOnly, minOutputKw, page, size);
    }
}
