package com.evalarm.charger.station.domain;

import com.evalarm.charger.common.text.Whitespace;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * 공공데이터 API의 충전기 상태 코드(stat).
 *
 * <p>실제 응답에서 확인한 내용: 3인 충전기에만 현재 충전 시작 시각(nowTsdt)이 있다.
 * 나머지 코드의 의미는 한국환경공단 OpenAPI 활용가이드의 코드표를 따른다.
 */
public enum ChargerStatus {
    COMMUNICATION_ERROR("1", "통신이상"),
    AVAILABLE("2", "충전대기"),
    CHARGING("3", "충전중"),
    SUSPENDED("4", "운영중지"),
    MAINTENANCE("5", "점검중"),
    UNKNOWN("9", "상태미확인");

    private static final Map<String, ChargerStatus> BY_CODE = new HashMap<>();

    static {
        for (ChargerStatus status : values()) {
            BY_CODE.put(status.code, status);
        }
    }

    private final String code;
    private final String description;

    ChargerStatus(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static ChargerStatus fromCode(@Nullable String code) {
        if (code == null) {
            return UNKNOWN;
        }
        ChargerStatus status = BY_CODE.get(Whitespace.trim(code));
        return status != null ? status : UNKNOWN;
    }
}
