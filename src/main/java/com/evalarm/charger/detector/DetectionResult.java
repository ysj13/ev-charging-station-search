package com.evalarm.charger.detector;

import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.station.domain.ChargerKey;
import java.util.List;
import java.util.Map;

public record DetectionResult(
        /** 이번 배치에서 '빈자리'가 생긴 충전기들 */
        List<ChargerAvailableEvent> events,
        /** 이번 배치로 상태가 갱신된 충전기의 최종 상태 */
        Map<ChargerKey, ChargerState> changed,
        /** 더 오래됐거나 중복이거나 갱신 시각이 없어 무시한 메시지 수 */
        int ignored,
        /** 빈자리 전이는 맞지만 너무 오래전 일이라 이벤트를 만들지 않은 수 (밀린 메시지·과거 기록 재처리) */
        int staleVacancies) {

    public DetectionResult(List<ChargerAvailableEvent> events, Map<ChargerKey, ChargerState> changed, int ignored) {
        this(events, changed, ignored, 0);
    }
}
