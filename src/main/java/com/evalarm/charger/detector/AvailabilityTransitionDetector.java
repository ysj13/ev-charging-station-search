package com.evalarm.charger.detector;

import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import com.evalarm.charger.station.domain.ChargerKey;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * 충전기 상태 메시지를 직전 상태와 비교해 '빈자리가 났다'는 전이를 찾는다. 외부 의존이 없는 순수 로직이다.
 * <ul>
 *   <li>같은 충전기의 메시지는 수집 경로(CHANGE/SNAPSHOT)에 따라 중복되거나 순서가 뒤바뀌어 올 수 있으므로,
 *   공공데이터의 상태갱신일시(statusUpdatedAt)가 직전보다 최신인 메시지만 반영한다.</li>
 *   <li>직전 상태를 모르는 충전기(처음 본 충전기)는 기준 상태로만 기록하고 이벤트를 만들지 않는다.</li>
 *   <li>freshSince보다 오래전에 일어난 빈자리는 상태만 반영하고 이벤트는 만들지 않는다.
 *   처음 배포하거나 장애 뒤 밀린 메시지를 다시 읽을 때 이미 지난 빈자리 알림이 쏟아지는 것을 막는다.</li>
 * </ul>
 */
@Component
public class AvailabilityTransitionDetector {

    public DetectionResult detect(Map<ChargerKey, ChargerState> current, List<ChargerStatusMessage> messages) {
        return detect(current, messages, Instant.MIN);
    }

    public DetectionResult detect(
            Map<ChargerKey, ChargerState> current, List<ChargerStatusMessage> messages, Instant freshSince) {
        Map<ChargerKey, ChargerState> working = new HashMap<>(current);
        Map<ChargerKey, ChargerState> changed = new LinkedHashMap<>();
        List<ChargerAvailableEvent> events = new ArrayList<>();
        int ignored = 0;
        int staleVacancies = 0;

        for (ChargerStatusMessage message : messages) {
            Instant updatedAt = message.statusUpdatedAt();
            if (updatedAt == null) {
                ignored++;
                continue;
            }
            ChargerKey key = new ChargerKey(message.stationId(), message.chargerId());
            ChargerState previous = working.get(key);
            if (previous != null && !updatedAt.isAfter(previous.statusUpdatedAt())) {
                ignored++;
                continue;
            }

            if (previous != null && isVacancy(previous.status(), message.status()) && updatedAt.isBefore(freshSince)) {
                staleVacancies++;
            } else if (previous != null && isVacancy(previous.status(), message.status())) {
                events.add(new ChargerAvailableEvent(
                        ChargerAvailableEvent.eventIdOf(key.stationId(), key.chargerId(), updatedAt),
                        key.stationId(),
                        key.chargerId(),
                        previous.status(),
                        updatedAt,
                        message.source()));
            }
            ChargerState next = new ChargerState(message.status(), updatedAt);
            working.put(key, next);
            changed.put(key, next);
        }
        return new DetectionResult(events, changed, ignored, staleVacancies);
    }

    /**
     * 빈자리로 보는 전이: 충전중 → 충전대기 (누군가 충전을 마치고 떠난 경우)만.
     *
     * <p>서울 실데이터(약 1시간, 857건)에서 운영중지·점검중 → 충전대기는 한 운영사가 충전소 단위로
     * 같은 초에 수십 대씩 일괄 전환하거나 정각에 점검이 끝나는 패턴이었다. 이는 자리가 난 것이 아니라
     * 운영 재개이므로 이벤트를 만들지 않는다. 통신이상·상태미확인에서 바뀐 것도 신호가 흔들린 것일 수 있어 제외한다.
     * 어느 경우든 상태 자체는 반영한다.
     */
    public static boolean isVacancy(ChargerStatus previous, ChargerStatus next) {
        return previous == ChargerStatus.CHARGING && next == ChargerStatus.AVAILABLE;
    }
}
