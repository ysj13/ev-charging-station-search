package com.evalarm.charger.detector;

import static com.evalarm.charger.station.domain.ChargerStatus.AVAILABLE;
import static com.evalarm.charger.station.domain.ChargerStatus.CHARGING;
import static com.evalarm.charger.station.domain.ChargerStatus.COMMUNICATION_ERROR;
import static com.evalarm.charger.station.domain.ChargerStatus.MAINTENANCE;
import static com.evalarm.charger.station.domain.ChargerStatus.SUSPENDED;
import static com.evalarm.charger.station.domain.ChargerStatus.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.station.domain.ChargerKey;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class AvailabilityTransitionDetectorTest {

    private final AvailabilityTransitionDetector detector = new AvailabilityTransitionDetector();
    private final ChargerKey key = new ChargerKey("ST001", "01");
    private final Instant t0 = Instant.parse("2026-11-21T12:00:00Z");

    @Test
    @DisplayName("충전중에서 충전대기로 바뀌면 빈자리 이벤트를 만든다")
    void chargingToAvailableCreatesVacancyEvent() {
        DetectionResult result = detector.detect(Map.of(key, state(CHARGING, 0)), List.of(message(AVAILABLE, 60)));

        assertEquals(1, result.events().size());
        ChargerAvailableEvent event = result.events().getFirst();
        assertEquals("ST001", event.stationId());
        assertEquals("01", event.chargerId());
        assertEquals(CHARGING, event.previousStatus());
        assertEquals(t0.plusSeconds(60), event.occurredAt());
        assertEquals("ST001:01:" + t0.plusSeconds(60).toEpochMilli(), event.eventId());
        assertEquals(state(AVAILABLE, 60), result.changed().get(key));
    }

    @ParameterizedTest
    @EnumSource(value = ChargerStatus.class, names = {"AVAILABLE", "COMMUNICATION_ERROR", "UNKNOWN", "MAINTENANCE", "SUSPENDED"})
    @DisplayName("충전중이 아니던 충전기가 충전대기가 된 것은 빈자리가 아니다")
    void availableFromNonChargingIsNotVacancy(ChargerStatus previous) {
        DetectionResult result = detector.detect(Map.of(key, state(previous, 0)), List.of(message(AVAILABLE, 60)));

        assertTrue(result.events().isEmpty());
        assertEquals(state(AVAILABLE, 60), result.changed().get(key)); // 상태는 반영한다
    }

    @Test
    @DisplayName("너무 오래전에 일어난 빈자리는 상태만 반영하고 이벤트는 만들지 않는다")
    void staleVacancyUpdatesStateWithoutEvent() {
        DetectionResult result = detector.detect(
                Map.of(key, state(CHARGING, 0)),
                List.of(message(AVAILABLE, 60)),
                t0.plusSeconds(61));

        assertTrue(result.events().isEmpty());
        assertEquals(1, result.staleVacancies());
        assertEquals(state(AVAILABLE, 60), result.changed().get(key));
    }

    @Test
    @DisplayName("기준 시각과 같거나 이후의 빈자리는 이벤트를 만든다")
    void vacancyAtOrAfterFreshSinceCreatesEvent() {
        DetectionResult result = detector.detect(
                Map.of(key, state(CHARGING, 0)),
                List.of(message(AVAILABLE, 60)),
                t0.plusSeconds(60));

        assertEquals(1, result.events().size());
        assertEquals(0, result.staleVacancies());
    }

    @Test
    @DisplayName("처음 보는 충전기는 기준 상태로만 기록한다")
    void unknownChargerIsRecordedAsBaseline() {
        DetectionResult result = detector.detect(Map.of(), List.of(message(AVAILABLE, 60)));

        assertTrue(result.events().isEmpty());
        assertEquals(state(AVAILABLE, 60), result.changed().get(key));
    }

    @Test
    @DisplayName("직전보다 오래됐거나 같은 시각의 메시지는 무시한다")
    void ignoresOlderOrSameTimeMessages() {
        Map<ChargerKey, ChargerState> current = Map.of(key, state(CHARGING, 60));
        DetectionResult result = detector.detect(current, List.of(message(AVAILABLE, 30), message(AVAILABLE, 60)));

        assertTrue(result.events().isEmpty());
        assertTrue(result.changed().isEmpty());
        assertEquals(2, result.ignored());
    }

    @Test
    @DisplayName("갱신 시각이 없는 메시지는 순서를 판단할 수 없어 무시한다")
    void ignoresMessagesWithoutUpdatedAt() {
        DetectionResult result = detector.detect(
                Map.of(key, state(CHARGING, 0)),
                List.of(new ChargerStatusMessage("ST001", "01", AVAILABLE, null, t0)));

        assertTrue(result.events().isEmpty());
        assertEquals(1, result.ignored());
    }

    @Test
    @DisplayName("한 배치 안의 여러 메시지를 순서대로 이어서 판단한다")
    void evaluatesMessagesInBatchSequentially() {
        // 충전중 → 대기(빈자리) → 충전중 → 대기(빈자리), 중간에 중복 SNAPSHOT 하나
        DetectionResult result = detector.detect(
                Map.of(key, state(CHARGING, 0)),
                List.of(
                        message(AVAILABLE, 60),
                        message(CHARGING, 120),
                        message(CHARGING, 120, StatusSource.SNAPSHOT),
                        message(AVAILABLE, 180)));

        assertEquals(
                List.of(60L, 180L),
                result.events().stream().map(it -> it.occurredAt().getEpochSecond() - t0.getEpochSecond()).toList());
        assertEquals(state(AVAILABLE, 180), result.changed().get(key));
        assertEquals(1, result.ignored());
    }

    @Test
    @DisplayName("늦게 도착한 오래된 SNAPSHOT이 최신 상태를 되돌리지 않는다")
    void lateOldSnapshotDoesNotRevertNewerState() {
        DetectionResult result = detector.detect(
                Map.of(key, state(CHARGING, 0)),
                List.of(message(AVAILABLE, 120), message(CHARGING, 60, StatusSource.SNAPSHOT)));

        assertEquals(1, result.events().size());
        assertEquals(state(AVAILABLE, 120), result.changed().get(key));
    }

    @Test
    @DisplayName("충전기마다 따로 판단한다")
    void evaluatesEachChargerSeparately() {
        ChargerKey other = new ChargerKey("ST001", "02");
        DetectionResult result = detector.detect(
                Map.of(key, state(CHARGING, 0), other, state(AVAILABLE, 0)),
                List.of(message(AVAILABLE, 60), message(CHARGING, 60, StatusSource.CHANGE, "02")));

        assertEquals(List.of("01"), result.events().stream().map(ChargerAvailableEvent::chargerId).toList());
        assertEquals(state(CHARGING, 60), result.changed().get(other));
    }

    @Test
    @DisplayName("통신이상 상태도 반영은 한다")
    void communicationErrorIsStillApplied() {
        DetectionResult result =
                detector.detect(Map.of(key, state(CHARGING, 0)), List.of(message(COMMUNICATION_ERROR, 60)));
        assertEquals(state(COMMUNICATION_ERROR, 60), result.changed().get(key));
        assertTrue(result.events().isEmpty());
        // 운영 재개(점검·운영중지 해제)는 빈자리가 아니다
        assertTrue(!AvailabilityTransitionDetector.isVacancy(UNKNOWN, CHARGING));
        assertTrue(!AvailabilityTransitionDetector.isVacancy(MAINTENANCE, AVAILABLE));
        assertTrue(!AvailabilityTransitionDetector.isVacancy(SUSPENDED, AVAILABLE));
        assertTrue(AvailabilityTransitionDetector.isVacancy(CHARGING, AVAILABLE));
    }

    private ChargerState state(ChargerStatus status, long seconds) {
        return new ChargerState(status, t0.plusSeconds(seconds));
    }

    private ChargerStatusMessage message(ChargerStatus status, long seconds) {
        return message(status, seconds, StatusSource.CHANGE, "01");
    }

    private ChargerStatusMessage message(ChargerStatus status, long seconds, StatusSource source) {
        return message(status, seconds, source, "01");
    }

    private ChargerStatusMessage message(ChargerStatus status, long seconds, StatusSource source, String chargerId) {
        return new ChargerStatusMessage(
                "ST001", chargerId, status, t0.plusSeconds(seconds), t0.plusSeconds(seconds), source);
    }
}
