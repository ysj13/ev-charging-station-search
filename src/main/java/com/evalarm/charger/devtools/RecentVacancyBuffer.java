package com.evalarm.charger.devtools;

import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * charger.available을 구독해 최근 빈자리 이벤트를 메모리에 보관한다 (개발·시연용).
 *
 * <p>인스턴스마다 새 컨슈머 그룹(무작위 ID)으로 최신 위치부터 읽는다. 그래서 재기동해도 꺼져 있던 동안의 이벤트를
 * 다시 쌓지 않고, 인스턴스가 여럿이어도 각자 모든 파티션의 이벤트를 받는다.
 * 빈자리 감지 결과를 실제 알림 소비자 없이 눈으로 확인하기 위한 용도다.
 */
@Component
@ConditionalOnProperty(prefix = "app.dev-tools", name = "enabled", havingValue = "true")
public class RecentVacancyBuffer {

    private static final int MAX_SIZE = 50;

    private final ArrayDeque<ChargerAvailableEvent> events = new ArrayDeque<>();

    @KafkaListener(
            id = "dev-tools-recent-vacancies",
            groupId = "dev-tools-recent-vacancies-${random.uuid}",
            topics = "${app.kafka.topics.charger-available.name}",
            properties = "auto.offset.reset=latest")
    public void onVacancy(@Nullable ChargerAvailableEvent event) {
        if (event == null) {
            return; // 역직렬화 실패
        }
        synchronized (events) {
            // 같은 이벤트가 다시 발행될 수 있어(at-least-once) eventId로 거른다
            for (ChargerAvailableEvent existing : events) {
                if (existing.eventId().equals(event.eventId())) {
                    return;
                }
            }
            events.addFirst(event);
            while (events.size() > MAX_SIZE) {
                events.removeLast();
            }
        }
    }

    public List<ChargerAvailableEvent> snapshot() {
        synchronized (events) {
            return new ArrayList<>(events);
        }
    }
}
