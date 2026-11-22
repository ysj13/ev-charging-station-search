package com.evalarm.charger.detector;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * 최근에 발행한 빈자리 이벤트 ID를 기억한다.
 *
 * <p>이벤트 발행 뒤 DB나 Redis 갱신이 실패하면 배치 전체가 다시 처리되면서 같은 이벤트가 또 감지된다.
 * 장애가 길어지면 재시도마다(최대 1분 간격) 같은 이벤트가 계속 발행되므로, 이미 발행한 것은 건너뛴다.
 *
 * <p>메모리에만 두므로 재기동하면 사라진다. 그 경우의 중복은 소비자가 eventId로 거른다(at-least-once).
 */
@Component
public class PublishedEventCache {

    /** 서울 기준 빈자리는 시간당 수백 건 수준이라, 하루 이상 분량을 넉넉히 담는다 */
    private static final int MAX_SIZE = 50_000;

    private final Map<String, Boolean> ids = new LinkedHashMap<>(MAX_SIZE, 0.75f) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > MAX_SIZE;
        }
    };

    public synchronized boolean contains(String eventId) {
        return ids.containsKey(eventId);
    }

    public synchronized void addAll(Collection<String> eventIds) {
        for (String eventId : eventIds) {
            ids.put(eventId, true);
        }
    }
}
