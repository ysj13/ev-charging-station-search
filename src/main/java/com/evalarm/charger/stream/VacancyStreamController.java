package com.evalarm.charger.stream;

import com.evalarm.charger.subscription.ClientKeys;
import org.jspecify.annotations.Nullable;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class VacancyStreamController {

    private final VacancyBroadcaster broadcaster;

    public VacancyStreamController(VacancyBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    /**
     * 빈자리 실시간 스트림 (Server-Sent Events).
     * 이벤트: connected(연결 직후 1회), vacancy(빈자리 발생), notification(구독한 충전소의 알림, clientKey를 줬을 때만).
     * 25초마다 주석으로 연결을 유지한다.
     * 브라우저의 EventSource는 헤더를 붙일 수 없어 clientKey는 쿼리 파라미터로 받는다.
     */
    @GetMapping(value = "/api/vacancies/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam(required = false) @Nullable String clientKey) {
        return broadcaster.connect(clientKey != null ? ClientKeys.require(clientKey) : null);
    }
}
