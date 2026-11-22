package com.evalarm.charger.subscription.api;

import com.evalarm.charger.common.text.Whitespace;
import com.evalarm.charger.subscription.ClientKeys;
import com.evalarm.charger.subscription.SubscriptionService;
import jakarta.validation.Valid;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 충전소 빈자리 알림 구독. 구독자는 X-Client-Key 헤더(브라우저가 만든 무작위 값)로 구분한다.
 */
@RestController
@RequestMapping("/api/subscriptions")
public class SubscriptionController {

    private final SubscriptionService service;

    public SubscriptionController(SubscriptionService service) {
        this.service = service;
    }

    /** 구독한 충전소 목록 (최근 구독 순) */
    @GetMapping
    public SubscriptionList list(@RequestHeader(value = ClientKeys.HEADER, required = false) @Nullable String clientKey) {
        return new SubscriptionList(service.list(ClientKeys.require(clientKey)));
    }

    /** 구독한다. 새로 켜면 201, 이미 구독 중이면 200 */
    @PostMapping
    public ResponseEntity<Map<String, String>> subscribe(
            @RequestHeader(value = ClientKeys.HEADER, required = false) @Nullable String clientKey,
            @Valid @RequestBody SubscribeRequest request) {
        String stationId = Whitespace.trim(Objects.requireNonNull(request.stationId()));
        boolean created = service.subscribe(ClientKeys.require(clientKey), stationId);
        return ResponseEntity.status(created ? HttpStatus.CREATED : HttpStatus.OK).body(Map.of("stationId", stationId));
    }

    /** 구독을 해지한다. 구독하지 않은 충전소여도 204 */
    @DeleteMapping("/{stationId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void unsubscribe(
            @RequestHeader(value = ClientKeys.HEADER, required = false) @Nullable String clientKey,
            @PathVariable String stationId) {
        service.unsubscribe(ClientKeys.require(clientKey), stationId);
    }
}
