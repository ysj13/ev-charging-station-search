package com.evalarm.charger.notification.api;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.notification.NotificationRepository;
import com.evalarm.charger.subscription.ClientKeys;
import org.jspecify.annotations.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NotificationController {

    private final NotificationRepository repository;
    private final AppProperties properties;

    public NotificationController(NotificationRepository repository, AppProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    /** 알림함: 이 구독자에게 보낸 최근 알림 (최신 순) */
    @GetMapping("/api/notifications")
    public NotificationList recent(
            @RequestHeader(value = ClientKeys.HEADER, required = false) @Nullable String clientKey) {
        return new NotificationList(
                repository.findRecentSent(ClientKeys.require(clientKey), properties.notification().inboxSize()));
    }
}
