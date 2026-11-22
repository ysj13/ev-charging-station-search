package com.evalarm.charger.notification;

import java.util.Objects;

/** Redis Pub/Sub로 인스턴스 사이에 전달하는 알림. clientKey는 받을 브라우저를 고르는 데만 쓰고 화면에는 보내지 않는다 */
public record NotificationPush(String clientKey, NotificationView notification) {

    public NotificationPush {
        // 받을 사람이 없는 알림이 엉뚱한 연결로 가지 않도록, 필수 값이 빠진 메시지는 읽는 단계에서 실패시킨다
        Objects.requireNonNull(clientKey, "clientKey");
        Objects.requireNonNull(notification, "notification");
    }
}
