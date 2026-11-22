package com.evalarm.charger.subscription;

import java.time.Instant;

/** 구독 화면에 보여 줄 구독 한 건 */
public record SubscriptionView(
        String stationId,
        String stationName,
        String address,
        /** 공공데이터에서 사라진 충전소면 false. 알림은 더 오지 않는다 */
        boolean stationActive,
        Instant subscribedAt) {
}
