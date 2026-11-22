package com.evalarm.charger.notification;

import com.evalarm.charger.stream.VacancyBroadcaster;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * 알림을 모든 앱 인스턴스에 퍼뜨리는 Redis Pub/Sub 채널.
 * 보낼 때는 채널에 올리고, 받을 때는 이 인스턴스에 연결된 해당 구독자의 실시간 연결로 넘긴다.
 * Pub/Sub은 저장하지 않으므로 그 순간 연결이 없으면 실시간 전달은 사라진다(기록은 DB의 알림함에 남는다).
 */
@Component
public class NotificationPushChannel implements MessageListener {

    public static final String CHANNEL = "notification.push";

    private static final Logger log = LoggerFactory.getLogger(NotificationPushChannel.class);

    private final StringRedisTemplate redis;
    private final JsonMapper jsonMapper;
    private final VacancyBroadcaster broadcaster;
    private final Counter deliveredCounter;

    public NotificationPushChannel(
            StringRedisTemplate redis, JsonMapper jsonMapper, VacancyBroadcaster broadcaster, MeterRegistry meterRegistry) {
        this.redis = redis;
        this.jsonMapper = jsonMapper;
        this.broadcaster = broadcaster;
        this.deliveredCounter = Counter.builder("notification.push.delivered").register(meterRegistry);
    }

    public void publish(NotificationPush push) {
        redis.convertAndSend(CHANNEL, jsonMapper.writeValueAsString(push));
    }

    @Override
    public void onMessage(Message message, byte @Nullable [] pattern) {
        NotificationPush push;
        try {
            push = jsonMapper.readValue(message.getBody(), NotificationPush.class);
        } catch (RuntimeException e) {
            log.warn("읽을 수 없는 알림 메시지를 건너뜁니다: {}", e.getMessage());
            return;
        }
        if (push == null) { // 본문이 JSON null
            log.warn("빈 알림 메시지를 건너뜁니다");
            return;
        }
        int delivered = broadcaster.notifyClient(push.clientKey(), push.notification());
        deliveredCounter.increment(delivered);
    }
}
