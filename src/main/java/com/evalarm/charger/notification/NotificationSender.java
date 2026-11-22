package com.evalarm.charger.notification;

import com.evalarm.charger.common.messaging.NotificationCommand;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ 발송 작업을 받아 구독자의 브라우저로 알림을 보낸다.
 * <ul>
 *   <li>보내는 길: Redis Pub/Sub 채널에 올리면, 앱 인스턴스마다 자기에게 연결된 그 구독자의 실시간 연결(SSE)로 전달한다.
 *   구독자가 어느 인스턴스에 연결돼 있든 받을 수 있다. 연결이 없으면 실시간으로는 못 받고 알림함에서 본다.</li>
 *   <li>실패하면(Redis 장애 등) 1초·2초 간격으로 다시 시도하고, 3번 모두 실패하면 FAILED로 기록하고 DLQ로 보낸다
 *   ({@link NotificationConfig}의 최종 실패 처리).</li>
 *   <li>매칭 뒤 발송 전에 구독을 해지했으면 보내지 않고 CANCELLED로 남긴다. 확인과 발송 사이의 아주 짧은 순간에 해지하면
 *   그대로 갈 수 있다(발송이 DB 밖에서 일어나 완전히 막을 수 없음).</li>
 *   <li>이미 보낸 작업이 다시 오면 건너뛴다. 보내고 SENT를 기록하기 전에 멈추면 한 번 더 갈 수 있어(최소 한 번 전달)
 *   화면은 notificationId로 중복을 거른다.</li>
 * </ul>
 */
@Component
public class NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(NotificationSender.class);

    private final NotificationRepository repository;
    private final NotificationPushChannel pushChannel;
    private final Clock clock;
    private final Counter sentCounter;
    private final Counter duplicateCounter;
    private final Counter cancelledCounter;

    public NotificationSender(
            NotificationRepository repository,
            NotificationPushChannel pushChannel,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.pushChannel = pushChannel;
        this.clock = clock;
        this.sentCounter = Counter.builder("notification.sent").register(meterRegistry);
        this.duplicateCounter = Counter.builder("notification.duplicates.skipped").register(meterRegistry);
        this.cancelledCounter = Counter.builder("notification.cancelled").register(meterRegistry);
    }

    @RabbitListener(
            id = "notification-sender",
            queues = "${app.rabbit.queue}",
            autoStartup = "${app.notification.enabled}")
    public void send(NotificationCommand command) {
        if (repository.cancelIfUnsubscribed(command.notificationId())) {
            cancelledCounter.increment();
            return;
        }
        if (!repository.beginAttempt(command.notificationId())) {
            duplicateCounter.increment();
            log.debug("이미 보냈거나 없는 알림이라 건너뜁니다: {}", command.notificationId());
            return;
        }
        Instant now = clock.instant();
        pushChannel.publish(new NotificationPush(
                command.subscriberClientKey(),
                new NotificationView(
                        command.notificationId(),
                        command.eventId(),
                        command.stationId(),
                        command.stationName(),
                        command.chargerId(),
                        command.occurredAt(),
                        now)));
        repository.markSent(command.notificationId(), now);
        sentCounter.increment();
    }
}
