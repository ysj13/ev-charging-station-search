package com.evalarm.charger.notification;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.NotificationCommand;
import com.evalarm.charger.common.messaging.StatusSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 빈자리 이벤트(Kafka {@code charger.available})를 구독과 맞춰 보고, 보낼 알림을 RabbitMQ 작업 큐에 넣는다.
 *
 * <p>처리 순서: 발송 기록(PENDING) 생성 → RabbitMQ 적재(브로커 확인까지 대기) → QUEUED 표시 → 오프셋 커밋
 * <ul>
 *   <li>기록을 먼저 남기고 큐에 넣으므로, 적재에 실패하면 Kafka가 같은 이벤트를 다시 준다. 다시 처리할 때는
 *   새 기록은 생기지 않고(유니크 제약) 남아 있는 PENDING만 다시 넣는다. 그래서 알림 작업을 잃지 않는다.</li>
 *   <li>적재는 됐는데 QUEUED 표시 전에 실패하면 같은 작업이 두 번 들어갈 수 있다. 발송기가 이미 보낸 작업을 건너뛴다.</li>
 *   <li>같은 충전소의 이벤트는 같은 파티션으로 와서 차례로 처리되므로, 같은 구독의 쿨다운 판단이 엇갈리지 않는다.</li>
 * </ul>
 */
@Component
public class NotificationMatcher {

    /** 발송 작업 메시지에 붙이는 발송 기록 ID. 최종 실패 처리(본문을 읽지 못한 경우 포함)에 쓴다 */
    public static final String NOTIFICATION_ID_HEADER = "x-notification-id";

    private static final int ENQUEUE_CHUNK_SIZE = 500;

    private static final Logger log = LoggerFactory.getLogger(NotificationMatcher.class);

    private final NotificationRepository repository;
    private final RabbitTemplate rabbitTemplate;
    private final AppProperties properties;
    private final Clock clock;
    private final Counter matchedCounter;
    private final Counter queuedCounter;
    private final Counter manualSkipped;
    private final Counter staleSkipped;

    public NotificationMatcher(
            NotificationRepository repository,
            RabbitTemplate rabbitTemplate,
            AppProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
        this.clock = clock;
        this.matchedCounter = Counter.builder("notification.matched").register(meterRegistry);
        this.queuedCounter = Counter.builder("notification.queued").register(meterRegistry);
        this.manualSkipped = skippedCounter(meterRegistry, "manual");
        this.staleSkipped = skippedCounter(meterRegistry, "stale");
    }

    @KafkaListener(
            id = "notification-matcher",
            groupId = "notification-matcher",
            topics = "${app.kafka.topics.charger-available.name}",
            concurrency = "${app.kafka.topics.charger-available.partitions}",
            autoStartup = "${app.notification.enabled}")
    public void onVacancy(@Nullable ChargerAvailableEvent event) {
        if (event == null) {
            return; // 역직렬화 실패
        }
        if (event.source() == StatusSource.MANUAL && !properties.notification().includeManualEvents()) {
            manualSkipped.increment();
            return;
        }
        Instant now = clock.instant();
        // 밀린 이벤트(장애 뒤 따라잡기 등)로 지난 빈자리를 알리지 않는다. 이미 만든 기록은 아래에서 마저 보낸다
        if (event.occurredAt().isBefore(now.minus(properties.detector().maxEventAge()))) {
            staleSkipped.increment();
        } else {
            // 시연용(MANUAL) 이벤트는 쿨다운 없이 매번 알리고, 실제 알림의 쿨다운 기준도 건드리지 않는다
            int created = repository.createPendingFor(event, now, event.source() != StatusSource.MANUAL);
            matchedCounter.increment(created);
        }
        enqueuePending(event.eventId());
    }

    /** 남은 PENDING을 묶음 단위로 넣는다. 구독자가 많아도 메모리와 쿼리 파라미터 수가 묶음 크기를 넘지 않는다 */
    private void enqueuePending(String eventId) {
        int total = 0;
        while (true) {
            List<NotificationCommand> commands = repository.findPendingCommands(eventId, ENQUEUE_CHUNK_SIZE);
            if (commands.isEmpty()) {
                break;
            }
            publishConfirmed(commands);
            List<Long> ids = new ArrayList<>(commands.size());
            for (NotificationCommand command : commands) {
                ids.add(command.notificationId());
            }
            repository.markQueued(ids);
            queuedCounter.increment(commands.size());
            total += commands.size();
            if (commands.size() < ENQUEUE_CHUNK_SIZE) {
                break;
            }
        }
        if (total > 0) {
            log.info("빈자리 알림 {}건을 발송 큐에 넣었습니다: {}", total, eventId);
        }
    }

    /** 브로커가 모든 작업을 받았다고 확인할 때까지 기다린다. 거절되거나 시간이 지나면 예외 → Kafka가 다시 처리 */
    private void publishConfirmed(List<NotificationCommand> commands) {
        AppProperties.Rabbit rabbit = properties.rabbit();
        rabbitTemplate.invoke(ops -> {
            for (NotificationCommand command : commands) {
                ops.convertAndSend(
                        rabbit.exchange(),
                        rabbit.routingKey(),
                        command,
                        (MessagePostProcessor) message -> {
                            message.getMessageProperties().setMessageId("notification-" + command.notificationId());
                            message.getMessageProperties().setHeader(NOTIFICATION_ID_HEADER, command.notificationId());
                            return message;
                        });
            }
            ops.waitForConfirmsOrDie(properties.notification().publishConfirmTimeout().toMillis());
            return null;
        });
    }

    private static Counter skippedCounter(MeterRegistry meterRegistry, String reason) {
        return Counter.builder("notification.events.skipped").tag("reason", reason).register(meterRegistry);
    }
}
