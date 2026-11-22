package com.evalarm.charger.notification;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class NotificationConfig implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(NotificationConfig.class);

    private static final int PUSH_THREADS = 4;
    private static final int PUSH_QUEUE_CAPACITY = 10_000;
    private static final long DROP_LOG_INTERVAL_MS = 60_000L;

    /** 실시간 전달용 스레드 풀. 컨테이너에 직접 넘긴 실행기는 컨테이너가 종료하지 않으므로 여기서 닫는다 */
    private @Nullable ThreadPoolTaskExecutor pushExecutor;
    private final AtomicLong lastDropLogAt = new AtomicLong(0);

    /**
     * 발송을 정해진 횟수만큼 다시 시도해도 실패한 작업의 마지막 처리.
     * 발송 기록을 FAILED로 남기고, 큐에 다시 넣지 않고 거절해 DLX를 거쳐 DLQ로 보낸다(나중에 원인을 보고 다시 넣을 수 있다).
     * Spring Boot가 이 빈을 @RabbitListener 재시도 설정(spring.rabbitmq.listener.simple.retry)에 연결한다.
     */
    @Bean
    public MessageRecoverer notificationMessageRecoverer(NotificationRepository repository, MeterRegistry meterRegistry) {
        Counter failedCounter = Counter.builder("notification.failed").register(meterRegistry);
        RejectAndDontRequeueRecoverer reject = new RejectAndDontRequeueRecoverer();
        return (message, cause) -> {
            failedCounter.increment();
            Object header = message.getMessageProperties().getHeader(NotificationMatcher.NOTIFICATION_ID_HEADER);
            Long notificationId = header != null ? parseLongOrNull(header.toString()) : null;
            Throwable rootCause = NestedExceptionUtils.getMostSpecificCause(cause);
            String reason = rootCause.getClass().getSimpleName() + ": " + rootCause.getMessage();
            log.error("알림 발송에 끝내 실패해 DLQ로 보냅니다: notificationId={}, 원인={}", notificationId, reason);
            if (notificationId != null) {
                // DB까지 장애여도 DLQ 이동은 막지 않는다. 기록은 PENDING·QUEUED로 남고 DLQ 메시지로 추적한다
                try {
                    repository.markFailed(notificationId);
                } catch (Throwable e) {
                    log.warn("FAILED 기록에 실패했습니다: notificationId={}, {}", notificationId, e.getMessage());
                }
            }
            reject.recover(message, cause);
        };
    }

    /**
     * 다른 인스턴스(또는 자신)가 올린 알림을 받아 이 인스턴스의 실시간 연결로 넘긴다.
     * 기본 실행기는 메시지마다 스레드를 새로 만들어, 알림이 몰리면 스레드가 폭증한다. 크기가 정해진 풀을 쓴다.
     * 대기열이 넘치면 그 알림의 실시간 전달은 버린다(기록은 알림함에 남는다).
     */
    @Bean
    public RedisMessageListenerContainer notificationPushListenerContainer(
            RedisConnectionFactory connectionFactory,
            NotificationPushChannel pushChannel,
            MeterRegistry meterRegistry) {
        Counter dropped = Counter.builder("notification.push.dropped").register(meterRegistry);
        // 빈으로 등록하지 않는다(Executor 빈이 있으면 Boot의 기본 작업 실행기가 물러난다). 종료는 destroy()에서 한다
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(PUSH_THREADS);
        executor.setMaxPoolSize(PUSH_THREADS);
        executor.setQueueCapacity(PUSH_QUEUE_CAPACITY);
        executor.setThreadNamePrefix("notification-push-");
        executor.setDaemon(true);
        executor.setRejectedExecutionHandler((runnable, threadPoolExecutor) -> {
            dropped.increment();
            // 과부하 때 로그가 쏟아지지 않게 1분에 한 번만 남긴다. 건수는 지표로 본다
            long now = System.currentTimeMillis();
            long last = lastDropLogAt.get();
            if (now - last >= DROP_LOG_INTERVAL_MS && lastDropLogAt.compareAndSet(last, now)) {
                log.warn("실시간 알림 전달이 밀려 일부를 건너뜁니다 (누적 {}건, 지표 notification.push.dropped)", (long) dropped.count());
            }
        });
        executor.initialize();
        pushExecutor = executor;
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setTaskExecutor(executor);
        container.addMessageListener(pushChannel, new ChannelTopic(NotificationPushChannel.CHANNEL));
        return container;
    }

    @Override
    public void destroy() {
        ThreadPoolTaskExecutor executor = pushExecutor;
        if (executor != null) {
            executor.shutdown();
        }
    }

    /** 문자열을 10진 정수로 읽는다. 형식이 틀리면 null */
    private static @Nullable Long parseLongOrNull(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
