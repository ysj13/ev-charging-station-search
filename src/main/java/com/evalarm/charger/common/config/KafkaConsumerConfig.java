package com.evalarm.charger.common.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    /** 리스너 예외는 ListenerExecutionFailedException으로 감싸져 오므로, 장애 원인(Redis·DB·Kafka)이 보이게 가장 안쪽 원인을 남긴다 */
    private static String rootCause(@Nullable Throwable ex) {
        if (ex == null) {
            return "알 수 없음";
        }
        Throwable cause = NestedExceptionUtils.getMostSpecificCause(ex);
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    /**
     * 리스너 오류 처리.
     *
     * <p>리스너가 실패하는 원인은 Redis·DB·Kafka 연결 같은 인프라 장애가 대부분이고, 이런 장애는 시간이 지나면 풀린다.
     * 메시지를 버리면 빈자리 이벤트를 영영 잃으므로, 같은 배치를 1초부터 최대 1분 간격으로 멈추지 않고 다시 시도한다.
     * 재시도가 이어지는 동안 해당 파티션은 막히므로 WARN 로그와 지표(kafka.listener.retries)로 드러낸다.
     * 컨슈머 랙(kafka_consumer_fetch_manager_records_lag) 경보와 함께 보면 된다.
     *
     * <p>역직렬화 오류는 ErrorHandlingDeserializer가 null 값으로 바꿔 리스너에서 건너뛰므로 여기서 막히지 않는다.
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(MeterRegistry meterRegistry) {
        Counter retries = Counter.builder("kafka.listener.retries").register(meterRegistry);
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(60_000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(backOff);
        // 배치 리스너의 재시도는 ConsumerRecords 버전이 호출되므로 두 버전을 모두 구현한다
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, @Nullable Exception ex, int deliveryAttempt) {
                retries.increment();
                log.warn("Kafka 메시지 처리 실패({}회째), 다시 시도합니다: {} / {}", deliveryAttempt, record.topic(), rootCause(ex));
            }

            @Override
            public void failedDelivery(ConsumerRecords<?, ?> records, Exception ex, int deliveryAttempt) {
                retries.increment();
                log.warn("Kafka 배치 처리 실패({}회째, {}건), 다시 시도합니다: {}", deliveryAttempt, records.count(), rootCause(ex));
            }
        });
        return handler;
    }
}
