package com.evalarm.charger.detector;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.station.domain.ChargerKey;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * {@code charger.status}를 소비해 빈자리를 감지하고 {@code charger.available}에 발행한다. 감지한 상태는 DB와 Redis에 반영한다.
 *
 * <p>처리 순서: 직전 상태 조회(Redis, 없으면 DB) → 전이 감지 → 이벤트 발행 확인 → DB 갱신 → Redis 갱신 → 오프셋 커밋
 * <ul>
 *   <li>중간에 실패하면 배치 전체가 다시 처리된다. Redis는 마지막에 갱신하므로 재처리 때 같은 전이를 다시 감지하고,
 *   이미 발행한 이벤트는 {@link PublishedEventCache}로 건너뛴다. 재기동 등으로 캐시가 비면 같은 eventId가 다시 나갈 수 있어
 *   전달 보장은 최소 한 번(at-least-once)이고, 소비자는 eventId로 중복을 거른다.</li>
 *   <li>Redis에 직전 상태가 없으면 DB의 상태를 기준으로 삼는다. Redis를 잃어도 빈자리를 놓치지 않기 위해서다.
 *   (DB를 먼저 갱신하는 순서로 바꾸면, 발행 실패 후 재처리할 때 DB가 이미 새 상태라 전이를 놓친다. 그래서 발행을 먼저 한다)</li>
 *   <li>같은 충전소의 메시지는 같은 파티션으로 오므로(키 = 충전소 ID) 한 충전기는 한 스레드에서만 순서대로 처리된다.</li>
 * </ul>
 */
@Component
public class AvailabilityDetectorListener {

    private static final Logger log = LoggerFactory.getLogger(AvailabilityDetectorListener.class);

    private static final long SEND_TIMEOUT_SECONDS = 30L;

    private final ChargerStateStore stateStore;
    private final AvailabilityTransitionDetector detector;
    private final ChargerStatusUpdateRepository statusRepository;
    private final PublishedEventCache publishedEvents;
    private final KafkaTemplate<String, ChargerAvailableEvent> kafkaTemplate;
    private final AppProperties properties;
    private final Clock clock;
    private final Counter vacancyCounter;
    private final Counter staleVacancyCounter;
    private final Counter duplicateCounter;
    private final Counter ignoredCounter;
    private final Counter invalidCounter;
    private final Map<StatusSource, Timer> lagTimers = new EnumMap<>(StatusSource.class);

    public AvailabilityDetectorListener(
            ChargerStateStore stateStore,
            AvailabilityTransitionDetector detector,
            ChargerStatusUpdateRepository statusRepository,
            PublishedEventCache publishedEvents,
            KafkaTemplate<String, ChargerAvailableEvent> kafkaTemplate,
            AppProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.stateStore = stateStore;
        this.detector = detector;
        this.statusRepository = statusRepository;
        this.publishedEvents = publishedEvents;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.clock = clock;
        this.vacancyCounter = Counter.builder("detector.vacancy.events").register(meterRegistry);
        this.staleVacancyCounter = Counter.builder("detector.vacancy.stale").register(meterRegistry);
        this.duplicateCounter = Counter.builder("detector.vacancy.duplicates.suppressed").register(meterRegistry);
        this.ignoredCounter = Counter.builder("detector.messages.ignored").register(meterRegistry);
        this.invalidCounter = Counter.builder("detector.messages.invalid").register(meterRegistry);
        for (StatusSource source : StatusSource.values()) {
            lagTimers.put(source, Timer.builder("detector.status.lag")
                    .description("공공데이터 상태 갱신 시각부터 감지기 처리까지 걸린 시간")
                    .tag("source", source.name())
                    .publishPercentiles(0.5, 0.9, 0.99)
                    .register(meterRegistry));
        }
    }

    @KafkaListener(
            id = "availability-detector",
            groupId = "availability-detector",
            topics = "${app.kafka.topics.charger-status.name}",
            batch = "true",
            concurrency = "${app.detector.concurrency}",
            autoStartup = "${app.detector.enabled}")
    public void onStatuses(List<ConsumerRecord<String, @Nullable ChargerStatusMessage>> records)
            throws InterruptedException, ExecutionException, TimeoutException {
        // 역직렬화에 실패한 메시지는 값이 null로 온다(ErrorHandlingDeserializer). 다시 처리해도 같으므로 건너뛴다
        List<ChargerStatusMessage> messages = new ArrayList<>(records.size());
        for (ConsumerRecord<String, @Nullable ChargerStatusMessage> record : records) {
            ChargerStatusMessage message = record.value();
            if (message != null) {
                messages.add(message);
            }
        }
        if (messages.size() < records.size()) {
            invalidCounter.increment(records.size() - messages.size());
            log.warn("역직렬화할 수 없는 상태 메시지 {}건을 건너뜁니다.", records.size() - messages.size());
        }
        if (messages.isEmpty()) {
            return;
        }

        Instant now = clock.instant();
        recordLag(messages, now);
        Set<ChargerKey> keys = new HashSet<>();
        for (ChargerStatusMessage message : messages) {
            keys.add(new ChargerKey(message.stationId(), message.chargerId()));
        }
        PreviousStates previous = loadPreviousStates(keys);
        DetectionResult result =
                detector.detect(previous.states(), messages, now.minus(properties.detector().maxEventAge()));

        List<ChargerAvailableEvent> toPublish = new ArrayList<>();
        for (ChargerAvailableEvent event : result.events()) {
            if (!publishedEvents.contains(event.eventId())) {
                toPublish.add(event);
            }
        }
        publish(toPublish);
        List<String> publishedIds = new ArrayList<>(toPublish.size());
        for (ChargerAvailableEvent event : toPublish) {
            publishedIds.add(event.eventId());
        }
        publishedEvents.addAll(publishedIds);
        int updatedRows = statusRepository.updateIfNewer(result.changed());
        // DB에서 채운 기준 상태도 Redis에 심어 둔다. 상태가 바뀌지 않은 충전기도 다음부터는 Redis에서 바로 읽는다
        Map<ChargerKey, ChargerState> toSave = new LinkedHashMap<>(previous.fromDb());
        toSave.putAll(result.changed());
        stateStore.saveIfNewer(toSave);

        vacancyCounter.increment(toPublish.size());
        duplicateCounter.increment(result.events().size() - toPublish.size());
        staleVacancyCounter.increment(result.staleVacancies());
        ignoredCounter.increment(result.ignored());
        log.info(
                "상태 {}건 처리: 반영 {}건(DB {}건), 무시 {}건, 빈자리 {}건 (중복 생략 {}건, 오래돼 생략 {}건)",
                messages.size(), result.changed().size(), updatedRows, result.ignored(), toPublish.size(),
                result.events().size() - toPublish.size(), result.staleVacancies());
    }

    private record PreviousStates(
            Map<ChargerKey, ChargerState> states,
            /** Redis에 없어 DB에서 채운 것 */
            Map<ChargerKey, ChargerState> fromDb) {
    }

    /** Redis에서 직전 상태를 읽고, 없는 충전기는 DB의 현재 상태로 채운다 */
    private PreviousStates loadPreviousStates(Set<ChargerKey> keys) {
        Map<ChargerKey, ChargerState> fromRedis = stateStore.load(keys);
        Set<ChargerKey> missing = new LinkedHashSet<>(keys);
        missing.removeAll(fromRedis.keySet());
        Map<ChargerKey, ChargerState> fromDb = missing.isEmpty() ? Map.of() : statusRepository.loadStates(missing);
        Map<ChargerKey, ChargerState> states = new LinkedHashMap<>(fromRedis);
        states.putAll(fromDb);
        return new PreviousStates(states, fromDb);
    }

    /**
     * 공공데이터의 상태 갱신 시각부터 감지기가 처리할 때까지 걸린 시간.
     * 오래된 빈자리 기준(max-event-age)이 실제 지연 분포에 맞는지 확인하는 데 쓴다.
     */
    private void recordLag(List<ChargerStatusMessage> messages, Instant now) {
        for (ChargerStatusMessage message : messages) {
            Instant updatedAt = message.statusUpdatedAt();
            if (updatedAt == null) {
                continue;
            }
            Duration lag = Duration.between(updatedAt, now);
            lagTimers.get(message.source()).record(lag.compareTo(Duration.ZERO) < 0 ? Duration.ZERO : lag);
        }
    }

    private void publish(List<ChargerAvailableEvent> events)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (events.isEmpty()) {
            return;
        }
        String topic = properties.kafka().topics().chargerAvailable().name();
        List<CompletableFuture<?>> sends = new ArrayList<>(events.size());
        for (ChargerAvailableEvent event : events) {
            sends.add(kafkaTemplate.send(topic, event.stationId(), event));
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
