package com.evalarm.charger.stream;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * 빈자리 이벤트(charger.available)를 연결된 브라우저 모두에게 SSE로 전달한다.
 * <ul>
 *   <li>인스턴스마다 무작위 컨슈머 그룹으로 최신 위치부터 구독한다. 그래서 인스턴스가 여럿이어도 각자 모든 이벤트를 받아
 *   자기에게 연결된 브라우저에 보낸다. 지난 이벤트는 다시 보내지 않는다(화면은 연결 때마다 검색 API로 현재 상태를 읽는다).
 *   재기동마다 새 그룹이 생기므로 컨슈머 랙 경보에서는 {@code vacancy-stream-} 그룹을 제외한다.</li>
 *   <li>연결 수는 자리를 먼저 예약하고(AtomicInteger) 연결이 끝나면 정확히 한 번 반납한다. 상한 근처의 동시 요청도 상한을 넘지 않는다.</li>
 *   <li>연결이 끊긴 브라우저는 보내기에 실패하는 순간 정리한다. 프록시가 유휴 연결을 끊지 않도록 25초마다 주석을 보낸다.</li>
 *   <li>연결할 때 clientKey를 주면 그 구독자에게 온 구독 알림(notification)도 이 연결로 받는다.</li>
 *   <li>앱이 멈출 때는 웹 서버의 graceful shutdown보다 먼저 모든 연결을 닫는다. 그러지 않으면 열린 SSE 요청 때문에
 *   종료가 제한 시간(30초)까지 막힌다.</li>
 * </ul>
 */
@Component
public class VacancyBroadcaster implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(VacancyBroadcaster.class);

    private static final long HEARTBEAT_SECONDS = 25L;

    private final AppProperties properties;
    private final Set<Connection> connections = ConcurrentHashMap.newKeySet();
    private final AtomicInteger reserved = new AtomicInteger(0);
    private final AtomicBoolean accepting = new AtomicBoolean(false);
    private @Nullable ScheduledExecutorService heartbeat;

    /** SSE 연결 하나. 자리 반납은 여러 경로(완료·타임아웃·오류·전송 실패·종료)에서 불려도 한 번만 한다 */
    private final class Connection {
        private final SseEmitter emitter;
        private final @Nullable String clientKey;
        private final AtomicBoolean released = new AtomicBoolean(false);

        Connection(SseEmitter emitter, @Nullable String clientKey) {
            this.emitter = emitter;
            this.clientKey = clientKey;
        }

        void release() {
            if (released.compareAndSet(false, true)) {
                connections.remove(this);
                reserved.decrementAndGet();
            }
        }
    }

    public VacancyBroadcaster(AppProperties properties, MeterRegistry meterRegistry) {
        this.properties = properties;
        meterRegistry.gauge("vacancy.stream.connections", reserved);
    }

    /** 구독 알림 없이 빈자리 이벤트만 받는 연결을 연다 */
    public SseEmitter connect() {
        return connect(null);
    }

    /** @param clientKey 구독 알림을 받을 구독자. 없으면 빈자리 이벤트만 받는다 */
    public SseEmitter connect(@Nullable String clientKey) {
        if (!accepting.get()) {
            throw new TooManyStreamConnectionsException();
        }
        if (reserved.incrementAndGet() > properties.web().maxStreamConnections()) {
            reserved.decrementAndGet();
            throw new TooManyStreamConnectionsException();
        }
        Connection connection = new Connection(new SseEmitter(properties.web().streamTimeout().toMillis()), clientKey);
        connection.emitter.onCompletion(connection::release);
        connection.emitter.onTimeout(connection.emitter::complete);
        connection.emitter.onError(error -> connection.release());
        connections.add(connection);
        // 위의 accepting 확인과 add 사이에 stop()이 끝났다면, closeAll()이 이 연결을 보지 못했다. 직접 닫고 거절한다
        if (!accepting.get()) {
            connection.release();
            completeQuietly(connection);
            throw new TooManyStreamConnectionsException();
        }
        // 연결 직후 한 번 보내 브라우저가 연결 성공을 바로 알 수 있게 한다
        send(connection, () -> SseEmitter.event().name("connected").data(Map.of("connectedAt", Instant.now())));
        return connection.emitter;
    }

    @KafkaListener(
            id = "vacancy-stream",
            groupId = "vacancy-stream-${random.uuid}",
            topics = "${app.kafka.topics.charger-available.name}",
            properties = "auto.offset.reset=latest")
    public void onVacancy(@Nullable ChargerAvailableEvent event) {
        if (event == null) {
            return; // 역직렬화 실패
        }
        VacancyPush push = new VacancyPush(
                event.eventId(), event.stationId(), event.chargerId(), event.occurredAt(), event.source());
        broadcast(() -> SseEmitter.event().name("vacancy").id(event.eventId()).data(push));
    }

    /**
     * 구독자 한 명에게 구독 알림을 보낸다. 같은 구독자가 여러 탭으로 연결해 있으면 모두에게 보낸다.
     *
     * @return 보낸 연결 수
     */
    public int notifyClient(String clientKey, Object notification) {
        List<Connection> targets = new ArrayList<>();
        for (Connection connection : connections) {
            // clientKey 없이 연결한 화면(익명)에는 구독 알림을 보내지 않는다
            if (connection.clientKey != null && connection.clientKey.equals(clientKey)) {
                targets.add(connection);
            }
        }
        for (Connection target : targets) {
            send(target, () -> SseEmitter.event().name("notification").data(notification));
        }
        return targets.size();
    }

    /** 이벤트 빌더는 보낼 때마다 내부 상태가 쌓이므로 연결마다 새로 만든다 */
    private void broadcast(Supplier<SseEmitter.SseEventBuilder> event) {
        for (Connection connection : connections) {
            send(connection, event);
        }
    }

    private void send(Connection connection, Supplier<SseEmitter.SseEventBuilder> event) {
        try {
            connection.emitter.send(event.get());
        } catch (Exception e) {
            // 브라우저가 떠난 연결. 자리를 반납하고 정리한다
            connection.release();
            completeQuietly(connection);
            log.debug("끊긴 실시간 연결을 정리했습니다: {}", e.getMessage());
        }
    }

    /** 새 연결을 막고 열린 연결을 모두 닫는다 */
    private void closeAll() {
        for (Connection connection : new ArrayList<>(connections)) {
            completeQuietly(connection);
            connection.release();
        }
    }

    /** 이미 끝났거나 끊긴 연결일 수 있어 complete()의 실패는 무시한다 */
    private static void completeQuietly(Connection connection) {
        try {
            connection.emitter.complete();
        } catch (Throwable ignored) {
            // 무시
        }
    }

    @Override
    public void start() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "vacancy-stream-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleAtFixedRate(
                () -> broadcast(() -> SseEmitter.event().comment("keep-alive")),
                HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
        heartbeat = executor;
        accepting.set(true);
    }

    @Override
    public void stop() {
        accepting.set(false);
        ScheduledExecutorService executor = heartbeat;
        if (executor != null) {
            executor.shutdownNow();
        }
        heartbeat = null;
        closeAll();
    }

    @Override
    public boolean isRunning() {
        return accepting.get();
    }

    /** 웹 서버 graceful shutdown(DEFAULT_PHASE - 1024)보다 먼저 멈춰 SSE 요청이 종료를 막지 않게 한다 */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE;
    }
}
