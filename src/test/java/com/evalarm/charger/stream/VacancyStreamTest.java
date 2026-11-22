package com.evalarm.charger.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.evalarm.charger.TestcontainersConfig;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.common.text.Whitespace;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.server.context.WebServerGracefulShutdownLifecycle;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.ContainerTestUtils;

/** 빈자리 이벤트가 실제 HTTP 연결(SSE)로 브라우저까지 전달되는지 검증한다 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "app.collector.enabled=false",
            "app.public-api.zcode=11",
            "app.detector.enabled=false",
            "app.dev-tools.enabled=false",
            "app.web.max-stream-connections=3",
        })
@Import(TestcontainersConfig.class)
class VacancyStreamTest {

    /** SSE 이벤트 하나 (이름, data) */
    private record SseEvent(String name, String data) {
    }

    @LocalServerPort
    int port;

    @Autowired
    KafkaTemplate<String, ChargerAvailableEvent> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    AppProperties properties;

    @Autowired
    VacancyBroadcaster broadcaster;

    private final HttpClient http = HttpClient.newHttpClient();

    private volatile boolean connectedSeen = false;

    @BeforeEach
    void setUp() throws Exception {
        // 앞선 테스트가 남긴 연결을 닫고 자리를 비워, 테스트 순서와 관계없이 같은 상태에서 시작한다
        broadcaster.stop();
        broadcaster.start();
        // 최신 위치부터 구독하므로, 파티션을 배정받기 전에 보낸 이벤트는 받지 못한다
        ContainerTestUtils.waitForAssignment(Objects.requireNonNull(listenerRegistry.getListenerContainer("vacancy-stream")), 3);
    }

    @Test
    @DisplayName("연결하면 connected를 받고 빈자리 이벤트가 발행되면 vacancy로 전달된다")
    void receivesConnectedThenVacancyEvents() throws Exception {
        String stationId = "SSE" + UUID.randomUUID().toString().substring(0, 6);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        HttpResponse<InputStream> response = openStream();
        try {
            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/event-stream"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8));

            Future<List<SseEvent>> events = executor.submit(() -> readEvents(reader, stationId));
            assertTrue(waitUntilConnected(events), "connected 이벤트를 받지 못했습니다");

            ChargerAvailableEvent event = new ChargerAvailableEvent(
                    stationId + ":01:1",
                    stationId,
                    "01",
                    ChargerStatus.CHARGING,
                    Instant.parse("2026-11-21T12:00:00Z"),
                    StatusSource.MANUAL);
            kafkaTemplate.send(properties.kafka().topics().chargerAvailable().name(), stationId, event).get();

            List<SseEvent> received = events.get(20, TimeUnit.SECONDS);
            SseEvent vacancy = null;
            for (SseEvent candidate : received) {
                if (candidate.name().equals("vacancy")) {
                    vacancy = candidate;
                }
            }
            assertNotNull(vacancy);
            assertTrue(vacancy.data().contains("\"stationId\":\"" + stationId + "\""));
            assertTrue(vacancy.data().contains("\"chargerId\":\"01\""));
            assertTrue(vacancy.data().contains("\"source\":\"MANUAL\""));
        } finally {
            response.body().close();
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("상한까지는 연결을 받고 넘으면 503과 Retry-After로 거절한다")
    void acceptsUpToLimitAndRejectsWith503AndRetryAfter() throws Exception {
        List<HttpResponse<InputStream>> opened = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                opened.add(openStream());
            }
            assertEquals(List.of(200, 200, 200), opened.stream().map(HttpResponse::statusCode).toList());

            HttpResponse<InputStream> rejected = openStream();
            assertEquals(503, rejected.statusCode());
            assertEquals("30", rejected.headers().firstValue("Retry-After").orElse(null));
            rejected.body().close();
        } finally {
            for (HttpResponse<InputStream> response : opened) {
                response.body().close();
            }
        }
    }

    @Test
    @DisplayName("웹 서버의 graceful shutdown보다 먼저 멈춘다")
    void stopsBeforeWebServerGracefulShutdown() {
        // 멈추는 순서는 phase가 큰 것부터다. 이 순서가 바뀌면 열린 SSE 연결 때문에 종료가 제한 시간까지 막힌다
        assertTrue(broadcaster.getPhase() > WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE);
    }

    @Test
    @DisplayName("멈추면 열린 연결을 모두 닫고 새 연결을 거절한다")
    void stopClosesOpenConnectionsAndRejectsNewOnes() throws Exception {
        HttpResponse<InputStream> response = openStream();
        assertEquals(200, response.statusCode());
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8));

        broadcaster.stop();

        // 서버가 연결을 닫으면 스트림이 끝난다 (종료가 열린 SSE 요청 때문에 막히지 않음)
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> ended = executor.submit(() -> {
                while (reader.readLine() != null) {
                    // 남은 이벤트 소비
                }
                return true;
            });
            assertTrue(ended.get(10, TimeUnit.SECONDS));
            HttpResponse<InputStream> afterStop = openStream();
            afterStop.body().close();
            assertEquals(503, afterStop.statusCode());
        } finally {
            executor.shutdownNow();
            response.body().close();
            broadcaster.start();
        }
    }

    private HttpResponse<InputStream> openStream() throws IOException, InterruptedException {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/vacancies/stream"))
                        .header("Accept", "text/event-stream")
                        .timeout(Duration.ofSeconds(30))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofInputStream());
    }

    private boolean waitUntilConnected(Future<?> events) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && !connectedSeen) {
            Thread.sleep(50);
        }
        return connectedSeen;
    }

    /** SSE 본문을 이벤트 단위(event, data)로 읽는다. until 문자열이 담긴 data를 받으면 멈춘다 */
    private List<SseEvent> readEvents(BufferedReader reader, String until) throws IOException {
        List<SseEvent> events = new ArrayList<>();
        String name = "message";
        StringBuilder data = new StringBuilder();
        while (true) {
            @Nullable String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("event:")) {
                name = Whitespace.trim(line.substring("event:".length()));
            } else if (line.startsWith("data:")) {
                data.append(line.substring("data:".length()));
            } else if (line.isEmpty() && (!data.isEmpty() || !name.equals("message"))) {
                events.add(new SseEvent(name, data.toString()));
                if (name.equals("connected")) {
                    connectedSeen = true;
                }
                if (data.toString().contains(until)) {
                    return events;
                }
                name = "message";
                data.setLength(0);
            }
        }
        return events;
    }
}
