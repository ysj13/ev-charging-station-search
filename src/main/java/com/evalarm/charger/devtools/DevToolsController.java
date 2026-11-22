package com.evalarm.charger.devtools;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.common.web.DevToolsEndpoint;
import com.evalarm.charger.station.domain.ChargerKey;
import com.evalarm.charger.station.domain.ChargerStatus;
import jakarta.validation.Valid;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 개발·시연용 도구. app.dev-tools.enabled=true일 때만 등록된다 (기본값 false, 운영에서는 엔드포인트 자체가 없음).
 *
 * <p>상태 주입은 실제 수집과 같은 경로(Kafka charger.status)로 메시지를 넣는다.
 * 그래서 빈자리 감지 → DB 반영 → 검색 API까지 실제 흐름을 그대로 탄다.
 * 출처는 MANUAL로 표시해 지연 지표와 빈자리 이벤트에서 실제 데이터와 구분한다.
 * 상태갱신일시는 현재 시각 이후로 넣으므로, 이후 실제 수집 데이터는 이보다 새로 갱신될 때만 반영된다.
 */
@RestController
@RequestMapping("/dev")
@ConditionalOnProperty(prefix = "app.dev-tools", name = "enabled", havingValue = "true")
public class DevToolsController implements DevToolsEndpoint {

    private static final long SEND_TIMEOUT_SECONDS = 10L;

    private final KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final RecentVacancyBuffer recentVacancies;
    private final AppProperties properties;
    private final Clock clock;

    /** 충전기별 마지막 주입 시각 (감지기가 DB에 반영하기 전에 다시 주입해도 시각이 앞서도록) */
    private final ConcurrentHashMap<ChargerKey, Instant> lastInjected = new ConcurrentHashMap<>();

    public DevToolsController(
            KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate,
            JdbcTemplate jdbcTemplate,
            RecentVacancyBuffer recentVacancies,
            AppProperties properties,
            Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.recentVacancies = recentVacancies;
        this.properties = properties;
        this.clock = clock;
    }

    @PostMapping("/chargers/{stationId}/{chargerId}/status")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public InjectStatusResponse injectStatus(
            @PathVariable String stationId,
            @PathVariable String chargerId,
            @Valid @RequestBody InjectStatusRequest request)
            throws InterruptedException, ExecutionException, TimeoutException {
        return inject(stationId, chargerId, Objects.requireNonNull(request.status()));
    }

    /**
     * 테스트용 빈자리를 만든다: 충전기를 충전중으로 바꾼 뒤 바로 충전대기로 바꾼다.
     * 두 상태가 같은 파티션에서 차례로 처리되므로 감지기가 빈자리로 감지하고, 알림까지 실제 경로를 그대로 탄다.
     * chargerId가 없으면 그 충전소의 첫 충전기를 쓴다.
     * 다른 사이트가 본문 없는 POST로 몰래 부르지 못하게(CSRF) X-Requested-With 헤더를 요구한다.
     * 사용자 지정 헤더가 붙은 요청은 브라우저가 다른 출처에서 사전 확인 없이 보내지 않는다
     */
    @PostMapping("/stations/{stationId}/test-vacancy")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public TestVacancyResponse testVacancy(
            @PathVariable String stationId,
            @RequestParam(required = false) @Nullable String chargerId,
            @RequestHeader("X-Requested-With") String requestedWith)
            throws InterruptedException, ExecutionException, TimeoutException {
        String target = chargerId;
        if (target == null) {
            List<String> chargerIds = jdbcTemplate.query(
                    "SELECT charger_id FROM charger WHERE station_id = ? ORDER BY charger_id LIMIT 1",
                    (rs, rowNum) -> rs.getString("charger_id"),
                    stationId);
            target = chargerIds.isEmpty() ? null : chargerIds.getFirst();
            if (target == null) {
                throw new UnknownChargerException(stationId, "(충전기 없음)");
            }
        }
        inject(stationId, target, ChargerStatus.CHARGING);
        InjectStatusResponse available = inject(stationId, target, ChargerStatus.AVAILABLE);
        return new TestVacancyResponse(stationId, target, available.statusUpdatedAt());
    }

    private InjectStatusResponse inject(String stationId, String chargerId, ChargerStatus status)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<Optional<Instant>> rows = jdbcTemplate.query(
                "SELECT status_updated_at FROM charger WHERE station_id = ? AND charger_id = ?",
                (rs, rowNum) -> {
                    OffsetDateTime value = rs.getObject("status_updated_at", OffsetDateTime.class);
                    return Optional.ofNullable(value != null ? value.toInstant() : null);
                },
                stationId, chargerId);
        if (rows.isEmpty()) {
            throw new UnknownChargerException(stationId, chargerId);
        }
        Optional<Instant> current = rows.getFirst();

        Instant updatedAt = nextUpdatedAt(new ChargerKey(stationId, chargerId), current.orElse(null));
        ChargerStatusMessage message = new ChargerStatusMessage(
                stationId,
                chargerId,
                status,
                updatedAt,
                clock.instant(),
                StatusSource.MANUAL);
        kafkaTemplate.send(properties.kafka().topics().chargerStatus().name(), stationId, message)
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return new InjectStatusResponse(stationId, chargerId, message.status(), updatedAt);
    }

    /**
     * 감지기는 상태갱신일시가 직전보다 늦을 때만 반영한다. 같은 초에 연달아 주입하거나 DB의 시각이 서버 시각보다
     * 앞서 있으면 주입이 조용히 무시되므로, 현재 시각·DB 시각·직전 주입 시각보다 항상 1초 이상 늦게 정한다.
     */
    private Instant nextUpdatedAt(ChargerKey key, @Nullable Instant dbUpdatedAt) {
        return Objects.requireNonNull(lastInjected.compute(key, (k, last) -> {
            Instant next = clock.instant().truncatedTo(ChronoUnit.SECONDS); // 공공데이터처럼 초 단위
            if (dbUpdatedAt != null && dbUpdatedAt.plusSeconds(1).isAfter(next)) {
                next = dbUpdatedAt.plusSeconds(1);
            }
            if (last != null && last.plusSeconds(1).isAfter(next)) {
                next = last.plusSeconds(1);
            }
            return next;
        }));
    }

    /** 최근 발행된 빈자리 이벤트 (최신순, 최대 50건). 이 인스턴스가 켜진 뒤 발행된 것만 보인다 */
    @GetMapping("/vacancies")
    public List<ChargerAvailableEvent> recentVacancies() {
        return recentVacancies.snapshot();
    }

    @ExceptionHandler(UnknownChargerException.class)
    public ProblemDetail handleUnknownCharger(UnknownChargerException e) {
        String message = e.getMessage();
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.NOT_FOUND, message != null ? message : "충전기를 찾을 수 없습니다");
        problem.setTitle("충전기 없음");
        return problem;
    }
}
