package com.evalarm.charger.notification;

import com.evalarm.charger.common.messaging.ChargerAvailableEvent;
import com.evalarm.charger.common.messaging.NotificationCommand;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 알림 발송 기록(notification_log).
 * 상태 흐름: PENDING(매칭) → QUEUED(RabbitMQ 적재 확인) → SENT(발송) / FAILED(재시도 끝에 실패) / CANCELLED(발송 전 해지)
 * 상태를 바꾸는 쿼리는 모두 "아직 SENT·CANCELLED가 아닐 때만" 바꿔, 늦게 온 갱신이 끝난 상태를 되돌리지 않게 한다.
 */
@Repository
public class NotificationRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public NotificationRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 쿨다운을 적용해 발송 기록을 만든다. {@link #createPendingFor(ChargerAvailableEvent, Instant, boolean)} 참고 */
    public int createPendingFor(ChargerAvailableEvent event, Instant now) {
        return createPendingFor(event, now, true);
    }

    /**
     * 빈자리 이벤트에 맞는 구독을 찾아 발송 기록(PENDING)을 만든다. 한 문장이라 원자적으로 처리된다.
     * <ul>
     *   <li>그 충전소를 구독 중이고, 쿨다운(같은 충전소 알림 최소 간격)이 지난 구독만 고른다.
     *   쿨다운은 이벤트 발생 시각 기준이라, 같은 이벤트를 다시 처리해도 결과가 같다.</li>
     *   <li>고른 구독의 마지막 알림 시각을 이벤트 발생 시각으로 바꾼다.</li>
     *   <li>(구독, 이벤트) 유니크 제약으로 같은 이벤트의 기록은 한 번만 생긴다.</li>
     * </ul>
     *
     * @param applyCooldown false면(시연용 이벤트) 쿨다운을 보지 않고 마지막 알림 시각도 바꾸지 않는다.
     *     시연 알림을 몇 번이고 보낼 수 있고, 실제 알림의 쿨다운에 영향을 주지 않는다
     * @return 새로 만든 발송 기록 수
     */
    public int createPendingFor(ChargerAvailableEvent event, Instant now, boolean applyCooldown) {
        return jdbc.update(
                """
                WITH matched AS (
                    UPDATE subscription s
                       SET last_notified_at = CASE WHEN CAST(:applyCooldown AS boolean)
                                                   THEN CAST(:occurredAt AS timestamptz) ELSE s.last_notified_at END
                     WHERE s.station_id = :stationId
                       AND s.active
                       AND (NOT CAST(:applyCooldown AS boolean)
                            OR s.last_notified_at IS NULL
                            OR s.last_notified_at <= CAST(:occurredAt AS timestamptz) - make_interval(mins => s.cooldown_minutes))
                       AND NOT EXISTS (
                           SELECT 1 FROM notification_log l WHERE l.subscription_id = s.id AND l.event_id = :eventId
                       )
                    RETURNING s.id
                )
                INSERT INTO notification_log
                    (subscription_id, event_id, station_id, charger_id, status, attempt_count, occurred_at, created_at)
                SELECT id, :eventId, :stationId, :chargerId, 'PENDING', 0, :occurredAt, :now FROM matched
                ON CONFLICT (subscription_id, event_id) DO NOTHING""",
                Map.of(
                        "eventId", event.eventId(),
                        "stationId", event.stationId(),
                        "chargerId", event.chargerId(),
                        "occurredAt", toOffset(event.occurredAt()),
                        "now", toOffset(now),
                        "applyCooldown", applyCooldown));
    }

    /** 이 이벤트로 만들었지만 아직 RabbitMQ에 넣었다고 확인되지 않은 발송 작업. 구독자가 많아도 한 번에 limit건씩 다룬다 */
    public List<NotificationCommand> findPendingCommands(String eventId, int limit) {
        return jdbc.query(
                """
                SELECT l.id, l.event_id, l.subscription_id, sub.client_key, l.station_id, st.name, l.charger_id, l.occurred_at
                  FROM notification_log l
                  JOIN subscription s ON s.id = l.subscription_id
                  JOIN subscriber sub ON sub.id = s.subscriber_id
                  JOIN station st ON st.station_id = l.station_id
                 WHERE l.event_id = :eventId AND l.status = 'PENDING'
                 ORDER BY l.id
                 LIMIT :limit""",
                Map.of("eventId", eventId, "limit", limit),
                (rs, rowNum) -> new NotificationCommand(
                        rs.getLong("id"),
                        rs.getString("event_id"),
                        rs.getLong("subscription_id"),
                        rs.getString("client_key"),
                        rs.getString("station_id"),
                        rs.getString("name"),
                        rs.getString("charger_id"),
                        rs.getObject("occurred_at", OffsetDateTime.class).toInstant()));
    }

    /** 브로커가 받았다고 확인한 작업을 QUEUED로. 그 사이 이미 발송됐으면 그대로 둔다 */
    public int markQueued(Collection<Long> ids) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbc.update(
                "UPDATE notification_log SET status = 'QUEUED' WHERE id IN (:ids) AND status = 'PENDING'",
                Map.of("ids", ids));
    }

    /**
     * 매칭된 뒤 발송 전에 구독을 해지했으면 CANCELLED로 바꾼다.
     *
     * @return 취소했으면 true. 이때는 보내지 않는다
     */
    public boolean cancelIfUnsubscribed(long id) {
        return jdbc.update(
                """
                UPDATE notification_log l SET status = 'CANCELLED'
                  FROM subscription s
                 WHERE l.id = :id AND s.id = l.subscription_id AND NOT s.active
                   AND l.status NOT IN ('SENT', 'CANCELLED')""",
                Map.of("id", id)) > 0;
    }

    /**
     * 발송 시도를 기록한다(시도 횟수 +1).
     *
     * @return 이미 발송·취소했거나 없는 기록이면 false. 이때는 보내지 않는다
     */
    public boolean beginAttempt(long id) {
        return jdbc.update(
                "UPDATE notification_log SET attempt_count = attempt_count + 1 WHERE id = :id AND status NOT IN ('SENT', 'CANCELLED')",
                Map.of("id", id)) > 0;
    }

    public int markSent(long id, Instant now) {
        return jdbc.update(
                "UPDATE notification_log SET status = 'SENT', sent_at = :now WHERE id = :id AND status NOT IN ('SENT', 'CANCELLED')",
                Map.of("id", id, "now", toOffset(now)));
    }

    public int markFailed(long id) {
        return jdbc.update(
                "UPDATE notification_log SET status = 'FAILED' WHERE id = :id AND status NOT IN ('SENT', 'CANCELLED')",
                Map.of("id", id));
    }

    /** 이 구독자에게 보낸 최근 알림 */
    public List<NotificationView> findRecentSent(String clientKey, int limit) {
        return jdbc.query(
                """
                SELECT l.id, l.event_id, l.station_id, st.name, l.charger_id, l.occurred_at, l.sent_at
                  FROM notification_log l
                  JOIN subscription s ON s.id = l.subscription_id
                  JOIN subscriber sub ON sub.id = s.subscriber_id
                  JOIN station st ON st.station_id = l.station_id
                 WHERE sub.client_key = :clientKey AND l.status = 'SENT'
                 ORDER BY l.created_at DESC, l.id DESC
                 LIMIT :limit""",
                Map.of("clientKey", clientKey, "limit", limit),
                (rs, rowNum) -> new NotificationView(
                        rs.getLong("id"),
                        rs.getString("event_id"),
                        rs.getString("station_id"),
                        rs.getString("name"),
                        rs.getString("charger_id"),
                        getInstant(rs, "occurred_at"),
                        getInstant(rs, "sent_at")));
    }

    private static @Nullable Instant getInstant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value != null ? value.toInstant() : null;
    }

    private static OffsetDateTime toOffset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
