package com.evalarm.charger.subscription;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SubscriptionRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public SubscriptionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean existsActiveStation(String stationId) {
        return Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM station WHERE station_id = :stationId AND active)",
                Map.of("stationId", stationId),
                boolean.class));
    }

    /**
     * 구독자를 만들거나 찾아 행 잠금을 건다. 같은 구독자의 구독 요청이 동시에 와도 한도 확인이 차례로 이뤄진다.
     */
    public long lockOrCreateSubscriber(String clientKey, Instant now) {
        jdbc.update(
                """
                INSERT INTO subscriber (client_key, created_at) VALUES (:clientKey, :now)
                ON CONFLICT (client_key) DO NOTHING""",
                Map.of("clientKey", clientKey, "now", toOffset(now)));
        return Objects.requireNonNull(jdbc.queryForObject(
                "SELECT id FROM subscriber WHERE client_key = :clientKey FOR UPDATE",
                Map.of("clientKey", clientKey),
                long.class));
    }

    public boolean isActive(long subscriberId, String stationId) {
        return Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM subscription WHERE subscriber_id = :subscriberId AND station_id = :stationId AND active)",
                Map.of("subscriberId", subscriberId, "stationId", stationId),
                boolean.class));
    }

    public int countActive(long subscriberId) {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM subscription WHERE subscriber_id = :subscriberId AND active",
                Map.of("subscriberId", subscriberId),
                int.class);
        return count != null ? count : 0;
    }

    /**
     * 구독을 켠다. 해지했던 구독이면 다시 켠다.
     * 마지막 알림 시각은 그대로 두어, 껐다 켜는 것으로 쿨다운을 건너뛰지 못하게 한다.
     * 다시 켜면 구독 시각을 새로 잡는다(목록은 최근 구독 순).
     */
    public void activate(long subscriberId, String stationId, int cooldownMinutes, Instant now) {
        jdbc.update(
                """
                INSERT INTO subscription (subscriber_id, station_id, active, cooldown_minutes, created_at)
                VALUES (:subscriberId, :stationId, TRUE, :cooldownMinutes, :now)
                ON CONFLICT (subscriber_id, station_id) DO UPDATE SET active = TRUE, created_at = EXCLUDED.created_at""",
                Map.of(
                        "subscriberId", subscriberId,
                        "stationId", stationId,
                        "cooldownMinutes", cooldownMinutes,
                        "now", toOffset(now)));
    }

    /** 구독을 끈다. 알림 이력이 구독을 참조하므로 행은 지우지 않는다 */
    public int deactivate(String clientKey, String stationId) {
        return jdbc.update(
                """
                UPDATE subscription s SET active = FALSE
                  FROM subscriber sub
                 WHERE sub.id = s.subscriber_id AND sub.client_key = :clientKey
                   AND s.station_id = :stationId AND s.active""",
                Map.of("clientKey", clientKey, "stationId", stationId));
    }

    public List<SubscriptionView> findActive(String clientKey) {
        return jdbc.query(
                """
                SELECT s.station_id, st.name, st.address, st.active AS station_active, s.created_at
                  FROM subscription s
                  JOIN subscriber sub ON sub.id = s.subscriber_id
                  JOIN station st ON st.station_id = s.station_id
                 WHERE sub.client_key = :clientKey AND s.active
                 ORDER BY s.created_at DESC, s.id DESC""",
                Map.of("clientKey", clientKey),
                (rs, rowNum) -> new SubscriptionView(
                        rs.getString("station_id"),
                        rs.getString("name"),
                        rs.getString("address"),
                        rs.getBoolean("station_active"),
                        rs.getObject("created_at", OffsetDateTime.class).toInstant()));
    }

    private static OffsetDateTime toOffset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
