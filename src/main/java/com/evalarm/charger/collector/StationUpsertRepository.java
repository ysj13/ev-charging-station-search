package com.evalarm.charger.collector;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.station.domain.ChargerKey;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 충전소 정보 대량 동기화용 저장소.
 * 서울만 해도 충전기가 7만 대가 넘어 JPA 엔티티 단위 저장 대신 JDBC 배치 UPSERT를 쓴다.
 */
@Repository
public class StationUpsertRepository {

    private static final int BATCH_SIZE = 1_000;

    private static final String UPSERT_STATION = """
            INSERT INTO station (station_id, name, address, location_detail, latitude, longitude, use_time,
                                 operator_id, operator_name, operator_call, zcode, zscode, parking_free,
                                 limited, limit_detail, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?)
            ON CONFLICT (station_id) DO UPDATE SET
                name = EXCLUDED.name,
                address = EXCLUDED.address,
                location_detail = EXCLUDED.location_detail,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                use_time = EXCLUDED.use_time,
                operator_id = EXCLUDED.operator_id,
                operator_name = EXCLUDED.operator_name,
                operator_call = EXCLUDED.operator_call,
                zcode = EXCLUDED.zcode,
                zscode = EXCLUDED.zscode,
                parking_free = EXCLUDED.parking_free,
                limited = EXCLUDED.limited,
                limit_detail = EXCLUDED.limit_detail,
                active = TRUE,
                updated_at = GREATEST(station.updated_at, EXCLUDED.updated_at)""";

    /**
     * 상태는 더 최신 값일 때만 덮어쓴다. 갱신 시각이 같으면 기존 값을 유지한다
     * (빈자리 감지의 DB·Redis 갱신과 같은 규칙. 시각이 같은데 상태가 다를 때 DB와 Redis가 어긋나지 않게 한다).
     * 충전기 상태는 빈자리 감지 컨슈머가 실시간으로 갱신하므로, 전체 동기화 값이 그보다 오래됐으면 되돌리지 않는다.
     * 새 값의 갱신 시각이 없으면(null) 기존 상태를 유지한다.
     */
    private static final String UPSERT_CHARGER = """
            INSERT INTO charger (station_id, charger_id, charger_type, output_kw, status, status_updated_at,
                                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, charger_id) DO UPDATE SET
                charger_type = EXCLUDED.charger_type,
                output_kw = EXCLUDED.output_kw,
                status = CASE
                    WHEN EXCLUDED.status_updated_at IS NOT NULL
                     AND (charger.status_updated_at IS NULL OR EXCLUDED.status_updated_at > charger.status_updated_at)
                    THEN EXCLUDED.status
                    ELSE charger.status END,
                status_updated_at = GREATEST(charger.status_updated_at, EXCLUDED.status_updated_at),
                -- 동기화가 겹쳐도(먼저 시작한 동기화가 늦게 쓰더라도) 더 최근 동기화의 표시를 되돌리지 않는다
                updated_at = GREATEST(charger.updated_at, EXCLUDED.updated_at)""";

    /** 빈자리 감지가 켜져 있을 때: 새 충전기만 초기 상태를 넣고, 이미 있는 충전기의 상태는 감지기에 맡긴다 */
    private static final String UPSERT_CHARGER_KEEP_STATUS = """
            INSERT INTO charger (station_id, charger_id, charger_type, output_kw, status, status_updated_at,
                                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, charger_id) DO UPDATE SET
                charger_type = EXCLUDED.charger_type,
                output_kw = EXCLUDED.output_kw,
                updated_at = GREATEST(charger.updated_at, EXCLUDED.updated_at)""";

    private static final String TOUCH_CHARGER =
            "UPDATE charger SET updated_at = GREATEST(updated_at, ?) WHERE station_id = ? AND charger_id = ?";

    private static final String COUNT_CHARGERS = """
            SELECT count(*) FROM charger c JOIN station s ON s.station_id = c.station_id WHERE s.zcode = ?""";

    private static final String COUNT_STALE_CHARGERS = """
            SELECT count(*) FROM charger c JOIN station s ON s.station_id = c.station_id
            WHERE s.zcode = ? AND c.updated_at < ?""";

    private static final String DELETE_STALE_CHARGERS = """
            DELETE FROM charger c
            USING station s
            WHERE c.station_id = s.station_id
              AND s.zcode = ?
              AND c.updated_at < ?""";

    private static final String DEACTIVATE_STATIONS_WITHOUT_CHARGERS = """
            UPDATE station s SET active = FALSE
            WHERE s.zcode = ?
              AND s.active
              AND NOT EXISTS (SELECT 1 FROM charger c WHERE c.station_id = s.station_id)""";

    private final JdbcTemplate jdbcTemplate;

    /**
     * 빈자리 감지가 켜져 있으면 이미 있는 충전기의 상태는 전체 동기화가 직접 쓰지 않는다.
     * 상태는 SNAPSHOT 메시지를 통해 감지기만 반영한다. 동기화가 DB를 먼저 새 상태로 바꿔 두면,
     * 감지기가 직전 상태를 DB에서 읽을 때(Redis 유실 등) 전이가 보이지 않아 빈자리를 놓치기 때문이다.
     * 감지기가 꺼져 있으면 DB 상태를 갱신할 곳이 동기화뿐이므로 직접 쓴다.
     */
    private final String upsertChargerSql;

    public StationUpsertRepository(JdbcTemplate jdbcTemplate, AppProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.upsertChargerSql = properties.detector().enabled() ? UPSERT_CHARGER_KEEP_STATUS : UPSERT_CHARGER;
    }

    public void upsertStations(List<StationRow> rows, Instant now) {
        if (rows.isEmpty()) {
            return;
        }
        OffsetDateTime ts = toOffset(now);
        jdbcTemplate.batchUpdate(UPSERT_STATION, rows, BATCH_SIZE, (ps, r) -> {
            ps.setString(1, r.stationId());
            ps.setString(2, r.name());
            ps.setString(3, r.address());
            ps.setString(4, r.locationDetail());
            ps.setDouble(5, r.latitude());
            ps.setDouble(6, r.longitude());
            ps.setString(7, r.useTime());
            ps.setString(8, r.operatorId());
            ps.setString(9, r.operatorName());
            ps.setString(10, r.operatorCall());
            ps.setString(11, r.zcode());
            ps.setString(12, r.zscode());
            ps.setObject(13, r.parkingFree(), Types.BOOLEAN);
            ps.setObject(14, r.limited(), Types.BOOLEAN);
            ps.setString(15, r.limitDetail());
            ps.setObject(16, ts);
            ps.setObject(17, ts);
        });
    }

    public void upsertChargers(List<ChargerRow> rows, Instant now) {
        if (rows.isEmpty()) {
            return;
        }
        OffsetDateTime ts = toOffset(now);
        jdbcTemplate.batchUpdate(upsertChargerSql, rows, BATCH_SIZE, (ps, r) -> {
            Instant statusUpdatedAt = r.statusUpdatedAt();
            ps.setString(1, r.stationId());
            ps.setString(2, r.chargerId());
            ps.setString(3, r.chargerType());
            ps.setObject(4, r.outputKw(), Types.INTEGER);
            ps.setString(5, r.status().name());
            ps.setObject(6, statusUpdatedAt != null ? toOffset(statusUpdatedAt) : null);
            ps.setObject(7, ts);
            ps.setObject(8, ts);
        });
    }

    /**
     * 응답에는 있었지만 검증에서 걸러져 UPSERT하지 않은 충전기의 updated_at만 갱신한다.
     * 이렇게 해야 데이터가 잠시 이상한 충전기가 "사라진 것"으로 오인돼 정리 단계에서 지워지지 않는다.
     */
    public void touchChargers(Collection<ChargerKey> keys, Instant now) {
        if (keys.isEmpty()) {
            return;
        }
        OffsetDateTime ts = toOffset(now);
        jdbcTemplate.batchUpdate(TOUCH_CHARGER, new ArrayList<>(keys), BATCH_SIZE, (ps, k) -> {
            ps.setObject(1, ts);
            ps.setString(2, k.stationId());
            ps.setString(3, k.chargerId());
        });
    }

    public int countChargers(String zcode) {
        Integer count = jdbcTemplate.queryForObject(COUNT_CHARGERS, int.class, zcode);
        return count != null ? count : 0;
    }

    public int countStaleChargers(String zcode, Instant syncStartedAt) {
        Integer count = jdbcTemplate.queryForObject(COUNT_STALE_CHARGERS, int.class, zcode, toOffset(syncStartedAt));
        return count != null ? count : 0;
    }

    /**
     * 전체 동기화가 완전하다고 확인됐을 때만 호출한다(호출자가 트랜잭션으로 감싼다).
     * 이번 동기화에서 갱신되지 않은 충전기를 지운다. 응답에서 사라진 충전기와
     * delYn=Y로 내려온 충전기(UPSERT하지 않으므로 갱신되지 않음)가 여기에 해당한다.
     * 충전기가 하나도 남지 않은 충전소는 비활성으로 표시한다.
     */
    public StaleCleanupResult cleanup(String zcode, Instant syncStartedAt) {
        int deleted = jdbcTemplate.update(DELETE_STALE_CHARGERS, zcode, toOffset(syncStartedAt));
        int deactivated = jdbcTemplate.update(DEACTIVATE_STATIONS_WITHOUT_CHARGERS, zcode);
        return new StaleCleanupResult(deleted, deactivated);
    }

    private static OffsetDateTime toOffset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
