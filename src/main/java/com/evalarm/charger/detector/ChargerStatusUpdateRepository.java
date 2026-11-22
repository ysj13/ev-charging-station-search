package com.evalarm.charger.detector;

import com.evalarm.charger.station.domain.ChargerKey;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

/**
 * 충전기 상태를 DB(검색 기준 데이터)에서 읽고 쓴다.
 * <ul>
 *   <li>더 최신 상태일 때만 덮어쓴다. 갱신 시각이 같으면 덮어쓰지 않는다 (Redis, 전체 동기화와 같은 규칙)</li>
 *   <li>updated_at은 건드리지 않는다. updated_at은 전체 동기화가 "응답에 있었던 충전기"를 표시하는 용도라,
 *   여기서 갱신하면 원본에서 사라진 충전기가 정리되지 않는다</li>
 *   <li>DB에 없는 충전기(아직 동기화 전)는 갱신되지 않고 다음 전체 동기화 때 들어온다</li>
 * </ul>
 */
@Repository
public class ChargerStatusUpdateRepository {

    private static final int CHUNK_SIZE = 1_000;

    private static final String LOAD_STATES = """
            SELECT c.station_id, c.charger_id, c.status, c.status_updated_at
            FROM charger c
            JOIN unnest(?::varchar[], ?::varchar[]) AS k(station_id, charger_id)
              ON c.station_id = k.station_id AND c.charger_id = k.charger_id
            WHERE c.status_updated_at IS NOT NULL""";

    private static final String UPDATE_IF_NEWER = """
            UPDATE charger SET status = ?, status_updated_at = ?
            WHERE station_id = ? AND charger_id = ?
              AND (status_updated_at IS NULL OR status_updated_at < ?)""";

    private final JdbcTemplate jdbcTemplate;

    public ChargerStatusUpdateRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Redis에 직전 상태가 없을 때(Redis 유실 등) 기준 상태로 쓸 DB의 현재 상태 */
    public Map<ChargerKey, ChargerState> loadStates(Collection<ChargerKey> keys) {
        if (keys.isEmpty()) {
            return Map.of();
        }
        Map<ChargerKey, ChargerState> result = new HashMap<>(keys.size());
        List<ChargerKey> all = new ArrayList<>(keys);
        for (int from = 0; from < all.size(); from += CHUNK_SIZE) {
            List<ChargerKey> chunk = all.subList(from, Math.min(from + CHUNK_SIZE, all.size()));
            String[] stationIds = new String[chunk.size()];
            String[] chargerIds = new String[chunk.size()];
            for (int i = 0; i < chunk.size(); i++) {
                stationIds[i] = chunk.get(i).stationId();
                chargerIds[i] = chunk.get(i).chargerId();
            }
            jdbcTemplate.query(
                    LOAD_STATES,
                    (PreparedStatementSetter) ps -> {
                        ps.setArray(1, ps.getConnection().createArrayOf("varchar", stationIds));
                        ps.setArray(2, ps.getConnection().createArrayOf("varchar", chargerIds));
                    },
                    (RowCallbackHandler) rs -> {
                        ChargerStatus status;
                        try {
                            status = ChargerStatus.valueOf(rs.getString("status"));
                        } catch (Exception e) {
                            status = null;
                        }
                        OffsetDateTime updatedAt = rs.getObject("status_updated_at", OffsetDateTime.class);
                        if (status != null && updatedAt != null) {
                            result.put(
                                    new ChargerKey(rs.getString("station_id"), rs.getString("charger_id")),
                                    new ChargerState(status, updatedAt.toInstant()));
                        }
                    });
        }
        return result;
    }

    public int updateIfNewer(Map<ChargerKey, ChargerState> states) {
        if (states.isEmpty()) {
            return 0;
        }
        int[][] batches = jdbcTemplate.batchUpdate(
                UPDATE_IF_NEWER, new ArrayList<>(states.entrySet()), CHUNK_SIZE, (ps, entry) -> {
                    ChargerKey key = entry.getKey();
                    ChargerState state = entry.getValue();
                    OffsetDateTime ts = OffsetDateTime.ofInstant(state.statusUpdatedAt(), ZoneOffset.UTC);
                    ps.setString(1, state.status().name());
                    ps.setObject(2, ts);
                    ps.setString(3, key.stationId());
                    ps.setString(4, key.chargerId());
                    ps.setObject(5, ts);
                });
        int updated = 0;
        for (int[] batch : batches) {
            for (int count : batch) {
                updated += count;
            }
        }
        return updated;
    }
}
