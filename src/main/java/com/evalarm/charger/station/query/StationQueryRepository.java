package com.evalarm.charger.station.query;

import com.evalarm.charger.station.domain.ChargerStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 충전소 검색·상세 조회 (읽기 전용).
 *
 * <p>충전기 상태를 집계해야 하고 조건 조합이 다양해 JPA 대신 SQL로 작성한다.
 * 조건에 따라 바뀌는 것은 SQL 구조뿐이고 값은 모두 바인딩 파라미터로 넘긴다.
 */
@Repository
public class StationQueryRepository {

    /** 하버사인과 위경도 범위 계산이 같은 값을 써야 경계 근처 충전소가 빠지지 않는다 */
    private static final double EARTH_RADIUS_METERS = 6_371_000.0;

    /** 위도 1도의 거리(m). 경도 1도는 여기에 cos(위도)를 곱한다 */
    private static final double METERS_PER_DEGREE = EARTH_RADIUS_METERS * Math.PI / 180;

    /** 위경도 범위를 1% 넓혀 부동소수점·근사 오차로 원 안의 충전소가 범위 밖으로 밀리지 않게 한다 */
    private static final double BOUNDING_BOX_MARGIN = 1.01;

    private static final String HAVERSINE_METERS = """
            (%s * 2 * asin(sqrt(
                power(sin(radians(s.latitude - :lat) / 2), 2)
                + cos(radians(:lat)) * cos(radians(s.latitude)) * power(sin(radians(s.longitude - :lng) / 2), 2)
            )))""".formatted(EARTH_RADIUS_METERS);

    private final NamedParameterJdbcTemplate jdbc;

    public StationQueryRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public StationSearchResult search(StationSearchCriteria criteria) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> stationFilters = new ArrayList<>(List.of("s.active"));
        List<String> chargerConditions = new ArrayList<>();
        List<String> aggregateConditions = new ArrayList<>();

        String keyword = criteria.keyword();
        if (keyword != null) {
            stationFilters.add("(s.name ILIKE :keyword ESCAPE '\\' OR s.address ILIKE :keyword ESCAPE '\\')");
            params.addValue("keyword", "%" + escapeLike(keyword) + "%");
        }
        String zscode = criteria.zscode();
        if (zscode != null) {
            stationFilters.add("s.zscode = :zscode");
            params.addValue("zscode", zscode);
        }
        GeoRadius geo = criteria.geo();
        String distanceExpr;
        if (geo != null) {
            // 위경도 범위로 먼저 걸러 인덱스를 타게 한 뒤, 정확한 거리(하버사인)로 다시 거른다.
            // 범위는 하버사인과 같은 지구 반지름으로 계산하고 여유를 둬, 원 경계 바로 안쪽 충전소가 빠지지 않게 한다
            double latDelta = geo.radiusMeters() * BOUNDING_BOX_MARGIN / METERS_PER_DEGREE;
            double lngDelta = latDelta / Math.cos(Math.toRadians(geo.latitude()));
            stationFilters.add("s.latitude BETWEEN :minLat AND :maxLat");
            stationFilters.add("s.longitude BETWEEN :minLng AND :maxLng");
            params.addValue("lat", geo.latitude())
                    .addValue("lng", geo.longitude())
                    .addValue("radius", geo.radiusMeters())
                    .addValue("minLat", geo.latitude() - latDelta)
                    .addValue("maxLat", geo.latitude() + latDelta)
                    .addValue("minLng", geo.longitude() - lngDelta)
                    .addValue("maxLng", geo.longitude() + lngDelta);
            distanceExpr = HAVERSINE_METERS;
        } else {
            distanceExpr = "NULL::double precision";
        }

        Integer minOutputKw = criteria.minOutputKw();
        if (minOutputKw != null) {
            chargerConditions.add("c.output_kw >= :minOutputKw");
            aggregateConditions.add("c.output_kw >= :minOutputKw");
            params.addValue("minOutputKw", minOutputKw);
        }
        if (criteria.availableOnly()) {
            chargerConditions.add("c.status = 'AVAILABLE'");
        }

        // 조건에 맞는 충전기가 있는 충전소만 남긴다. 이 단계에서는 충전기를 집계하지 않고 존재 여부만 본다
        String chargerExists = joinAnd("c.station_id = s.station_id", chargerConditions);
        String filteredCte = """
                WITH base AS (
                    SELECT s.station_id, s.name, s.address, s.latitude, s.longitude, s.operator_name, s.use_time,
                           s.parking_free, %s AS distance_m
                    FROM station s
                    WHERE %s
                      AND EXISTS (SELECT 1 FROM charger c WHERE %s)
                ),
                filtered AS (
                    SELECT * FROM base %s
                )""".formatted(
                distanceExpr,
                String.join(" AND ", stationFilters),
                chargerExists,
                geo != null ? "WHERE distance_m <= :radius" : "");

        // 정렬 키는 요청 사이에 바뀌지 않는 값만 쓴다. 실시간으로 바뀌는 빈자리 수로 정렬하면
        // 페이지를 넘기는 사이 순서가 바뀌어 같은 충전소가 두 번 나오거나 빠진다
        String orderBy = geo != null ? "distance_m, station_id" : "name, station_id";
        params.addValue("limit", criteria.size()).addValue("offset", (long) criteria.page() * criteria.size());

        // 충전기 집계는 현재 페이지의 충전소(최대 100곳)에 대해서만 한다
        String sql = """
                %s,
                page AS (
                    SELECT f.*, count(*) OVER () AS total_count
                    FROM filtered f
                    ORDER BY %s
                    LIMIT :limit OFFSET :offset
                )
                SELECT p.*, a.charger_count, a.available_count, a.charging_count, a.max_output_kw, a.status_updated_at
                FROM page p
                CROSS JOIN LATERAL (
                    SELECT count(*) AS charger_count,
                           count(*) FILTER (WHERE c.status = 'AVAILABLE') AS available_count,
                           count(*) FILTER (WHERE c.status = 'CHARGING') AS charging_count,
                           max(c.output_kw) AS max_output_kw,
                           max(c.status_updated_at) AS status_updated_at
                    FROM charger c
                    WHERE %s
                ) a
                ORDER BY %s""".formatted(
                filteredCte,
                orderBy,
                joinAnd("c.station_id = p.station_id", aggregateConditions),
                orderBy);

        long[] totalCount = {0L};
        List<StationSummary> items = jdbc.query(sql, params, (rs, rowNum) -> {
            totalCount[0] = rs.getLong("total_count");
            Double distance = getNullableDouble(rs, "distance_m");
            return new StationSummary(
                    rs.getString("station_id"),
                    rs.getString("name"),
                    rs.getString("address"),
                    rs.getDouble("latitude"),
                    rs.getDouble("longitude"),
                    distance != null ? (int) Math.round(distance) : null,
                    rs.getString("operator_name"),
                    rs.getString("use_time"),
                    getNullableBoolean(rs, "parking_free"),
                    rs.getInt("charger_count"),
                    rs.getInt("available_count"),
                    rs.getInt("charging_count"),
                    getNullableInt(rs, "max_output_kw"),
                    getInstant(rs, "status_updated_at"));
        });
        if (items.isEmpty() && criteria.page() > 0) {
            // 범위를 넘은 페이지: 전체 건수만 따로 구한다 (충전기 집계 없이)
            Long count = jdbc.queryForObject(filteredCte + " SELECT count(*) FROM filtered", params, long.class);
            totalCount[0] = count != null ? count : 0L;
        }
        return new StationSearchResult(items, criteria.page(), criteria.size(), totalCount[0]);
    }

    public @Nullable StationDetail findDetail(String stationId) {
        MapSqlParameterSource params = new MapSqlParameterSource("stationId", stationId);
        List<StationDetail> stations = jdbc.query(
                """
                SELECT station_id, name, address, location_detail, latitude, longitude, operator_name, operator_call,
                       use_time, parking_free, limited, limit_detail
                FROM station WHERE station_id = :stationId AND active""",
                params,
                (rs, rowNum) -> toStationHeader(rs));
        if (stations.isEmpty()) {
            return null;
        }
        StationDetail station = stations.getFirst();

        List<ChargerDetail> chargers = jdbc.query(
                """
                SELECT charger_id, charger_type, output_kw, status, status_updated_at
                FROM charger WHERE station_id = :stationId
                ORDER BY charger_id""",
                params,
                (rs, rowNum) -> {
                    ChargerStatus status = ChargerStatus.valueOf(rs.getString("status"));
                    return new ChargerDetail(
                            rs.getString("charger_id"),
                            rs.getString("charger_type"),
                            getNullableInt(rs, "output_kw"),
                            status,
                            status.getDescription(),
                            getInstant(rs, "status_updated_at"));
                });
        int availableCount = (int) chargers.stream().filter(c -> c.status() == ChargerStatus.AVAILABLE).count();
        return new StationDetail(
                station.stationId(),
                station.name(),
                station.address(),
                station.locationDetail(),
                station.latitude(),
                station.longitude(),
                station.operatorName(),
                station.operatorCall(),
                station.useTime(),
                station.parkingFree(),
                station.limited(),
                station.limitDetail(),
                availableCount,
                chargers);
    }

    private static StationDetail toStationHeader(ResultSet rs) throws SQLException {
        return new StationDetail(
                rs.getString("station_id"),
                rs.getString("name"),
                rs.getString("address"),
                rs.getString("location_detail"),
                rs.getDouble("latitude"),
                rs.getDouble("longitude"),
                rs.getString("operator_name"),
                rs.getString("operator_call"),
                rs.getString("use_time"),
                getNullableBoolean(rs, "parking_free"),
                getNullableBoolean(rs, "limited"),
                rs.getString("limit_detail"),
                0,
                List.of());
    }

    private static String joinAnd(String first, List<String> rest) {
        List<String> conditions = new ArrayList<>(rest.size() + 1);
        conditions.add(first);
        conditions.addAll(rest);
        return String.join(" AND ", conditions);
    }

    private static @Nullable Instant getInstant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value != null ? value.toInstant() : null;
    }

    private static @Nullable Integer getNullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static @Nullable Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static @Nullable Boolean getNullableBoolean(ResultSet rs, String column) throws SQLException {
        boolean value = rs.getBoolean(column);
        return rs.wasNull() ? null : value;
    }

    /** LIKE 패턴의 특수문자(%, _, \)를 글자 그대로 찾도록 이스케이프한다 */
    private static String escapeLike(String keyword) {
        return keyword.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
