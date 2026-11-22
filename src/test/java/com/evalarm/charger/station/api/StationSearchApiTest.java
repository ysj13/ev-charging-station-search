package com.evalarm.charger.station.api;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.evalarm.charger.TestcontainersConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {
    "app.collector.enabled=false", "app.public-api.zcode=11", "app.dev-tools.enabled=false",
    "app.detector.enabled=false", "app.web.kakao-map-js-key=test-js-key",
})
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
class StationSearchApiTest {

    /** 검색 쿼리와 같은 지구 반지름 기준의 위도 1도 거리(m) */
    private static final double METERS_PER_DEGREE = 6_371_000.0 * Math.PI / 180;

    private static final Pattern STATION_ID = Pattern.compile("\"stationId\":\"([A-Z_]+)\"");

    @Autowired
    MockMvc mockMvc;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void seed() {
        jdbcTemplate.execute("TRUNCATE notification_log, subscription, subscriber, charger, station CASCADE");
        // 기준점 (37.4979, 127.0276) 주변
        station("NEAR", "강남역 공영주차장", "서울특별시 강남구 강남대로 396", 37.4979, 127.0276, "11680");
        station("MID", "역삼 충전소", "서울특별시 강남구 테헤란로 1", 37.5024, 127.0276, "11680"); // 약 500m 북쪽
        station("FAR", "선릉 충전소", "서울특별시 강남구 선릉로 1", 37.5249, 127.0276, "11680"); // 약 3km 북쪽
        station("MAPO", "100%_충전소", "서울특별시 마포구 월드컵로 1", 37.5663, 126.9019, "11440");
        station("GONE", "폐지된 강남 충전소", "서울특별시 강남구 폐지로 1", 37.4980, 127.0277, "11680", false);
        // 7kW만 비어 있는 충전소 (약 1.6km 남쪽)
        station("SLOW", "서초 완속 충전소", "서울특별시 서초구 서초대로 1", 37.4837, 127.0324, "11650");
        // 기준점 (37.6, 127.1)에서 정남쪽으로 999.5m, 1000.5m 떨어진 충전소 (반경 1km 경계)
        station("EDGE_IN", "경계 안 충전소", "서울특별시 노원구 경계로 1", 37.6 - 999.5 / METERS_PER_DEGREE, 127.1, "11350");
        station("EDGE_OUT", "경계 밖 충전소", "서울특별시 노원구 경계로 2", 37.6 - 1000.5 / METERS_PER_DEGREE, 127.1, "11350");

        charger("NEAR", "01", "AVAILABLE", 50);
        charger("NEAR", "02", "CHARGING", 50);
        charger("NEAR", "03", "AVAILABLE", 7);
        charger("MID", "01", "CHARGING", 100);
        charger("FAR", "01", "AVAILABLE", 7);
        charger("MAPO", "01", "AVAILABLE", 200);
        charger("GONE", "01", "AVAILABLE", 50);
        charger("SLOW", "01", "AVAILABLE", 7);
        charger("SLOW", "02", "CHARGING", 50);
        charger("EDGE_IN", "01", "AVAILABLE", 50);
        charger("EDGE_OUT", "01", "AVAILABLE", 50);
    }

    @Test
    @DisplayName("좌표 반경 검색은 반경 안의 충전소를 가까운 순으로 거리와 함께 돌려준다")
    void radiusSearchReturnsNearestFirstWithDistance() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("lat", "37.4979")
                        .param("lng", "127.0276")
                        .param("radius", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[*].stationId").value(contains("NEAR", "MID")))
                .andExpect(jsonPath("$.items[0].distanceMeters").value(0))
                .andExpect(jsonPath("$.items[1].distanceMeters").value(greaterThan(450)))
                .andExpect(jsonPath("$.items[1].distanceMeters").value(lessThan(550)))
                .andExpect(jsonPath("$.totalCount").value(2));
    }

    @Test
    @DisplayName("충전기 수와 사용 가능 대수를 집계한다")
    void aggregatesChargerAndAvailableCounts() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("lat", "37.4979")
                        .param("lng", "127.0276")
                        .param("radius", "100"))
                .andExpect(jsonPath("$.items[0].stationId").value("NEAR"))
                .andExpect(jsonPath("$.items[0].chargerCount").value(3))
                .andExpect(jsonPath("$.items[0].availableCount").value(2))
                .andExpect(jsonPath("$.items[0].chargingCount").value(1))
                .andExpect(jsonPath("$.items[0].maxOutputKw").value(50));
    }

    @Test
    @DisplayName("빈자리 있는 곳만 보면 사용 가능한 충전기가 없는 충전소는 빠진다")
    void availableOnlyExcludesStationsWithoutAvailableChargers() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("lat", "37.4979")
                        .param("lng", "127.0276")
                        .param("radius", "5000")
                        .param("availableOnly", "true"))
                .andExpect(jsonPath("$.items[*].stationId").value(contains("NEAR", "SLOW", "FAR")));
    }

    @Test
    @DisplayName("빈자리와 최소 출력을 함께 주면 해당 출력 이상의 충전기가 비어 있는 곳만 남는다")
    void availableOnlyWithMinOutputKeepsStationsWithAvailableFastChargers() throws Exception {
        // SLOW는 7kW만 비어 있고 50kW는 충전중이라 빠진다
        mockMvc.perform(get("/api/stations")
                        .param("lat", "37.4979")
                        .param("lng", "127.0276")
                        .param("radius", "5000")
                        .param("availableOnly", "true")
                        .param("minOutputKw", "50"))
                .andExpect(jsonPath("$.items[*].stationId").value(contains("NEAR")))
                .andExpect(jsonPath("$.items[0].chargerCount").value(2))
                .andExpect(jsonPath("$.items[0].availableCount").value(1));
    }

    @Test
    @DisplayName("반경 경계 바로 안쪽 충전소는 포함하고 바로 바깥 충전소는 제외한다")
    void includesJustInsideAndExcludesJustOutsideRadius() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("lat", "37.6")
                        .param("lng", "127.1")
                        .param("radius", "1000"))
                .andExpect(jsonPath("$.items[*].stationId").value(contains("EDGE_IN")))
                .andExpect(jsonPath("$.items[0].distanceMeters").value(1000)); // 999.5m 반올림
    }

    @Test
    @DisplayName("페이지를 넘기는 사이 빈자리 수가 바뀌어도 중복이나 누락이 없다")
    void paginationIsStableWhileAvailabilityChanges() throws Exception {
        List<String> first = page(0);
        // 0페이지를 본 뒤 실시간 상태가 바뀜: NEAR는 모두 충전중, MID는 비어 있음
        jdbcTemplate.update("UPDATE charger SET status = 'CHARGING' WHERE station_id = 'NEAR'");
        jdbcTemplate.update("UPDATE charger SET status = 'AVAILABLE' WHERE station_id = 'MID'");
        List<String> all = new ArrayList<>(first);
        all.addAll(page(1));
        all.addAll(page(2));

        // 빈자리 수로 정렬했다면 NEAR가 다시 나오고 FAR가 빠진다. 이름순이면 항상 강남역 < 선릉 < 역삼
        assertEquals(List.of("NEAR", "FAR", "MID"), all);
    }

    @Test
    @DisplayName("앞 1만 건을 넘어서는 페이지는 400")
    void pageBeyondFirstTenThousandIs400() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("q", "충전소")
                        .param("size", "100")
                        .param("page", "99")) // 9,901~10,000번째
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/stations")
                        .param("q", "충전소")
                        .param("size", "100")
                        .param("page", "100")) // 10,001~10,100번째
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("공백을 뺀 검색어가 2자 미만이면 400")
    void keywordShorterThanTwoWithoutSpacesIs400() throws Exception {
        mockMvc.perform(get("/api/stations").param("q", "  a "))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("최소 출력 조건은 해당 출력 이상의 충전기만 집계한다")
    void minOutputAggregatesOnlyMatchingChargers() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("zscode", "11680")
                        .param("minOutputKw", "50"))
                // FAR은 7kW만 있어 빠지고, NEAR는 50kW 충전기 2대만 센다
                .andExpect(jsonPath("$.items[*].stationId").value(containsInAnyOrder("NEAR", "MID")))
                .andExpect(jsonPath("$.items[?(@.stationId == 'NEAR')].chargerCount").value(contains(2)));
    }

    @Test
    @DisplayName("키워드는 이름과 주소를 부분 일치로 찾고 폐지된 충전소는 제외한다")
    void keywordMatchesNameAndAddressAndExcludesInactive() throws Exception {
        mockMvc.perform(get("/api/stations").param("q", "강남"))
                .andExpect(jsonPath("$.items[*].stationId").value(containsInAnyOrder("NEAR", "MID", "FAR")));
    }

    @Test
    @DisplayName("키워드의 퍼센트와 밑줄은 글자 그대로 찾는다")
    void keywordTreatsPercentAndUnderscoreLiterally() throws Exception {
        mockMvc.perform(get("/api/stations").param("q", "0%_"))
                .andExpect(jsonPath("$.items[*].stationId").value(contains("MAPO")));
        mockMvc.perform(get("/api/stations").param("q", "%%"))
                .andExpect(jsonPath("$.totalCount").value(0));
    }

    @Test
    @DisplayName("페이지를 나눠도 전체 건수를 알려준다")
    void reportsTotalCountAcrossPages() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("zscode", "11680")
                        .param("size", "2")
                        .param("page", "1"))
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.totalCount").value(3));
        mockMvc.perform(get("/api/stations")
                        .param("zscode", "11680")
                        .param("size", "2")
                        .param("page", "5"))
                .andExpect(jsonPath("$.items.length()").value(0))
                .andExpect(jsonPath("$.totalCount").value(3));
    }

    @Test
    @DisplayName("검색 조건이 하나도 없으면 400")
    void noSearchConditionIs400() throws Exception {
        mockMvc.perform(get("/api/stations"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("잘못된 검색 조건"));
    }

    @Test
    @DisplayName("위도만 주면 400")
    void latitudeOnlyIs400() throws Exception {
        mockMvc.perform(get("/api/stations").param("lat", "37.5"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("범위를 벗어난 파라미터는 필드별 오류와 함께 400")
    void outOfRangeParametersReturnFieldErrors() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("q", "강남")
                        .param("size", "101")
                        .param("page", "-1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").value(hasKey("size")))
                .andExpect(jsonPath("$.errors").value(hasKey("page")));
    }

    @Test
    @DisplayName("형식이 틀린 파라미터는 필드별 오류와 함께 400")
    void malformedParametersReturnFieldErrors() throws Exception {
        mockMvc.perform(get("/api/stations")
                        .param("q", "강남")
                        .param("radius", "abc"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.radius").value("형식이 올바르지 않습니다"));
    }

    @Test
    @DisplayName("상세는 충전기별 상태를 돌려준다")
    void detailReturnsChargerStatuses() throws Exception {
        mockMvc.perform(get("/api/stations/NEAR"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.availableCount").value(2))
                .andExpect(jsonPath("$.chargers[*].chargerId").value(contains("01", "02", "03")))
                .andExpect(jsonPath("$.chargers[1].status").value("CHARGING"))
                .andExpect(jsonPath("$.chargers[1].statusDescription").value("충전중"));
    }

    @Test
    @DisplayName("검색 화면과 화면 설정을 제공하고 이전 테스트 페이지는 없다")
    void servesSearchPageAndClientConfigWithoutOldTestPage() throws Exception {
        mockMvc.perform(get("/index.html"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("<title>EV 충전 빈자리</title>")));
        mockMvc.perform(get("/js/app.js"))
                .andExpect(status().isOk())
                // 배포 뒤 브라우저가 이전 화면 코드를 쓰지 않도록 매번 바뀌었는지 확인하게 한다
                .andExpect(header().string("Cache-Control", containsString("no-cache")));
        mockMvc.perform(get("/api/client-config"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.kakaoMapJsKey").value("test-js-key"))
                .andExpect(jsonPath("$.devToolsEnabled").value(false))
                .andExpect(header().string("Cache-Control", containsString("no-cache")));
        mockMvc.perform(get("/api-test.html")).andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("개발 도구를 켜지 않으면 상태 주입 엔드포인트가 없다")
    void devToolsEndpointsAbsentWhenDisabled() throws Exception {
        mockMvc.perform(post("/dev/chargers/NEAR/01/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"AVAILABLE\"}"))
                .andExpect(status().isNotFound());
        mockMvc.perform(get("/dev/vacancies")).andExpect(status().isNotFound());
        mockMvc.perform(post("/dev/stations/NEAR/test-vacancy").header("X-Requested-With", "fetch"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("없거나 폐지된 충전소 상세는 404")
    void unknownOrInactiveStationDetailIs404() throws Exception {
        mockMvc.perform(get("/api/stations/NOPE")).andExpect(status().isNotFound());
        mockMvc.perform(get("/api/stations/GONE")).andExpect(status().isNotFound());
    }

    private List<String> page(int n) throws Exception {
        String body = mockMvc.perform(get("/api/stations")
                        .param("zscode", "11680")
                        .param("size", "1")
                        .param("page", String.valueOf(n)))
                .andReturn().getResponse().getContentAsString();
        List<String> ids = new ArrayList<>();
        Matcher matcher = STATION_ID.matcher(body);
        while (matcher.find()) {
            ids.add(matcher.group(1));
        }
        return ids;
    }

    private void station(String id, String name, String address, double lat, double lng, String zscode) {
        station(id, name, address, lat, lng, zscode, true);
    }

    private void station(
            String id, String name, String address, double lat, double lng, String zscode, boolean active) {
        jdbcTemplate.update(
                """
                INSERT INTO station (station_id, name, address, latitude, longitude, zcode, zscode, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, '11', ?, ?, now(), now())""",
                id, name, address, lat, lng, zscode, active);
    }

    private void charger(String stationId, String chargerId, String status, int outputKw) {
        jdbcTemplate.update(
                """
                INSERT INTO charger (station_id, charger_id, charger_type, output_kw, status, status_updated_at, created_at, updated_at)
                VALUES (?, ?, '06', ?, ?, now(), now(), now())""",
                stationId, chargerId, outputKw, status);
    }
}
