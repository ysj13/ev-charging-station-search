package com.evalarm.charger.subscription.api;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.evalarm.charger.TestcontainersConfig;
import java.util.UUID;
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
import org.springframework.test.web.servlet.ResultActions;

@SpringBootTest(properties = {
    "app.collector.enabled=false", "app.public-api.zcode=11", "app.dev-tools.enabled=false",
    "app.detector.enabled=false", "app.notification.enabled=false",
    "app.notification.max-subscriptions-per-client=2",
})
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
class SubscriptionApiTest {

    private static final String HEADER = "X-Client-Key";

    @Autowired
    MockMvc mockMvc;

    @Autowired
    JdbcTemplate jdbcTemplate;

    private final String clientKey = UUID.randomUUID().toString();

    @BeforeEach
    void seed() {
        jdbcTemplate.execute("TRUNCATE notification_log, subscription, subscriber, charger, station CASCADE");
        station("S1", "강남역 충전소");
        station("S2", "역삼 충전소");
        station("S3", "선릉 충전소");
        station("GONE", "폐지된 충전소", false);
    }

    @Test
    @DisplayName("구독하면 201, 같은 충전소를 다시 구독하면 200이고 목록에는 한 번만 나온다")
    void subscribeReturns201ThenOkAndListsOnce() throws Exception {
        subscribe("S1").andExpect(status().isCreated());
        subscribe("S1").andExpect(status().isOk());

        mockMvc.perform(get("/api/subscriptions").header(HEADER, clientKey))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items", hasSize(1)))
                .andExpect(jsonPath("$.items[0].stationId").value("S1"))
                .andExpect(jsonPath("$.items[0].stationName").value("강남역 충전소"));
    }

    @Test
    @DisplayName("해지하면 목록에서 빠지고, 다시 구독할 수 있다")
    void unsubscribeRemovesFromListAndCanResubscribe() throws Exception {
        subscribe("S1");
        subscribe("S2");

        mockMvc.perform(delete("/api/subscriptions/S1").header(HEADER, clientKey)).andExpect(status().isNoContent());
        mockMvc.perform(delete("/api/subscriptions/S1").header(HEADER, clientKey)).andExpect(status().isNoContent());
        mockMvc.perform(get("/api/subscriptions").header(HEADER, clientKey))
                .andExpect(jsonPath("$.items[*].stationId").value(contains("S2")));

        subscribe("S1").andExpect(status().isCreated());
        // 해지했던 구독을 되살린다 (행을 새로 만들지 않음). 다시 켠 구독이 목록 맨 앞에 온다
        assertEquals(2, jdbcTemplate.queryForObject("SELECT count(*) FROM subscription", int.class));
        mockMvc.perform(get("/api/subscriptions").header(HEADER, clientKey))
                .andExpect(jsonPath("$.items[*].stationId").value(contains("S1", "S2")));
    }

    @Test
    @DisplayName("구독 한도를 넘으면 409, 해지하면 다시 구독할 수 있다")
    void exceedingLimitIs409AndUnsubscribeFreesSlot() throws Exception {
        subscribe("S1").andExpect(status().isCreated());
        subscribe("S2").andExpect(status().isCreated());
        subscribe("S3")
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title").value("구독 한도 초과"));

        mockMvc.perform(delete("/api/subscriptions/S2").header(HEADER, clientKey));
        subscribe("S3").andExpect(status().isCreated());
    }

    @Test
    @DisplayName("다른 브라우저의 구독은 보이지 않는다")
    void otherBrowsersSubscriptionsAreHidden() throws Exception {
        subscribe("S1");
        mockMvc.perform(get("/api/subscriptions").header(HEADER, UUID.randomUUID().toString()))
                .andExpect(jsonPath("$.items", hasSize(0)));
    }

    @Test
    @DisplayName("없거나 폐지된 충전소는 404")
    void unknownOrInactiveStationIs404() throws Exception {
        subscribe("NOPE").andExpect(status().isNotFound());
        subscribe("GONE").andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("clientKey가 없거나 형식이 틀리면 400")
    void missingOrMalformedClientKeyIs400() throws Exception {
        mockMvc.perform(get("/api/subscriptions"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("잘못된 구독자 식별값"));
        mockMvc.perform(get("/api/subscriptions").header(HEADER, "short")).andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/notifications").header(HEADER, "<script>alert(1)</script>-xxxxxxxx"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/vacancies/stream").param("clientKey", "short")).andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("충전소 ID가 비어 있으면 400")
    void blankStationIdIs400() throws Exception {
        mockMvc.perform(post("/api/subscriptions")
                        .header(HEADER, clientKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"stationId\": \" \"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.stationId").exists());
    }

    @Test
    @DisplayName("알림함은 보낸 알림이 없으면 빈 목록")
    void inboxIsEmptyWithoutSentNotifications() throws Exception {
        mockMvc.perform(get("/api/notifications").header(HEADER, clientKey))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items", hasSize(0)));
    }

    private ResultActions subscribe(String stationId) throws Exception {
        return mockMvc.perform(post("/api/subscriptions")
                .header(HEADER, clientKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"stationId\": \"" + stationId + "\"}"));
    }

    private void station(String id, String name) {
        station(id, name, true);
    }

    private void station(String id, String name, boolean active) {
        jdbcTemplate.update(
                """
                INSERT INTO station (station_id, name, address, latitude, longitude, zcode, zscode, active, created_at, updated_at)
                VALUES (?, ?, '서울특별시 강남구', 37.5, 127.0, '11', '11680', ?, now(), now())""",
                id, name, active);
    }
}
