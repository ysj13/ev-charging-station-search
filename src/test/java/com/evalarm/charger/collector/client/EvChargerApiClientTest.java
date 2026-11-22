package com.evalarm.charger.collector.client;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.queryParam;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withException;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import com.evalarm.charger.common.config.AppProperties;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

class EvChargerApiClientTest {

    private static final String BASE_URL = "https://apis.data.go.kr/B552584/EvCharger";
    private static final String DECODED_KEY = "ab+c/d==";

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private MockRestServiceServer server;
    private EvChargerApiClient client;

    @BeforeEach
    void setUp() {
        client = clientWithKey(DECODED_KEY);
    }

    private EvChargerApiClient clientWithKey(String key) {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        return new EvChargerApiClient(builder.build(), properties(key), jsonMapper);
    }

    @Test
    @DisplayName("충전소 정보 응답을 파싱한다")
    void parsesChargerInfoResponse() throws Exception {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerInfo")))
                .andExpect(queryParam("zcode", "11"))
                .andExpect(queryParam("dataType", "JSON"))
                .andRespond(withSuccess(fixture("charger-info-page.json"), MediaType.APPLICATION_JSON));

        PublicApiPage<ChargerInfoItem> page = client.fetchChargerInfo(1, 9999);

        assertEquals(76_593, page.totalCount());
        assertEquals(3, page.items().size());
        ChargerInfoItem first = page.items().getFirst();
        assertEquals("ME174013", first.statId());
        assertEquals("낙성대동주민센터", first.statNm());
        assertEquals("11620", first.zscode());
    }

    @Test
    @DisplayName("충전기 상태 응답을 파싱한다")
    void parsesChargerStatusResponse() throws Exception {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                .andExpect(queryParam("period", "5"))
                .andRespond(withSuccess(fixture("charger-status-page.json"), MediaType.APPLICATION_JSON));

        PublicApiPage<ChargerStatusItem> page = client.fetchChargerStatus(1, 9999, 5);

        assertEquals(List.of("2", "3", "9"), page.items().stream().map(ChargerStatusItem::stat).toList());
        assertEquals(
                Instant.parse("2026-11-21T14:50:42Z"), // 20261121235042 KST
                PublicApiValues.parseDateTime(page.items().getFirst().statUpdDt()));
    }

    @Test
    @DisplayName("인증키의 특수문자를 엄격하게 인코딩한다")
    void encodesServiceKeyStrictly() throws Exception {
        server.expect(requestTo(containsString("serviceKey=ab%2Bc%2Fd%3D%3D&")))
                .andRespond(withSuccess(fixture("charger-status-page.json"), MediaType.APPLICATION_JSON));

        client.fetchChargerStatus(1, 10, 5);
        server.verify();
    }

    @Test
    @DisplayName("Encoding 키가 들어와도 한 번만 인코딩해서 보낸다")
    void encodesAlreadyEncodedKeyOnlyOnce() throws Exception {
        EvChargerApiClient encodedKeyClient = clientWithKey("ab%2Bc%2Fd%3D%3D");
        server.expect(requestTo(containsString("serviceKey=ab%2Bc%2Fd%3D%3D&")))
                .andRespond(withSuccess(fixture("charger-status-page.json"), MediaType.APPLICATION_JSON));

        encodedKeyClient.fetchChargerStatus(1, 10, 5);
        server.verify();
    }

    @Test
    @DisplayName("인증키 오류 응답을 원인 코드와 함께 예외로 바꾼다")
    void convertsServiceKeyErrorWithReasonCode() throws Exception {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                .andRespond(
                        withStatus(HttpStatus.FORBIDDEN)
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(fixture("error-service-key.json")));

        PublicApiException e = assertThrows(PublicApiException.class, () -> client.fetchChargerStatus(1, 10, 5));
        assertEquals("30", e.getReasonCode());
        assertTrue(e.getMessage().contains("SERVICE_KEY_IS_NOT_REGISTERED_ERROR"));
        // 인증키 오류는 기다려도 나아지지 않으므로 재시도 대상이 아니다 (4xx는 즉시 실패: 요청 1회)
        assertFalse(e.isRetryable());
        server.verify();
    }

    @Test
    @DisplayName("네트워크 오류는 한 번 재시도한다")
    void retriesOnceOnNetworkError() throws Exception {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                .andRespond(withException(new SocketTimeoutException("read timed out")));
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                .andRespond(withSuccess(fixture("charger-status-page.json"), MediaType.APPLICATION_JSON));

        PublicApiPage<ChargerStatusItem> page = client.fetchChargerStatus(1, 10, 5);

        assertEquals(3, page.items().size());
        server.verify();
    }

    @Test
    @DisplayName("재시도 후에도 실패하면 예외를 던진다")
    void throwsWhenRetriesAreExhausted() {
        for (int i = 0; i < 3; i++) {
            server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                    .andRespond(withException(new SocketTimeoutException("read timed out")));
        }

        PublicApiException e = assertThrows(PublicApiException.class, () -> client.fetchChargerStatus(1, 10, 5));
        assertTrue(e.getMessage().contains("3회 시도"));
        server.verify();
    }

    @Test
    @DisplayName("게이트웨이 504는 일시적 오류로 보고 재시도한다")
    void retriesGatewayTimeout() throws Exception {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerInfo")))
                .andRespond(
                        withStatus(HttpStatus.GATEWAY_TIMEOUT)
                                .contentType(MediaType.APPLICATION_JSON)
                                .body("""
                                        {"OpenAPI_ServiceResponse":{"cmmMsgHeader":{"errMsg":"SERVICETIMEOUT_ERROR",
                                           "returnAuthMsg":"서비스 연결실패 에러","returnReasonCode":"05"}}}"""));
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerInfo")))
                .andRespond(withSuccess(fixture("charger-info-page.json"), MediaType.APPLICATION_JSON));

        PublicApiPage<ChargerInfoItem> page = client.fetchChargerInfo(1, 9999);

        assertEquals(3, page.items().size());
        server.verify();
    }

    @Test
    @DisplayName("결과가 1건이라 item이 배열이 아닌 객체로 와도 파싱한다")
    void parsesSingleItemObjectAsList() throws Exception {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                .andRespond(
                        withSuccess(
                                """
                                {"resultCode":"00","resultMsg":"NORMAL SERVICE.","totalCount":1,"pageNo":1,"numOfRows":10,
                                   "items":{"item":{"statId":"ME174027","chgerId":"01","stat":"2"}}}""",
                                MediaType.APPLICATION_JSON));

        PublicApiPage<ChargerStatusItem> page = client.fetchChargerStatus(1, 10, 5);

        assertEquals(List.of("ME174027"), page.items().stream().map(ChargerStatusItem::statId).toList());
    }

    @Test
    @DisplayName("200 응답에 XML 오류가 와도 예외로 바꾼다")
    void convertsXmlErrorIn200Response() {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                .andRespond(
                        withSuccess(
                                "<OpenAPI_ServiceResponse><cmmMsgHeader><errMsg>SERVICE ERROR</errMsg>"
                                        + "<returnReasonCode>22</returnReasonCode></cmmMsgHeader></OpenAPI_ServiceResponse>",
                                MediaType.TEXT_XML));

        PublicApiException e = assertThrows(PublicApiException.class, () -> client.fetchChargerStatus(1, 10, 5));
        assertTrue(e.getMessage().contains("XML"));
    }

    @Test
    @DisplayName("totalCount가 없으면 예외를 던진다")
    void throwsWhenTotalCountIsMissing() {
        server.expect(requestTo(startsWith(BASE_URL + "/getChargerStatus")))
                .andRespond(withSuccess("{\"resultCode\":\"00\",\"items\":{\"item\":[]}}", MediaType.APPLICATION_JSON));

        PublicApiException e = assertThrows(PublicApiException.class, () -> client.fetchChargerStatus(1, 10, 5));
        assertTrue(e.getMessage().contains("totalCount"));
    }

    @Test
    @DisplayName("날짜 값이 비어 있으면 null로 변환한다")
    void convertsBlankDateToNull() {
        assertNull(PublicApiValues.parseDateTime(""));
        assertNull(PublicApiValues.parseDateTime(null));
    }

    private static String fixture(String name) throws IOException {
        return new ClassPathResource("fixtures/" + name).getContentAsString(StandardCharsets.UTF_8);
    }

    private static AppProperties properties(String key) {
        return new AppProperties(
                new AppProperties.PublicApi(
                        BASE_URL,
                        key,
                        "11",
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        Duration.ZERO),
                new AppProperties.Collector(
                        false,
                        "-",
                        Duration.ofMinutes(2),
                        5,
                        10,
                        0.05,
                        9999),
                new AppProperties.Kafka(
                        new AppProperties.Kafka.Topics(
                                new AppProperties.Kafka.Topic("charger.status", 1, (short) 1),
                                new AppProperties.Kafka.Topic("charger.available", 1, (short) 1))),
                new AppProperties.Rabbit("x", "r", "q", "dlx", "dlq"),
                AppProperties.Detector.defaults(),
                AppProperties.Web.defaults(),
                AppProperties.Notification.defaults());
    }
}
