package com.evalarm.charger.collector.client;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.text.Whitespace;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/** 한국환경공단 전기자동차 충전소 정보 API 클라이언트 */
@Component
public class EvChargerApiClient {

    private static final Logger log = LoggerFactory.getLogger(EvChargerApiClient.class);

    private static final String RESULT_OK = "00";
    private static final int MAX_ATTEMPTS = 3;

    /** 게이트웨이 오류 중 기다려도 나아지지 않는 것: 서비스 접근 거부(20), 미등록·만료·미등록 IP 인증키(30~32) */
    private static final Set<String> NON_RETRYABLE_REASON_CODES = Set.of("20", "30", "31", "32");

    private final RestClient restClient;
    private final AppProperties properties;
    private final JsonMapper jsonMapper;
    private final String serviceKey;
    private final Duration retryBackoff;

    public EvChargerApiClient(
            @Qualifier("evChargerRestClient") RestClient restClient, AppProperties properties, JsonMapper jsonMapper) {
        this.restClient = restClient;
        this.properties = properties;
        this.jsonMapper = jsonMapper;
        this.serviceKey = PublicApiValues.normalizeServiceKey(properties.publicApi().serviceKey());
        this.retryBackoff = properties.publicApi().retryBackoff();
    }

    public boolean hasServiceKey() {
        return !Whitespace.isBlank(serviceKey);
    }

    /** 충전소·충전기 정보 (지역 전체, 페이지 단위) */
    public PublicApiPage<ChargerInfoItem> fetchChargerInfo(int pageNo, int numOfRows) throws InterruptedException {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("pageNo", pageNo);
        params.put("numOfRows", numOfRows);
        params.put("zcode", properties.publicApi().zcode());
        return fetch("getChargerInfo", params, ChargerInfoItem.class);
    }

    /** 최근 periodMinutes 분 안에 상태가 갱신된 충전기 */
    public PublicApiPage<ChargerStatusItem> fetchChargerStatus(int pageNo, int numOfRows, int periodMinutes)
            throws InterruptedException {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("pageNo", pageNo);
        params.put("numOfRows", numOfRows);
        params.put("period", periodMinutes);
        params.put("zcode", properties.publicApi().zcode());
        return fetch("getChargerStatus", params, ChargerStatusItem.class);
    }

    private <T> PublicApiPage<T> fetch(String operation, Map<String, Object> params, Class<T> itemType)
            throws InterruptedException {
        if (!hasServiceKey()) {
            throw new PublicApiException(
                    operation, null, "인증키가 없습니다. .env의 DATA_GO_KR_SERVICE_KEY를 설정하세요.", null, false);
        }
        String body = requestWithRetry(operation, buildUri(operation, params));
        return parse(operation, body, itemType);
    }

    /**
     * 공공데이터 게이트웨이는 간헐적으로 응답 없이 연결을 붙잡고 있거나 504(SERVICETIMEOUT_ERROR)를 돌려준다.
     * 네트워크 오류와 5xx만 재시도하고, 인증키 오류 같은 4xx 응답은 바로 실패시킨다.
     */
    private String requestWithRetry(String operation, URI uri) throws InterruptedException {
        int attempt = 1;
        while (true) {
            try {
                String body = restClient.get().uri(uri).retrieve().body(String.class);
                if (body == null) {
                    throw new PublicApiException(operation, null, "빈 응답");
                }
                return body;
            } catch (RestClientResponseException e) {
                PublicApiException error = toGatewayException(operation, e);
                // 5xx(게이트웨이 타임아웃 SERVICETIMEOUT_ERROR 등)는 일시적이라 재시도하고, 4xx(인증키 오류 등)는 바로 실패
                if (!e.getStatusCode().is5xxServerError() || attempt >= MAX_ATTEMPTS) {
                    throw error;
                }
                log.warn("[{}] 서버 오류로 재시도합니다 ({}/{}): {}", operation, attempt, MAX_ATTEMPTS, error.getMessage());
                backoff();
                attempt++;
            } catch (ResourceAccessException e) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw new PublicApiException(operation, null, "호출 실패(" + attempt + "회 시도): " + e.getMessage(), e);
                }
                log.warn("[{}] 네트워크 오류로 재시도합니다 ({}/{}): {}", operation, attempt, MAX_ATTEMPTS, e.getMessage());
                backoff();
                attempt++;
            } catch (RestClientException e) {
                throw new PublicApiException(operation, null, "호출 실패: " + e.getMessage(), e);
            }
        }
    }

    private void backoff() throws InterruptedException {
        try {
            Thread.sleep(retryBackoff.toMillis());
        } catch (InterruptedException interrupted) {
            // 종료 중이면 재시도하지 않고 인터럽트 상태를 살려 호출자에게 알린다
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    /**
     * 인증키에는 +, /, = 가 들어 있다. 쿼리에 그대로 넣으면 +가 공백으로 해석되므로
     * URI 변수로 넘겨 엄격하게 인코딩한다(UriComponentsBuilder.encode + buildAndExpand).
     */
    private URI buildUri(String operation, Map<String, Object> params) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(properties.publicApi().baseUrl())
                .pathSegment(operation)
                .queryParam("serviceKey", "{serviceKey}")
                .queryParam("dataType", "JSON");
        params.forEach((name, value) -> builder.queryParam(name, value));
        return builder.encode().buildAndExpand(Map.of("serviceKey", serviceKey)).toUri();
    }

    private <T> PublicApiPage<T> parse(String operation, String body, Class<T> itemType) {
        if (Whitespace.trimStart(body).startsWith("<")) {
            // 트래픽 초과 등 일부 오류는 dataType과 관계없이 XML로 내려온다
            throw new PublicApiException(operation, null, "XML 오류 응답: " + take(body, 300));
        }
        EvChargerApiResponse<T> response;
        try {
            response = jsonMapper.readValue(
                    body,
                    jsonMapper.getTypeFactory().constructParametricType(EvChargerApiResponse.class, itemType));
        } catch (JacksonException e) {
            throw new PublicApiException(operation, null, "응답 파싱 실패: " + take(body, 300), e);
        }

        EvChargerApiResponse.GatewayError gatewayError = response.gatewayError();
        EvChargerApiResponse.GatewayError.Header header = gatewayError != null ? gatewayError.cmmMsgHeader() : null;
        if (header != null) {
            String reasonCode = header.returnReasonCode();
            throw new PublicApiException(
                    operation,
                    reasonCode,
                    header.errMsg() + " (" + header.returnAuthMsg() + ")",
                    null,
                    reasonCode == null || !NON_RETRYABLE_REASON_CODES.contains(reasonCode));
        }
        if (!RESULT_OK.equals(response.resultCode())) {
            String resultMsg = response.resultMsg();
            throw new PublicApiException(operation, response.resultCode(), resultMsg != null ? resultMsg : "알 수 없는 오류");
        }
        // 전체 건수가 없으면 페이지 종료 조건을 판단할 수 없다. 첫 페이지만 처리하고 조용히 끝나는 것을 막는다
        Integer totalCount = response.totalCount();
        if (totalCount == null) {
            throw new PublicApiException(operation, response.resultCode(), "응답에 totalCount가 없습니다");
        }
        Integer pageNo = response.pageNo();
        EvChargerApiResponse.Items<T> items = response.items();
        List<T> itemList = items != null ? items.item() : null;
        return new PublicApiPage<>(totalCount, pageNo != null ? pageNo : 0, itemList != null ? itemList : List.of());
    }

    private PublicApiException toGatewayException(String operation, RestClientResponseException e) {
        EvChargerApiResponse.GatewayError.@Nullable Header header;
        try {
            EvChargerApiResponse.GatewayError gatewayError =
                    jsonMapper.readValue(e.getResponseBodyAsString(), EvChargerApiResponse.class).gatewayError();
            header = gatewayError != null ? gatewayError.cmmMsgHeader() : null;
        } catch (JacksonException ignored) {
            header = null;
        }
        String detail = header != null
                ? header.errMsg() + " (" + header.returnAuthMsg() + ")"
                : take(e.getResponseBodyAsString(), 300);
        return new PublicApiException(
                operation,
                header != null ? header.returnReasonCode() : null,
                "HTTP " + e.getStatusCode().value() + ": " + detail,
                e,
                e.getStatusCode().is5xxServerError());
    }

    private static String take(String value, int length) {
        return value.length() <= length ? value : value.substring(0, length);
    }
}
