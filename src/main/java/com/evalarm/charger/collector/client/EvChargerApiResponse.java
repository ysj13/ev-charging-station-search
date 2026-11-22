package com.evalarm.charger.collector.client;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * 한국환경공단 전기자동차 충전소 정보 API의 JSON 응답(dataType=JSON).
 *
 * <p>정상 응답: { resultCode, resultMsg, totalCount, pageNo, numOfRows, items: { item: [...] } }
 * 게이트웨이 오류(인증키 등): { OpenAPI_ServiceResponse: { cmmMsgHeader: {...} } }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EvChargerApiResponse<T>(
        @Nullable String resultCode,
        @Nullable String resultMsg,
        @Nullable Integer totalCount,
        @Nullable Integer pageNo,
        @Nullable Integer numOfRows,
        @Nullable Items<T> items,
        @JsonProperty("OpenAPI_ServiceResponse") @Nullable GatewayError gatewayError) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Items<T>(
            // 결과가 1건이면 배열이 아닌 객체로 올 수 있어 단일 값도 리스트로 받는다
            @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) @Nullable List<T> item) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GatewayError(@Nullable Header cmmMsgHeader) {

        @JsonIgnoreProperties(ignoreUnknown = true)
        public record Header(
                @Nullable String errMsg,
                @Nullable String returnAuthMsg,
                @Nullable String returnReasonCode) {
        }
    }
}
