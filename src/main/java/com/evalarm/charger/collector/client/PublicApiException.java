package com.evalarm.charger.collector.client;

import org.jspecify.annotations.Nullable;

/** 공공데이터 API 호출 실패. 원인 코드와 메시지를 그대로 담는다. */
public class PublicApiException extends RuntimeException {

    private final String operation;
    private final @Nullable String reasonCode;
    /** 시간이 지나면 나아질 수 있는 오류인지 (네트워크, 5xx). 인증키 오류(4xx) 등은 false */
    private final boolean retryable;

    public PublicApiException(
            String operation, @Nullable String reasonCode, String message, @Nullable Throwable cause, boolean retryable) {
        super("[" + operation + "] " + message, cause);
        this.operation = operation;
        this.reasonCode = reasonCode;
        this.retryable = retryable;
    }

    public PublicApiException(String operation, @Nullable String reasonCode, String message, @Nullable Throwable cause) {
        this(operation, reasonCode, message, cause, true);
    }

    public PublicApiException(String operation, @Nullable String reasonCode, String message) {
        this(operation, reasonCode, message, null, true);
    }

    public String getOperation() {
        return operation;
    }

    public @Nullable String getReasonCode() {
        return reasonCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
