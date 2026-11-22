package com.evalarm.charger.common.web;

import com.evalarm.charger.station.api.InvalidSearchRequestException;
import com.evalarm.charger.station.api.StationNotFoundException;
import com.evalarm.charger.stream.TooManyStreamConnectionsException;
import com.evalarm.charger.subscription.InvalidClientKeyException;
import com.evalarm.charger.subscription.SubscriptionLimitExceededException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * API 오류를 RFC 9457 ProblemDetail 형식으로 응답한다.
 * 입력 오류는 어떤 파라미터가 왜 잘못됐는지 errors 필드에 담는다.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(InvalidSearchRequestException.class)
    public ProblemDetail handleInvalidSearch(InvalidSearchRequestException e) {
        return problem(HttpStatus.BAD_REQUEST, e.getMessage(), "잘못된 요청입니다", "잘못된 검색 조건");
    }

    @ExceptionHandler(StationNotFoundException.class)
    public ProblemDetail handleNotFound(StationNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, e.getMessage(), "찾을 수 없습니다", "충전소 없음");
    }

    @ExceptionHandler(InvalidClientKeyException.class)
    public ProblemDetail handleInvalidClientKey(InvalidClientKeyException e) {
        return problem(HttpStatus.BAD_REQUEST, e.getMessage(), "구독자 식별값이 올바르지 않습니다", "잘못된 구독자 식별값");
    }

    @ExceptionHandler(SubscriptionLimitExceededException.class)
    public ProblemDetail handleSubscriptionLimit(SubscriptionLimitExceededException e) {
        return problem(HttpStatus.CONFLICT, e.getMessage(), "구독 한도를 넘었습니다", "구독 한도 초과");
    }

    /** 실시간 연결 수 상한 초과. 브라우저(EventSource)는 잠시 뒤 자동으로 다시 연결한다 */
    @ExceptionHandler(TooManyStreamConnectionsException.class)
    public ResponseEntity<ProblemDetail> handleTooManyStreams(TooManyStreamConnectionsException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "30")
                .body(problem(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage(), "잠시 뒤 다시 시도해 주세요", "실시간 연결 초과"));
    }

    /** @Valid 검증 실패: 필드별 메시지를 모아 400으로 응답한다 (타입 변환 실패 포함) */
    @Override
    protected @Nullable ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            String message = error.isBindingFailure() ? "형식이 올바르지 않습니다" : error.getDefaultMessage();
            errors.put(error.getField(), message != null ? message : "올바르지 않은 값입니다");
        }
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "요청 파라미터가 올바르지 않습니다");
        problem.setTitle("잘못된 요청 파라미터");
        problem.setProperty("errors", errors);
        return handleExceptionInternal(ex, problem, headers, HttpStatus.BAD_REQUEST, request);
    }

    private static ProblemDetail problem(HttpStatus status, @Nullable String detail, String fallbackDetail, String title) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail != null ? detail : fallbackDetail);
        problem.setTitle(title);
        return problem;
    }
}
