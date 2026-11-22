package com.evalarm.charger.common.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 애플리케이션 설정 (app.*). 값은 application.yml과 .env에서 주입한다.
 * detector, web, notification은 생략하면 기본값을 쓴다.
 */
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        PublicApi publicApi,
        Collector collector,
        Kafka kafka,
        Rabbit rabbit,
        @DefaultValue Detector detector,
        @DefaultValue Web web,
        @DefaultValue Notification notification) {

    public record Notification(
            /** 알림 매칭(Kafka)과 발송(RabbitMQ) 리스너를 띄울지 */
            @DefaultValue("true") boolean enabled,
            /**
             * 개발 도구로 넣은 상태(source=MANUAL)에서 생긴 빈자리도 알림을 보낼지.
             * 실제 사용자에게 가짜 알림이 가지 않도록 기본은 false, 시연 환경(DEV_TOOLS_ENABLED=true)에서만 켠다
             */
            @DefaultValue("false") boolean includeManualEvents,
            /** 한 브라우저(clientKey)가 구독할 수 있는 최대 충전소 수 */
            @DefaultValue("20") int maxSubscriptionsPerClient,
            /** 알림함에 보여 줄 최근 알림 수 */
            @DefaultValue("20") int inboxSize,
            /** RabbitMQ 적재 확인(publisher confirm) 대기 시간 */
            @DefaultValue("5s") Duration publishConfirmTimeout) {

        public static Notification defaults() {
            return new Notification(true, false, 20, 20, Duration.ofSeconds(5));
        }
    }

    public record Web(
            /**
             * 카카오맵 JavaScript 키. 브라우저에 그대로 전달되는 공개 키이며, 카카오 개발자 콘솔에 등록한 도메인에서만 동작한다.
             * .env의 KAKAO_MAP_JS_KEY에서 주입. 비어 있으면 화면은 지도 없이 목록만 보여 준다
             */
            @DefaultValue("") String kakaoMapJsKey,
            /** 빈자리 실시간 스트림(SSE)에 동시에 연결할 수 있는 최대 수 */
            @DefaultValue("500") int maxStreamConnections,
            /** SSE 연결 하나의 최대 유지 시간. 지나면 끊고, 브라우저(EventSource)가 자동으로 다시 연결한다 */
            @DefaultValue("30m") Duration streamTimeout) {

        public static Web defaults() {
            return new Web("", 500, Duration.ofMinutes(30));
        }
    }

    public record Detector(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("3") int concurrency,
            /** 이보다 오래전에 일어난 빈자리는 이벤트로 발행하지 않는다 (밀린 메시지·과거 기록 재처리 시 알림 폭주 방지) */
            @DefaultValue("15m") Duration maxEventAge) {

        public static Detector defaults() {
            return new Detector(true, 3, Duration.ofMinutes(15));
        }
    }

    public record PublicApi(
            String baseUrl,
            /** data.go.kr 인증키. .env의 DATA_GO_KR_SERVICE_KEY에서 주입 (Encoding/Decoding 모두 허용) */
            String serviceKey,
            /** 수집 대상 시도 코드 (예: 11 = 서울) */
            String zcode,
            Duration connectTimeout,
            /** 본문 수신까지 포함한 응답 전체 시간 상한 */
            Duration readTimeout,
            /** 네트워크 오류 재시도 전 대기 시간 */
            Duration retryBackoff) {
    }

    public record Collector(
            /** false면 스케줄 수집을 하지 않는다 (테스트 등) */
            boolean enabled,
            /** 충전소 정보 전체 동기화 주기 (cron, 한국 시간) */
            String stationSyncCron,
            /** 충전기 상태 수집 간격 */
            Duration statusInterval,
            /** getChargerStatus의 기본 period. 수집 간격보다 길게 잡아 누락을 막는다 */
            int statusPeriodMinutes,
            /** 수집이 연속 실패했을 때 마지막 성공 시점부터 다시 조회할 수 있는 최대 period (API가 약 10분까지만 조회) */
            int statusMaxPeriodMinutes,
            /** 전체 동기화에서 한 번에 지울 수 있는 충전기 비율 상한. 넘으면 원본 이상으로 보고 정리를 건너뛴다 */
            double maxStaleRatio,
            /** 페이지당 건수 (API 최대 9,999) */
            int pageSize) {
    }

    public record Kafka(Topics topics) {

        public record Topics(Topic chargerStatus, Topic chargerAvailable) {
        }

        public record Topic(String name, int partitions, short replicas) {
        }
    }

    public record Rabbit(
            String exchange,
            String routingKey,
            String queue,
            String deadLetterExchange,
            String deadLetterQueue) {
    }
}
