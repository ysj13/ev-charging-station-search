package com.evalarm.charger.notification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.stream.VacancyBroadcaster;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.json.JsonMapper;

/** Redis로 받은 알림이 받을 사람이 분명할 때만 실시간 연결로 넘어가는지 검증한다 */
class NotificationPushChannelTest {

    private final VacancyBroadcaster broadcaster = mock(VacancyBroadcaster.class);
    private final NotificationPushChannel channel = new NotificationPushChannel(
            mock(StringRedisTemplate.class), JsonMapper.builder().build(), broadcaster, new SimpleMeterRegistry());

    @Test
    @DisplayName("받을 사람과 알림 내용이 있으면 그 구독자의 연결로 넘긴다")
    void deliversToClient() {
        when(broadcaster.notifyClient(eq("client-key-0001-abcd"), any())).thenReturn(1);

        channel.onMessage(message("""
                {"clientKey":"client-key-0001-abcd","notification":{"notificationId":7,"eventId":"S1:01:1",
                "stationId":"S1","stationName":"테스트 충전소","chargerId":"01",
                "occurredAt":"2026-11-22T12:00:00Z","sentAt":"2026-11-22T12:00:01Z"}}
                """), null);

        verify(broadcaster).notifyClient(eq("client-key-0001-abcd"), any());
    }

    @ParameterizedTest
    @DisplayName("받을 사람이나 알림 내용이 빠진 메시지는 아무 연결로도 보내지 않는다")
    @ValueSource(strings = {
        // clientKey 없음: 익명 연결로 새어 나가면 안 된다
        """
        {"notification":{"notificationId":7,"eventId":"S1:01:1","stationId":"S1","stationName":"테스트 충전소","chargerId":"01"}}
        """,
        // 알림 내용 없음
        """
        {"clientKey":"client-key-0001-abcd"}
        """,
        // 알림 내용의 필수 값(eventId) 없음
        """
        {"clientKey":"client-key-0001-abcd","notification":{"notificationId":7,"stationId":"S1","stationName":"테스트 충전소","chargerId":"01"}}
        """,
        // 본문이 JSON null
        "null",
        // JSON이 아님
        "{not-json",
    })
    void skipsIncompleteMessage(String body) {
        channel.onMessage(message(body), null);

        verify(broadcaster, never()).notifyClient(any(), any());
    }

    @Test
    @DisplayName("실시간 연결은 clientKey가 정확히 같은 연결에만 알림을 보낸다 (익명 연결 제외)")
    void broadcasterSendsOnlyToMatchingClient() {
        var properties = new AppProperties(
                null, null, null, null,
                AppProperties.Detector.defaults(),
                AppProperties.Web.defaults(),
                AppProperties.Notification.defaults());
        VacancyBroadcaster real = new VacancyBroadcaster(properties, new SimpleMeterRegistry());
        real.start();
        try {
            real.connect(null); // 익명
            real.connect("client-key-0001-abcd");
            real.connect("client-key-0002-efgh");

            assertEquals(1, real.notifyClient("client-key-0001-abcd", "알림"));
            assertEquals(0, real.notifyClient("client-key-9999-none", "알림"));
        } finally {
            real.stop();
        }
    }

    private static DefaultMessage message(String body) {
        return new DefaultMessage(
                NotificationPushChannel.CHANNEL.getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
    }
}
