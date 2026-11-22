package com.evalarm.charger.collector.client;

import com.evalarm.charger.common.text.Whitespace;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import org.jspecify.annotations.Nullable;

/** 공공데이터 응답 값 변환 유틸. 응답 값은 모두 문자열이고 빈 문자열이 자주 섞여 있다. */
public final class PublicApiValues {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private PublicApiValues() {
    }

    /** "20261121235042" (한국 시간) → Instant */
    public static @Nullable Instant parseDateTime(@Nullable String value) {
        String text = blankToNull(value);
        if (text == null) {
            return null;
        }
        try {
            return LocalDateTime.parse(text, DATE_TIME).atZone(KST).toInstant();
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /** "Y" → true, "N" → false, 그 외 → null */
    public static @Nullable Boolean parseYn(@Nullable String value) {
        if (value == null) {
            return null;
        }
        return switch (Whitespace.trim(value).toUpperCase(Locale.ROOT)) {
            case "Y" -> Boolean.TRUE;
            case "N" -> Boolean.FALSE;
            default -> null;
        };
    }

    public static @Nullable String blankToNull(@Nullable String value) {
        if (value == null) {
            return null;
        }
        String trimmed = Whitespace.trim(value);
        return trimmed.isEmpty() ? null : trimmed;
    }

    /**
     * 포털은 인증키를 Encoding/Decoding 두 형태로 보여준다.
     * 요청 시 키를 직접 인코딩하므로, 이미 인코딩된 키(%가 포함됨)가 들어오면 한 번 디코딩해 이중 인코딩을 막는다.
     */
    public static String normalizeServiceKey(String key) {
        String trimmed = Whitespace.trim(key);
        return trimmed.indexOf('%') >= 0 ? URLDecoder.decode(trimmed, StandardCharsets.UTF_8) : trimmed;
    }
}
