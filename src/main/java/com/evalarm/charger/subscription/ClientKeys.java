package com.evalarm.charger.subscription;

import com.evalarm.charger.common.text.Whitespace;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * 로그인 없이 구독자를 구분하는 clientKey.
 * 브라우저가 처음 접속할 때 무작위 값(UUID)을 만들어 저장해 두고, 구독·알림함 요청과 실시간 연결에 함께 보낸다.
 * 추측하기 어려운 값이어야 하므로 짧은 값은 받지 않는다.
 */
public final class ClientKeys {

    public static final String HEADER = "X-Client-Key";
    private static final Pattern PATTERN = Pattern.compile("^[A-Za-z0-9-]{16,64}$");

    private ClientKeys() {
    }

    public static String require(@Nullable String value) {
        if (value == null || Whitespace.isBlank(value)) {
            throw new InvalidClientKeyException(HEADER + " 헤더가 필요합니다");
        }
        if (!PATTERN.matcher(value).matches()) {
            throw new InvalidClientKeyException(HEADER + " 형식이 올바르지 않습니다 (영문·숫자·하이픈 16~64자)");
        }
        return value;
    }
}
